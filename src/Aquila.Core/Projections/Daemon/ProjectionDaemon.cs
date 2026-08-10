using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Reflection;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Background hosted service that polls global event sequence and dispatches event batches to registered async projections.
/// </summary>
public class ProjectionDaemon : BackgroundService, IProjectionDaemon
{
    private readonly IDocumentStore _documentStore;
    private readonly IProjectionCheckpointStore _checkpointStore;
    private readonly ILogger<ProjectionDaemon>? _logger;
    private readonly ConcurrentDictionary<string, bool> _stoppedProjections = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo> _processSingleStreamMethodCache = new();

    public ProjectionDaemon(IDocumentStore documentStore, IProjectionCheckpointStore checkpointStore, ILogger<ProjectionDaemon>? logger = null)
    {
        _documentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
        _checkpointStore = checkpointStore ?? throw new ArgumentNullException(nameof(checkpointStore));
        _logger = logger;
    }

    public ProjectionDaemon(
        StoreOptions options,
        IProjectionCheckpointStore checkpointStore,
        ILogger<ProjectionDaemon>? logger = null)
        : this(new DocumentStore(options), checkpointStore, logger)
    {
    }

    public Task StartProjectionAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        _stoppedProjections.TryRemove(projectionName, out _);
        return Task.CompletedTask;
    }

    public Task StopProjectionAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        _stoppedProjections[projectionName] = true;
        return Task.CompletedTask;
    }

    public async Task RebuildProjectionAsync<TProjection>(CancellationToken ct = default) where TProjection : IProjection
    {
        var projection = _documentStore.Options.Projections.Projections.FirstOrDefault(p => p is TProjection)
            ?? throw new InvalidOperationException($"Projection '{typeof(TProjection).Name}' is not registered.");

        await StopProjectionAsync(projection.Name, ct).ConfigureAwait(false);

        await _checkpointStore.SaveCheckpointAsync(projection.Name, 0, ct).ConfigureAwait(false);

        await ClearProjectionDocumentsAsync(projection, ct).ConfigureAwait(false);

        // 2. Reprocess historical events
        await CatchUpProjectionAsync(projection.Name, ct).ConfigureAwait(false);
    }

    public async Task RebuildProjectionAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);

        var proj = _documentStore.Options.Projections.Projections
            .FirstOrDefault(p => p.Name.Equals(projectionName, StringComparison.OrdinalIgnoreCase));

        if (proj != null)
        {
            await ClearProjectionDocumentsAsync(proj, ct).ConfigureAwait(false);
        }

        // 1. Reset checkpoint sequence to 0
        await _checkpointStore.SaveCheckpointAsync(projectionName, 0, ct).ConfigureAwait(false);

        // 2. Reprocess historical events
        await CatchUpProjectionAsync(projectionName, ct).ConfigureAwait(false);
    }

    private async Task ClearProjectionDocumentsAsync(IProjection proj, CancellationToken ct)
    {
        var docType = proj is IMultiStreamProjection multiProj ? multiProj.ReadModelType : proj.AggregateType;

        var queryMethod = typeof(IDocumentStorageProvider)
            .GetMethod(nameof(IDocumentStorageProvider.QueryDocumentsAsync))!
            .MakeGenericMethod(docType);

        var envelopeType = typeof(DocumentEnvelope<>).MakeGenericType(docType);
        var param = Expression.Parameter(envelopeType, "env");
        var lambda = Expression.Lambda(Expression.Constant(true), param);

        var queryTask = (Task)queryMethod.Invoke(_documentStore.Options.DocumentStorage, new object?[] { lambda, null, ct })!;
        await queryTask.ConfigureAwait(false);

        var resultProperty = queryTask.GetType().GetProperty("Result")!;
        var envelopes = (IEnumerable)resultProperty.GetValue(queryTask)!;

        var deleteMethod = typeof(IDocumentStorageProvider)
            .GetMethod(nameof(IDocumentStorageProvider.DeleteDocumentAsync))!
            .MakeGenericMethod(docType);

        foreach (object envelope in envelopes)
        {
            var idProp = envelope.GetType().GetProperty("Id")!;
            var pkProp = envelope.GetType().GetProperty("PartitionKey")!;
            string id = (string)idProp.GetValue(envelope)!;
            string pk = (string)pkProp.GetValue(envelope)!;

            var deleteTask = (Task)deleteMethod.Invoke(_documentStore.Options.DocumentStorage, new object[] { id, pk, ct })!;
            await deleteTask.ConfigureAwait(false);
        }
    }

    public async Task CatchUpAsync(CancellationToken ct = default)
    {
        var asyncProjections = GetActiveAsyncProjections();
        if (asyncProjections.Count == 0) return;

        bool hasMoreEvents = true;
        while (hasMoreEvents && !ct.IsCancellationRequested)
        {
            hasMoreEvents = await ProcessNextBatchAsync(asyncProjections, ct).ConfigureAwait(false);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var asyncProjections = GetActiveAsyncProjections();
                if (asyncProjections.Count == 0)
                {
                    await Task.Delay(100, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                bool processedAny = await ProcessNextBatchAsync(asyncProjections, stoppingToken).ConfigureAwait(false);
                if (!processedAny)
                {
                    await Task.Delay(100, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error processing projection daemon loop");
                await Task.Delay(500, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private List<IProjection> GetActiveAsyncProjections()
    {
        return _documentStore.Options.Projections.Projections
            .Where(p => p.Lifecycle == ProjectionLifecycle.Async)
            .Where(p => !_stoppedProjections.TryGetValue(p.Name, out var isStopped) || !isStopped)
            .ToList();
    }

    private async Task<bool> ProcessNextBatchAsync(List<IProjection> projections, CancellationToken ct)
    {
        if (projections.Count == 0) return false;

        long minSequence = long.MaxValue;
        var projectionCheckpoints = new Dictionary<string, long>();

        foreach (var proj in projections)
        {
            var seq = await _checkpointStore.GetCheckpointAsync(proj.Name, ct).ConfigureAwait(false);
            projectionCheckpoints[proj.Name] = seq;
            if (seq < minSequence)
            {
                minSequence = seq;
            }
        }

        if (minSequence == long.MaxValue) return false;

        var eventStorage = _documentStore.Options.EventStorage;
        var batch = await eventStorage.FetchGlobalEventsAsync(minSequence, batchSize: 100, tenantId: null, ct: ct).ConfigureAwait(false);
        if (batch.Count == 0) return false;

        foreach (var proj in projections)
        {
            var lastSeq = projectionCheckpoints[proj.Name];
            var newEvents = batch.Where(e => e.GlobalSequence > lastSeq).ToList();
            if (newEvents.Count == 0) continue;

            await ProcessEventsForProjectionAsync(proj, newEvents, ct).ConfigureAwait(false);

            long maxSeq = newEvents.Max(e => e.GlobalSequence);
            await _checkpointStore.SaveCheckpointAsync(proj.Name, maxSeq, ct).ConfigureAwait(false);
        }

        return true;
    }

    private async Task CatchUpProjectionAsync(string projectionName, CancellationToken ct)
    {
        var proj = _documentStore.Options.Projections.Projections
            .FirstOrDefault(p => p.Name.Equals(projectionName, StringComparison.OrdinalIgnoreCase));

        if (proj == null) return;

        bool hasMore = true;
        while (hasMore && !ct.IsCancellationRequested)
        {
            var lastSeq = await _checkpointStore.GetCheckpointAsync(proj.Name, ct).ConfigureAwait(false);
            var eventStorage = _documentStore.Options.EventStorage;
            var batch = await eventStorage.FetchGlobalEventsAsync(lastSeq, batchSize: 100, tenantId: null, ct: ct).ConfigureAwait(false);

            if (batch.Count == 0)
            {
                hasMore = false;
                break;
            }

            await ProcessEventsForProjectionAsync(proj, batch, ct).ConfigureAwait(false);

            long maxSeq = batch.Max(e => e.GlobalSequence);
            await _checkpointStore.SaveCheckpointAsync(proj.Name, maxSeq, ct).ConfigureAwait(false);
        }
    }

    private async Task ProcessEventsForProjectionAsync(IProjection proj, IReadOnlyList<IEvent> events, CancellationToken ct)
    {
        using var session = (DocumentSession)_documentStore.OpenSession();

        if (proj is IMultiStreamProjection multiProj)
        {
            foreach (var evt in events)
            {
                await multiProj.ProcessEventAsync(session, evt, ct).ConfigureAwait(false);
            }
        }
        else
        {
            var method = _processSingleStreamMethodCache.GetOrAdd(proj.AggregateType, t =>
                typeof(ProjectionDaemon).GetMethod(nameof(ProcessSingleStreamEventsAsync), BindingFlags.NonPublic | BindingFlags.Instance)!
                    .MakeGenericMethod(t));

            var task = (Task)method.Invoke(this, new object[] { session, proj, events, ct })!;
            await task.ConfigureAwait(false);
        }
    }

    private async Task ProcessSingleStreamEventsAsync<TAggregate>(
        DocumentSession session,
        IProjection proj,
        IReadOnlyList<IEvent> events,
        CancellationToken ct) where TAggregate : class
    {
        foreach (var evt in events)
        {
            var aggregateId = evt.StreamId;
            var existingAggregate = await session.LoadAsync<TAggregate>(aggregateId, aggregateId, ct).ConfigureAwait(false)
                                     ?? (TAggregate)Activator.CreateInstance(typeof(TAggregate))!;

            proj.ApplyEvent(evt, existingAggregate);

            var envelope = new DocumentEnvelope<TAggregate>
            {
                Id = aggregateId,
                PartitionKey = aggregateId,
                DocType = typeof(TAggregate).Name,
                TenantId = session.TenantId,
                IsDeleted = false,
                Data = existingAggregate
            };

            await _documentStore.Options.DocumentStorage.UpsertDocumentAsync(envelope, ct).ConfigureAwait(false);
        }
    }
}
