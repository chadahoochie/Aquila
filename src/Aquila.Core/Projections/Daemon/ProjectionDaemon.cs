using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Background hosted service that polls global event sequence and dispatches event batches to registered async projections.
/// </summary>
public sealed class ProjectionDaemon : BackgroundService, IProjectionDaemon
{
    private readonly IDocumentStore _documentStore;
    private readonly IProjectionCheckpointStore _checkpointStore;
    private readonly ILogger<ProjectionDaemon>? _logger;
    private readonly ProjectionDaemonOptions _options;
    private readonly ConcurrentDictionary<string, bool> _stoppedProjections = new();

    public ProjectionDaemonOptions Options => _options;

    public ProjectionDaemon(
        IDocumentStore documentStore,
        IProjectionCheckpointStore checkpointStore,
        ILogger<ProjectionDaemon>? logger = null,
        ProjectionDaemonOptions? options = null)
    {
        _documentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
        _checkpointStore = checkpointStore ?? throw new ArgumentNullException(nameof(checkpointStore));
        _logger = logger;
        _options = options ?? new ProjectionDaemonOptions();
    }

    public ProjectionDaemon(
        StoreOptions options,
        IProjectionCheckpointStore checkpointStore,
        ILogger<ProjectionDaemon>? logger = null,
        ProjectionDaemonOptions? daemonOptions = null)
        : this(new DocumentStore(options), checkpointStore, logger, daemonOptions)
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

        // Reprocess historical events
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
        await _documentStore.Options.ProjectionStorage.PurgeProjectionAsync(proj.Name, docType, ct).ConfigureAwait(false);
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
                    await Task.Delay(_options.IdlePollingIntervalMs, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                bool processedAny = await ProcessNextBatchAsync(asyncProjections, stoppingToken).ConfigureAwait(false);
                if (!processedAny)
                {
                    await Task.Delay(_options.PollingIntervalMs, stoppingToken).ConfigureAwait(false);
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

        var checkpointTasks = projections.Select(async proj =>
        {
            var seq = await _checkpointStore.GetCheckpointAsync(proj.Name, ct).ConfigureAwait(false);
            return (Projection: proj, Sequence: seq);
        });

        var checkpointResults = await Task.WhenAll(checkpointTasks).ConfigureAwait(false);
        if (checkpointResults.Length == 0) return false;

        var projectionCheckpoints = checkpointResults.ToDictionary(r => r.Projection.Name, r => r.Sequence);
        long minSequence = checkpointResults.Min(r => r.Sequence);

        if (minSequence == long.MaxValue) return false;

        var eventStorage = _documentStore.Options.EventStorage;
        var batch = await eventStorage.FetchGlobalEventsAsync(minSequence, batchSize: _options.BatchSize, tenantId: null, ct: ct).ConfigureAwait(false);
        if (batch.Count == 0) return false;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, _options.MaxProjectionConcurrency),
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(projections, parallelOptions, async (proj, token) =>
        {
            var lastSeq = projectionCheckpoints[proj.Name];
            var newEvents = batch.Where(e => e.GlobalSequence > lastSeq).ToList();
            if (newEvents.Count == 0) return;

            await ProcessEventsForProjectionAsync(proj, newEvents, token).ConfigureAwait(false);

            long maxSeq = newEvents.Max(e => e.GlobalSequence);
            await _checkpointStore.SaveCheckpointAsync(proj.Name, maxSeq, token).ConfigureAwait(false);
        }).ConfigureAwait(false);

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
            var batch = await eventStorage.FetchGlobalEventsAsync(lastSeq, batchSize: _options.BatchSize, tenantId: null, ct: ct).ConfigureAwait(false);

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

    private Task ProcessEventsForProjectionAsync(IProjection proj, IReadOnlyList<IEvent> events, CancellationToken ct)
    {
        return BoundedParallelEventDispatcher.DispatchAsync(_documentStore, proj, events, _options.MaxEventGroupConcurrency, ct);
    }
}
