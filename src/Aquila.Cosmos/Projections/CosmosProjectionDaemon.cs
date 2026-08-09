using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Projections;

/// <summary>
/// Cosmos DB adapter for <see cref="IProjectionDaemon"/> that processes change feed items and dispatches events to registered async projections.
/// </summary>
public sealed class CosmosProjectionDaemon : BackgroundService, IProjectionDaemon
{
    private readonly IDocumentStore _documentStore;
    private readonly IProjectionCheckpointStore _checkpointStore;
    private readonly ILogger<CosmosProjectionDaemon>? _logger;
    private readonly ConcurrentDictionary<string, bool> _stoppedProjections = new();
    private static readonly ConcurrentDictionary<string, Type?> _typeCache = new();
    private static readonly ConcurrentDictionary<Type, MethodInfo> _processSingleStreamMethodCache = new();

    public CosmosProjectionDaemon(
        Container container,
        StoreOptions options,
        IProjectionCheckpointStore checkpointStore,
        ILogger<CosmosProjectionDaemon>? logger = null)
        : this(new DocumentStore(EnsureCosmosStorage(options, container)), checkpointStore, logger)
    {
    }

    public CosmosProjectionDaemon(
        CosmosClient client,
        StoreOptions options,
        IProjectionCheckpointStore checkpointStore,
        string databaseName = "AquilaDB",
        string containerName = "Documents",
        ILogger<CosmosProjectionDaemon>? logger = null)
        : this(new DocumentStore(EnsureCosmosStorage(options, client, databaseName, containerName)), checkpointStore, logger)
    {
    }

    public CosmosProjectionDaemon(
        StoreOptions options,
        IProjectionCheckpointStore checkpointStore,
        ILogger<CosmosProjectionDaemon>? logger = null)
        : this(new DocumentStore(options), checkpointStore, logger)
    {
    }

    [ActivatorUtilitiesConstructor]
    public CosmosProjectionDaemon(
        IDocumentStore documentStore,
        IProjectionCheckpointStore checkpointStore,
        ILogger<CosmosProjectionDaemon>? logger = null)
    {
        _documentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
        _checkpointStore = checkpointStore ?? throw new ArgumentNullException(nameof(checkpointStore));
        _logger = logger;
    }

    private static StoreOptions EnsureCosmosStorage(StoreOptions options, Container container)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(container);
        if (options.StorageProvider == null)
        {
            options.UseStorageProvider(new CosmosStorageProvider(container.Database.Client));
        }
        return options;
    }

    private static StoreOptions EnsureCosmosStorage(StoreOptions options, CosmosClient client, string databaseName, string containerName)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);
        if (options.StorageProvider == null)
        {
            options.UseStorageProvider(new CosmosStorageProvider(client, databaseName, containerName));
        }
        return options;
    }

    public Task StartProjectionAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        _stoppedProjections[projectionName] = false;
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
        var projectionName = typeof(TProjection).Name;
        await RebuildProjectionAsync(projectionName, ct).ConfigureAwait(false);
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

        await _checkpointStore.SaveCheckpointAsync(projectionName, 0, ct).ConfigureAwait(false);
        await CatchUpAsync(ct).ConfigureAwait(false);
    }

    private async Task ClearProjectionDocumentsAsync(IProjection proj, CancellationToken ct)
    {
        var docType = proj is IMultiStreamProjection multiProj ? multiProj.ReadModelType : proj.AggregateType;

        var queryMethod = typeof(IDocumentStorageProvider)
            .GetMethod(nameof(IDocumentStorageProvider.QueryDocumentsAsync))!
            .MakeGenericMethod(docType);

        var envelopeType = typeof(DocumentEnvelope<>).MakeGenericType(docType);
        var param = System.Linq.Expressions.Expression.Parameter(envelopeType, "env");
        var lambda = System.Linq.Expressions.Expression.Lambda(System.Linq.Expressions.Expression.Constant(true), param);

        var queryTask = (Task)queryMethod.Invoke(_documentStore.Options.StorageProvider.Documents, new object?[] { lambda, null, ct })!;
        await queryTask.ConfigureAwait(false);

        var resultProperty = queryTask.GetType().GetProperty("Result")!;
        var envelopes = (System.Collections.IEnumerable)resultProperty.GetValue(queryTask)!;

        var deleteMethod = typeof(IDocumentStorageProvider)
            .GetMethod(nameof(IDocumentStorageProvider.DeleteDocumentAsync))!
            .MakeGenericMethod(docType);

        foreach (object envelope in envelopes)
        {
            var idProp = envelope.GetType().GetProperty("Id")!;
            var pkProp = envelope.GetType().GetProperty("PartitionKey")!;
            string id = (string)idProp.GetValue(envelope)!;
            string pk = (string)pkProp.GetValue(envelope)!;

            var deleteTask = (Task)deleteMethod.Invoke(_documentStore.Options.StorageProvider.Documents, new object[] { id, pk, ct })!;
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
            hasMoreEvents = await ProcessNextBatchFromStorageAsync(asyncProjections, ct).ConfigureAwait(false);
        }
    }

    private async Task<bool> ProcessNextBatchFromStorageAsync(List<IProjection> projections, CancellationToken ct)
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

        var eventStorage = _documentStore.Options.StorageProvider.Events;
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

    /// <summary>
    /// Processes a batch of Cosmos DB change feed items, filtering for $event documents, deserializing event envelopes, and dispatching to projections.
    /// </summary>
    public async Task ProcessChangeFeedBatchAsync(IEnumerable<object> changeFeedItems, CancellationToken ct = default)
    {
        if (changeFeedItems == null) return;

        var activeProjections = GetActiveAsyncProjections();
        if (activeProjections.Count == 0) return;

        var events = new List<IEvent>();

        foreach (var item in changeFeedItems)
        {
            if (item == null) continue;

            if (IsEventDocument(item))
            {
                var @event = ExtractEvent(item);
                if (@event != null)
                {
                    events.Add(@event);
                }
            }
        }

        if (events.Count == 0) return;

        foreach (var proj in activeProjections)
        {
            var lastSeq = await _checkpointStore.GetCheckpointAsync(proj.Name, ct).ConfigureAwait(false);
            var newEvents = events.Where(e => e.GlobalSequence > lastSeq).OrderBy(e => e.GlobalSequence).ToList();

            if (newEvents.Count == 0) continue;

            await ProcessEventsForProjectionAsync(proj, newEvents, ct).ConfigureAwait(false);

            long maxSeq = newEvents.Max(e => e.GlobalSequence);
            await _checkpointStore.SaveCheckpointAsync(proj.Name, maxSeq, ct).ConfigureAwait(false);
        }
    }

    private static bool IsEventDocument(object item)
    {
        if (item is CosmosDocumentEnvelope envelope)
        {
            return string.Equals(envelope.DocType, "$event", StringComparison.Ordinal);
        }

        if (item is JObject jobj)
        {
            var docType = jobj["_docType"]?.ToString() ?? jobj["DocType"]?.ToString();
            return string.Equals(docType, "$event", StringComparison.Ordinal);
        }

        if (item is JsonElement jElem)
        {
            if (jElem.TryGetProperty("_docType", out var dt1) || jElem.TryGetProperty("DocType", out dt1))
            {
                return string.Equals(dt1.GetString(), "$event", StringComparison.Ordinal);
            }
            return false;
        }

        var prop = item.GetType().GetProperty("DocType") ?? item.GetType().GetProperty("_docType");
        if (prop != null)
        {
            var val = prop.GetValue(item)?.ToString();
            return string.Equals(val, "$event", StringComparison.Ordinal);
        }

        return false;
    }

    private static IEvent? ExtractEvent(object item)
    {
        object? rawData = null;

        if (item is JObject jobj)
        {
            rawData = jobj["data"] ?? jobj["Data"];
        }
        else if (item is JsonElement jElem)
        {
            if (jElem.TryGetProperty("data", out var dataElem) || jElem.TryGetProperty("Data", out dataElem))
            {
                rawData = dataElem;
            }
        }
        else
        {
            var dataProp = item.GetType().GetProperty("Data") ?? item.GetType().GetProperty("data");
            if (dataProp != null)
            {
                rawData = dataProp.GetValue(item);
            }
        }

        if (rawData == null) return null;

        IEvent? evt = rawData as IEvent;

        if (evt == null && rawData is JObject jData)
        {
            evt = jData.ToObject<EventEnvelope<object>>();
        }
        else if (evt == null && rawData is JsonElement dataElem)
        {
            var jsonText = dataElem.GetRawText();
            evt = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(jsonText);
        }
        else if (evt == null && rawData is string strJson)
        {
            evt = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(strJson);
        }
        else if (evt == null && rawData != null)
        {
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(rawData);
            evt = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(json);
        }

        if (evt != null)
        {
            EnsureTypedPayload(evt);
        }

        return evt;
    }

    private static void EnsureTypedPayload(IEvent evt)
    {
        if (evt == null || evt.Data == null) return;

        var currentPayload = evt.Data;
        var eventTypeStr = evt.EventType;

        if (currentPayload is JToken jToken)
        {
            var targetType = ResolveType(eventTypeStr);
            if (targetType != null)
            {
                var deserialized = jToken.ToObject(targetType);
                if (deserialized != null)
                {
                    SetPayloadData(evt, deserialized);
                }
            }
        }
        else if (currentPayload is JsonElement jElem)
        {
            var targetType = ResolveType(eventTypeStr);
            if (targetType != null)
            {
                var rawText = jElem.GetRawText();
                var deserialized = Newtonsoft.Json.JsonConvert.DeserializeObject(rawText, targetType);
                if (deserialized != null)
                {
                    SetPayloadData(evt, deserialized);
                }
            }
        }
    }

    private static void SetPayloadData(IEvent evt, object payload)
    {
        var prop = evt.GetType().GetProperty(nameof(IEvent.Data));
        if (prop != null && prop.CanWrite)
        {
            prop.SetValue(evt, payload);
        }
        else
        {
            var field = evt.GetType().GetField($"<{nameof(IEvent.Data)}>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
            field?.SetValue(evt, payload);
        }
    }

    private static Type? ResolveType(string eventTypeName)
    {
        if (string.IsNullOrWhiteSpace(eventTypeName)) return null;

        return _typeCache.GetOrAdd(eventTypeName, name =>
        {
            var type = Type.GetType(name);
            if (type != null) return type;

            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                type = asm.GetType(name);
                if (type != null) return type;

                type = asm.GetTypes().FirstOrDefault(t => t.Name == name || t.FullName == name);
                if (type != null) return type;
            }

            return null;
        });
    }

    private List<IProjection> GetActiveAsyncProjections()
    {
        return _documentStore.Options.Projections.Projections
            .Where(p => p.Lifecycle == ProjectionLifecycle.Async)
            .Where(p => !_stoppedProjections.TryGetValue(p.Name, out var isStopped) || !isStopped)
            .ToList();
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
                typeof(CosmosProjectionDaemon).GetMethod(nameof(ProcessSingleStreamEventsAsync), BindingFlags.NonPublic | BindingFlags.Instance)!
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

            await _documentStore.Options.StorageProvider.Documents.UpsertDocumentAsync(envelope, ct).ConfigureAwait(false);
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

                await CatchUpAsync(stoppingToken).ConfigureAwait(false);
                await Task.Delay(100, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error processing Cosmos projection daemon loop");
                await Task.Delay(500, stoppingToken).ConfigureAwait(false);
            }
        }
    }
}
