using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosStorageProvider : IAquilaStorageProvider, IDocumentStorageProvider, IEventStorageProvider
{
    private readonly CosmosClient _client;
    private Container _container = null!;
    private readonly string _databaseName;
    private readonly string _containerName;
    private long _globalSequence;

    public string ProviderName => "AzureCosmosDB";
    public IDocumentStorageProvider Documents => this;
    public IEventStorageProvider Events => this;

    public CosmosStorageProvider(string connectionString, string databaseName = "AquilaDB", string containerName = "Documents")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        _databaseName = databaseName;
        _containerName = containerName;
        _client = new CosmosClient(connectionString, new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Direct
        });
    }

    public CosmosStorageProvider(CosmosClient client, string databaseName = "AquilaDB", string containerName = "Documents")
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        _client = client;
        _databaseName = databaseName;
        _containerName = containerName;
    }

    private Container Container => _container ??= _client.GetContainer(_databaseName, _containerName);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type?> _eventTypeCache = new();

    // Cosmos round-trips event payloads through JSON, so a plain EventEnvelope<object> deserialization
    // leaves Data as a JObject/JsonElement instead of the original event record. Projections that
    // pattern-match on the concrete event type (e.g. MultiStreamProjection.Apply) need it rehydrated.
    private static void EnsureTypedPayload(EventEnvelope<object> evt)
    {
        if (evt.Data == null) return;

        Type? targetType = null;
        string? rawJson = null;

        if (evt.Data is Newtonsoft.Json.Linq.JToken jToken)
        {
            targetType = ResolveEventType(evt.EventType);
            rawJson = jToken.ToString(Newtonsoft.Json.Formatting.None);
        }
        else if (evt.Data is System.Text.Json.JsonElement jsonElement)
        {
            targetType = ResolveEventType(evt.EventType);
            rawJson = jsonElement.GetRawText();
        }

        if (targetType == null || rawJson == null) return;

        var deserialized = Newtonsoft.Json.JsonConvert.DeserializeObject(rawJson, targetType);
        if (deserialized != null)
        {
            evt.Data = deserialized;
        }
    }

    private static Type? ResolveEventType(string eventTypeName)
    {
        if (string.IsNullOrWhiteSpace(eventTypeName)) return null;

        return _eventTypeCache.GetOrAdd(eventTypeName, name =>
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

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var db = await _client.CreateDatabaseIfNotExistsAsync(_databaseName, cancellationToken: ct);
        var containerResp = await db.Database.CreateContainerIfNotExistsAsync(_containerName, "/pk", cancellationToken: ct);
        _container = containerResp.Container;

        _globalSequence = await GetMaxGlobalSequenceAsync(ct).ConfigureAwait(false);
    }

    // The container is shared across process instances (e.g. multiple DocumentStores pointed
    // at the same physical container), so the in-memory counter must be seeded from existing
    // data instead of always starting at 0, or new events can collide with GlobalSequence
    // values already persisted by a different instance.
    private async Task<long> GetMaxGlobalSequenceAsync(CancellationToken ct)
    {
        var max = 0L;
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c._docType = '$event'");
        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(queryDef);
        if (iterator == null) return max;

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
            foreach (var item in response)
            {
                IEvent? @event = item.Data as IEvent;
                if (@event == null && item.Data != null)
                {
                    var rawJson = item.Data.ToString();
                    if (!string.IsNullOrEmpty(rawJson))
                    {
                        @event = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(rawJson);
                    }
                }

                if (@event != null && @event.GlobalSequence > max)
                {
                    max = @event.GlobalSequence;
                }
            }
        }

        return max;
    }

    // --- DocumentStorageProvider ---

    public async Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        if (id.Contains('/'))
        {
            return await QuerySingleDocumentAsync<T>(id, partitionKey, ct);
        }

        try
        {
            var response = await Container.ReadItemAsync<CosmosDocumentEnvelope<T>>(
                id,
                new PartitionKey(partitionKey),
                cancellationToken: ct);

            if (response?.Resource == null || response.Resource.IsDeleted) return null;

            return MapToEnvelope(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private async Task<DocumentEnvelope<T>?> QuerySingleDocumentAsync<T>(string id, string partitionKey, CancellationToken ct) where T : class
    {
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", id);

        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<T>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                if (item != null && !item.IsDeleted)
                {
                    return MapToEnvelope(item);
                }
            }
        }

        return null;
    }

    public async Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        var requestOptions = new QueryRequestOptions();
        if (options != null)
        {
            if (!string.IsNullOrEmpty(options.PartitionKey))
            {
                requestOptions.PartitionKey = new PartitionKey(options.PartitionKey);
            }
            if (options.MaxItemCount.HasValue)
            {
                requestOptions.MaxItemCount = options.MaxItemCount.Value;
            }
        }

        var docType = typeof(T).Name;
        IQueryable<CosmosDocumentEnvelope<T>>? queryable = Container.GetItemLinqQueryable<CosmosDocumentEnvelope<T>>(
            false,
            options?.ContinuationToken,
            requestOptions);

        if (queryable == null || queryable.Provider == null)
        {
            return Array.Empty<DocumentEnvelope<T>>();
        }

        queryable = queryable.Where(x => x.DocType == docType && !x.IsDeleted);

        if (predicate != null)
        {
            var rewritten = CosmosExpressionRewriter.Rewrite(predicate);
            if (rewritten != null)
            {
                queryable = queryable.Where(rewritten);
            }
        }

        var results = new List<DocumentEnvelope<T>>();

        try
        {
            var queryDef = queryable.ToQueryDefinition();
            var sql = queryDef.QueryText;

            if (sql.StartsWith("SELECT VALUE root FROM root"))
            {
                sql = "SELECT * FROM c" + sql.Substring("SELECT VALUE root FROM root".Length);
                queryDef = new QueryDefinition(sql);
            }

            using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<T>>(
                queryDef,
                continuationToken: options?.ContinuationToken,
                requestOptions: requestOptions);

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(ct);
                foreach (var item in response)
                {
                    results.Add(MapToEnvelope(item));
                }
            }
        }
        catch (Exception ex) when (ex is InvalidOperationException || ex is ArgumentOutOfRangeException || ex is ArgumentException)
        {
            foreach (var item in queryable)
            {
                results.Add(MapToEnvelope(item));
            }
        }

        return results;
    }

    public async Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.Id);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.PartitionKey);

        var cosmosEnvelope = new CosmosDocumentEnvelope<T>
        {
            Id = envelope.Id,
            PartitionKey = envelope.PartitionKey,
            DocType = envelope.DocType,
            TenantId = envelope.TenantId,
            IsDeleted = envelope.IsDeleted,
            Version = envelope.Version,
            ETag = envelope.ETag,
            Data = envelope.Data
        };

        await Container.UpsertItemAsync(cosmosEnvelope, new PartitionKey(envelope.PartitionKey), cancellationToken: ct);
    }

    public async Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        try
        {
            await Container.DeleteItemAsync<CosmosDocumentEnvelope<T>>(id, new PartitionKey(partitionKey), cancellationToken: ct);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound || ex.StatusCode == System.Net.HttpStatusCode.BadRequest)
        {
        }
    }

    public async Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operations);

        foreach (var op in operations)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(op.Id);
            ArgumentException.ThrowIfNullOrWhiteSpace(op.PartitionKey);

            if (op.OperationType == StorageOperationType.Upsert)
            {
                await Container.UpsertItemAsync(op.Document, new PartitionKey(op.PartitionKey), cancellationToken: ct);
            }
            else if (op.OperationType == StorageOperationType.Delete)
            {
                await Container.DeleteItemAsync<object>(op.Id, new PartitionKey(op.PartitionKey), cancellationToken: ct);
            }
            else if (op.OperationType == StorageOperationType.Patch)
            {
                if (op.PatchOperations == null || op.PatchOperations.Count == 0)
                {
                    continue;
                }

                var cosmosPatchOperations = op.PatchOperations.Select(BuildCosmosPatchOperation).ToList();
                await Container.PatchItemAsync<CosmosDocumentEnvelope<object>>(op.Id, new PartitionKey(op.PartitionKey), cosmosPatchOperations, cancellationToken: ct);
            }
        }
    }

    private static PatchOperation BuildCosmosPatchOperation(PatchOperationData patchData)
    {
        return patchData.Action switch
        {
            PatchAction.Set => PatchOperation.Replace(patchData.Path, patchData.Value),
            PatchAction.Increment => PatchOperation.Increment(patchData.Path, Convert.ToInt64(patchData.Value)),
            PatchAction.Remove => PatchOperation.Remove(patchData.Path),
            PatchAction.Append => PatchOperation.Add($"{patchData.Path}/-", patchData.Value),
            _ => throw new NotSupportedException($"Patch action '{patchData.Action}' is not supported.")
        };
    }

    // --- EventStorageProvider ---

    public async Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);

        var eventList = events.ToList();
        if (eventList.Count == 0) return;

        var tenantId = eventList.FirstOrDefault()?.TenantId ?? "default";
        var header = await GetStreamHeaderAsync(streamId, tenantId, ct);
        long currentVersion = header?.Version ?? 0;

        if (expectedVersion >= 0 && currentVersion != expectedVersion)
        {
            throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), currentVersion.ToString());
        }

        foreach (var @evt in eventList)
        {
            currentVersion++;
            @evt.SetVersion(currentVersion);
            if (@evt.GlobalSequence == 0)
            {
                @evt.SetGlobalSequence(Interlocked.Increment(ref _globalSequence));
            }
            var doc = new CosmosDocumentEnvelope<object>
            {
                Id = $"$event_{streamId}_v{currentVersion}",
                PartitionKey = streamId,
                DocType = "$event",
                TenantId = @evt.TenantId,
                IsDeleted = false,
                Version = currentVersion.ToString(),
                Data = @evt
            };

            await Container.UpsertItemAsync(doc, new PartitionKey(streamId), cancellationToken: ct);
        }

        var updatedHeader = new EventStreamHeader
        {
            StreamId = streamId,
            Version = currentVersion,
            TenantId = tenantId,
            CreatedAt = header?.CreatedAt ?? DateTimeOffset.UtcNow,
            LastModified = DateTimeOffset.UtcNow
        };

        var headerDoc = new CosmosDocumentEnvelope<EventStreamHeader>
        {
            Id = $"$stream_{streamId}",
            PartitionKey = streamId,
            DocType = "$stream_header",
            TenantId = tenantId,
            IsDeleted = false,
            Version = currentVersion.ToString(),
            Data = updatedHeader
        };

        await Container.UpsertItemAsync(headerDoc, new PartitionKey(streamId), cancellationToken: ct);
    }

    public async Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var queryText = string.IsNullOrEmpty(tenantId)
            ? "SELECT * FROM c WHERE c.pk = @streamId AND c._docType = '$event'"
            : "SELECT * FROM c WHERE c.pk = @streamId AND c._docType = '$event' AND c._tenantId = @tenantId";

        var queryDef = new QueryDefinition(queryText)
            .WithParameter("@streamId", streamId);

        if (!string.IsNullOrEmpty(tenantId))
        {
            queryDef = queryDef.WithParameter("@tenantId", tenantId);
        }

        var events = new List<IEvent>();
        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(streamId) });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                IEvent? @event = item.Data as IEvent;
                if (@event == null && item.Data != null)
                {
                    var rawJson = item.Data.ToString();
                    if (!string.IsNullOrEmpty(rawJson))
                    {
                        var envelope = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(rawJson);
                        if (envelope != null)
                        {
                            EnsureTypedPayload(envelope);
                        }
                        @event = envelope;
                    }
                }

                if (@event != null)
                {
                    if (long.TryParse(item.Version, out var itemVer) && itemVer > 0 && @event.Version == 0)
                    {
                        @event.SetVersion(itemVer);
                    }

                    if ((string.IsNullOrEmpty(tenantId) || item.TenantId == tenantId || @event.TenantId == tenantId) && @event.Version >= fromVersion)
                    {
                        events.Add(@event);
                    }
                }
            }
        }

        return events.OrderBy(e => e.Version).ToList();
    }

    public async Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default)
    {
        if (batchSize <= 0)
        {
            return Array.Empty<IEvent>();
        }

        var queryText = string.IsNullOrEmpty(tenantId)
            ? "SELECT * FROM c WHERE c._docType = '$event'"
            : "SELECT * FROM c WHERE c._docType = '$event' AND c._tenantId = @tenantId";

        var queryDef = new QueryDefinition(queryText);
        if (!string.IsNullOrEmpty(tenantId))
        {
            queryDef = queryDef.WithParameter("@tenantId", tenantId);
        }

        var events = new List<IEvent>();
        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(queryDef);

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                IEvent? @event = item.Data as IEvent;
                if (@event == null && item.Data != null)
                {
                    var rawJson = item.Data.ToString();
                    if (!string.IsNullOrEmpty(rawJson))
                    {
                        var envelope = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(rawJson);
                        if (envelope != null)
                        {
                            EnsureTypedPayload(envelope);
                        }
                        @event = envelope;
                    }
                }

                if (@event != null)
                {
                    if (long.TryParse(item.Version, out var itemVer) && itemVer > 0 && @event.Version == 0)
                    {
                        @event.SetVersion(itemVer);
                    }

                    if ((string.IsNullOrEmpty(tenantId) || item.TenantId == tenantId || @event.TenantId == tenantId) && @event.GlobalSequence > fromGlobalSequence)
                    {
                        events.Add(@event);
                    }
                }
            }
        }

        return events.OrderBy(e => e.GlobalSequence).Take(batchSize).ToList();
    }

    public async Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var targetId = $"$stream_{streamId}";
        if (targetId.Contains('/'))
        {
            return await QueryStreamHeaderAsync(streamId, targetId, tenantId, ct);
        }

        try
        {
            var resp = await Container.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
                targetId,
                new PartitionKey(streamId),
                cancellationToken: ct);

            if (resp?.Resource == null) return null;
            if (!string.IsNullOrEmpty(tenantId) && (resp.Resource.TenantId != tenantId || resp.Resource.Data?.TenantId != tenantId))
            {
                return null;
            }

            return resp.Resource.Data;
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private async Task<EventStreamHeader?> QueryStreamHeaderAsync(string streamId, string targetId, string? tenantId, CancellationToken ct)
    {
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", targetId);

        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<EventStreamHeader>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(streamId) });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                if (item != null && !item.IsDeleted)
                {
                    if (!string.IsNullOrEmpty(tenantId) && (item.TenantId != tenantId || item.Data?.TenantId != tenantId))
                    {
                        return null;
                    }
                    return item.Data;
                }
            }
        }

        return null;
    }

    public async Task SaveSnapshotAsync<TAggregate>(string streamId, long version, TAggregate snapshot, string tenantId = "default", CancellationToken ct = default) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(snapshot);

        var snapshotDoc = new CosmosDocumentEnvelope<TAggregate>
        {
            Id = $"$snapshot_{streamId}",
            PartitionKey = streamId,
            DocType = "$snapshot",
            TenantId = tenantId,
            IsDeleted = false,
            Version = version.ToString(),
            Data = snapshot
        };

        await Container.UpsertItemAsync(snapshotDoc, new PartitionKey(streamId), cancellationToken: ct);
    }

    public async Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var targetId = $"$snapshot_{streamId}";
        if (targetId.Contains('/'))
        {
            return await QuerySnapshotAsync<TAggregate>(streamId, targetId, tenantId, ct);
        }

        try
        {
            var resp = await Container.ReadItemAsync<CosmosDocumentEnvelope<TAggregate>>(
                targetId,
                new PartitionKey(streamId),
                cancellationToken: ct);

            if (resp?.Resource == null || resp.Resource.IsDeleted) return (null, 0);
            if (!string.IsNullOrEmpty(tenantId) && resp.Resource.TenantId != tenantId)
            {
                return (null, 0);
            }

            if (long.TryParse(resp.Resource.Version, out var snapshotVersion))
            {
                return (resp.Resource.Data, snapshotVersion);
            }

            return (resp.Resource.Data, 0);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return (null, 0);
        }
    }

    private async Task<(TAggregate? Snapshot, long SnapshotVersion)> QuerySnapshotAsync<TAggregate>(string streamId, string targetId, string tenantId, CancellationToken ct) where TAggregate : class
    {
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", targetId);

        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<TAggregate>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(streamId) });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                if (item != null && !item.IsDeleted)
                {
                    if (!string.IsNullOrEmpty(tenantId) && item.TenantId != tenantId)
                    {
                        return (null, 0);
                    }

                    if (long.TryParse(item.Version, out var snapshotVersion))
                    {
                        return (item.Data, snapshotVersion);
                    }

                    return (item.Data, 0);
                }
            }
        }

        return (null, 0);
    }

    private static DocumentEnvelope<T> MapToEnvelope<T>(CosmosDocumentEnvelope<T> item)
    {
        return new DocumentEnvelope<T>
        {
            Id = item.Id,
            PartitionKey = item.PartitionKey,
            DocType = item.DocType,
            TenantId = item.TenantId,
            IsDeleted = item.IsDeleted,
            Version = item.Version,
            ETag = item.ETag,
            Data = item.Data
        };
    }

    public void Dispose() => _client?.Dispose();
    public ValueTask DisposeAsync()
    {
        _client?.Dispose();
        return ValueTask.CompletedTask;
    }
}
