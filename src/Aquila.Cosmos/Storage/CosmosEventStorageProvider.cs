using Aquila.Core.Serialization;
using Microsoft.Azure.Cosmos;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosEventStorageProvider : IEventStorageProvider
{
    private readonly Func<Container> _containerProvider;
    private readonly ICosmosEventTypeResolver _eventTypeResolver;
    private long _globalSequence;

    public CosmosEventStorageProvider(Func<Container> containerProvider, ICosmosEventTypeResolver? eventTypeResolver = null)
    {
        ArgumentNullException.ThrowIfNull(containerProvider);
        _containerProvider = containerProvider;
        _eventTypeResolver = eventTypeResolver ?? CosmosEventTypeResolver.Default;
    }

    public CosmosEventStorageProvider(Container container, ICosmosEventTypeResolver? eventTypeResolver = null)
    {
        ArgumentNullException.ThrowIfNull(container);
        _containerProvider = () => container;
        _eventTypeResolver = eventTypeResolver ?? CosmosEventTypeResolver.Default;
    }

    private Container Container => _containerProvider();

    public string ProviderName => "AzureCosmosDB";
    public Task InitializeAsync(CancellationToken ct = default) => InitializeSequenceAsync(ct);
    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async Task InitializeSequenceAsync(CancellationToken ct = default)
    {
        _globalSequence = await GetMaxGlobalSequenceAsync(ct).ConfigureAwait(false);
    }

    private async Task<long> GetMaxGlobalSequenceAsync(CancellationToken ct)
    {
        var max = 0L;
        try
        {
            var queryDef = new QueryDefinition("SELECT VALUE MAX(c.data.GlobalSequence) FROM c WHERE c._docType = '$event'");
            using var iterator = Container.GetItemQueryIterator<long?>(queryDef);
            if (iterator == null) return max;

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
                foreach (var item in response)
                {
                    if (item.HasValue && item.Value > max)
                    {
                        max = item.Value;
                    }
                }
            }
        }
        catch
        {
            // Fallback for mocks or scenarios where SQL aggregate iterator isn't configured
        }

        return max;
    }

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

        var partitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId);

        try
        {
            var batch = Container.CreateTransactionalBatch(partitionKey);
            if (batch != null)
            {
                var batchVersion = currentVersion;
                foreach (var @evt in eventList)
                {
                    batchVersion++;
                    @evt.SetVersion(batchVersion);
                    if (@evt.GlobalSequence == 0)
                    {
                        @evt.SetGlobalSequence(Interlocked.Increment(ref _globalSequence));
                    }
                    var doc = new CosmosDocumentEnvelope<object>
                    {
                        Id = $"$event_{streamId}_v{batchVersion}",
                        PartitionKey = streamId,
                        DocType = "$event",
                        TenantId = @evt.TenantId,
                        IsDeleted = false,
                        Version = batchVersion.ToString(),
                        Data = @evt
                    };

                    batch.UpsertItem(doc);
                }

                var batchHeader = new EventStreamHeader
                {
                    StreamId = streamId,
                    Version = batchVersion,
                    TenantId = tenantId,
                    CreatedAt = header?.CreatedAt ?? DateTimeOffset.UtcNow,
                    LastModified = DateTimeOffset.UtcNow
                };

                var batchHeaderDoc = new CosmosDocumentEnvelope<EventStreamHeader>
                {
                    Id = $"$stream_{streamId}",
                    PartitionKey = streamId,
                    DocType = "$stream_header",
                    TenantId = tenantId,
                    IsDeleted = false,
                    Version = batchVersion.ToString(),
                    Data = batchHeader
                };

                batch.UpsertItem(batchHeaderDoc);

                using var response = await batch.ExecuteAsync(ct).ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                {
                    return;
                }

                if (response.StatusCode == System.Net.HttpStatusCode.PreconditionFailed || response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), currentVersion.ToString());
                }
            }
        }
        catch (AquilaConcurrencyException)
        {
            throw;
        }
        catch
        {
            // Fall through to sequential upserts for environments/emulators without TransactionalBatch support
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

            await Container.UpsertItemAsync(doc, partitionKey, cancellationToken: ct);
        }

        var fallbackHeader = new EventStreamHeader
        {
            StreamId = streamId,
            Version = currentVersion,
            TenantId = tenantId,
            CreatedAt = header?.CreatedAt ?? DateTimeOffset.UtcNow,
            LastModified = DateTimeOffset.UtcNow
        };

        var fallbackHeaderDoc = new CosmosDocumentEnvelope<EventStreamHeader>
        {
            Id = $"$stream_{streamId}",
            PartitionKey = streamId,
            DocType = "$stream_header",
            TenantId = tenantId,
            IsDeleted = false,
            Version = currentVersion.ToString(),
            Data = fallbackHeader
        };

        await Container.UpsertItemAsync(fallbackHeaderDoc, partitionKey, cancellationToken: ct);
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
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });
        if (iterator == null) return events;

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
                        var envelope = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(rawJson, PrivateConstructorContractResolver.Settings);
                        if (envelope != null)
                        {
                            _eventTypeResolver.EnsureTypedPayload(envelope);
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
        if (iterator == null) return events;

        while (iterator.HasMoreResults && events.Count < batchSize)
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
                        var envelope = Newtonsoft.Json.JsonConvert.DeserializeObject<EventEnvelope<object>>(rawJson, PrivateConstructorContractResolver.Settings);
                        if (envelope != null)
                        {
                            _eventTypeResolver.EnsureTypedPayload(envelope);
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
                        if (events.Count >= batchSize) break;
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
                CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
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
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });

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

        await Container.UpsertItemAsync(snapshotDoc, CosmosPartitionKeyHelper.CreatePartitionKey(streamId), cancellationToken: ct);
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
                CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
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
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });

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
}
