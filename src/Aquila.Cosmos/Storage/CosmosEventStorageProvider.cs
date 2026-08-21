using Aquila.Core.Serialization;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosEventStorageProvider : IEventStorageProvider
{
    private readonly Func<Container> _eventContainerProvider;
    private readonly Func<Container>? _snapshotContainerProvider;
    private readonly ICosmosEventTypeResolver _eventTypeResolver;
    private readonly ILogger<CosmosEventStorageProvider>? _logger;
    private long _globalSequence;

    public CosmosEventStorageProvider(
        Func<Container> eventContainerProvider,
        Func<Container>? snapshotContainerProvider = null,
        ICosmosEventTypeResolver? eventTypeResolver = null,
        ILogger<CosmosEventStorageProvider>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(eventContainerProvider);
        _eventContainerProvider = eventContainerProvider;
        _snapshotContainerProvider = snapshotContainerProvider;
        _eventTypeResolver = eventTypeResolver ?? CosmosEventTypeResolver.Default;
        _logger = logger;
    }

    public CosmosEventStorageProvider(
        Container container,
        ICosmosEventTypeResolver? eventTypeResolver = null,
        ILogger<CosmosEventStorageProvider>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(container);
        _eventContainerProvider = () => container;
        _snapshotContainerProvider = null;
        _eventTypeResolver = eventTypeResolver ?? CosmosEventTypeResolver.Default;
        _logger = logger;
    }

    private Container EventContainer => _eventContainerProvider();
    private Container SnapshotContainer => (_snapshotContainerProvider ?? _eventContainerProvider)();

    public string ProviderName => "AzureCosmosDB";
    public double LastRequestCharge { get; private set; }
    public double CumulativeRequestCharge { get; private set; }

    private void RecordCharge(double charge)
    {
        LastRequestCharge = charge;
        CumulativeRequestCharge += charge;
    }

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
            using var iterator = EventContainer.GetItemQueryIterator<long?>(queryDef);
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
        catch (OperationCanceledException)
        {
            // Cancellation is the caller's decision, not a seeding failure.
            throw;
        }
        catch (Exception ex)
        {
            // Seeding is best-effort: a mock container or a provider without SQL aggregate support
            // cannot answer this query. Swallowing silently, however, hands back 0 and re-issues
            // sequence numbers that already exist, so the failure has to be visible.
            _logger?.LogWarning(ex,
                "Could not read MAX(GlobalSequence) from the event container; the global sequence will seed at 0. " +
                "Events appended by this process may reuse sequence numbers already present in storage.");
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
        var headerEnvelope = await GetStreamHeaderEnvelopeAsync(streamId, tenantId, ct).ConfigureAwait(false);
        var header = headerEnvelope?.Data;
        var headerETag = headerEnvelope?.ETag;
        long currentVersion = header?.Version ?? 0;

        if (expectedVersion >= 0 && currentVersion != expectedVersion)
        {
            throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), currentVersion.ToString());
        }

        var partitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId);
        bool fallbackToSequential = false;

        try
        {
            var batch = EventContainer.CreateTransactionalBatch(partitionKey);
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

                    // CreateItem, not UpsertItem: event ids are deterministic ($event_{stream}_v{n}),
                    // so a create that collides is precisely the signal that another writer already
                    // claimed this version. An upsert would silently overwrite their event.
                    batch.CreateItem(doc);
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

                if (headerETag == null)
                {
                    // New stream: two concurrent creators must not both succeed.
                    batch.CreateItem(batchHeaderDoc);
                }
                else
                {
                    // Existing stream: the batch commits only if the header is still at the version
                    // we validated against, making the whole append conditional rather than atomic-only.
                    // Upsert rather than Replace: Replace takes the id as a separate argument that
                    // Cosmos puts in the request path, and stream ids legitimately contain '/'
                    // (for example "orders/ord-1"), which makes the path invalid. Upsert carries the
                    // id in the document body, and honours If-Match just the same.
                    batch.UpsertItem(batchHeaderDoc, new TransactionalBatchItemRequestOptions { IfMatchEtag = headerETag });
                }

                using var response = await batch.ExecuteAsync(ct).ConfigureAwait(false);
                if (response != null)
                {
                    RecordCharge(response.RequestCharge);
                }

                if (response != null && response.IsSuccessStatusCode)
                {
                    return;
                }

                if (response == null)
                {
                    fallbackToSequential = true;
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.PreconditionFailed || response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), currentVersion.ToString());
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.BadRequest ||
                         response.StatusCode == System.Net.HttpStatusCode.NotImplemented ||
                         response.StatusCode == System.Net.HttpStatusCode.MethodNotAllowed ||
                         response.StatusCode == System.Net.HttpStatusCode.InternalServerError)
                {
                    _logger?.LogWarning("TransactionalBatch returned {StatusCode}. Falling back to sequential upserts.", response.StatusCode);
                    fallbackToSequential = true;
                }
                else
                {
                    throw new CosmosException(response.ErrorMessage ?? $"Batch execution failed with {response.StatusCode}", response.StatusCode, 0, response.ActivityId, response.RequestCharge);
                }
            }
        }
        catch (AquilaConcurrencyException)
        {
            throw;
        }
        catch (NotSupportedException ex)
        {
            _logger?.LogWarning(ex, "TransactionalBatch not supported. Falling back to sequential upserts.");
            fallbackToSequential = true;
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.BadRequest ||
                                         ex.StatusCode == System.Net.HttpStatusCode.NotImplemented ||
                                         ex.StatusCode == System.Net.HttpStatusCode.MethodNotAllowed ||
                                         ex.StatusCode == System.Net.HttpStatusCode.InternalServerError)
        {
            _logger?.LogWarning(ex, "TransactionalBatch returned {StatusCode}. Falling back to sequential upserts.", ex.StatusCode);
            fallbackToSequential = true;
        }

        if (!fallbackToSequential && EventContainer.CreateTransactionalBatch(partitionKey) != null)
        {
            return;
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

            try
            {
                await EventContainer.CreateItemAsync(doc, partitionKey, cancellationToken: ct).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.Conflict)
            {
                RecordCharge(ex.RequestCharge);
                throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), currentVersion.ToString());
            }
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

        try
        {
            if (headerETag == null)
            {
                await EventContainer.CreateItemAsync(fallbackHeaderDoc, partitionKey, cancellationToken: ct).ConfigureAwait(false);
            }
            else
            {
                // Upsert, not Replace: see the batch path above -- stream ids may contain '/', which
                // Replace cannot express because it puts the id in the request path.
                await EventContainer.UpsertItemAsync(
                    fallbackHeaderDoc,
                    partitionKey,
                    new ItemRequestOptions { IfMatchEtag = headerETag },
                    ct).ConfigureAwait(false);
            }
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.Conflict ||
                                         ex.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
        {
            RecordCharge(ex.RequestCharge);
            throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), currentVersion.ToString());
        }
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
        using var iterator = EventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });
        if (iterator == null) return events;

        double totalCharge = 0.0;
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            totalCharge += response.RequestCharge;
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

        RecordCharge(totalCharge);
        return events.OrderBy(e => e.Version).ToList();
    }

    public async Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default)
    {
        if (batchSize <= 0)
        {
            return Array.Empty<IEvent>();
        }

        var queryText = string.IsNullOrEmpty(tenantId)
            ? "SELECT * FROM c WHERE c._docType = '$event' AND c.data.GlobalSequence > @fromGlobalSequence ORDER BY c.data.GlobalSequence"
            : "SELECT * FROM c WHERE c._docType = '$event' AND c._tenantId = @tenantId AND c.data.GlobalSequence > @fromGlobalSequence ORDER BY c.data.GlobalSequence";

        var queryDef = new QueryDefinition(queryText)
            .WithParameter("@fromGlobalSequence", fromGlobalSequence);

        if (!string.IsNullOrEmpty(tenantId))
        {
            queryDef = queryDef.WithParameter("@tenantId", tenantId);
        }

        var requestOptions = new QueryRequestOptions { MaxItemCount = batchSize };

        var events = new List<IEvent>();
        using var iterator = EventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(queryDef, requestOptions: requestOptions);
        if (iterator == null) return events;

        double globalCharge = 0.0;
        try
        {
            while (iterator.HasMoreResults && events.Count < batchSize)
            {
                var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
                globalCharge += response.RequestCharge;
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

            RecordCharge(globalCharge);
            return events.OrderBy(e => e.GlobalSequence).Take(batchSize).ToList();
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.InternalServerError || ex.StatusCode == System.Net.HttpStatusCode.BadRequest)
        {
            _logger?.LogWarning(ex, "Server-side sorted FetchGlobalEventsAsync query failed ({StatusCode}). Falling back to unsorted server-side query with client sort.", ex.StatusCode);
            return await FetchGlobalEventsUnsortedFallbackAsync(fromGlobalSequence, batchSize, tenantId, ct).ConfigureAwait(false);
        }
    }

    private async Task<IReadOnlyList<IEvent>> FetchGlobalEventsUnsortedFallbackAsync(long fromGlobalSequence, int batchSize, string? tenantId, CancellationToken ct)
    {
        var sql = "SELECT * FROM c WHERE c._docType = '$event' AND c.data.GlobalSequence > @fromGlobalSequence";
        if (!string.IsNullOrEmpty(tenantId))
        {
            sql += " AND c._tenantId = @tenantId";
        }

        var queryDef = new QueryDefinition(sql)
            .WithParameter("@fromGlobalSequence", fromGlobalSequence);

        if (!string.IsNullOrEmpty(tenantId))
        {
            queryDef = queryDef.WithParameter("@tenantId", tenantId);
        }

        var requestOptions = new QueryRequestOptions { MaxItemCount = batchSize };
        var events = new List<IEvent>();

        using var iterator = EventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(queryDef, requestOptions: requestOptions);
        if (iterator == null) return events;

        double totalCharge = 0.0;
        try
        {
            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
                totalCharge += response.RequestCharge;
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
                        }
                    }
                }
            }

            RecordCharge(totalCharge);
            return events.OrderBy(e => e.GlobalSequence).Take(batchSize).ToList();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to fetch global events with unsorted fallback query.");
            return events.OrderBy(e => e.GlobalSequence).Take(batchSize).ToList();
        }
    }

    private async Task<IReadOnlyList<IEvent>> FetchGlobalEventsDocTypeFallbackAsync(long fromGlobalSequence, int batchSize, string? tenantId, CancellationToken ct)
    {
        var sql = "SELECT * FROM c WHERE c._docType = '$event'";
        if (!string.IsNullOrEmpty(tenantId))
        {
            sql += " AND c._tenantId = @tenantId";
        }

        var queryDef = new QueryDefinition(sql);
        if (!string.IsNullOrEmpty(tenantId))
        {
            queryDef = queryDef.WithParameter("@tenantId", tenantId);
        }

        var events = new List<IEvent>();
        using var iterator = EventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(queryDef);
        if (iterator == null) return events;

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
                    }
                }
            }
        }

        return events.OrderBy(e => e.GlobalSequence).Take(batchSize).ToList();
    }

    public async Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default)
    {
        var envelope = await GetStreamHeaderEnvelopeAsync(streamId, tenantId, ct).ConfigureAwait(false);
        return envelope?.Data;
    }

    /// <summary>
    /// Reads the stream header envelope rather than just its payload, so the append path can carry
    /// the document's <c>_etag</c> into an If-Match precondition. Without the ETag the version check
    /// is a check-then-act: two writers both observe version N and both write N+1.
    /// </summary>
    private async Task<CosmosDocumentEnvelope<EventStreamHeader>?> GetStreamHeaderEnvelopeAsync(string streamId, string? tenantId = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var targetId = $"$stream_{streamId}";
        if (targetId.Contains('/'))
        {
            return await QueryStreamHeaderAsync(streamId, targetId, tenantId, ct);
        }

        try
        {
            var resp = await EventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
                targetId,
                CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
                cancellationToken: ct);

            RecordCharge(resp.RequestCharge);

            if (resp?.Resource == null) return null;
            if (!string.IsNullOrEmpty(tenantId) && (resp.Resource.TenantId != tenantId || resp.Resource.Data?.TenantId != tenantId))
            {
                return null;
            }

            // ReadItemAsync surfaces the ETag on the response; the deserialized body may not carry it.
            resp.Resource.ETag ??= resp.ETag;
            return resp.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            RecordCharge(ex.RequestCharge);
            return null;
        }
        catch (CosmosException ex)
        {
            RecordCharge(ex.RequestCharge);
            return await QueryStreamHeaderAsync(streamId, targetId, tenantId, ct);
        }
    }

    private async Task<CosmosDocumentEnvelope<EventStreamHeader>?> QueryStreamHeaderAsync(string streamId, string targetId, string? tenantId, CancellationToken ct)
    {
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", targetId);

        using var iterator = EventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<EventStreamHeader>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });

        if (iterator == null) return null;

        double charge = 0.0;
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            charge += response.RequestCharge;
            foreach (var item in response)
            {
                if (item != null && !item.IsDeleted)
                {
                    RecordCharge(charge);
                    if (!string.IsNullOrEmpty(tenantId) && (item.TenantId != tenantId || item.Data?.TenantId != tenantId))
                    {
                        return null;
                    }
                    return item;
                }
            }
        }

        RecordCharge(charge);
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

        var response = await SnapshotContainer.UpsertItemAsync(snapshotDoc, CosmosPartitionKeyHelper.CreatePartitionKey(streamId), cancellationToken: ct);
        RecordCharge(response.RequestCharge);
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
            var resp = await SnapshotContainer.ReadItemAsync<CosmosDocumentEnvelope<TAggregate>>(
                targetId,
                CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
                cancellationToken: ct);

            RecordCharge(resp.RequestCharge);

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
            RecordCharge(ex.RequestCharge);
            return (null, 0);
        }
        catch (CosmosException ex)
        {
            RecordCharge(ex.RequestCharge);
            return await QuerySnapshotAsync<TAggregate>(streamId, targetId, tenantId, ct);
        }
    }

    private async Task<(TAggregate? Snapshot, long SnapshotVersion)> QuerySnapshotAsync<TAggregate>(string streamId, string targetId, string tenantId, CancellationToken ct) where TAggregate : class
    {
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", targetId);

        using var iterator = SnapshotContainer.GetItemQueryIterator<CosmosDocumentEnvelope<TAggregate>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });

        if (iterator == null) return (null, 0);

        double charge = 0.0;
        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            charge += response.RequestCharge;
            foreach (var item in response)
            {
                if (item != null && !item.IsDeleted)
                {
                    RecordCharge(charge);
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

        RecordCharge(charge);
        return (null, 0);
    }
}
