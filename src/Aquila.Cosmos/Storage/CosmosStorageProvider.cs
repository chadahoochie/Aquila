using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
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

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var db = await _client.CreateDatabaseIfNotExistsAsync(_databaseName, cancellationToken: ct);
        var containerResp = await db.Database.CreateContainerIfNotExistsAsync(_containerName, "/pk", cancellationToken: ct);
        _container = containerResp.Container;
    }

    // --- DocumentStorageProvider ---

    public async Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        try
        {
            var response = await Container.ReadItemAsync<CosmosDocumentEnvelope<T>>(
                id,
                new PartitionKey(partitionKey),
                cancellationToken: ct);

            if (response.Resource == null || response.Resource.IsDeleted) return null;

            return new DocumentEnvelope<T>
            {
                Id = response.Resource.Id,
                PartitionKey = response.Resource.PartitionKey,
                DocType = response.Resource.DocType,
                TenantId = response.Resource.TenantId,
                IsDeleted = response.Resource.IsDeleted,
                Version = response.Resource.Version,
                ETag = response.Resource.ETag,
                Data = response.Resource.Data
            };
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var docType = typeof(T).Name;
        var query = Container.GetItemLinqQueryable<CosmosDocumentEnvelope<T>>()
            .Where(x => x.DocType == docType && !x.IsDeleted);

        using var iterator = query.ToFeedIterator();
        var results = new List<DocumentEnvelope<T>>();

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                results.Add(new DocumentEnvelope<T>
                {
                    Id = item.Id,
                    PartitionKey = item.PartitionKey,
                    DocType = item.DocType,
                    TenantId = item.TenantId,
                    IsDeleted = item.IsDeleted,
                    Version = item.Version,
                    ETag = item.ETag,
                    Data = item.Data
                });
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

        await Container.DeleteItemAsync<CosmosDocumentEnvelope<T>>(id, new PartitionKey(partitionKey), cancellationToken: ct);
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
        }
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
            ? "SELECT * FROM c WHERE c.pk = @streamId AND c._docType = '$event' AND c.data.version >= @fromVersion ORDER BY c.data.version ASC"
            : "SELECT * FROM c WHERE c.pk = @streamId AND c._docType = '$event' AND c._tenantId = @tenantId AND c.data.version >= @fromVersion ORDER BY c.data.version ASC";

        var queryDef = new QueryDefinition(queryText)
            .WithParameter("@streamId", streamId)
            .WithParameter("@fromVersion", fromVersion);

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
                if (item.Data is IEvent @event)
                {
                    if (string.IsNullOrEmpty(tenantId) || item.TenantId == tenantId || @event.TenantId == tenantId)
                    {
                        events.Add(@event);
                    }
                }
            }
        }

        return events;
    }

    public async Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        try
        {
            var resp = await Container.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
                $"$stream_{streamId}",
                new PartitionKey(streamId),
                cancellationToken: ct);

            if (resp.Resource == null) return null;
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

    public void Dispose() => _client?.Dispose();
    public ValueTask DisposeAsync()
    {
        _client?.Dispose();
        return ValueTask.CompletedTask;
    }
}
