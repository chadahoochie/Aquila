using System.Collections.ObjectModel;
using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Aquila.Core.Events;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosStorageProvider : IAquilaStorageProvider, IDocumentStorageProvider, IEventStorageProvider
{
    private readonly CosmosClient _client;
    private Container _container = null!;
    private readonly string _databaseName;
    private readonly string _containerName;
    private readonly bool _ownsClient;

    private readonly CosmosDocumentStorageProvider _documents;
    private readonly CosmosEventStorageProvider _events;

    public string ProviderName => "AzureCosmosDB";
    public IDocumentStorageProvider Documents => _documents;
    public IEventStorageProvider Events => _events;

    public CosmosStorageProvider(string connectionString, string databaseName = "AquilaDB", string containerName = "Documents", ICosmosEventTypeResolver? eventTypeResolver = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        _databaseName = databaseName;
        _containerName = containerName;
        _client = new CosmosClient(connectionString, new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Direct,
            Serializer = new AquilaCosmosJsonSerializer()
        });
        _ownsClient = true;

        _documents = new CosmosDocumentStorageProvider(() => Container);
        _events = new CosmosEventStorageProvider(() => Container, eventTypeResolver);
    }

    public CosmosStorageProvider(CosmosClient client, string databaseName = "AquilaDB", string containerName = "Documents", ICosmosEventTypeResolver? eventTypeResolver = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        _client = client;
        _databaseName = databaseName;
        _containerName = containerName;
        _ownsClient = false;

        _documents = new CosmosDocumentStorageProvider(() => Container);
        _events = new CosmosEventStorageProvider(() => Container, eventTypeResolver);
    }

    private Container Container => _container ??= _client.GetContainer(_databaseName, _containerName);

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await InitializeAsync((ContainerProperties?)null, ct).ConfigureAwait(false);
    }

    public async Task InitializeAsync(ContainerProperties? customProperties, CancellationToken ct = default)
    {
        var db = await _client.CreateDatabaseIfNotExistsAsync(_databaseName, cancellationToken: ct).ConfigureAwait(false);

        var properties = customProperties ?? CreateDefaultContainerProperties(_containerName);
        var containerResp = await db.Database.CreateContainerIfNotExistsAsync(properties, cancellationToken: ct).ConfigureAwait(false);
        _container = containerResp.Container;

        await _events.InitializeSequenceAsync(ct).ConfigureAwait(false);
    }

    public static ContainerProperties CreateDefaultContainerProperties(string containerName, string partitionKeyPath = "/pk")
    {
        var props = new ContainerProperties(containerName, partitionKeyPath);

        props.IndexingPolicy.CompositeIndexes.Add(new Collection<CompositePath>
        {
            new CompositePath { Path = "/_docType", Order = CompositePathSortOrder.Ascending },
            new CompositePath { Path = "/_tenantId", Order = CompositePathSortOrder.Ascending }
        });

        props.IndexingPolicy.CompositeIndexes.Add(new Collection<CompositePath>
        {
            new CompositePath { Path = "/_docType", Order = CompositePathSortOrder.Ascending },
            new CompositePath { Path = "/data/GlobalSequence", Order = CompositePathSortOrder.Ascending }
        });

        return props;
    }

    // --- IDocumentStorageProvider forwarding ---

    public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _documents.ReadDocumentAsync<T>(id, partitionKey, ct);

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _documents.QueryDocumentsAsync(predicate, options, ct);

    public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
        _documents.UpsertDocumentAsync(envelope, ct);

    public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _documents.DeleteDocumentAsync<T>(id, partitionKey, ct);

    public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) =>
        _documents.ExecuteBatchAsync(operations, ct);

    // --- IEventStorageProvider forwarding ---

    public Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default) =>
        _events.AppendEventsAsync(streamId, events, expectedVersion, ct);

    public Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default) =>
        _events.FetchEventsAsync(streamId, tenantId, fromVersion, ct);

    public Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default) =>
        _events.FetchGlobalEventsAsync(fromGlobalSequence, batchSize, tenantId, ct);

    public Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default) =>
        _events.GetStreamHeaderAsync(streamId, tenantId, ct);

    public Task SaveSnapshotAsync<TAggregate>(string streamId, long version, TAggregate snapshot, string tenantId = "default", CancellationToken ct = default) where TAggregate : class =>
        _events.SaveSnapshotAsync(streamId, version, snapshot, tenantId, ct);

    public Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class =>
        _events.GetSnapshotAsync<TAggregate>(streamId, tenantId, ct);

    public void Dispose()
    {
        if (_ownsClient)
        {
            _client?.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_ownsClient)
        {
            _client?.Dispose();
        }
        return ValueTask.CompletedTask;
    }
}
