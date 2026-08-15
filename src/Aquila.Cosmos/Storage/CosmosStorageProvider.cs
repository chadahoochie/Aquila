using System.Collections.ObjectModel;
using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Storage;
using Aquila.Cosmos.Configuration;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosStorageProvider : IDocumentStorageProvider, IEventStorageProvider
{
    private readonly CosmosClient _client;
    private readonly CosmosContainerResolver _resolver;
    private readonly bool _ownsClient;

    private readonly CosmosDocumentStorageProvider _documents;
    private readonly CosmosEventStorageProvider _events;

    public string ProviderName => "AzureCosmosDB";
    public CosmosStorageOptions Options => _resolver.Options;
    public CosmosContainerResolver Resolver => _resolver;

    public CosmosStorageProvider(
        string connectionString,
        CosmosStorageOptions options,
        StoreOptions? storeOptions = null,
        ICosmosEventTypeResolver? eventTypeResolver = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(options);

        _client = new CosmosClient(connectionString, CreateDefaultClientOptions());
        _ownsClient = true;
        _resolver = new CosmosContainerResolver(_client, options, storeOptions);

        _documents = new CosmosDocumentStorageProvider(type => _resolver.GetContainerForDocumentType(type));
        _events = new CosmosEventStorageProvider(() => _resolver.GetEventsContainer(), () => _resolver.GetSnapshotsContainer(), eventTypeResolver);
    }

    public CosmosStorageProvider(
        CosmosClient client,
        CosmosStorageOptions options,
        StoreOptions? storeOptions = null,
        ICosmosEventTypeResolver? eventTypeResolver = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);

        _client = client;
        _ownsClient = false;
        _resolver = new CosmosContainerResolver(_client, options, storeOptions);

        _documents = new CosmosDocumentStorageProvider(type => _resolver.GetContainerForDocumentType(type));
        _events = new CosmosEventStorageProvider(() => _resolver.GetEventsContainer(), () => _resolver.GetSnapshotsContainer(), eventTypeResolver);
    }

    public CosmosStorageProvider(string connectionString, string databaseName = "AquilaDB", string containerName = "Documents", ICosmosEventTypeResolver? eventTypeResolver = null)
        : this(connectionString, CreateLegacyOptions(databaseName, containerName), null, eventTypeResolver)
    {
    }

    public CosmosStorageProvider(CosmosClient client, string databaseName = "AquilaDB", string containerName = "Documents", ICosmosEventTypeResolver? eventTypeResolver = null)
        : this(client, CreateLegacyOptions(databaseName, containerName), null, eventTypeResolver)
    {
    }

    public static CosmosClientOptions CreateDefaultClientOptions()
    {
        return new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Direct,
            AllowBulkExecution = true,
            MaxRetryAttemptsOnRateLimitedRequests = 9,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
            Serializer = new AquilaCosmosJsonSerializer()
        };
    }

    private static CosmosStorageOptions CreateLegacyOptions(string databaseName, string containerName)
    {
        var options = new CosmosStorageOptions { DefaultDatabase = databaseName };
        options.EventsLocation(containerName, databaseName);
        options.SnapshotsLocation(containerName, databaseName);
        options.DocumentsLocation(containerName, databaseName);
        return options;
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await InitializeAsync((ContainerProperties?)null, ct).ConfigureAwait(false);
    }

    public async Task InitializeAsync(ContainerProperties? customProperties, CancellationToken ct = default)
    {
        var configuredContainers = _resolver.GetAllConfiguredContainers();
        var uniqueDatabases = configuredContainers.Select(c => c.Database).Distinct().ToList();

        var dbClients = new Dictionary<string, Database>();
        foreach (var dbName in uniqueDatabases)
        {
            var dbResp = await _client.CreateDatabaseIfNotExistsAsync(dbName, cancellationToken: ct).ConfigureAwait(false);
            dbClients[dbName] = dbResp.Database;
        }

        foreach (var (dbName, containerName, isEvents, isSnapshots, throughput) in configuredContainers)
        {
            var db = dbClients[dbName];
            var props = customProperties ?? (isEvents
                ? CreateDefaultEventsContainerProperties(containerName)
                : CreateDefaultContainerProperties(containerName));

            if (throughput != null)
            {
                await db.CreateContainerIfNotExistsAsync(props, throughputProperties: throughput, cancellationToken: ct).ConfigureAwait(false);
            }
            else
            {
                await db.CreateContainerIfNotExistsAsync(props, cancellationToken: ct).ConfigureAwait(false);
            }
        }

        await _events.InitializeSequenceAsync(ct).ConfigureAwait(false);
    }

    public static ContainerProperties CreateDefaultEventsContainerProperties(string containerName, string partitionKeyPath = "/pk")
    {
        var props = new ContainerProperties(containerName, partitionKeyPath);

        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
        props.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/data/*" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/_docType/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/_tenantId/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/data/GlobalSequence/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/pk/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/id/?" });

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

    public static ContainerProperties CreateDefaultContainerProperties(string containerName, string partitionKeyPath = "/pk")
    {
        var props = new ContainerProperties(containerName, partitionKeyPath);

        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
        props.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/data/*" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/_docType/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/_tenantId/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/data/GlobalSequence/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/pk/?" });
        props.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/id/?" });

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
