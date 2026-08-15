using System.Collections.Concurrent;
using Microsoft.Azure.Cosmos;
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Configuration;

namespace Aquila.Cosmos.Storage;

/// <summary>
/// Thread-safe container resolver that maps document types, event streams, snapshots, and projections to target Cosmos DB containers.
/// </summary>
public sealed class CosmosContainerResolver
{
    private readonly CosmosClient _client;
    private readonly CosmosStorageOptions _options;
    private readonly StoreOptions? _storeOptions;
    private readonly ConcurrentDictionary<(string Database, string Container), Container> _containerCache = new();
    private readonly ConcurrentDictionary<Type, (string Database, string Container)> _typeResolutionCache = new();

    public CosmosStorageOptions Options => _options;
    public CosmosClient Client => _client;

    public CosmosContainerResolver(CosmosClient client, CosmosStorageOptions options, StoreOptions? storeOptions = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);

        _client = client;
        _options = options;
        _storeOptions = storeOptions;
    }

    public Container GetContainer(string databaseName, string containerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        return _containerCache.GetOrAdd((databaseName, containerName), key => _client.GetContainer(key.Database, key.Container));
    }

    public Container GetEventsContainer()
    {
        var (db, container) = _options.Events.Resolve(_options.DefaultDatabase);
        return GetContainer(db, container);
    }

    public Container GetSnapshotsContainer()
    {
        var (db, container) = _options.Snapshots.Resolve(_options.DefaultDatabase);
        return GetContainer(db, container);
    }

    public Container GetDocumentsContainer()
    {
        var (db, container) = _options.Documents.Resolve(_options.DefaultDatabase);
        return GetContainer(db, container);
    }

    public Container GetContainerForDocumentType(Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentType);

        var (db, container) = _typeResolutionCache.GetOrAdd(documentType, ResolveCoordinatesForType);
        return GetContainer(db, container);
    }

    private (string Database, string Container) ResolveCoordinatesForType(Type documentType)
    {
        // 1. Check if type is or belongs to a projection
        IProjection? matchingProjection = null;
        if (_storeOptions?.Projections != null)
        {
            matchingProjection = _storeOptions.Projections.Projections.FirstOrDefault(p =>
                p.GetType() == documentType ||
                p.AggregateType == documentType ||
                (p is IMultiStreamProjection multi && multi.ReadModelType == documentType));
        }

        if (matchingProjection != null || typeof(IProjection).IsAssignableFrom(documentType))
        {
            var projType = matchingProjection?.GetType() ?? documentType;

            // Check projection overrides
            if (_options.Projections.Overrides.TryGetValue(projType, out var projOverride) ||
                _options.Projections.Overrides.TryGetValue(documentType, out projOverride))
            {
                var overrideDb = string.IsNullOrWhiteSpace(projOverride.Database)
                    ? (_options.Projections.Database ?? _options.DefaultDatabase)
                    : projOverride.Database;
                return (overrideDb, projOverride.Container);
            }

            switch (_options.Projections.Mode)
            {
                case ProjectionStorageMode.AutoContainerPerProjection:
                {
                    var targetDb = _options.Projections.Database ?? _options.DefaultDatabase;
                    var containerName = _options.Projections.ContainerNameFormatter(projType != typeof(IProjection) ? projType : documentType);
                    return (targetDb, containerName);
                }
                case ProjectionStorageMode.DedicatedContainer:
                {
                    var targetDb = _options.Projections.Database ?? _options.DefaultDatabase;
                    var containerName = _options.Projections.Container ?? "Projections";
                    return (targetDb, containerName);
                }
                case ProjectionStorageMode.InheritDocuments:
                default:
                    return _options.Documents.Resolve(_options.DefaultDatabase);
            }
        }

        // Standard document
        return _options.Documents.Resolve(_options.DefaultDatabase);
    }

    /// <summary>
    /// Collects all unique (Database, Container) pairs currently configured for initialization along with their resolved throughput.
    /// </summary>
    public IReadOnlyList<(string Database, string Container, bool IsEvents, bool IsSnapshots, ThroughputProperties? Throughput)> GetAllConfiguredContainers()
    {
        var result = new Dictionary<(string Database, string Container), (bool IsEvents, bool IsSnapshots, ThroughputProperties? Throughput)>();

        void RegisterContainer(string db, string cont, bool isEvents, bool isSnapshots, ThroughputProperties? throughput)
        {
            var key = (db, cont);
            if (result.TryGetValue(key, out var existing))
            {
                result[key] = (
                    existing.IsEvents || isEvents,
                    existing.IsSnapshots || isSnapshots,
                    existing.Throughput ?? throughput);
            }
            else
            {
                result[key] = (isEvents, isSnapshots, throughput);
            }
        }

        var (evDb, evCont) = _options.Events.Resolve(_options.DefaultDatabase);
        RegisterContainer(evDb, evCont, isEvents: true, isSnapshots: false, _options.Events.Throughput?.ToThroughputProperties());

        var (snapDb, snapCont) = _options.Snapshots.Resolve(_options.DefaultDatabase);
        RegisterContainer(snapDb, snapCont, isEvents: false, isSnapshots: true, _options.Snapshots.Throughput?.ToThroughputProperties());

        var (docDb, docCont) = _options.Documents.Resolve(_options.DefaultDatabase);
        RegisterContainer(docDb, docCont, isEvents: false, isSnapshots: false, _options.Documents.Throughput?.ToThroughputProperties());

        if (_options.Projections.Mode == ProjectionStorageMode.DedicatedContainer && !string.IsNullOrWhiteSpace(_options.Projections.Container))
        {
            var projDb = _options.Projections.Database ?? _options.DefaultDatabase;
            RegisterContainer(projDb, _options.Projections.Container, false, false, _options.Projections.Throughput?.ToThroughputProperties());
        }
        else if (_options.Projections.Mode == ProjectionStorageMode.AutoContainerPerProjection && _storeOptions?.Projections != null)
        {
            var projDb = _options.Projections.Database ?? _options.DefaultDatabase;
            foreach (var proj in _storeOptions.Projections.Projections)
            {
                var containerName = _options.Projections.ContainerNameFormatter(proj.GetType());
                RegisterContainer(projDb, containerName, false, false, _options.Projections.Throughput?.ToThroughputProperties());
            }
        }

        foreach (var (type, loc) in _options.Projections.Overrides)
        {
            var (db, cont) = loc.Resolve(_options.Projections.Database ?? _options.DefaultDatabase);
            RegisterContainer(db, cont, false, false, loc.Throughput?.ToThroughputProperties() ?? _options.Projections.Throughput?.ToThroughputProperties());
        }

        return result.Select(kvp => (kvp.Key.Database, kvp.Key.Container, kvp.Value.IsEvents, kvp.Value.IsSnapshots, kvp.Value.Throughput)).ToList();
    }
}
