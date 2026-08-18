# Implementation Plan: Tripartite Storage Architecture & Redis Projection Store for Aquila

## Goal Description
Evolve Aquila from a dual-store SPI (`DocumentStorage` + `EventStorage`) into a **tripartite polyglot storage architecture** featuring three first-class, independent SPI contracts:
1. **`IEventStorageProvider`**: Append-only event streams, aggregate rehydration, global sequences, snapshots (e.g. Azure Cosmos DB, EventStoreDB, PostgreSQL).
2. **`IDocumentStorageProvider`**: Primary domain documents, dirty tracking, units of work, optimistic concurrency, and complex LINQ querying (e.g. Azure Cosmos DB, PostgreSQL).
3. **`IProjectionStorageProvider`**: Materialized read models, point views, high-throughput batch updates, native instantaneous zero-RU rebuilds, and ultra-low latency reads (e.g. **Redis**, dedicated Cosmos DB read containers).

In addition, implement the dedicated **`Aquila.Redis`** package (`net10.0`) providing `RedisProjectionStorageProvider`, `RedisDocumentStorageProvider`, and `RedisProjectionCheckpointStore`.

```mermaid
flowchart TD
    subgraph Application ["Application Layer"]
        Session["DocumentSession / QuerySession"]
        Daemon["Projection Daemon (Change Feed / Polling)"]
    end

    subgraph StoreOptions ["Aquila.Core Tripartite SPI (Precomputed Type Registry)"]
        EventSPI["IEventStorageProvider (Cosmos DB)"]
        DocSPI["IDocumentStorageProvider (Cosmos DB)"]
        ProjSPI["IProjectionStorageProvider (Redis)"]
        CheckSPI["IProjectionCheckpointStore (Redis / Cosmos / Memory)"]
    end

    subgraph PhysicalBackends ["Physical Storage Engines"]
        CosmosEvents[("Cosmos DB: Events & Snapshots")]
        CosmosDocs[("Cosmos DB: Primary Documents")]
        RedisProjections[("Redis: Materialized Read Models")]
        RedisCheckpoints[("Redis / Cosmos: Checkpoints")]
    end

    Session -- "Events.Append()" --> EventSPI --> CosmosEvents
    Session -- "LoadAsync<Customer>()" --> DocSPI --> CosmosDocs
    Session -- "LoadAsync<CustomerSummary>()" --> ProjSPI --> RedisProjections
    Daemon -- "FetchGlobalEventsAsync()" --> EventSPI --> CosmosEvents
    Daemon -- "MultiStream Apply (Enrichment from Docs)" --> DocSPI --> CosmosDocs
    Daemon -- "Upsert Read Model" --> ProjSPI --> RedisProjections
    Daemon -- "Get/Save Checkpoint" --> CheckSPI --> RedisCheckpoints
    Daemon -- "PurgeProjectionAsync() (Rebuild)" --> ProjSPI -- "Fast Non-blocking UNLINK" --> RedisProjections
```

---

## Architectural Decisions & Guardrails

> [!IMPORTANT]
> **1. Fail-Fast Validation on Polyglot Inline Projections**:
> When `ProjectionStorage` and `EventStorage` reside on distinct physical backends (e.g., Cosmos DB + Redis), `ProjectionLifecycle.Inline` is **strictly forbidden**. Attempting to configure an inline projection in a polyglot setup throws an `InvalidOperationException` during `StoreOptions.Freeze()` with clear remediation instructions. Polyglot projections must use `ProjectionLifecycle.Async` or `ProjectionLifecycle.Live` to prevent distributed dual writes without two-phase commit (2PC).

> [!TIP]
> **2. Full LINQ-Capable Projection SPI**:
> `IProjectionStorageProvider` extends `IDocumentStorageProvider` with native projection management (`PurgeProjectionAsync`), supporting point reads (`LoadAsync<T>`), paged queries (`QueryPagedAsync<T>`), and reactive streaming (`StreamAsync<T>`).

> [!NOTE]
> **3. Unified Session API with $O(1)$ Zero-Allocation Routing**:
> Developers interact exclusively with standard session APIs (`session.LoadAsync<T>()`, `session.QueryAsync<T>()`, `session.Store<T>()`). On store freeze, an immutable `FrozenSet<Type>` / type registry maps projection read models to `ProjectionStorage` and domain documents to `DocumentStorage`.

> [!NOTE]
> **4. Independent Checkpoint Store**:
> `IProjectionCheckpointStore` remains an independent pluggable SPI that can be placed in Redis (`RedisProjectionCheckpointStore`), Cosmos DB (`DocumentStorageProjectionCheckpointStore`), or In-Memory independently of where read models land.

> [!TIP]
> **5. Cross-Store Enrichment in Projections**:
> Multi-stream projections have access to query primary documents (`IDocumentStorageProvider`) via the session during event processing while persisting projected views to `IProjectionStorageProvider`.

> [!IMPORTANT]
> **6. Shared Singleton Lifetime for `IConnectionMultiplexer`**:
> Per Redis standards, `IConnectionMultiplexer` is designed to be a long-lived, thread-safe Singleton. Creating multiple connections across extensions causes socket exhaustion and thread pool starvation. Extensions accept existing `IConnectionMultiplexer` instances or register a shared singleton in `IServiceCollection`.

> [!TIP]
> **7. Zero-Allocation UTF-8 Byte Serialization**:
> `Aquila.Redis` avoids `Newtonsoft.Json` reflection and intermediate string allocations. Payloads serialize directly to/from UTF-8 byte buffers (`ReadOnlyMemory<byte>` and `byte[]`) over `RedisValue`, maximizing throughput and Native AOT compatibility.

> [!TIP]
> **8. Non-Blocking Key Purge via Streaming `SCAN` and Server `UNLINK`**:
> Instantaneous projection rebuilds must never block Redis's single-threaded event loop. `PurgeProjectionAsync` streams keys via `server.KeysAsync(...)` without buffering the entire keyspace in RAM, and dispatches chunked `KeyUnlinkAsync` (`UNLINK`) for asynchronous memory deallocation on the Redis engine.

> [!TIP]
> **9. Pipelined Batch Operations (`IBatch`)**:
> `RedisDocumentStorageProvider.ExecuteBatchAsync` queues commands via `IDatabase.CreateBatch()` before awaiting tasks, dispatching multi-operation writes in a single TCP frame to reduce network latency from $O(N \times \text{RTT})$ to $1 \times \text{RTT}$.

> [!NOTE]
> **10. Keyspace Sharding with Cluster Hash Tags**:
> Keys follow the format `"{tenantId:partitionKey}:docType:id"`. The `{tenantId:partitionKey}` hash tag ensures co-located partition keys land on the same Redis cluster slot (preventing `CROSSSLOT` errors during multi-key operations) while evenly distributing different partitions across cluster nodes.

> [!NOTE]
> **11. Monotonic Lock-Free Checkpoint Progression**:
> `RedisProjectionCheckpointStore.SaveCheckpointAsync` executes an atomic Redis Lua script ensuring checkpoint sequences advance monotonically (`seq > current`), preventing sequence regression during daemon failover without external distributed locks.

> [!TIP]
> **12. Zero-Reflection Rebuilds in Projection Daemons**:
> Projection Daemons (`ProjectionDaemon` and `CosmosProjectionDaemon`) invoke `IProjectionStorageProvider.PurgeProjectionAsync(...)` directly during rebuilds, eliminating runtime reflection and $O(N)$ document deletion scans.

---

## Proposed Changes

```
Aquila/
├── src/
│   ├── Aquila.Core/
│   │   ├── Storage/
│   │   │   ├── StorageContracts.cs                  # [MODIFY] Add IProjectionStorageProvider interface with PurgeProjectionAsync
│   │   │   └── InMemoryStorageProvider.cs           # [MODIFY] Implement IProjectionStorageProvider on InMemory provider
│   │   ├── Configuration/
│   │   │   ├── StoreOptions.cs                      # [MODIFY] Add ProjectionStorage property, polyglot validation in Freeze()
│   │   │   ├── StoreMetadata.cs                     # [MODIFY] Expose ProjectionStorageProvider in metadata
│   │   │   └── IStoreMetadata.cs                    # [MODIFY] Expose ProjectionStorageProvider property
│   │   ├── Sessions/
│   │   │   ├── QuerySession.cs                      # [MODIFY] O(1) type routing between DocumentStorage & ProjectionStorage
│   │   │   └── DocumentSession.cs                   # [MODIFY] Split pending operations by target provider in SaveChangesAsync
│   │   └── Projections/
│   │       ├── SingleStreamProjection.cs            # [MODIFY] Route read-model writes to ProjectionStorage
│   │       ├── MultiStreamProjection.cs             # [MODIFY] Route read-model writes/deletes to ProjectionStorage
│   │       └── Daemon/
│   │           └── ProjectionDaemon.cs              # [MODIFY] Use PurgeProjectionAsync directly (zero reflection)
│   ├── Aquila.Cosmos/
│   │   ├── Storage/
│   │   │   ├── CosmosStorageProvider.cs             # [MODIFY] Implement IProjectionStorageProvider
│   │   │   └── CosmosProjectionStorageProvider.cs   # [NEW] Dedicated Cosmos IProjectionStorageProvider implementation
│   │   ├── Configuration/
│   │   │   └── CosmosStorageOptions.cs              # [MODIFY] Separate Document, Event, and Projection configuration
│   │   ├── Extensions/
│   │   │   └── ServiceCollectionExtensions.cs       # [MODIFY] Add UseCosmosProjections, UseCosmosEvents, UseCosmosDocuments
│   │   └── Projections/
│   │       └── CosmosProjectionDaemon.cs            # [MODIFY] Invoke PurgeProjectionAsync directly on rebuild
│   ├── Aquila.Redis/                                # [NEW PROJECT] Dedicated Redis provider for Aquila
│   │   ├── Aquila.Redis.csproj                      # [NEW] net10.0 project with StackExchange.Redis, System.Text.Json
│   │   ├── Configuration/
│   │   │   └── RedisStorageOptions.cs               # [NEW] KeyPrefix, Database, BatchSize, SerializerOptions, KeyFormatters
│   │   ├── Storage/
│   │   │   ├── RedisDocumentStorageProvider.cs      # [NEW] IDocumentStorageProvider with CreateBatch pipelining & UTF-8 serialization
│   │   │   ├── RedisProjectionStorageProvider.cs    # [NEW] IProjectionStorageProvider with non-blocking UNLINK purge
│   │   │   └── RedisProjectionCheckpointStore.cs    # [NEW] IProjectionCheckpointStore with Lua monotonic progression
│   │   └── Extensions/
│   │       └── RedisServiceCollectionExtensions.cs  # [NEW] UseRedisProjections, UseRedisDocuments, AddRedisCheckpointStore
│   └── Aquila.Samples/                              # [MODIFY] Add polyglot Cosmos + Redis sample demo
├── tests/
│   ├── Aquila.Core.Tests/
│   │   ├── Storage/
│   │   │   └── TripartiteStorageRoutingTests.cs     # [NEW] Tests for DocumentStorage vs ProjectionStorage routing & validation
│   │   └── Projections/
│   │       └── PolyglotProjectionValidationTests.cs # [NEW] Test fail-fast validation on inline polyglot projections
│   ├── Aquila.Cosmos.Tests/
│   │   └── Projections/
│   │       └── CosmosProjectionStorageTests.cs      # [NEW] Tests for Cosmos projection purge and segregation
│   └── Aquila.Redis.Tests/                          # [NEW PROJECT] Tests for Redis provider
│       ├── Aquila.Redis.Tests.csproj                # [NEW] Test project with xUnit, Shouldly, Testcontainers.Redis
│       ├── Storage/
│       │   ├── RedisDocumentStorageProviderTests.cs # [NEW] Point reads, writes, deletes, batching, queries
│       │   ├── RedisProjectionStorageProviderTests.cs# [NEW] Projection storage and purge tests
│       │   └── RedisProjectionCheckpointStoreTests.cs# [NEW] Checkpoint persistence and monotonic safety tests
│       └── Integration/
│           └── CosmosRedisPolyglotIntegrationTests.cs# [NEW] End-to-end: Cosmos events + Redis projections + Daemon + Rebuild
└── Aquila.slnx                                      # [MODIFY] Register Aquila.Redis and Aquila.Redis.Tests projects
```

---

### Component 1: `Aquila.Core` Tripartite SPI Contracts

#### `[MODIFY]` `src/Aquila.Core/Storage/StorageContracts.cs`
Add `IProjectionStorageProvider`:
```csharp
/// <summary>
/// Provider interface for underlying projection read-model persistence.
/// Extends <see cref="IDocumentStorageProvider"/> with projection-lifecycle operations.
/// </summary>
public interface IProjectionStorageProvider : IDocumentStorageProvider
{
    /// <summary>
    /// Purges all materialized read-model documents for the specified projection, enabling instantaneous rebuilds.
    /// </summary>
    Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default);
}
```

#### `[MODIFY]` `src/Aquila.Core/Configuration/StoreOptions.cs`
```csharp
public sealed class StoreOptions
{
    private IDocumentStorageProvider _documentStorage = new InMemoryStorageProvider();
    private IEventStorageProvider _eventStorage = new InMemoryStorageProvider();
    private IProjectionStorageProvider _projectionStorage = new InMemoryStorageProvider();
    private FrozenSet<Type>? _projectionReadModelTypes;

    public IDocumentStorageProvider DocumentStorage
    {
        get => _documentStorage;
        set { AssertNotFrozen(); _documentStorage = value ?? throw new ArgumentNullException(nameof(value)); }
    }

    public IEventStorageProvider EventStorage
    {
        get => _eventStorage;
        set { AssertNotFrozen(); _eventStorage = value ?? throw new ArgumentNullException(nameof(value)); }
    }

    public IProjectionStorageProvider ProjectionStorage
    {
        get => _projectionStorage;
        set { AssertNotFrozen(); _projectionStorage = value ?? throw new ArgumentNullException(nameof(value)); }
    }

    public bool IsProjectionReadModel(Type type) =>
        _projectionReadModelTypes != null && _projectionReadModelTypes.Contains(type);

    public void Freeze()
    {
        if (_isFrozen) return;

        // 1. Precompute immutable projection type registry for O(1) routing
        var types = new HashSet<Type>();
        foreach (var proj in Projections.Projections)
        {
            if (proj is IMultiStreamProjection multi)
            {
                types.Add(multi.ReadModelType);
            }
            types.Add(proj.AggregateType);
        }
        _projectionReadModelTypes = types.ToFrozenSet();

        // 2. Fail-Fast Polyglot Inline Validation
        bool isPolyglot = !ReferenceEquals(ProjectionStorage, EventStorage);
        if (isPolyglot)
        {
            foreach (var proj in Projections.Projections)
            {
                if (proj.Lifecycle == ProjectionLifecycle.Inline)
                {
                    throw new InvalidOperationException(
                        $"Projection '{proj.Name}' is registered with ProjectionLifecycle.Inline, but ProjectionStorage ({ProjectionStorage.ProviderName}) " +
                        $"and EventStorage ({EventStorage.ProviderName}) are different physical providers. " +
                        "Polyglot projections must use ProjectionLifecycle.Async or ProjectionLifecycle.Live to prevent distributed partial-failure dual writes without 2PC.");
                }
            }
        }

        _isFrozen = true;
    }
}
```

#### `[MODIFY]` `src/Aquila.Core/Sessions/QuerySession.cs` & `DocumentSession.cs`
Update `QuerySessionBase` and `DocumentSession` to resolve target document storage provider via precomputed type check:
```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
protected IDocumentStorageProvider GetStorageForType<T>() =>
    Options.IsProjectionReadModel(typeof(T)) ? Options.ProjectionStorage : Options.DocumentStorage;

[MethodImpl(MethodImplOptions.AggressiveInlining)]
protected IDocumentStorageProvider GetStorageForType(Type type) =>
    Options.IsProjectionReadModel(type) ? Options.ProjectionStorage : Options.DocumentStorage;
```
In `SaveChangesAsync()`:
- Pending operations are partitioned into document mutations and projection mutations, executing batch writes in parallel on `DocumentStorage` and `ProjectionStorage`.

#### `[MODIFY]` `src/Aquila.Core/Projections/Daemon/ProjectionDaemon.cs` & `CosmosProjectionDaemon.cs`
Replace reflection-based deletion in `ClearProjectionDocumentsAsync` with direct SPI invocation:
```csharp
private async Task ClearProjectionDocumentsAsync(IProjection proj, CancellationToken ct)
{
    var docType = proj is IMultiStreamProjection multiProj ? multiProj.ReadModelType : proj.AggregateType;
    await _documentStore.Options.ProjectionStorage.PurgeProjectionAsync(proj.Name, docType, ct).ConfigureAwait(false);
}
```

---

### Component 2: `Aquila.Redis` Package (`net10.0`)

#### `[NEW]` `src/Aquila.Redis/Aquila.Redis.csproj`
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <Title>Aquila.Redis</Title>
    <Description>High-performance Redis projection storage provider, document store, and checkpoint persistence for the Aquila Document Store & Event Sourcing Framework.</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\Aquila.Core\Aquila.Core.csproj" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="StackExchange.Redis" Version="2.8.24" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="10.0.10" />
    <PackageReference Include="Microsoft.Extensions.Options" Version="10.0.10" />
  </ItemGroup>

  <ItemGroup>
    <InternalsVisibleTo Include="Aquila.Redis.Tests" />
    <InternalsVisibleTo Include="DynamicProxyGenAssembly2" />
  </ItemGroup>
</Project>
```

#### `[NEW]` `src/Aquila.Redis/Configuration/RedisStorageOptions.cs`
```csharp
namespace Aquila.Redis.Configuration;

public sealed class RedisStorageOptions
{
    public string KeyPrefix { get; set; } = "aquila:";
    public int Database { get; set; } = 0;
    public int BatchChunkSize { get; set; } = 500;
    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Builds a cluster-shard-aware key with hash tag: "{tenant:partitionKey}:docType:id".
    /// </summary>
    public string BuildKey(string tenantId, string docType, string partitionKey, string id)
    {
        var safeTenant = string.IsNullOrWhiteSpace(tenantId) ? "default" : tenantId;
        var safePk = string.IsNullOrWhiteSpace(partitionKey) ? id : partitionKey;
        return $"{KeyPrefix}{{{safeTenant}:{safePk}}}:{docType}:{id}";
    }

    /// <summary>
    /// Builds a search pattern for a document type across all partitions.
    /// </summary>
    public string BuildTypePattern(string docType) => $"{KeyPrefix}*:{docType}:*";
}
```

#### `[NEW]` `src/Aquila.Redis/Storage/RedisDocumentStorageProvider.cs`
```csharp
namespace Aquila.Redis.Storage;

public sealed class RedisDocumentStorageProvider : IDocumentStorageProvider
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly RedisStorageOptions _options;

    public string ProviderName => "Redis";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    public RedisDocumentStorageProvider(IConnectionMultiplexer multiplexer, RedisStorageOptions? options = null)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _options = options ?? new RedisStorageOptions();
    }

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

    public async Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        var db = _multiplexer.GetDatabase(_options.Database);
        var key = _options.BuildKey("default", typeof(T).Name, partitionKey, id);
        
        byte[]? bytes = await db.StringGetAsync(key).ConfigureAwait(false);
        if (bytes == null || bytes.Length == 0) return null;

        return JsonSerializer.Deserialize<DocumentEnvelope<T>>(bytes, _options.SerializerOptions);
    }

    public async Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var db = _multiplexer.GetDatabase(_options.Database);
        var key = _options.BuildKey(envelope.TenantId, envelope.DocType, envelope.PartitionKey, envelope.Id);
        
        var bytes = JsonSerializer.SerializeToUtf8Bytes(envelope, _options.SerializerOptions);
        await db.StringSetAsync(key, bytes).ConfigureAwait(false);
    }

    public async Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        var db = _multiplexer.GetDatabase(_options.Database);
        var key = _options.BuildKey("default", typeof(T).Name, partitionKey, id);
        await db.KeyUnlinkAsync(key).ConfigureAwait(false);
    }

    public async Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var db = _multiplexer.GetDatabase(_options.Database);
        var batch = db.CreateBatch();
        var tasks = new List<Task>();

        foreach (var op in operations)
        {
            var key = _options.BuildKey("default", op.DocType, op.PartitionKey, op.Id);
            if (op.OperationType == StorageOperationType.Upsert)
            {
                var bytes = JsonSerializer.SerializeToUtf8Bytes(op.Document, _options.SerializerOptions);
                tasks.Add(batch.StringSetAsync(key, bytes));
            }
            else if (op.OperationType == StorageOperationType.Delete)
            {
                tasks.Add(batch.KeyUnlinkAsync(key));
            }
        }

        batch.Execute();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class
    {
        var result = await QueryPagedDocumentsAsync(predicate, options, ct).ConfigureAwait(false);
        return result.Documents;
    }

    public async Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class
    {
        var db = _multiplexer.GetDatabase(_options.Database);
        var endpoints = _multiplexer.GetEndPoints();
        var pattern = _options.BuildTypePattern(typeof(T).Name);
        var compiled = predicate?.Compile();

        var documents = new List<DocumentEnvelope<T>>();
        int maxItems = options?.MaxItemCount ?? 100;

        foreach (var endpoint in endpoints)
        {
            var server = _multiplexer.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica) continue;

            await foreach (var key in server.KeysAsync(_options.Database, pattern).WithCancellation(ct).ConfigureAwait(false))
            {
                byte[]? bytes = await db.StringGetAsync(key).ConfigureAwait(false);
                if (bytes == null || bytes.Length == 0) continue;

                var env = JsonSerializer.Deserialize<DocumentEnvelope<T>>(bytes, _options.SerializerOptions);
                if (env != null && (compiled == null || compiled(env)))
                {
                    documents.Add(env);
                    if (documents.Count >= maxItems) break;
                }
            }
            if (documents.Count >= maxItems) break;
        }

        return new StorageQueryResult<T>(documents, continuationToken: null, totalCount: documents.Count);
    }

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
```

#### `[NEW]` `src/Aquila.Redis/Storage/RedisProjectionStorageProvider.cs`
```csharp
namespace Aquila.Redis.Storage;

public sealed class RedisProjectionStorageProvider : IProjectionStorageProvider
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly RedisStorageOptions _options;
    private readonly RedisDocumentStorageProvider _innerDocumentProvider;

    public string ProviderName => "Redis";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    public RedisProjectionStorageProvider(IConnectionMultiplexer multiplexer, RedisStorageOptions? options = null)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _options = options ?? new RedisStorageOptions();
        _innerDocumentProvider = new RedisDocumentStorageProvider(multiplexer, _options);
    }

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.ReadDocumentAsync<T>(id, partitionKey, ct);

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.QueryDocumentsAsync(predicate, options, ct);

    public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.QueryPagedDocumentsAsync(predicate, options, ct);

    public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.UpsertDocumentAsync(envelope, ct);

    public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.DeleteDocumentAsync<T>(id, partitionKey, ct);

    public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) =>
        _innerDocumentProvider.ExecuteBatchAsync(operations, ct);

    /// <summary>
    /// Purges all projection read models asynchronously using non-blocking streaming UNLINK batches.
    /// </summary>
    public async Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        ArgumentNullException.ThrowIfNull(readModelType);

        var db = _multiplexer.GetDatabase(_options.Database);
        var endpoints = _multiplexer.GetEndPoints();
        var pattern = _options.BuildTypePattern(readModelType.Name);

        foreach (var endpoint in endpoints)
        {
            var server = _multiplexer.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica) continue;

            var batch = new List<RedisKey>(_options.BatchChunkSize);
            await foreach (var key in server.KeysAsync(_options.Database, pattern).WithCancellation(ct).ConfigureAwait(false))
            {
                batch.Add(key);
                if (batch.Count >= _options.BatchChunkSize)
                {
                    await db.KeyUnlinkAsync(batch.ToArray()).ConfigureAwait(false);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await db.KeyUnlinkAsync(batch.ToArray()).ConfigureAwait(false);
            }
        }
    }

    public void Dispose() => _innerDocumentProvider.Dispose();
    public ValueTask DisposeAsync() => _innerDocumentProvider.DisposeAsync();
}
```

#### `[NEW]` `src/Aquila.Redis/Storage/RedisProjectionCheckpointStore.cs`
```csharp
namespace Aquila.Redis.Storage;

public sealed class RedisProjectionCheckpointStore : IProjectionCheckpointStore
{
    private static readonly LuaScript MonotonicSaveScript = LuaScript.Prepare(@"
        local cur = redis.call('GET', @key)
        if not cur or tonumber(@seq) > tonumber(cur) then
            redis.call('SET', @key, @seq)
            return 1
        end
        return 0
    ");

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly string _keyPrefix;
    private readonly int _database;

    public RedisProjectionCheckpointStore(IConnectionMultiplexer multiplexer, string keyPrefix = "aquila:checkpoints:", int database = 0)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _keyPrefix = keyPrefix;
        _database = database;
    }

    public async Task<long> GetCheckpointAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var db = _multiplexer.GetDatabase(_database);
        var value = await db.StringGetAsync($"{_keyPrefix}{projectionName}").ConfigureAwait(false);
        return value.HasValue && long.TryParse(value, out var seq) ? seq : 0L;
    }

    public async Task SaveCheckpointAsync(string projectionName, long sequence, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var db = _multiplexer.GetDatabase(_database);
        var key = (RedisKey)$"{_keyPrefix}{projectionName}";

        await db.ScriptEvaluateAsync(MonotonicSaveScript, new { key = key, seq = sequence }).ConfigureAwait(false);
    }
}
```

#### `[NEW]` `src/Aquila.Redis/Extensions/RedisServiceCollectionExtensions.cs`
```csharp
namespace Aquila.Redis.Extensions;

public static class RedisServiceCollectionExtensions
{
    public static StoreOptions UseRedisProjections(this StoreOptions options, IConnectionMultiplexer multiplexer, Action<RedisStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(multiplexer);

        var redisOptions = new RedisStorageOptions();
        configure?.Invoke(redisOptions);
        options.ProjectionStorage = new RedisProjectionStorageProvider(multiplexer, redisOptions);
        return options;
    }

    public static StoreOptions UseRedisDocuments(this StoreOptions options, IConnectionMultiplexer multiplexer, Action<RedisStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(multiplexer);

        var redisOptions = new RedisStorageOptions();
        configure?.Invoke(redisOptions);
        options.DocumentStorage = new RedisDocumentStorageProvider(multiplexer, redisOptions);
        return options;
    }

    public static IServiceCollection AddRedisCheckpointStore(this IServiceCollection services, IConnectionMultiplexer multiplexer, string keyPrefix = "aquila:checkpoints:", int database = 0)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(multiplexer);

        services.AddSingleton<IProjectionCheckpointStore>(new RedisProjectionCheckpointStore(multiplexer, keyPrefix, database));
        return services;
    }

    public static IServiceCollection AddAquilaRedis(this IServiceCollection services, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(connectionString));
        return services;
    }
}
```

---

## Verification Plan

### Automated Tests
1. **Aquila.Core Tests**:
   - `TripartiteStorageRoutingTests`:
     - Verify `LoadAsync<T>` routes to `ProjectionStorage` for registered read models, and `DocumentStorage` for domain documents.
     - Verify `QueryPagedAsync<T>` and `StreamAsync<T>` route correctly with precomputed $O(1)$ type lookup.
     - Verify `SaveChangesAsync` splits mutations into document batch and projection batch.
   - `PolyglotProjectionValidationTests`:
     - Verify `options.Freeze()` throws `InvalidOperationException` if `ProjectionLifecycle.Inline` is used when `ProjectionStorage != EventStorage`.
     - Verify `options.Freeze()` succeeds when all projections are `ProjectionLifecycle.Async` or `ProjectionLifecycle.Live`.
2. **Aquila.Redis Tests**:
   - `RedisDocumentStorageProviderTests`: CRUD, point read `< 1ms`, batch execution with pipelining (`CreateBatch`), in-memory predicate filtering, continuation token paging.
   - `RedisProjectionStorageProviderTests`: `PurgeProjectionAsync` wipes keys matching `{KeyPrefix}*:{ReadModel}:*` via non-blocking `UNLINK` streaming.
   - `RedisProjectionCheckpointStoreTests`: `GetCheckpointAsync`, `SaveCheckpointAsync`, monotonic progression script, restart and recovery.
3. **End-to-End Polyglot Integration Tests** (`Aquila.Redis.Tests/Integration`):
   - Real Cosmos DB (or Cosmos test container) for Event Store.
   - Real Redis (via `Testcontainers.Redis`) for Projection Store and Checkpoint Store.
   - Append 100 events -> run `CosmosProjectionDaemon` -> assert projections materialized in Redis.
   - Test cross-store enrichment: Multi-stream projection queries `Customer` document from Cosmos DB while mutating `CustomerSummary` in Redis.
   - Test zero-downtime rebuild: `RebuildProjectionAsync<T>()` invokes `PurgeProjectionAsync`, resets Redis checkpoint, replays full event log, and asserts consistent final state in Redis.

Command to run all test suites:
```bash
dotnet test Aquila.slnx
```

### Manual Verification
- Verify sample application running with Cosmos events and Redis projection read models.
