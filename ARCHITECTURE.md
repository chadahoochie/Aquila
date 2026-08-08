# Aquila Architecture & Design

This document details the architectural principles, SPI storage design, system flows, performance optimizations, and security controls powering the **Aquila** framework.

---

## 1. Solution Sitemap & Project Breakdown

The Aquila repository (`Aquila.slnx`) is organized into modular assemblies designed for clean separation of concerns and pluggable storage integration.

```
Aquila/
├── src/
│   ├── Aquila.Core/                    # Core SPI abstractions, session engines, event store & projections
│   │   ├── Abstractions/               # IDocumentStore, IDocumentSession, IQuerySession, IEventStore
│   │   ├── Configuration/               # StoreOptions, DocumentMapping<T>, SchemaPolicy, StoreMetadata
│   │   ├── Events/                     # IEvent, EventEnvelope<T>, IEventUpcaster, UpcasterRegistry, ISnapshotStrategy
│   │   ├── Exceptions/                 # AquilaException, AquilaConcurrencyException
│   │   ├── Patching/                   # IPatchExpression<T>, PatchExpression<T>
│   │   ├── Projections/                # IProjection, SingleStreamProjection<T>, MultiStreamProjection<TDoc,TId>
│   │   │   └── Daemon/                 # IProjectionDaemon, ProjectionDaemon, IProjectionCheckpointStore
│   │   ├── Queries/                    # ICompiledQuery<TDoc,TResult>, CompiledQueryCache
│   │   ├── Sessions/                   # DocumentSession, QuerySession, IIdentityMap, TrackingMode, DocumentStore
│   │   └── Storage/                    # StorageContracts (SPI), InMemoryStorageProvider, ISqlExpressionTranslator
│   └── Aquila.Cosmos/                   # Azure Cosmos DB SPI storage provider & Cosmos event store
│       ├── Storage/                    # CosmosStorageProvider, CosmosDocumentEnvelope<T>, CosmosExpressionRewriter
│       ├── Events/                     # CosmosEventStore
│       ├── Projections/                # CosmosProjectionDaemon (Change Feed-aware)
│       └── Extensions/                 # ServiceCollectionExtensions (AddAquila, UseCosmos), CosmosDaemonExtensions
├── tests/
│   ├── Aquila.Core.Tests/     # Unit test suite for Aquila.Core (xUnit v3, NSubstitute, Shouldly)
│   ├── Aquila.Cosmos.Tests/   # Unit test suite for Aquila.Cosmos (xUnit v3, NSubstitute, Shouldly)
│   └── Aquila.Tests/          # Solution-wide integration test suite
└── samples/
    └── Aquila.Samples/        # Demo application showcasing pluggable storage & projections
```

### Project Responsibilities

| Project | Target Framework | Core Responsibilities |
| :--- | :--- | :--- |
| [`Aquila.Core`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core) | `net10.0` | Core abstractions (`IDocumentStore`, `IDocumentSession`, `IQuerySession`, `IEventStore`), session life-cycle management (`TrackingMode`, `IIdentityMap`), Schema/Mapping policies, the Projection engine (`SingleStreamProjection<T>`, `MultiStreamProjection<TDoc,TId>`) and its async `ProjectionDaemon`, the partial-document `Patching` API, `ICompiledQuery` caching, event upcasting/snapshotting, Storage Provider SPI contracts, and the built-in [`InMemoryStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/InMemoryStorageProvider.cs). |
| [`Aquila.Cosmos`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos) | `net10.0` | Azure Cosmos DB implementation of [`IAquilaStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L71), custom Cosmos document envelopes (`CosmosDocumentEnvelope<T>`), predicate rewriting via `CosmosExpressionRewriter` onto the native Cosmos LINQ provider, the Change Feed-aware `CosmosProjectionDaemon`, and ASP.NET Core DI extensions (`AddAquila`, `UseCosmos`, `AddCosmosDaemon`). |
| [`Aquila.Samples`](file:///home/chad/source/dotnet/Aquila/samples/Aquila.Samples) | `net10.0` | Runnable demonstration program illustrating document store configuration, event stream appending, aggregate rehydration, and inline projection handling. |
| [`Aquila.Tests`](file:///home/chad/source/dotnet/Aquila/tests/Aquila.Tests) | `net10.0` | Automated test suite verifying session contracts, storage providers, optimistic concurrency exceptions, projection lifecycles, and security boundary isolation. |

---

## 2. Pluggable Storage SPI Design

Aquila decouples business domain semantics (sessions, units-of-work, aggregates, and projections) from physical storage engines through the Service Provider Interface (SPI) defined in [`Aquila.Core.Storage`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs).

```mermaid
classDiagram
    class IAquilaStorageProvider {
        +string ProviderName
        +IDocumentStorageProvider Documents
        +IEventStorageProvider Events
        +InitializeAsync(CancellationToken) Task
    }
    class IDocumentStorageProvider {
        +ReadDocumentAsync~T~(id, partitionKey) Task~DocumentEnvelope~T~~
        +QueryDocumentsAsync~T~(predicate) Task~IReadOnlyList~DocumentEnvelope~T~~~
        +UpsertDocumentAsync~T~(envelope) Task
        +DeleteDocumentAsync~T~(id, partitionKey) Task
        +ExecuteBatchAsync(operations) Task
    }
    class IEventStorageProvider {
        +AppendEventsAsync(streamId, events, expectedVersion) Task
        +FetchEventsAsync(streamId, tenantId, fromVersion) Task~IReadOnlyList~IEvent~~
        +GetStreamHeaderAsync(streamId, tenantId) Task~EventStreamHeader~
    }
    class CosmosStorageProvider {
        +string ProviderName
        +IDocumentStorageProvider Documents
        +IEventStorageProvider Events
    }
    class InMemoryStorageProvider {
        +string ProviderName
        +IDocumentStorageProvider Documents
        +IEventStorageProvider Events
    }

    IAquilaStorageProvider --> IDocumentStorageProvider : Documents
    IAquilaStorageProvider --> IEventStorageProvider : Events
    CosmosStorageProvider ..|> IAquilaStorageProvider
    CosmosStorageProvider ..|> IDocumentStorageProvider
    CosmosStorageProvider ..|> IEventStorageProvider
    InMemoryStorageProvider ..|> IAquilaStorageProvider
    InMemoryStorageProvider ..|> IDocumentStorageProvider
    InMemoryStorageProvider ..|> IEventStorageProvider
```

### Storage Envelope Format

All storage providers serialize documents using the unified [`DocumentEnvelope<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L13) schema:

```csharp
public sealed class DocumentEnvelope<T>
{
    public string Id { get; set; } = string.Empty;
    public string PartitionKey { get; set; } = string.Empty;
    public string DocType { get; set; } = typeof(T).Name;
    public string TenantId { get; set; } = "default";
    public bool IsDeleted { get; set; }
    public string Version { get; set; } = Guid.NewGuid().ToString();
    public string? ETag { get; set; }
    public T Data { get; set; } = default!;
}
```

---

## 3. Sequence Flowcharts

### Document Session Commit Flow (`SaveChangesAsync`)

When [`DocumentSession.SaveChangesAsync()`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Sessions/DocumentSession.cs#L196) is invoked, Aquila flushes deferred operations, appends pending event streams, executes batch storage operations, and triggers registered inline projections.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Code
    participant Session as DocumentSession
    participant EvtStore as CoreEventStore
    participant Storage as IAquilaStorageProvider
    participant Proj as SingleStreamProjection

    App->>Session: SaveChangesAsync(ct)
    
    alt Pending Deferred Operations
        Session->>Session: Execute deferred SoftDeleteAsync tasks
    end

    alt Uncommitted Events Exist
        Session->>EvtStore: Read UncommittedEvents & StreamExpectedVersions
        Session->>Storage: Events.AppendEventsAsync(streamId, events, expectedVersion)
        Storage-->>Session: Success / Throw AquilaConcurrencyException
    end

    alt Pending Storage Operations Exist
        Session->>Storage: Documents.ExecuteBatchAsync(operations)
        Storage-->>Session: Execute batch upserts/deletes
    end

    alt Inline Projections Registered
        loop For each Inline Projection & Uncommitted Event
            Session->>Session: Load or create aggregate document instance
            Session->>Proj: ApplyEvent(event, aggregate)
            Session->>Storage: Documents.UpsertDocumentAsync(projectedEnvelope)
        end
    end

    Session->>EvtStore: ClearUncommittedEvents()
    Session->>Session: Clear pending operations
    Session-->>App: Task Completed
```

---

### Event Stream Append Flow

Appending events validates expected versions, wraps event payloads into strongly typed [`EventEnvelope<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/IEvent.cs#L31) wrappers, updates stream metadata headers, and persists items atomically.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Code
    participant Session as DocumentSession
    participant EvtStore as CoreEventStore
    participant Provider as CosmosStorageProvider

    App->>Session: Events.Append(streamId, expectedVersion, eventPayloads)
    Session->>EvtStore: Append(streamId, expectedVersion, eventPayloads)
    
    loop For each event payload
        EvtStore->>EvtStore: CreateEnvelope (using compiled expression tree)
        EvtStore->>EvtStore: Add to _uncommittedEvents
    end

    App->>Session: SaveChangesAsync()
    Session->>Provider: AppendEventsAsync(streamId, events, expectedVersion)
    
    Provider->>Provider: ReadStreamHeader(streamId, tenantId)
    
    alt expectedVersion >= 0 AND currentVersion != expectedVersion
        Provider-->>Session: THROW AquilaConcurrencyException
        Session-->>App: Exception Thrown
    else Concurrency Check Passes
        loop For each event
            Provider->>Provider: Upsert item "$event_{streamId}_v{version}"
        end
        Provider->>Provider: Upsert stream header "$stream_{streamId}"
        Provider-->>Session: Commit Complete
    end
```

---

### Aggregate Rehydration Flow (`AggregateStreamAsync`)

Aggregate rehydration fetches all events associated with a stream ID from storage and sequentially invokes matching `Apply(TEvent)` methods via compiled delegates.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Code
    participant EvtStore as CoreEventStore / CosmosEventStore
    participant Provider as IEventStorageProvider
    participant Agg as TAggregate

    App->>EvtStore: AggregateStreamAsync<TAggregate>(streamId, version = 0)
    EvtStore->>Provider: FetchEventsAsync(streamId, tenantId, fromVersion = 0)
    Provider-->>EvtStore: IReadOnlyList<IEvent> (ordered by Version ASC)

    alt Events Count == 0
        EvtStore-->>App: return null
    else Events Exist
        EvtStore->>Agg: Instantiate new TAggregate()
        loop For each event in stream (up to max target version)
            EvtStore->>EvtStore: Lookup cached Apply(TEvent) compiled delegate
            EvtStore->>Agg: Apply(event.Data)
        end
        EvtStore-->>App: return populated TAggregate instance
    end
```

---

### Partial Document Patch Flow

`Patch<T>()` defers the JSON-pointer path resolution and operation queuing until `SaveChangesAsync()`, allowing Cosmos DB to apply the mutation server-side without a full document round-trip.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Code
    participant Session as DocumentSession
    participant Patch as PatchExpression<T>
    participant Provider as IDocumentStorageProvider

    App->>Session: Patch<T>(id, partitionKey)
    Session-->>App: return PatchExpression<T>
    App->>Patch: Set(prop, value) / Increment(prop) / Append(prop, elem) / Remove(prop, elem)
    Patch->>Patch: Resolve lambda to JSON pointer path, append PatchOperationData
    Session->>Session: Queue StorageOperation(Patch, Operations)

    App->>Session: SaveChangesAsync()
    Session->>Provider: ExecuteBatchAsync([... Patch operation ...])
    Provider->>Provider: Translate each PatchOperationData to native patch call
    Note over Provider: Cosmos: PatchOperation.Replace/Increment/Add/Remove<br/>InMemory: reflection-based path walk
    Provider-->>Session: Batch committed
```

---

### Async Projection Daemon — Catch-Up & Rebuild Flow

`ProjectionLifecycle.Async` projections are decoupled from the write path. A background `IProjectionDaemon` polls the global event sequence and advances each projection's durable checkpoint independently, enabling zero-downtime rebuilds by resetting a checkpoint to `0` and replaying history.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application Code
    participant Daemon as ProjectionDaemon (BackgroundService)
    participant Checkpoints as IProjectionCheckpointStore
    participant Provider as IEventStorageProvider
    participant Proj as IProjection (Async)
    participant Docs as IDocumentStorageProvider

    loop Poll every 100ms (idle) / 500ms (on error)
        Daemon->>Checkpoints: GetCheckpointAsync(projectionName) for each active projection
        Daemon->>Daemon: Compute minSequence across all checkpoints
        Daemon->>Provider: FetchGlobalEventsAsync(fromSequence = minSequence, batchSize = 100)
        Provider-->>Daemon: IReadOnlyList<IEvent>
        loop For each projection with unconsumed events
            Daemon->>Proj: ApplyEvent(event) / ProcessEventAsync(event, session)
            Proj->>Docs: UpsertDocumentAsync / DeleteDocumentAsync
            Daemon->>Checkpoints: SaveCheckpointAsync(projectionName, newSequence)
        end
    end

    Note over Daemon: Zero-Downtime Rebuild (triggered explicitly)
    App->>Daemon: RebuildProjectionAsync<TProjection>()
    Daemon->>Daemon: StopProjectionAsync(name)
    Daemon->>Docs: Query & delete all existing read-model documents
    Daemon->>Checkpoints: SaveCheckpointAsync(name, sequence = 0)
    Daemon->>Provider: FetchGlobalEventsAsync(fromSequence = 0)
    Daemon->>Proj: Replay full event history via ApplyEvent
    Daemon->>Daemon: StartProjectionAsync(name)
```

On Cosmos DB, [`CosmosProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Projections/CosmosProjectionDaemon.cs) additionally supports `ProcessChangeFeedBatchAsync`, consuming Cosmos DB Change Feed items directly instead of re-polling `FetchGlobalEventsAsync`, reducing end-to-end projection lag.

---

## 4. Performance Optimizations & Security Controls

### Performance Optimizations

1. **Sealed Class Hierarchy**: Core internal and public infrastructure classes (e.g., `DocumentSession`, `QuerySession`, `DocumentStore`, `InMemoryStorageProvider`, `CosmosStorageProvider`, `DocumentEnvelope<T>`, `CosmosDocumentEnvelope<T>`, `StoreOptions`) are explicitly marked `sealed` to allow devirtualization and inline method optimization by the JIT compiler.
2. **Compiled Expression Trees for Reflection Hot-Paths**:
   - **ID Resolution**: [`DocumentMapping<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Configuration/StoreOptions.cs#L9) compiles expression trees at startup to extract `Id`/`id` property values without runtime reflection overhead.
   - **Event Envelope Factory**: [`CoreEventStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Sessions/QuerySession.cs#L136) caches compiled `Func<string, long, object, string, IEvent>` delegates in a `ConcurrentDictionary` to instantiate generic `EventEnvelope<T>` instances without invoking `Activator.CreateInstance`.
   - **Aggregate Method Invocation**: `CoreEventStore` caches compiled `Action<object, object>` delegates targeting `Apply(TEvent)` methods on aggregates to eliminate reflection overhead during stream rehydration.
   - **Property Copier**: [`SingleStreamProjection<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/SingleStreamProjection.cs#L78) compiles static block expressions for copying properties between aggregate instances during projection execution.
3. **1-RU Point Read Execution**: [`CosmosStorageProvider.ReadDocumentAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs#L60) executes direct `ReadItemAsync` calls using both `Id` and `PartitionKey`, bypassing SQL parsing and utilizing Cosmos DB 1-RU point read efficiency.
4. **Sync-over-Async Prevention**: Calling synchronous `Query<T>()` throws `NotSupportedException` to prevent thread pool starvation in async web application workloads. Developers must use asynchronous `QueryAsync<T>()`.
5. **Compiled Query Caching**: [`CompiledQueryCache`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Queries/CompiledQueryCache.cs) compiles each `ICompiledQuery<TDoc,TResult>` type's `QueryIs()` expression tree into a delegate exactly once (keyed by `Type` in a `ConcurrentDictionary`). A `QueryParameterBindingVisitor` rewrites closed-over instance members into a rebindable parameter so the same compiled delegate serves every future call with that query shape, regardless of parameter values.
6. **Compiled Event Upcasting**: [`UpcasterRegistry`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/UpcasterRegistry.cs) builds a compiled expression-tree factory (`CreateUpcastEnvelope`, cached per source type) to copy `IEvent` metadata onto a new `EventEnvelope<TNew>` after an upcast, rather than using reflection-based property copying on every fetch.
7. **Cosmos Native LINQ + Expression Rewriting**: [`CosmosStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs) rewrites storage-agnostic `DocumentEnvelope<T>` predicates onto `CosmosDocumentEnvelope<T>` via [`CosmosExpressionRewriter`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosExpressionRewriter.cs), then hands the rewritten predicate to the Cosmos SDK's native LINQ provider for server-side SQL generation — avoiding a hand-rolled SQL translation layer on the hot query path (see note below on `ISqlExpressionTranslator`).

> **Note:** [`ISqlExpressionTranslator`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/ISqlExpressionTranslator.cs) and its [`DefaultSqlExpressionTranslator`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/DefaultSqlExpressionTranslator.cs) implementation are available as an extensibility SPI for storage providers that need to translate LINQ predicates into a native query language by hand. `CosmosStorageProvider` does not currently invoke this translator — it relies on the Cosmos SDK's own LINQ-to-SQL provider instead. Custom storage providers targeting databases without a native LINQ provider (or SDKs you want to bypass) are the intended consumer of this SPI.

---

### Security & Data Safety Controls

1. **Document State Snapshotting**:
   - Calling [`DocumentSession.Store(doc)`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Sessions/DocumentSession.cs#L23) immediately serializes and snapshots the document state. Subsequent mutations to the original object in application code do not pollute the pending unit-of-work state prior to `SaveChangesAsync()`.
2. **Strict Multi-Tenant Scoping & Data Isolation**:
   - Every `DocumentEnvelope<T>`, `CosmosDocumentEnvelope<T>`, and `EventStreamHeader` carries an immutable `TenantId`.
   - `QuerySession` and `DocumentSession` bind all queries and loads to the session's designated `TenantId`. Cross-tenant queries return `null` or filter out unauthorized tenant records.
3. **Cosmos DB SQL Injection Protection**:
   - All dynamic event fetching queries in [`CosmosStorageProvider.FetchEventsAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs#L233) use parameterized [`QueryDefinition`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs#L241) objects (`@streamId`, `@fromVersion`, `@tenantId`), eliminating SQL injection vulnerabilities.
4. **Input Parameter Guards**:
   - All framework methods enforce `ArgumentNullException.ThrowIfNull` and `ArgumentException.ThrowIfNullOrWhiteSpace` checks across IDs, partition keys, stream names, and configuration parameters.
