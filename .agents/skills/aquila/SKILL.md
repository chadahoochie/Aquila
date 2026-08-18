---
name: aquila
description: Enforces architectural standards, configuration patterns, document storage, event sourcing, projections, patching, compiled queries, and multi-tenant isolation for the Aquila Framework (.NET 10 Cosmos DB native document store & event sourcing). Activate this skill when writing, refactoring, or reviewing code using Aquila.
---

# Aquila Framework Architecture & Coding Standards

This skill defines the mandatory architectural rules, implementation patterns, performance guidelines, and testing conventions for applications using or extending the **Aquila Framework**—a high-performance, Azure Cosmos DB and Redis native Document Store and Event Sourcing engine for .NET 10.

---

## 1. Architectural Overview & Tripartite Storage SPI

Aquila decouples business domain semantics (sessions, units-of-work, aggregates, and projections) from physical storage engines using a pluggable **Tripartite Storage Architecture** defined in [`StorageContracts.cs`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs):

- **`Aquila.Core`**: Target framework `net10.0`. Core abstractions ([`IDocumentStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L86), [`IDocumentSession`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L59), [`IQuerySession`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L35), [`IEventStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L14)), session lifecycle & tracking modes ([`TrackingMode`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Sessions/TrackingMode.cs)), mapping policies ([`SchemaPolicy`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Configuration/StoreOptions.cs#L85)), compiled query caching ([`CompiledQueryCache`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Queries/CompiledQueryCache.cs)), patch expressions ([`IPatchExpression<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Patching/IPatchExpression.cs)), event upcasting ([`IEventUpcaster`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/IEventUpcaster.cs)), snapshot strategies ([`ISnapshotStrategy<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/ISnapshotStrategy.cs)), projections ([`SingleStreamProjection<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/SingleStreamProjection.cs), [`MultiStreamProjection<TDoc, TId>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/MultiStreamProjection.cs)), background daemon ([`IProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/Daemon/IProjectionDaemon.cs)), and built-in [`InMemoryStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/InMemoryStorageProvider.cs).
- **`Aquila.Cosmos`**: Target framework `net10.0`. Cosmos DB SPI storage providers ([`CosmosDocumentStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosDocumentStorageProvider.cs), [`CosmosEventStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosEventStorageProvider.cs), [`CosmosProjectionStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosProjectionStorageProvider.cs), [`CosmosStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs)), container resolver ([`CosmosContainerResolver`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosContainerResolver.cs)), partition key builder ([`CosmosPartitionKeyHelper`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosPartitionKeyHelper.cs)), LINQ rewriter ([`CosmosExpressionRewriter`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosExpressionRewriter.cs)), Change Feed daemon ([`CosmosProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Projections/CosmosProjectionDaemon.cs)), and DI extensions (`AddAquila`, `UseCosmos`, `UseCosmosDocuments`, `UseCosmosEvents`, `UseCosmosProjections`, `AddCosmosDaemon`).
- **`Aquila.Redis`**: Target framework `net10.0`. Redis SPI providers ([`RedisProjectionStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Redis/Storage/RedisProjectionStorageProvider.cs), [`RedisDocumentStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Redis/Storage/RedisDocumentStorageProvider.cs), [`RedisProjectionCheckpointStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Redis/Storage/RedisProjectionCheckpointStore.cs)), cluster hash tag routing (`{tenant:pk}`), `CreateBatch()` TCP pipelining, non-blocking `UNLINK` streaming purges, and atomic monotonic Lua checkpoint scripts.

### Storage SPI Contracts
- [`IDocumentStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L78): Atomic reads, queries, upserts, deletes, and batch execution of `StorageOperation`s.
- [`IEventStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L92): Append stream events, fetch streams/global sequences, get stream headers, save/get aggregate snapshots.
- [`IProjectionStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L153): Materialized read models, point views, high-throughput batch updates, and native instantaneous zero-RU rebuilds ([`PurgeProjectionAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L159)).
- [`IProjectionCheckpointStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/Daemon/IProjectionCheckpointStore.cs): Durable checkpoint sequence persistence for async projection daemons.

---

## 2. Dependency Injection & Configuration Patterns

### 1. Simple Setup: Mono-Provider (Single Provider for All)

In a mono-provider configuration, all three SPI roles point to the same physical storage engine. Synchronous `ProjectionLifecycle.Inline` projections are supported.

#### Option A: Azure Cosmos DB
```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;
using Microsoft.Extensions.DependencyInjection;

services.AddAquila(options =>
{
    // Cosmos DB storage provider configuration (segregated or shared)
    options.UseCosmos(configuration.GetConnectionString("CosmosDb")!, cosmos =>
    {
        cosmos.DefaultDatabase = "ProductionDB";
        cosmos.ConfigureEvents("EventsContainer", "EventsDB");
        cosmos.ConfigureSnapshots("SnapshotsContainer", "SnapshotsDB");
        cosmos.ConfigureDocuments("DocumentsContainer", "ProductionDB");
        cosmos.Projections.ToContainer("ProjectionsContainer", "ReadModelsDB");
        // Or auto-container per projection: cosmos.Projections.AutoContainerPerProjection("ReadModelsDB");
    });

    // Seamless aggregate snapshotting every N events
    options.Events.SnapshotEvery<OrderAggregate>(threshold: 50);

    options.DefaultTenantId = "tenant-primary";

    // Mapping policies for entity types
    options.Schema.For<Customer>()
        .Identity(c => c.Id)
        .PartitionKey(c => c.Region)
        .SoftDeleted()
        .UseOptimisticConcurrency(enabled: true);

    // Register projections with execution lifecycles
    options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Inline);
    options.Projections.Add<CustomerHistoryProjection>(ProjectionLifecycle.Async);
});

// Register Change Feed-aware background projection daemon for Cosmos DB
services.AddCosmosDaemon();
```

#### Option B: In-Memory Storage Provider (Testing & Local Development)
```csharp
services.AddAquila(options =>
{
    options.UseInMemoryStorage();
    options.DefaultTenantId = "dev-tenant";
});

// Register standard polling projection daemon
services.AddAquilaDaemon();
```

---

### 2. Complex Setup: Polyglot (Cosmos DB for Events & Documents + Redis for Projections)

```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;
using Aquila.Redis.Configuration;
using Aquila.Redis.Extensions;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

// 1. Register shared IConnectionMultiplexer singleton
services.AddAquilaRedis(configuration.GetConnectionString("Redis")!);

// 2. Configure Aquila with Polyglot Tripartite Storage
services.AddAquila(options =>
{
    options.DefaultTenantId = "tenant-primary";

    // Cosmos DB for DocumentStorage & EventStorage
    options.UseCosmos(configuration.GetConnectionString("CosmosDb")!, cosmos =>
    {
        cosmos.DefaultDatabase = "ProductionDB";
        cosmos.ConfigureEvents("EventsContainer", "EventsDB");
        cosmos.ConfigureSnapshots("SnapshotsContainer", "SnapshotsDB");
        cosmos.ConfigureDocuments("DocumentsContainer", "ProductionDB");
    });

    // Redis for ProjectionStorage
    options.UseRedisProjections(configuration.GetConnectionString("Redis")!, (RedisStorageOptions redis) =>
    {
        redis.KeyPrefix = "aquila:readmodels:";
        redis.Database = 0;
        redis.BatchChunkSize = 500;
    });

    options.Events.SnapshotEvery<OrderAggregate>(threshold: 50);

    options.Schema.For<Customer>()
        .Identity(c => c.Id)
        .PartitionKey(c => c.Region);

    options.Schema.For<OrderSummary>()
        .Identity(s => s.OrderId)
        .PartitionKey(s => s.OrderId);

    // IMPORTANT: Polyglot projections MUST use Async or Live
    options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);
    options.Projections.Add<CustomerOrderHistoryProjection>(ProjectionLifecycle.Async);
});

// 3. Register Redis Checkpoint Store
services.AddRedisCheckpointStore(
    multiplexer: ConnectionMultiplexer.Connect(configuration.GetConnectionString("Redis")!),
    keyPrefix: "aquila:checkpoints:",
    database: 0
);

// 4. Register Background Projection Daemon
services.AddCosmosDaemon();
```

> [!IMPORTANT]
> **Polyglot Fail-Fast Rule**: When `ProjectionStorage` and `EventStorage` are separate physical stores (e.g. Cosmos DB + Redis), `ProjectionLifecycle.Inline` throws an `InvalidOperationException` at `StoreOptions.Freeze()`. Polyglot projections must use `ProjectionLifecycle.Async` or `ProjectionLifecycle.Live`.

---

## 3. Session Tracking Modes & Unit of Work Rules

Choose session tracking modes based on operational needs:

| Tracking Mode | Identity Map | Dirty Checking | Primary Use Case |
| :--- | :--- | :--- | :--- |
| `TrackingMode.Lightweight` | Disabled | Disabled | High-performance read-only queries or isolated writes where instance tracking is unneeded. `LoadAsync` always re-fetches from storage. |
| `TrackingMode.IdentityMap` | Enabled | Disabled | Workflows where identical document identity must return the same CLR instance within a session, but mutations require explicit `session.Store()` calls. |
| `TrackingMode.DirtyTracking` (Default) | Enabled | Automatic (JSON diff) | Standard load-mutate-`SaveChangesAsync()` unit of work. Entities are snapshotted at load/store time; `SaveChangesAsync()` auto-detects diffs and queues updates without explicit `Store()` calls. |

### Crucial Execution Rules:
1. **$O(1)$ Zero-Allocation Type Routing**: `session.LoadAsync<T>()`, `session.QueryAsync<T>()`, and `session.Store<T>()` inspect `Options.IsProjectionReadModel(typeof(T))`. Projection read models route directly to `ProjectionStorage` (Redis); primary domain documents route to `DocumentStorage` (Cosmos DB).
2. **Asynchronous Query Execution**: All querying is purely non-blocking and asynchronous via `await session.QueryAsync<T>(...)`, `await session.QueryPagedAsync<T>(...)`, or `await foreach (var item in session.StreamAsync<T>(...))`. Synchronous blocking LINQ queries are excluded by design to prevent thread-pool starvation.
3. **Document State Snapshotting on `Store(doc)`**: `session.Store()` immediately serializes and snapshots the object state. Subsequent mutations to the original entity instance in application code will NOT pollute the pending unit-of-work state prior to `SaveChangesAsync()`.
4. **Session Lifecycle Management**: All sessions implement `IDisposable` and `IAsyncDisposable`. Always wrap session usage in `using var session = store.OpenSession()` or `await using var session = store.OpenSession()`.

---

## 4. Document CRUD & 1-RU Point Reads

```csharp
// 1-RU Point Read (Direct Cosmos DB ReadItemAsync optimization using Id + PartitionKey)
var customer = await session.LoadAsync<Customer>("C-100", partitionKey: "US-East");

// Storing Documents (DirtyTracking auto-detects changes on SaveChangesAsync)
session.Store(new Customer { Id = "C-101", Name = "Acme Corp", Region = "US-West" });
await session.SaveChangesAsync();

// Async Querying with Predicate Pushdown and Ordering
var customers = await session.QueryAsync<Customer>(
    predicate: c => c.Data.Region == "US-East",
    orderBy: c => c.Data.Name,
    sortOrder: SortOrder.Ascending
);

// Soft Deletion (Flags IsDeleted = true, automatically filtered out from Load & Query calls)
await session.SoftDeleteAsync<Customer>("C-100", partitionKey: "US-East");
await session.SaveChangesAsync();
```

---

## 5. Event Sourcing & Aggregate Rehydration

Event stream management is exposed via `session.Events`:

### Stream Appends & Concurrency Checks
```csharp
var streamId = Guid.NewGuid().ToString();

// Start a new event stream
session.Events.StartStream<OrderAggregate>(streamId,
    new OrderPlaced(streamId, "CUST-1", 150.00m)
);
await session.SaveChangesAsync();

// Append subsequent events with expectedVersion concurrency verification
try
{
    session.Events.Append(streamId, expectedVersion: 1, new ItemAdded(streamId, "SKU-99", 25.00m));
    await session.SaveChangesAsync();
}
catch (AquilaConcurrencyException ex)
{
    // Raised when stream's actual version disagrees with expectedVersion
    Console.WriteLine($"Concurrency failure on stream '{ex.StreamId}': Expected {ex.ExpectedVersion}, Got {ex.ActualVersion}");
}
```

### Aggregate Rehydration
Aggregates rehydrate their state by declaring public or internal `Apply(TEvent)` methods:

```csharp
public class OrderAggregate
{
    public string Id { get; set; } = string.Empty;
    public decimal Total { get; set; }

    public void Apply(OrderPlaced @event)
    {
        Id = @event.OrderId;
        Total = @event.TotalAmount;
    }

    public void Apply(ItemAdded @event)
    {
        Total += @event.Price;
    }
}

// Rehydrate aggregate state to current version (uses cached compiled expression trees for zero-reflection Apply calls)
var order = await session.Events.AggregateStreamAsync<OrderAggregate>(streamId);

// Rehydrate aggregate state to specific historical version
var historicalOrder = await session.Events.AggregateStreamAsync<OrderAggregate>(streamId, version: 1);
```

---

## 6. Projections & Background Daemon

Projections transform raw domain events into materialized read models across three lifecycles:

1. **`ProjectionLifecycle.Inline`**: Executes synchronously inside `SaveChangesAsync()` within the same storage transaction commit (mono-store only).
2. **`ProjectionLifecycle.Async`**: Processed in background batches by the `IProjectionDaemon` (mono-store and polyglot).
3. **`ProjectionLifecycle.Live`**: Evaluated on-the-fly via `session.LiveStreamAsync<TDoc>(streamId)` without persisting read-model documents.

### SingleStreamProjection
Folds events from a single stream into a document keyed by stream ID:
```csharp
public class OrderSummaryProjection : SingleStreamProjection<OrderAggregate>
{
    public OrderSummaryProjection()
    {
        CreateEvent<OrderPlaced>(e => new OrderAggregate { Id = e.OrderId, Total = e.TotalAmount });
        ProjectEvent<ItemAdded>((e, agg) => agg.Total += e.Price);
    }
}
```

### MultiStreamProjection & Cross-Store Enrichment
Aggregates events across *multiple* streams into a shared read-model document:
```csharp
public class CustomerOrderSummaryProjection : MultiStreamProjection<CustomerSummary, string>
{
    protected override string Identity(IEvent @event) =>
        @event.Data switch
        {
            OrderPlaced e => e.CustomerId,
            _ => string.Empty
        };

    public override bool Apply(IEvent @event, CustomerSummary doc)
    {
        if (@event.Data is OrderPlaced e)
        {
            doc.CustomerId = e.CustomerId;
            doc.TotalOrders++;
        }
        return true; // Return false to delete the target read-model document
    }
}
```

### Daemon Management & Zero-Downtime Rebuilds
```csharp
var daemon = serviceProvider.GetRequiredService<IProjectionDaemon>();

// Block until all async projections catch up to the latest global sequence
await daemon.CatchUpAsync();

// Zero-Downtime Rebuild: Invokes PurgeProjectionAsync (non-blocking UNLINK on Redis), resets checkpoint to 0, and replays history
await daemon.RebuildProjectionAsync<CustomerOrderSummaryProjection>();
```

---

## 7. Partial Document Patching

Avoid full read-modify-write round-trips for low-payload updates using fluent `Patch<T>()`:

```csharp
session.Patch<Customer>("C-100", partitionKey: "US-East")
    .Set(c => c.Status, "Active")
    .Increment(c => c.LoginCount)                  // Defaults to +1 (maps to Cosmos PatchOperation.Increment)
    .Append(c => c.Tags, "VIP")                   // Array tail append (Cosmos PatchOperation.Add /- index)
    .Remove(c => c.Tags, "Trial");

await session.SaveChangesAsync();
```

---

## 8. Document Paging & Asynchronous Streaming

Ensure scalable, constant-RU pagination and reactive document consumption:

```csharp
// 1. Continuation-Token Paging with Ordering (Constant RU cost across deep pages)
PagedResult<Customer> page1 = await session.QueryPagedAsync<Customer>(
    predicate: c => c.Data.Region == "US-East",
    orderBy: c => c.Data.CreatedAt,
    sortOrder: SortOrder.Descending,
    pageSize: 20
);

if (page1.HasMore)
{
    PagedResult<Customer> page2 = await session.QueryPagedAsync<Customer>(
        predicate: c => c.Data.Region == "US-East",
        orderBy: c => c.Data.CreatedAt,
        sortOrder: SortOrder.Descending,
        pageSize: 20,
        continuationToken: page1.ContinuationToken
    );
}

// 2. Offset-Based Paging with Ordering (Skip/Take for random page navigation)
PagedResult<Customer> page3 = await session.QueryPagedByOffsetAsync<Customer>(
    pageNumber: 3,
    pageSize: 10,
    predicate: c => c.Data.Status == "Active",
    orderBy: c => c.Data.Name,
    sortOrder: SortOrder.Ascending
);

// 3. Reactive IAsyncEnumerable Streaming with Ordering (Zero unbounded memory buffering)
await foreach (var customer in session.StreamAsync<Customer>(
    orderBy: c => c.Data.CreatedAt,
    sortOrder: SortOrder.Ascending,
    batchSize: 100))
{
    Process(customer);
}

// 4. Compiled Paged Query with Ordering
public class ActiveCustomersPagedQuery : ICompiledPagedQuery<Customer>
{
    public int PageSize { get; init; } = 25;
    public string? ContinuationToken { get; init; }
    public string? PartitionKey { get; init; }
    public Expression<Func<DocumentEnvelope<Customer>, bool>>? Predicate() =>
        env => env.Data.Status == "Active";
    public Expression<Func<DocumentEnvelope<Customer>, object?>>? OrderBy() =>
        env => env.Data.Name;
    public SortOrder SortOrder => SortOrder.Ascending;
}

PagedResult<Customer> compiledResults = await session.QueryPagedAsync(new ActiveCustomersPagedQuery());
```

---

## 9. Compiled Queries & Expression Caching

Eliminate LINQ expression tree compilation overhead for high-frequency query shapes:

```csharp
public class ActiveCustomersByRegion : ICompiledQuery<Customer, IQueryable<Customer>>
{
    public string Region { get; }
    public ActiveCustomersByRegion(string region) => Region = region;

    public Expression<Func<IQueryable<Customer>, IQueryable<Customer>>> QueryIs() =>
        customers => customers.Where(c => c.Region == Region && c.Status == "Active");
}

// Execution (CompiledQueryCache compiles expression delegate once per query type and rebinds parameter values)
var results = await session.QueryAsync(new ActiveCustomersByRegion("US-East"));
```

---

## 10. Event Upcasting (Schema Evolution)

Evolve event payload schemas transparently without altering historical event logs in storage:

```csharp
public record UserRegisteredV1(string UserId, string Name);
public record UserRegistered(string UserId, string FirstName, string LastName);

public class UserRegisteredUpcaster : EventUpcaster<UserRegisteredV1, UserRegistered>
{
    public override UserRegistered Upcast(UserRegisteredV1 old)
    {
        var parts = old.Name.Split(' ', 2);
        return new UserRegistered(old.UserId, parts[0], parts.Length > 1 ? parts[1] : string.Empty);
    }
}

// Registration in StoreOptions
options.Events.RegisterUpcaster<UserRegisteredUpcaster>();
```

---

## 11. Multi-Tenancy, Tracing & Data Isolation Rules

1. **Tenant Isolation**: Every `DocumentEnvelope<T>` and event stream header includes an immutable `TenantId`. Cross-tenant queries return `null` or filter out unauthorized records.
2. **Cosmos DB SQL Injection Safety**: `CosmosStorageProvider` executes all event/stream queries using parameterized `QueryDefinition` instances (`@streamId`, `@fromVersion`, `@tenantId`).
3. **Hierarchical Partition Keys**: For multi-level partition key routing, use pipe-delimited strings (`"TenantA|Region1|Dept5"`). `CosmosPartitionKeyHelper` splits on `'|'` and invokes Cosmos DB's `PartitionKeyBuilder`.
4. **Tracing Propagation**: Assign `session.CorrelationId`, `session.CausationId`, and `session.SetHeader("key", val)`. Headers automatically flow onto every `IEvent` envelope generated within that session.

---

## 12. Code Quality & Performance Checklist

- [ ] Are internal/public infrastructure classes explicitly marked `sealed` for JIT devirtualization?
- [ ] Are hot execution paths free of runtime reflection (using compiled expression delegates)?
- [ ] Are all query operations non-blocking and asynchronous (using `QueryAsync<T>()`, `QueryPagedAsync<T>()`, `StreamAsync<T>()`)?
- [ ] Are polyglot projections configured as `ProjectionLifecycle.Async` or `ProjectionLifecycle.Live` (never `Inline`)?
- [ ] Are null checks enforced on all method inputs (`ArgumentNullException.ThrowIfNull`)?
- [ ] Do all automated tests pass via `dotnet test Aquila.slnx`?

