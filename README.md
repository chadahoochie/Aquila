# Aquila Framework

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](#building--testing)
[![.NET](https://img.shields.io/badge/.NET-10.0-blue)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-green)](#)

**Aquila** is a high-performance, Cosmos DB and Redis native Document Store and Event Sourcing framework for .NET 10, inspired by MartenDB. Designed from the ground up for modern cloud-native architectures, Aquila enables seamless document persistence, atomic batch mutations, event stream management, live & asynchronous projections, zero-downtime projection rebuilds, and multi-tenant isolation over Azure Cosmos DB, Redis, or in-memory test providers.

---

## Key Features

- 🌌 **Cosmos DB Native**: Direct connection mode support optimized for Azure Cosmos DB container structure using single-container partitioning (`/pk`) and hierarchical partition keys via `PartitionKeyBuilder`.
- ⚡ **Redis Projections & Sub-Millisecond Reads**: Dedicated [`Aquila.Redis`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Redis) package delivering ultra-low latency reads, pipelined batching (`IBatch`), cluster hash tag sharding (`{tenant:pk}`), and non-blocking `UNLINK` key purging.
- 🔌 **Tripartite Polyglot Storage SPI**: Three independent, first-class storage SPI contracts:
  - [`IEventStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L92): Append-only event streams, aggregate rehydration, global sequences, snapshots (Cosmos DB, In-Memory).
  - [`IDocumentStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L78): Primary domain documents, dirty tracking, units of work, optimistic concurrency, and complex LINQ querying (Cosmos DB, Redis, In-Memory).
  - [`IProjectionStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L153): Materialized read models, point views, high-throughput batch updates, native instantaneous zero-RU rebuilds ([`PurgeProjectionAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L159)), and ultra-low latency reads (Redis, Cosmos DB, In-Memory).
- 🔀 **$O(1)$ Zero-Allocation Type Routing**: Unified session APIs (`session.LoadAsync<T>()`, `session.QueryAsync<T>()`, `session.Store<T>()`) automatically route read models to `ProjectionStorage` and domain documents to `DocumentStorage` via an immutable `FrozenSet<Type>` registry compiled on store freeze.
- ⚡ **1-RU Point Reads**: High-efficiency point reads (`LoadAsync<T>`) executing direct `ReadItemAsync` operations on Cosmos DB (~1 RU) or sub-millisecond string gets on Redis.
- 📜 **Event Sourcing & CQRS**: First-class stream append operations (`StartStream`, `Append`), expected version concurrency checks, stream fetching (`FetchStreamAsync`), and aggregate rehydration (`AggregateStreamAsync`).
- 🔁 **Event Upcasting & Snapshotting**: Transparent schema evolution via [`IEventUpcaster`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/IEventUpcaster.cs) chains, plus [`ISnapshotStrategy<TAggregate>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/ISnapshotStrategy.cs)-driven aggregate snapshots to avoid full-stream replay on rehydration.
- 📊 **Projections**: Read-model generation via [`SingleStreamProjection<TAggregate>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/SingleStreamProjection.cs) and [`MultiStreamProjection<TDoc,TId>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/MultiStreamProjection.cs), offering `Inline` (transaction-scoped for mono-stores), `Async` (background daemon), and `Live` (on-the-fly, unpersisted) execution lifecycles.
- 🛰️ **Async Projection Daemon & Zero-Downtime Rebuilds**: A background [`IProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/Daemon/IProjectionDaemon.cs) with durable checkpointing ([`IProjectionCheckpointStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/Daemon/IProjectionCheckpointStore.cs)), `CatchUpAsync()`, and zero-downtime `RebuildProjectionAsync()` with instant key purging — plus a Cosmos DB Change Feed-aware variant ([`CosmosProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Projections/CosmosProjectionDaemon.cs)) via `AddCosmosDaemon()`.
- ✂️ **Partial Document Patching**: Fluent [`IPatchExpression<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Patching/IPatchExpression.cs) API (`Set`, `Increment`, `Append`, `Remove`) for low-payload mutations that skip full read-modify-write round-trips, executed server-side on Cosmos DB via the Patch API.
- 📄 **Pagination & Async Streaming**: Constant-RU cursor pagination via continuation tokens ([`PagedResult<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Queries/PagedResult.cs)), offset paging (`QueryPagedByOffsetAsync`), compiled paged queries ([`ICompiledPagedQuery<TDoc>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Queries/ICompiledPagedQuery.cs)), and reactive `IAsyncEnumerable<T>` streaming (`StreamAsync`, `StreamPagesAsync`).
- 🧮 **Compiled Queries**: [`ICompiledQuery<TDoc,TResult>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Queries/ICompiledQuery.cs) with a `CompiledQueryCache` that compiles each query shape's expression tree once and reuses it across parameter values.
- 🎛️ **Session Tracking Modes**: Choose `Lightweight`, `IdentityMap`, or `DirtyTracking` per session to control identity-map caching and automatic JSON-snapshot dirty checking.
- 🏢 **Multi-Tenant Isolation**: Tenant scoping enforced natively at session, document envelope, query, and event store levels.
- 🛡️ **Built-in Safety & Fail-Fast Polyglot Guardrails**:
  - Polyglot Inline Validation: Prohibits `ProjectionLifecycle.Inline` across heterogeneous physical storage providers to prevent distributed partial-failure dual writes without 2PC.
  - Compiled Expression Trees for zero-reflection event instantiation, property copying, ID selector resolution, upcast envelope creation, and compiled-query execution.
  - Automatic document state snapshotting on `Store()` to prevent post-store object mutations.
  - Sync-over-async thread starvation protection (blocking synchronous queries throw `NotSupportedException`).

---

## Configuration & Dependency Injection

Register Aquila in your `Program.cs` or service initialization extensions using the fluent `AddAquila` API.

### 1. Simple Setup: Mono-Provider (Single Provider for All)

In a single-provider setup, all three SPI roles (`DocumentStorage`, `EventStorage`, `ProjectionStorage`) share the same physical storage backend. This allows synchronous `ProjectionLifecycle.Inline` projections as well as `Async` and `Live` lifecycles.

#### Option A: Azure Cosmos DB (All-in-One)

```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;
using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAquila(options =>
{
    // Single provider for DocumentStorage, EventStorage, and ProjectionStorage
    options.UseCosmos(
        connectionString: builder.Configuration.GetConnectionString("CosmosDb")!,
        databaseName: "ProductionDB",
        containerName: "AquilaStore"
    );

    options.DefaultTenantId = "tenant-primary";

    // Configure document schema
    options.Schema.For<Customer>()
        .Identity(c => c.Id)
        .PartitionKey(c => c.Region);

    // Mono-provider supports Inline, Async, and Live projections
    options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Inline);
    options.Projections.Add<CustomerHistoryProjection>(ProjectionLifecycle.Async);
});

// Register background projection daemon for async projections
builder.Services.AddCosmosDaemon();
```

#### Option B: In-Memory (Local Testing & Development)

```csharp
builder.Services.AddAquila(options =>
{
    options.UseInMemoryStorage();
    options.DefaultTenantId = "dev-tenant";

    options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Inline);
});

builder.Services.AddAquilaDaemon();
```

---

### 2. Complex Setup: Polyglot (Cosmos DB for Events & Documents + Redis for Projections)

In high-throughput CQRS architectures, offload read models to Redis for sub-millisecond reads and zero-cost rebuilds while using Azure Cosmos DB for append-only event streams and primary document storage:

```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;
using Aquila.Redis.Extensions;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// 1. Register shared IConnectionMultiplexer singleton for Redis
builder.Services.AddAquilaRedis(builder.Configuration.GetConnectionString("Redis")!);

// 2. Configure Aquila with Polyglot Tripartite Storage
builder.Services.AddAquila(options =>
{
    options.DefaultTenantId = "tenant-primary";

    // Bind DocumentStorage and EventStorage to Cosmos DB
    options.UseCosmos(builder.Configuration.GetConnectionString("CosmosDb")!, cosmos =>
    {
        cosmos.DefaultDatabase = "ProductionDB";
        cosmos.ConfigureEvents("EventsContainer", database: "EventsDB");
        cosmos.ConfigureSnapshots("SnapshotsContainer", database: "SnapshotsDB");
        cosmos.ConfigureDocuments("DocumentsContainer", database: "ProductionDB");
    });

    // Bind ProjectionStorage to Redis (overrides ProjectionStorage SPI)
    options.UseRedisProjections(
        connectionString: builder.Configuration.GetConnectionString("Redis")!,
        configure: redis =>
        {
            redis.KeyPrefix = "aquila:readmodels:";
            redis.Database = 0;
            redis.BatchChunkSize = 500;
        });

    // Automatic aggregate snapshotting every 50 events in Cosmos DB
    options.Events.SnapshotEvery<OrderAggregate>(threshold: 50);

    // Configure primary document mapping (stored in Cosmos DB)
    options.Schema.For<Customer>()
        .Identity(c => c.Id)
        .PartitionKey(c => c.Region);

    // Configure projection read models (stored in Redis)
    options.Schema.For<OrderSummary>()
        .Identity(s => s.OrderId)
        .PartitionKey(s => s.OrderId);

    // IMPORTANT: Polyglot projections MUST use ProjectionLifecycle.Async or Live
    options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);
    options.Projections.Add<CustomerOrdersSummaryProjection>(ProjectionLifecycle.Async);
});

// 3. Register Redis Checkpoint Store for durable sequence tracking
builder.Services.AddRedisCheckpointStore(
    multiplexer: ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis")!),
    keyPrefix: "aquila:checkpoints:",
    database: 0
);

// 4. Register Change Feed / Polling Projection Daemon
builder.Services.AddCosmosDaemon(daemonOptions =>
{
    daemonOptions.BatchSize = 200;
    daemonOptions.PollingIntervalMs = 100;
    daemonOptions.MaxProjectionConcurrency = 8;
});
```

> [!IMPORTANT]
> **Polyglot Fail-Fast Rule**: When `ProjectionStorage` and `EventStorage` reside on distinct physical storage backends (e.g. Cosmos DB + Redis), `ProjectionLifecycle.Inline` is strictly prohibited. Attempting to register an inline projection throws an `InvalidOperationException` on startup during `StoreOptions.Freeze()`. Use `ProjectionLifecycle.Async` or `ProjectionLifecycle.Live` to ensure high-performance eventual consistency without distributed 2PC dual writes.

---

## Quickstart Guide

### 1. Document CRUD Operations (Routes to Cosmos DB)

```csharp
using Aquila.Core.Abstractions;

public class CustomerService
{
    private readonly IDocumentStore _store;

    public CustomerService(IDocumentStore store)
    {
        _store = store;
    }

    public async Task ManageCustomerAsync()
    {
        // Open a unit-of-work document session
        using var session = _store.OpenSession();

        var customer = new Customer
        {
            Id = Guid.NewGuid().ToString(),
            Name = "Jane Doe",
            Email = "jane@example.com",
            Region = "US-East"
        };

        // Store document (snapshots state immediately)
        session.Store(customer);
        await session.SaveChangesAsync();

        // 1-RU Point Read by Id and Partition Key (routes to DocumentStorage: Cosmos DB)
        var loaded = await session.LoadAsync<Customer>(customer.Id, partitionKey: "US-East");

        // Query documents asynchronously
        var eastCoastCustomers = await session.QueryAsync<Customer>(
            c => c.Data.Region == "US-East"
        );

        // Soft delete document
        await session.SoftDeleteAsync<Customer>(customer.Id, partitionKey: "US-East");
        await session.SaveChangesAsync();
    }
}
```

### 2. Event Sourcing & Aggregate Rehydration (Routes to Cosmos DB)

```csharp
// Define Event Records
public record OrderPlaced(string OrderId, string CustomerId, decimal TotalAmount);
public record ItemAdded(string OrderId, string ItemSku, decimal Price);
public record OrderCompleted(string OrderId, DateTimeOffset CompletedAt);

// Define Aggregate with Apply methods
public class OrderAggregate
{
    public string Id { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public bool IsCompleted { get; set; }

    public void Apply(OrderPlaced @event)
    {
        Id = @event.OrderId;
        CustomerId = @event.CustomerId;
        Total = @event.TotalAmount;
    }

    public void Apply(ItemAdded @event)
    {
        Total += @event.Price;
    }

    public void Apply(OrderCompleted @event)
    {
        IsCompleted = true;
    }
}

// Start Stream & Rehydrate Aggregate
public async Task ProcessOrderStreamAsync(IDocumentStore store, string orderId)
{
    using var session = store.OpenSession();

    // Start a new event stream in Cosmos DB
    session.Events.StartStream<OrderAggregate>(orderId,
        new OrderPlaced(orderId, "CUST-100", 99.99m),
        new ItemAdded(orderId, "SKU-ABC", 25.00m)
    );
    await session.SaveChangesAsync();

    // Append subsequent events with concurrency check (expectedVersion)
    session.Events.Append(orderId, expectedVersion: 2, new OrderCompleted(orderId, DateTimeOffset.UtcNow));
    await session.SaveChangesAsync();

    // Rehydrate state by replaying event stream (with snapshot acceleration)
    var order = await session.Events.AggregateStreamAsync<OrderAggregate>(orderId);
    Console.WriteLine($"Order {order?.Id} Total: ${order?.Total}, Completed: {order?.IsCompleted}");
}
```

### 3. Read Model Projections & Sub-Millisecond Reads (Routes to Redis)

```csharp
// Define Read Model POCO
public class OrderSummary
{
    public string OrderId { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public bool IsCompleted { get; set; }
}

// Define SingleStreamProjection for read-model generation
public class OrderSummaryProjection : SingleStreamProjection<OrderSummary>
{
    public OrderSummaryProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;

        CreateEvent<OrderPlaced>(e => new OrderSummary
        {
            OrderId = e.OrderId,
            CustomerId = e.CustomerId,
            Total = e.TotalAmount
        });

        ProjectEvent<ItemAdded>((e, summary) => summary.Total += e.Price);
        ProjectEvent<OrderCompleted>((e, summary) => summary.IsCompleted = true);
    }
}

// Consuming Projections: Automatically routed to Redis with sub-millisecond point reads
public async Task<OrderSummary?> GetOrderSummaryAsync(IDocumentStore store, string orderId)
{
    using var session = store.OpenSession(TrackingMode.Lightweight);
    // Automatically routed to Redis ProjectionStorage via precomputed type registry!
    return await session.LoadAsync<OrderSummary>(orderId, partitionKey: orderId);
}
```

---

## Building & Testing

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)

### Build Solution

```bash
dotnet build Aquila.slnx
```

### Run Unit & Integration Tests

```bash
dotnet test Aquila.slnx
```

---

## Documentation

- 📐 [Architecture & Design Guide](ARCHITECTURE.md) - Tripartite polyglot SPI, zero-allocation type routing, sequence diagrams, and security controls.
- 📖 [Usage & Features Guide](USAGE.md) - Deep dive into document mapping, polyglot recipes, soft deletes, optimistic concurrency, event sourcing, single/multi-stream projections, projection daemon, patching, upcasting, snapshots, compiled queries, and multi-tenancy.
- 🌌 [Cosmos DB Container Segregation Guide](docs/COSMOS_PROJECTION_CONTAINER_SEGREGATION_GUIDE.md) - Strategies for segregating Cosmos DB write models from read models for RU isolation.
- 🚀 [Tripartite Polyglot Architecture Plan](docs/TRIPARTITE_POLYGLOT_PROJECTION_STORE_PLAN.md) - Detailed technical specification of the tripartite storage engine and Redis integration.

---

## License

This project is licensed under the MIT License.

