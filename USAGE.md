# Aquila Usage & Feature Guide

This guide provides detailed documentation and working code examples for all core features of the **Aquila** framework.

---

## 1. Document Mapping Policies

Aquila allows you to customize document identity, partition key routing, soft delete behavior, and optimistic concurrency rules using the fluent [`SchemaPolicy`](src/Aquila.Core/Configuration/StoreOptions.cs#L85) API.

### Configuration Example

```csharp
using Aquila.Core.Configuration;

var options = new StoreOptions();

options.Schema.For<Product>()
    .Identity(p => p.Sku)                           // Custom document ID selector
    .PartitionKey(p => p.Category)                  // Custom partition key selector
    .SoftDeleted()                                  // Enable soft delete behavior
    .UseOptimisticConcurrency(enabled: true);        // Require version checks
```

### Identity Resolution Defaults

- If `.Identity(...)` is omitted, Aquila automatically searches for a property named `Id` or `id` on the target type and compiles a fast getter expression.
- If no `Id` property exists, Aquila falls back to generating a new `Guid.NewGuid().ToString()` upon document storage.

### Partition Key Routing Defaults

- If `.PartitionKey(...)` is omitted, Aquila defaults the partition key to the C# class type name (`typeof(T).Name`).
- **Hierarchical Partition Keys**: When targeting Azure Cosmos DB containers with hierarchical partition keys, pass pipe-delimited values (e.g. `"TenantA|Region1|Dept5"`). Aquila's `CosmosPartitionKeyHelper` automatically splits the key on `'|'` and uses Cosmos DB's native `PartitionKeyBuilder` to construct multi-level partition keys.

---

## 2. Soft Delete Management

Aquila supports soft deletes out of the box. Soft-deleted documents remain stored in the underlying persistence layer with `IsDeleted = true` but are automatically filtered out of all [`LoadAsync`](src/Aquila.Core/Abstractions/IDocumentStore.cs#L39), [`LoadManyAsync`](src/Aquila.Core/Abstractions/IDocumentStore.cs#L41), and [`QueryAsync`](src/Aquila.Core/Abstractions/IDocumentStore.cs#L44) operations.

### Soft Delete APIs

```csharp
using Aquila.Core.Abstractions;

public async Task SoftDeleteExamplesAsync(IDocumentSession session, Order order)
{
    // 1. Soft delete by entity instance (synchronous queue)
    session.SoftDelete(order);

    // 2. Soft delete by ID and explicit partition key (synchronous queue)
    session.SoftDelete<Order>(id: "ORD-5001", partitionKey: "Electronics");

    // 3. Asynchronous soft delete by entity instance
    await session.SoftDeleteAsync(order);

    // 4. Asynchronous soft delete by ID (fetches existing record before flagging)
    await session.SoftDeleteAsync<Order>(id: "ORD-5002", partitionKey: "Electronics");

    // Commit changes to persistence provider
    await session.SaveChangesAsync();
}
```

---

## 3. Optimistic Concurrency Control

Aquila enforces optimistic concurrency controls during event stream appends and batch document mutations to prevent concurrent write collisions.

### Handling Concurrency Exceptions

When appending events with an explicit `expectedVersion`, Aquila validates that the stream's current version matches `expectedVersion`. If a mismatch occurs, an [`AquilaConcurrencyException`](src/Aquila.Core/Exceptions/AquilaException.cs) is raised.

```csharp
using Aquila.Core.Exceptions;

try
{
    using var session = store.OpenSession();
    
    // Attempt to append with expected version check
    session.Events.Append(
        streamId: "stream-123",
        expectedVersion: 5,
        new PaymentProcessed("stream-123", 150.00m)
    );

    await session.SaveChangesAsync();
}
catch (AquilaConcurrencyException ex)
{
    Console.WriteLine($"Concurrency conflict detected on stream '{ex.StreamId}'!");
    Console.WriteLine($"Expected Version: {ex.ExpectedVersion}, Actual Version: {ex.ActualVersion}");
}
```

---

## 4. Event Sourcing & Aggregate Streams

Aquila provides an event store subsystem accessed via [`session.Events`](src/Aquila.Core/Abstractions/IDocumentStore.cs#L37).

### 1. Starting an Event Stream

```csharp
var streamId = Guid.NewGuid().ToString();

using var session = store.OpenSession();

session.Events.StartStream<AccountAggregate>(streamId,
    new AccountOpened(streamId, Owner: "Alice", InitialBalance: 500.00m),
    new MoneyDeposited(streamId, Amount: 200.00m)
);

await session.SaveChangesAsync();
```

### 2. Appending Events to Existing Streams

```csharp
using var session = store.OpenSession();

// Append without explicit expected version check
session.Events.Append(streamId, new MoneyWithdrawn(streamId, Amount: 50.00m));

// Append with explicit expected version check (concurrency control)
session.Events.Append(streamId, expectedVersion: 3, new MoneyWithdrawn(streamId, Amount: 100.00m));

await session.SaveChangesAsync();
```

### 3. Fetching Event Streams

```csharp
using var session = store.OpenSession();

// Fetch all events for a stream
IReadOnlyList<IEvent> allEvents = await session.Events.FetchStreamAsync(streamId);

// Fetch events starting from version 2
IReadOnlyList<IEvent> partialEvents = await session.Events.FetchStreamAsync(streamId, fromVersion: 2);
```

### 4. Aggregate Rehydration

Aggregates rehydrate their state by defining `Apply(TEvent)` methods for each domain event.

```csharp
public class AccountAggregate
{
    public string Id { get; set; } = string.Empty;
    public string Owner { get; set; } = string.Empty;
    public decimal Balance { get; set; }

    public void Apply(AccountOpened @event)
    {
        Id = @event.AccountId;
        Owner = @event.Owner;
        Balance = @event.InitialBalance;
    }

    public void Apply(MoneyDeposited @event)
    {
        Balance += @event.Amount;
    }

    public void Apply(MoneyWithdrawn @event)
    {
        Balance -= @event.Amount;
    }
}

// Rehydrate aggregate state up to current version
var account = await session.Events.AggregateStreamAsync<AccountAggregate>(streamId);

// Rehydrate aggregate state up to specific historical version
var historicalAccount = await session.Events.AggregateStreamAsync<AccountAggregate>(streamId, version: 2);
```

---

## 5. SingleStreamProjections

Projections transform event streams into materialized read-model documents automatically.

### Projection Implementation

Inherit from [`SingleStreamProjection<TAggregate>`](src/Aquila.Core/Projections/SingleStreamProjection.cs#L36) and register handlers in the constructor using `CreateEvent` and `ProjectEvent`.

```csharp
using Aquila.Core.Projections;

public class AccountSummaryProjection : SingleStreamProjection<AccountAggregate>
{
    public AccountSummaryProjection()
    {
        // Initial creation handler for stream initialization
        CreateEvent<AccountOpened>(e => new AccountAggregate
        {
            Id = e.AccountId,
            Owner = e.Owner,
            Balance = e.InitialBalance
        });

        // Subsequent event transformation handlers
        ProjectEvent<MoneyDeposited>((e, agg) => agg.Balance += e.Amount);
        ProjectEvent<MoneyWithdrawn>((e, agg) => agg.Balance -= e.Amount);
    }
}
```

### Projection Lifecycles

Register projections with desired lifecycles in `StoreOptions`:

```csharp
options.Projections.Add<AccountSummaryProjection>(ProjectionLifecycle.Inline);
```

- **`ProjectionLifecycle.Inline`**: Executes synchronously inside `SaveChangesAsync()`. Aggregate read-models are stored in the document store within the same transactional commit.
- **`ProjectionLifecycle.Async`**: Designed for background execution via Cosmos DB Change Feed Processors.

---

## 6. Multi-Tenancy & Tenant Isolation

Aquila provides native multi-tenant isolation across all document and event store operations.

### Configuring Default Tenant ID

```csharp
services.AddAquila(options =>
{
    options.DefaultTenantId = "tenant-central";
});
```

### Opening Tenant-Scoped Sessions

Pass a tenant ID when opening sessions to guarantee cross-tenant data isolation:

```csharp
// Open document session isolated to Tenant Alpha
using var alphaSession = store.OpenSession(tenantId: "tenant-alpha");

alphaSession.Store(new Customer { Id = "C-1", Name = "Alpha Corp" });
await alphaSession.SaveChangesAsync();

// Open query session isolated to Tenant Beta
using var betaSession = store.QuerySession(tenantId: "tenant-beta");

// Attempts to load Tenant Alpha's document from Tenant Beta session will return null
var customer = await betaSession.LoadAsync<Customer>("C-1"); // Returns null
```

### Tenant Isolation in Event Store

Event streams, event envelopes, and stream headers track the `TenantId`. Appending or fetching streams inside a tenant-scoped session guarantees events are isolated from other tenants.

---

## 7. Session Tracking Modes

Every session — query or document — operates under a [`TrackingMode`](src/Aquila.Core/Sessions/TrackingMode.cs) that governs identity-map caching and dirty-checking behavior. Choose the mode based on the read/write pattern of the unit of work.

| Mode | Identity Map | Dirty Checking | Typical Use |
| :--- | :--- | :--- | :--- |
| `TrackingMode.Lightweight` | Disabled | None | High-throughput read-only queries or one-off writes where tracking overhead is unnecessary. `Store()` still queues a write, but repeated `LoadAsync` calls always re-fetch from storage. |
| `TrackingMode.IdentityMap` | Enabled | None | Ensures the same logical document returns the same CLR instance within a session, but requires explicit `Store()` calls to persist mutations. |
| `TrackingMode.DirtyTracking` (default) | Enabled | Automatic (JSON snapshot diff) | Load-mutate-`SaveChangesAsync()` workflows. Every loaded/tracked document is snapshotted; on `SaveChangesAsync()`, Aquila re-serializes tracked entities and diffs the bytes to auto-queue changed documents without an explicit `Store()` call. |

```csharp
using Aquila.Core.Sessions;

// Explicit tracking mode selection
using var session = store.OpenSession(TrackingMode.DirtyTracking);

var customer = await session.LoadAsync<Customer>("C-1");
customer!.Email = "updated@example.com"; // Mutate the tracked instance directly

// No explicit Store() call needed — DirtyTracking detects the change via snapshot diff
await session.SaveChangesAsync();

// Lightweight sessions skip identity map and dirty-checking overhead entirely
using var lightweight = store.LightweightSession();
var readOnly = await lightweight.LoadAsync<Customer>("C-1");
```

Dirty checking is implemented in [`DocumentSession.DetectAndQueueDirtyEntities`](src/Aquila.Core/Sessions/DocumentSession.cs#L305), which walks all entities tracked in the [`IIdentityMap`](src/Aquila.Core/Sessions/IIdentityMap.cs), re-serializes each with `System.Text.Json`, and compares the resulting UTF-8 bytes against the snapshot recorded at load/store time.

---

## 8. Partial Document Patching

For high-frequency, low-payload mutations (counters, status flags, list append/remove), Aquila exposes a fluent [`IPatchExpression<T>`](src/Aquila.Core/Patching/IPatchExpression.cs) API that translates property-lambda expressions into JSON-pointer paths, avoiding full document read-modify-write round-trips.

```csharp
using var session = store.OpenSession();

session.Patch<Order>(id: "ORD-1001", partitionKey: "Electronics")
    .Set(o => o.Status, "Shipped")
    .Increment(o => o.RevisionNumber)          // defaults to +1
    .Append(o => o.Tags, "expedited")
    .Remove(o => o.Tags, "backorder");

await session.SaveChangesAsync();
```

### Supported Patch Operations

| Method | Behavior |
| :--- | :--- |
| `Set(property, value)` | Replaces the property value at the resolved JSON pointer path. |
| `Increment(property, value = 1)` | Atomically increments a numeric property. On Cosmos DB this maps to `PatchOperation.Increment`, executed server-side. |
| `Append(property, element)` | Appends an element to a collection property (`PatchOperation.Add` at the `/-` array-tail index on Cosmos). |
| `Remove(property, element)` | Removes a matching element from a collection property. |

Patch paths are resolved by walking the property-access lambda into a `/Data/PropertyName[/NestedProperty...]` JSON pointer (see [`PatchExpression<T>.BuildJsonPointerPath`](src/Aquila.Core/Patching/PatchExpression.cs#L68)). `Patch<T>()` queues a `StorageOperationType.Patch` [`StorageOperation`](src/Aquila.Core/Storage/StorageContracts.cs#L59) that is flushed by `ExecuteBatchAsync` on `SaveChangesAsync()`, alongside upserts and deletes in the same batch. The [`InMemoryStorageProvider`](src/Aquila.Core/Storage/InMemoryStorageProvider.cs#L122) applies patches via reflection for local testing parity with Cosmos DB semantics.

---

## 9. Multi-Stream Projections

Where [`SingleStreamProjection<TAggregate>`](src/Aquila.Core/Projections/SingleStreamProjection.cs) folds events from exactly one stream into a document keyed by that stream's ID, [`MultiStreamProjection<TDoc, TId>`](src/Aquila.Core/Projections/MultiStreamProjection.cs) builds read models that aggregate events from *many* streams into a differently-keyed read model — e.g. a per-customer order history document fed by events from many individual order streams.

```csharp
using Aquila.Core.Events;
using Aquila.Core.Projections;

public class CustomerOrderHistory
{
    public string CustomerId { get; set; } = string.Empty;
    public int OrderCount { get; set; }
    public decimal LifetimeValue { get; set; }
}

public class CustomerOrderHistoryProjection : MultiStreamProjection<CustomerOrderHistory, string>
{
    // Route each event to the read-model document ID it belongs to.
    protected override string Identity(IEvent @event) =>
        @event.Data switch
        {
            OrderPlaced e => e.CustomerId,
            _ => string.Empty
        };

    // Return false to delete the read model instead of upserting it.
    public override bool Apply(IEvent @event, CustomerOrderHistory document)
    {
        if (@event.Data is OrderPlaced placed)
        {
            document.CustomerId = placed.CustomerId;
            document.OrderCount++;
            document.LifetimeValue += placed.TotalAmount;
        }
        return true;
    }
}

options.Projections.Add<CustomerOrderHistoryProjection>(ProjectionLifecycle.Inline);
```

`Identity(@event)` determines which read-model document instance the event applies to; `Apply(@event, document)` mutates it. Returning `false` from `Apply` causes the projection runner to delete the read-model document instead of upserting it — useful for cross-stream cleanup (e.g. an `OrderCancelled` event removing a summary row). Multi-stream projections can run under any `ProjectionLifecycle`, including `Inline` (processed synchronously in `SaveChangesAsync`) and `Async` (processed by the projection daemon — see §10).

---

## 10. Asynchronous Projections & the Projection Daemon

`ProjectionLifecycle.Async` projections do not run inline inside `SaveChangesAsync()`. Instead, a background [`IProjectionDaemon`](src/Aquila.Core/Projections/Daemon/IProjectionDaemon.cs) hosted service polls the event store's global sequence and dispatches new event batches to registered async projections, tracking progress via a durable [`IProjectionCheckpointStore`](src/Aquila.Core/Projections/Daemon/IProjectionCheckpointStore.cs).

### Registering the Daemon

```csharp
using Aquila.Core.Projections.Daemon;
using Aquila.Cosmos.Extensions; // for AddCosmosDaemon when using Cosmos

builder.Services.AddAquila(options =>
{
    // Option A: Single shared container (default/legacy)
    // options.UseCosmos(connectionString, "ProductionDB", "AquilaDocuments");

    // Option B: Segregated storage across Events, Snapshots, Documents, and Projections
    options.UseCosmos(connectionString, cosmos =>
    {
        cosmos.DefaultDatabase = "ProductionDB";

        // 1. Events Store Definition
        cosmos.ConfigureEvents("EventsContainer", database: "EventsDB");

        // 2. Snapshots Store Definition
        cosmos.ConfigureSnapshots("SnapshotsContainer", database: "SnapshotsDB");

        // 3. Documents Store Definition
        cosmos.ConfigureDocuments("DocumentsContainer", database: "ProductionDB");

        // 4. Projections Definition (Default: inherits Documents definition)
        // Option 4a: Dedicated DB & Container for all projections
        // cosmos.Projections.ToContainer("ProjectionsContainer", database: "ReadModelsDB");

        // Option 4b: Auto-container per projection
        cosmos.Projections.AutoContainerPerProjection(database: "ReadModelsDB", type => type.Name);

        // Option 4c: Individual projection override
        cosmos.Projections.For<CustomerOrderHistoryProjection>("CustomerHistoryContainer", database: "ReadModelsDB");
    });

    // Seamless snapshotting: Snapshot every 50 events per aggregate
    options.Events.SnapshotEvery<OrderAggregate>(threshold: 50);

    options.Projections.Add<CustomerOrderHistoryProjection>(ProjectionLifecycle.Async);
});

// InMemory / generic storage-backed polling daemon
builder.Services.AddAquilaDaemon();

// -- or, for Cosmos DB, the change-feed-aware daemon variant --
builder.Services.AddCosmosDaemon();
```

`AddAquilaDaemon()` registers [`ProjectionDaemon`](src/Aquila.Core/Projections/Daemon/ProjectionDaemon.cs) as a `BackgroundService` that continuously polls `IEventStorageProvider.FetchGlobalEventsAsync` in 100-event batches. `AddCosmosDaemon()` registers [`CosmosProjectionDaemon`](src/Aquila.Cosmos/Projections/CosmosProjectionDaemon.cs), which additionally exposes `ProcessChangeFeedBatchAsync` for wiring into an Azure Cosmos DB Change Feed Processor, deserializing `$event`-tagged change feed items directly from the Events container instead of re-polling.

By default, `IProjectionCheckpointStore` durably persists each projection's `LastCompletedSequence` as a document via [`DocumentStorageProjectionCheckpointStore`](src/Aquila.Core/Projections/Daemon/IProjectionCheckpointStore.cs#L21). Pass a custom factory to `AddAquilaDaemon(checkpointStoreFactory)` to use `InMemoryProjectionCheckpointStore` (testing only — checkpoints do not survive process restarts) or a bespoke store.

### Daemon Operations

```csharp
var daemon = serviceProvider.GetRequiredService<IProjectionDaemon>();

// Pause / resume a specific async projection without stopping the whole daemon
await daemon.StopProjectionAsync(nameof(CustomerOrderHistoryProjection));
await daemon.StartProjectionAsync(nameof(CustomerOrderHistoryProjection));

// Block until all active async projections have caught up to the current global sequence
await daemon.CatchUpAsync();

// Zero-Downtime Rebuild: delete existing read-model documents, reset the checkpoint to 0,
// and replay the entire event history from the beginning.
await daemon.RebuildProjectionAsync<CustomerOrderHistoryProjection>();
```

`RebuildProjectionAsync` is the mechanism for the "drop and replay" pattern described in [ARCHITECTURE.md](ARCHITECTURE.md): it deletes every document of the projection's read-model type, resets the checkpoint to sequence `0`, and reprocesses the full event history. Use this after a breaking read-model schema change instead of mutating documents in place.

---

## 11. Event Upcasting (Schema Evolution)

As event-carrying types evolve, Aquila supports transforming old event payload shapes into new ones transparently at read time via [`IEventUpcaster`](src/Aquila.Core/Events/IEventUpcaster.cs), without rewriting historical events in the journal.

```csharp
using Aquila.Core.Events;

// V1 event shape (retained for historical deserialization only)
public record CustomerRegisteredV1(string CustomerId, string FullName);

// V2 event shape — FullName split into structured parts
public record CustomerRegistered(string CustomerId, string FirstName, string LastName);

public class CustomerRegisteredUpcaster : EventUpcaster<CustomerRegisteredV1, CustomerRegistered>
{
    public override CustomerRegistered Upcast(CustomerRegisteredV1 oldEvent)
    {
        var parts = oldEvent.FullName.Split(' ', 2);
        return new CustomerRegistered(oldEvent.CustomerId, parts[0], parts.Length > 1 ? parts[1] : string.Empty);
    }
}

// Registration
options.Events.RegisterUpcaster<CustomerRegisteredUpcaster>();
```

Upcasters are chained: [`UpcasterRegistry.Upcast`](src/Aquila.Core/Events/UpcasterRegistry.cs#L25) repeatedly applies matching upcasters (keyed by `SourceType`) until no further upcaster matches the current payload type, so `V1 → V2 → V3` migrations compose automatically as long as each step is registered. Upcasting happens transparently inside `FetchStreamAsync` and `FetchGlobalEventsAsync` on [`CoreEventStore`](src/Aquila.Core/Sessions/QuerySession.cs) — application code, aggregates, and projections only ever see the latest event shape.

---

## 12. Aggregate Snapshots

For long-lived streams, replaying every event on every rehydration becomes expensive. Aquila provides seamless, automatic point-in-time snapshotting driven by configurable per-aggregate thresholds:

```csharp
using Aquila.Core.Events;

// Automatically persist a snapshot every 50 events for OrderAggregate
options.Events.SnapshotEvery<OrderAggregate>(threshold: 50);

// Or register a custom snapshot evaluation strategy
options.Events.RegisterSnapshotStrategy<OrderAggregate>(new DefaultSnapshotStrategy<OrderAggregate>(threshold: 50));
```

### Seamless Automatic Persistence
When `session.SaveChangesAsync()` is called, Aquila automatically evaluates if the committed stream reached the configured threshold. If so, it rehydrates the aggregate up to the current version and persists the snapshot to the configured Snapshots storage container seamlessly without requiring explicit snapshot save calls in application code.

### Manual Snapshot Persistence
You can also manually save snapshots at any time:
```csharp
await storageProvider.Events.SaveSnapshotAsync(streamId, version: 50, aggregate);
```

`AggregateStreamAsync<TAggregate>` automatically checks for an existing snapshot via `GetSnapshotAsync` before replaying: if a snapshot exists at or below the requested target version, Aquila rehydrates from the snapshot and only replays events *after* the snapshot's version, rather than the whole stream from version `0`. Both [`InMemoryStorageProvider`](src/Aquila.Core/Storage/InMemoryStorageProvider.cs#L386) and [`CosmosStorageProvider`](src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs#L418) implement snapshot persistence — on Cosmos DB with segregated storage, snapshots live cleanly in their own dedicated container.

---

## 13. Compiled Queries

For query shapes that are executed repeatedly with different parameter values (e.g. "find active customers in region X"), [`ICompiledQuery<TDoc, TResult>`](src/Aquila.Core/Queries/ICompiledQuery.cs) lets you define a reusable, parameterized LINQ query once and have its expression tree compiled and cached on first use.

```csharp
using System.Linq;
using Aquila.Core.Queries;

public class CustomersByRegion : ICompiledQuery<Customer, IQueryable<Customer>>
{
    public string Region { get; }
    public CustomersByRegion(string region) => Region = region;

    public Expression<Func<IQueryable<Customer>, IQueryable<Customer>>> QueryIs() =>
        customers => customers.Where(c => c.Region == Region);
}

// Usage
var eastCoast = await session.QueryAsync(new CustomersByRegion("US-East"));
```

`IQuerySession.QueryAsync<TDoc, TResult>(ICompiledQuery<TDoc, TResult> query)` loads all documents of type `TDoc` for the session's tenant, then executes the query via [`CompiledQueryCache.Execute`](src/Aquila.Core/Queries/CompiledQueryCache.cs). The first execution of a given `ICompiledQuery` type compiles its `QueryIs()` expression into a cached delegate keyed by the query's `Type`; a `QueryParameterBindingVisitor` rewrites closed-over instance-field/property references (like `Region` above) into a parameter so the *compiled delegate* is reused across instances with different parameter values — only the LINQ compilation cost is paid once per query type, not once per call.

---

## 14. Correlation, Causation & Custom Headers

Sessions carry optional `CorrelationId`, `CausationId`, and an arbitrary `Headers` bag that are propagated onto every event envelope appended during that session — useful for distributed tracing and audit trails across event-driven workflows.

```csharp
using var session = store.OpenSession();

session.CorrelationId = httpContext.TraceIdentifier;
session.CausationId = incomingCommandId;
session.SetHeader("initiated-by", "billing-service");

session.Events.Append(streamId, new PaymentProcessed(streamId, 150.00m));
await session.SaveChangesAsync();
```

Each appended [`IEvent`](src/Aquila.Core/Events/IEvent.cs) envelope inherits the session's `CorrelationId`/`CausationId`/`Headers` at the moment of `Append`/`StartStream` (see [`CoreEventStore.ApplyHeaders`](src/Aquila.Core/Sessions/QuerySession.cs#L120)), falling back to values already present on the source event object (e.g. from a prior upcast) when the session does not set its own.
