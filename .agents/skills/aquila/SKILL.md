---
name: aquila
description: Enforces architectural standards, configuration patterns, document storage, event sourcing, projections, patching, compiled queries, and multi-tenant isolation for the Aquila Framework (.NET 10 Cosmos DB native document store & event sourcing). Activate this skill when writing, refactoring, or reviewing code using Aquila.
---

# Aquila Framework Architecture & Coding Standards

This skill defines the mandatory architectural rules, implementation patterns, performance guidelines, and testing conventions for applications using or extending the **Aquila Framework**—a high-performance, Azure Cosmos DB native Document Store and Event Sourcing engine for .NET 10.

---

## 1. Architectural Overview & Component Structure

Aquila decouples business domain semantics (sessions, units-of-work, aggregates, and projections) from physical storage engines using a pluggable Service Provider Interface (SPI):

- **`Aquila.Core`**: Target framework `net10.0`. Core abstractions ([`IDocumentStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L86), [`IDocumentSession`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L59), [`IQuerySession`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L35), [`IEventStore`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L14)), session lifecycle & tracking modes ([`TrackingMode`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Sessions/TrackingMode.cs)), mapping policies ([`SchemaPolicy`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Configuration/StoreOptions.cs#L85)), compiled query caching ([`CompiledQueryCache`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Queries/CompiledQueryCache.cs)), patch expressions ([`IPatchExpression<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Patching/IPatchExpression.cs)), event upcasting ([`IEventUpcaster`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/IEventUpcaster.cs)), snapshot strategies ([`ISnapshotStrategy<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Events/ISnapshotStrategy.cs)), projections ([`SingleStreamProjection<T>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/SingleStreamProjection.cs), [`MultiStreamProjection<TDoc, TId>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/MultiStreamProjection.cs)), background daemon ([`IProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/Daemon/IProjectionDaemon.cs)), and built-in [`InMemoryStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/InMemoryStorageProvider.cs).
- **`Aquila.Cosmos`**: Target framework `net10.0`. Cosmos DB SPI storage providers ([`CosmosDocumentStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosDocumentStorageProvider.cs), [`CosmosEventStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosEventStorageProvider.cs), [`CosmosStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs)), partition key builder ([`CosmosPartitionKeyHelper`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosPartitionKeyHelper.cs)), LINQ rewriter ([`CosmosExpressionRewriter`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Storage/CosmosExpressionRewriter.cs)), Change Feed daemon ([`CosmosProjectionDaemon`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Cosmos/Projections/CosmosProjectionDaemon.cs)), and ASP.NET Core DI extensions (`AddAquila`, `UseCosmos`, `AddCosmosDaemon`).

### Storage SPI Contracts

All storage backends implement the SPI contracts defined in [`StorageContracts.cs`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs):
- [`IDocumentStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L78): Atomic reads, queries, upserts, deletes, and batch execution of `StorageOperation`s.
- [`IEventStorageProvider`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Storage/StorageContracts.cs#L92): Append stream events, fetch streams/global sequences, get stream headers, save/get aggregate snapshots.

---

## 2. Dependency Injection & Configuration Patterns

Register Aquila in `Program.cs` or service initialization extensions using fluent builder calls:

### Azure Cosmos DB Provider
```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;
using Microsoft.Extensions.DependencyInjection;

services.AddAquila(options =>
{
    // Cosmos DB storage provider configuration
    options.UseCosmos(
        connectionString: configuration.GetConnectionString("CosmosDb")!,
        databaseName: "ProductionDB",
        containerName: "AquilaDocuments"
    );

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

### In-Memory Storage Provider (Testing & Local Development)
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

## 3. Session Tracking Modes & Unit of Work Rules

Choose session tracking modes based on operational needs:

| Tracking Mode | Identity Map | Dirty Checking | Primary Use Case |
| :--- | :--- | :--- | :--- |
| `TrackingMode.Lightweight` | Disabled | Disabled | High-performance read-only queries or isolated writes where instance tracking is unneeded. `LoadAsync` always re-fetches from storage. |
| `TrackingMode.IdentityMap` | Enabled | Disabled | Workflows where identical document identity must return the same CLR instance within a session, but mutations require explicit `session.Store()` calls. |
| `TrackingMode.DirtyTracking` (Default) | Enabled | Automatic (JSON diff) | Standard load-mutate-`SaveChangesAsync()` unit of work. Entities are snapshotted at load/store time; `SaveChangesAsync()` auto-detects diffs and queues updates without explicit `Store()` calls. |

### Crucial Execution Rules:
1. **Never Invoke Synchronous `Query<T>()`**: Calling synchronous `session.Query<T>()` throws `NotSupportedException` to prevent thread-pool starvation in async applications. Always use `await session.QueryAsync<T>(...)`.
2. **Document State Snapshotting on `Store(doc)`**: `session.Store()` immediately serializes and snapshots the object state. Subsequent mutations to the original entity instance in application code will NOT pollute the pending unit-of-work state prior to `SaveChangesAsync()`.
3. **Session Lifecycle Management**: All sessions implement `IDisposable` and `IAsyncDisposable`. Always wrap session usage in `using var session = store.OpenSession()` or `await using var session = store.OpenSession()`.

---

## 4. Document CRUD & 1-RU Point Reads

```csharp
// 1-RU Point Read (Direct Cosmos DB ReadItemAsync optimization using Id + PartitionKey)
var customer = await session.LoadAsync<Customer>("C-100", partitionKey: "US-East");

// Storing Documents (DirtyTracking auto-detects changes on SaveChangesAsync)
session.Store(new Customer { Id = "C-101", Name = "Acme Corp", Region = "US-West" });
await session.SaveChangesAsync();

// Async Querying with Predicate Pushdown
var customers = await session.QueryAsync<Customer>(c => c.Data.Region == "US-East");

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

1. **`ProjectionLifecycle.Inline`**: Executes synchronously inside `SaveChangesAsync()` within the same storage transaction commit.
2. **`ProjectionLifecycle.Async`**: Processed in background batches by the `IProjectionDaemon`.
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

### MultiStreamProjection
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

// Zero-Downtime Rebuild: Deletes read-model documents, resets checkpoint sequence to 0, and replays full event history
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

## 8. Compiled Queries & Expression Caching

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

## 9. Event Upcasting (Schema Evolution)

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

## 10. Multi-Tenancy, Tracing & Data Isolation Rules

1. **Tenant Isolation**: Every `DocumentEnvelope<T>` and event stream header includes an immutable `TenantId`. Cross-tenant queries return `null` or filter out unauthorized records.
2. **Cosmos DB SQL Injection Safety**: `CosmosStorageProvider` executes all event/stream queries using parameterized `QueryDefinition` instances (`@streamId`, `@fromVersion`, `@tenantId`).
3. **Hierarchical Partition Keys**: For multi-level partition key routing, use pipe-delimited strings (`"TenantA|Region1|Dept5"`). `CosmosPartitionKeyHelper` splits on `'|'` and invokes Cosmos DB's `PartitionKeyBuilder`.
4. **Tracing Propagation**: Assign `session.CorrelationId`, `session.CausationId`, and `session.SetHeader("key", val)`. Headers automatically flow onto every `IEvent` envelope generated within that session.

---

## 11. Code Quality & Performance Checklist

- [ ] Are internal/public infrastructure classes explicitly marked `sealed` for JIT devirtualization?
- [ ] Are hot execution paths free of runtime reflection (using compiled expression delegates)?
- [ ] Are all LINQ queries asynchronous (no sync-over-async blocking via `.Query<T>()`)?
- [ ] Are null checks enforced on all method inputs (`ArgumentNullException.ThrowIfNull`)?
- [ ] Do all automated tests pass via `dotnet test Aquila.slnx`?
