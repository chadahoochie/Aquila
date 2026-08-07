# Aquila Usage & Feature Guide

This guide provides detailed documentation and working code examples for all core features of the **Aquila** framework.

---

## 1. Document Mapping Policies

Aquila allows you to customize document identity, partition key routing, soft delete behavior, and optimistic concurrency rules using the fluent [`SchemaPolicy`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Configuration/StoreOptions.cs#L85) API.

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

---

## 2. Soft Delete Management

Aquila supports soft deletes out of the box. Soft-deleted documents remain stored in the underlying persistence layer with `IsDeleted = true` but are automatically filtered out of all [`LoadAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L39), [`LoadManyAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L41), and [`QueryAsync`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L44) operations.

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

When appending events with an explicit `expectedVersion`, Aquila validates that the stream's current version matches `expectedVersion`. If a mismatch occurs, an [`AquilaConcurrencyException`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Exceptions/AquilaException.cs) is raised.

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

Aquila provides an event store subsystem accessed via [`session.Events`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Abstractions/IDocumentStore.cs#L37).

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

Inherit from [`SingleStreamProjection<TAggregate>`](file:///home/chad/source/dotnet/Aquila/src/Aquila.Core/Projections/SingleStreamProjection.cs#L36) and register handlers in the constructor using `CreateEvent` and `ProjectEvent`.

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
