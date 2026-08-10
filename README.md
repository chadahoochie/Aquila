# Aquila Framework

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](#building--testing)
[![.NET](https://img.shields.io/badge/.NET-10.0-blue)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-green)](#)

**Aquila** is a high-performance, Cosmos DB native Document Store and Event Sourcing framework for .NET, inspired by MartenDB. Designed from the ground up for modern cloud-native architectures, Aquila enables seamless document persistence, atomic batch mutations, event stream management, live projections, and multi-tenant isolation over Azure Cosmos DB or in-memory test providers.

---

## Key Features

- 🌌 **Cosmos DB Native**: Direct connection mode support optimized for Azure Cosmos DB container structure using single-container partitioning (`/pk`) and hierarchical partition keys via `PartitionKeyBuilder`.
- ⚡ **1-RU Point Reads**: High-efficiency point reads (`LoadAsync<T>`) executing direct `ReadItemAsync` operations requiring ~1 Request Unit.
- 🔌 **Decoupled Storage SPI**: Independent document ([`IDocumentStorageProvider`](src/Aquila.Core/Storage/StorageContracts.cs#L78)) and event ([`IEventStorageProvider`](src/Aquila.Core/Storage/StorageContracts.cs#L92)) storage engines, supported by [`CosmosStorageProvider`](src/Aquila.Cosmos/Storage/CosmosStorageProvider.cs) (`CosmosDocumentStorageProvider` & `CosmosEventStorageProvider`) for cloud persistence and [`InMemoryStorageProvider`](src/Aquila.Core/Storage/InMemoryStorageProvider.cs) for testing.
- 📜 **Event Sourcing**: First-class stream append operations (`StartStream`, `Append`), expected version concurrency checks, stream fetching (`FetchStreamAsync`), and aggregate rehydration (`AggregateStreamAsync`).
- 🔁 **Event Upcasting & Snapshotting**: Transparent schema evolution via [`IEventUpcaster`](src/Aquila.Core/Events/IEventUpcaster.cs) chains, plus [`ISnapshotStrategy<TAggregate>`](src/Aquila.Core/Events/ISnapshotStrategy.cs)-driven aggregate snapshots to avoid full-stream replay on rehydration.
- 📊 **Projections**: Read-model generation via [`SingleStreamProjection<TAggregate>`](src/Aquila.Core/Projections/SingleStreamProjection.cs) and [`MultiStreamProjection<TDoc,TId>`](src/Aquila.Core/Projections/MultiStreamProjection.cs), offering `Inline` (transaction-scoped), `Async` (background daemon), and `Live` (on-the-fly, unpersisted) execution lifecycles.
- 🛰️ **Async Projection Daemon**: A background [`IProjectionDaemon`](src/Aquila.Core/Projections/Daemon/IProjectionDaemon.cs) with durable checkpointing, `CatchUpAsync()`, and zero-downtime `RebuildProjectionAsync()` — plus a Cosmos DB Change Feed-aware variant ([`CosmosProjectionDaemon`](src/Aquila.Cosmos/Projections/CosmosProjectionDaemon.cs)) via `AddCosmosDaemon()`.
- ✂️ **Partial Document Patching**: Fluent [`IPatchExpression<T>`](src/Aquila.Core/Patching/IPatchExpression.cs) API (`Set`, `Increment`, `Append`, `Remove`) for low-payload mutations that skip full read-modify-write round-trips, executed server-side on Cosmos DB via the Patch API.
- 🧮 **Compiled Queries**: [`ICompiledQuery<TDoc,TResult>`](src/Aquila.Core/Queries/ICompiledQuery.cs) with a `CompiledQueryCache` that compiles each query shape's expression tree once and reuses it across parameter values.
- 🎛️ **Session Tracking Modes**: Choose `Lightweight`, `IdentityMap`, or `DirtyTracking` per session to control identity-map caching and automatic JSON-snapshot dirty checking.
- 🏢 **Multi-Tenant Isolation**: Tenant scoping enforced natively at session, document envelope, query, and event store levels.
- 🛡️ **Built-in Safety & Performance**:
  - Compiled Expression Trees for zero-reflection event instantiation, property copying, ID selector resolution, upcast envelope creation, and compiled-query execution.
  - Automatic document state snapshotting on `Store()` to prevent post-store object mutations.
  - Sync-over-async thread starvation protection (blocking synchronous queries throw `NotSupportedException`).

---

## ASP.NET Core Dependency Injection Setup

Register Aquila in your `Program.cs` or `Startup.cs` using the fluent `AddAquila` extension method.

### Azure Cosmos DB Provider

```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;
using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAquila(options =>
{
    // Configure Cosmos DB storage provider
    options.UseCosmos(
        connectionString: builder.Configuration.GetConnectionString("CosmosDb")!,
        databaseName: "ProductionDB",
        containerName: "AquilaDocuments"
    );

    // Configure document identity and partition key policies
    options.Schema.For<UserAccount>()
        .Identity(u => u.Id)
        .PartitionKey(u => u.Region);

    // Register projections
    options.Projections.Add<UserProfileProjection>(ProjectionLifecycle.Inline);
});
```

### In-Memory Storage Provider (Testing / Local Dev)

```csharp
builder.Services.AddAquila(options =>
{
    options.UseInMemoryStorage();
    options.DefaultTenantId = "dev-tenant";
});
```

---

## Quickstart Guide

### 1. Document CRUD Operations

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

        // 1-RU Point Read by Id and Partition Key
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

### 2. Event Sourcing & Aggregate Rehydration

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

    // Start a new event stream
    session.Events.StartStream<OrderAggregate>(orderId,
        new OrderPlaced(orderId, "CUST-100", 99.99m),
        new ItemAdded(orderId, "SKU-ABC", 25.00m)
    );
    await session.SaveChangesAsync();

    // Append subsequent events with concurrency check (expectedVersion)
    session.Events.Append(orderId, expectedVersion: 2, new OrderCompleted(orderId, DateTimeOffset.UtcNow));
    await session.SaveChangesAsync();

    // Rehydrate state by replaying event stream
    var order = await session.Events.AggregateStreamAsync<OrderAggregate>(orderId);
    Console.WriteLine($"Order {order?.Id} Total: ${order?.Total}, Completed: {order?.IsCompleted}");
}
```

### 3. Projections

```csharp
// Define SingleStreamProjection for read-model generation
public class OrderSummaryProjection : SingleStreamProjection<OrderAggregate>
{
    public OrderSummaryProjection()
    {
        CreateEvent<OrderPlaced>(e => new OrderAggregate
        {
            Id = e.OrderId,
            CustomerId = e.CustomerId,
            Total = e.TotalAmount
        });

        ProjectEvent<ItemAdded>((e, agg) => agg.Total += e.Price);
        ProjectEvent<OrderCompleted>((e, agg) => agg.IsCompleted = true);
    }
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

- 📐 [Architecture Guide](ARCHITECTURE.md) - Detailed sitemap, SPI storage engine specs, sequence diagrams, and security controls.
- 📖 [Usage & Features Guide](USAGE.md) - Deep dive into document mapping, soft deletes, optimistic concurrency, event sourcing, projections (single/multi-stream/live/async), the projection daemon, patching, upcasting, snapshotting, compiled queries, session tracking modes, and multi-tenancy.

---

## License

This project is licensed under the MIT License.
