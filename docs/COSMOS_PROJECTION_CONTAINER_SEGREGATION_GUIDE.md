# Azure Cosmos DB Projection Container Segregation Guide

This guide provides architectural patterns, configuration recipes, performance optimizations, and cost management strategies for segregating **Aquila Read-Model Projections** (specifically [`MultiStreamProjection<TDoc, TId>`](../src/Aquila.Core/Projections/MultiStreamProjection.cs)) into dedicated Azure Cosmos DB containers.

---

## 1. Architectural Overview

When using Event Sourcing and CQRS with Azure Cosmos DB, segregating transactional write models (Events & Snapshots) from denormalized read models (Projections) guarantees **RU isolation**, **hot-partition avoidance**, **minimal write costs**, and **instant zero-downtime rebuilds**.

```mermaid
flowchart TD
    subgraph WriteSide ["Write Side (OLTP)"]
        Cmd[Client Command] --> Session[Aquila DocumentSession]
        Session -->|AppendEventsAsync| EventsContainer["Events Container\n(/TenantId|/StreamId)\nDatabase: EventsDB"]
        Session -->|Auto Snapshot| SnapshotsContainer["Snapshots Container\nDatabase: SnapshotsDB"]
        Session -->|Store / Patch| DocsContainer["Documents Container\nDatabase: ProductionDB"]
    end

    subgraph AsyncPipeline ["Async Projection Pipeline"]
        EventsContainer -->|Change Feed / Polling| Daemon["CosmosProjectionDaemon\n(Bounded Parallel Dispatcher)"]
    end

    subgraph ReadSide ["Segregated Read Side (ReadModelsDB - Shared Throughput)"]
        Daemon -->|Point Upsert| Proj1["CustomerMetrics Container\n(/id)"]
        Daemon -->|Point Upsert| Proj2["OrderSummary Container\n(/id)"]
        Daemon -->|Point Upsert| Proj3["ProductCatalogSummary Container\n(/Category)"]
    end

    subgraph QuerySide ["User Read Queries"]
        Client[Query API / UI] -->|1-RU Point Read| Proj1
        Client -->|Filtered Range Query| Proj2
    end

    classDef write fill:#e1f5fe,stroke:#0288d1,stroke-width:1.5px;
    classDef daemon fill:#ede7f6,stroke:#512da8,stroke-width:1.5px;
    classDef read fill:#e8f5e9,stroke:#2e7d32,stroke-width:1.5px;

    class Cmd,Session,EventsContainer,SnapshotsContainer,DocsContainer write;
    class Daemon AsyncPipeline daemon;
    class Proj1,Proj2,Proj3,Client QuerySide read;
```

---

## 2. Why Segregate Multi-Stream Projections?

[`MultiStreamProjection<TDoc, TId>`](../src/Aquila.Core/Projections/MultiStreamProjection.cs) aggregates events across thousands of independent event streams into a single read model document.

| Problem in Shared Container | Solution in Dedicated Segregated Container | Impact |
| :--- | :--- | :--- |
| **Noisy Neighbor / Throttling** | Dedicated throughput budget for projections isolated from user OLTP traffic. | Eliminates HTTP 429 errors on transactional operations. |
| **Default Partition Key Bottleneck** | Container is partitioned directly on `/id` or a high-cardinality property (`/CustomerId`). | Avoids the 20 GB / 10,000 RU/s physical partition bottleneck. |
| **Index Write RU Overhead** | Custom indexing policy excluding unqueried properties and large nested objects. | **30%–70% reduction** in RU cost per projection write. |
| **Slow Projection Rebuilds** | Drops and recreates the container or uses blue/green collection swaps. | Rebuild completes in seconds at **0 RU** (vs. millions of point deletes). |
| **Change Feed Feedback Loops** | Events container contains only `$event` documents; read models never touch the event feed. | Prevents Change Feed Processor from processing non-event mutations. |

---

## 3. Configuration Recipes in .NET 10

### 3.1 Recommended Pattern: Database-Level Shared Throughput

To avoid paying the 400 RU/s (or 1,000 RU autoscale) minimum for *every* individual projection container, provision throughput at the **Database level** on `ReadModelsDB`. All auto-generated projection containers share this throughput pool.

```csharp
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Configuration;
using Aquila.Cosmos.Extensions;
using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAquila(options =>
{
    // Configure default tenant
    options.DefaultTenantId = "tenant-primary";

    // Configure Cosmos DB Storage Segregation
    options.UseCosmos(builder.Configuration.GetConnectionString("CosmosDb")!, cosmos =>
    {
        cosmos.DefaultDatabase = "ProductionDB";

        // 1. Transactional Event Store
        cosmos.ConfigureEvents("EventsContainer", database: "EventsDB");

        // 2. Aggregate Snapshots Store
        cosmos.ConfigureSnapshots("SnapshotsContainer", database: "SnapshotsDB");

        // 3. Domain Documents Container
        cosmos.ConfigureDocuments("DocumentsContainer", database: "ProductionDB");

        // 4. Auto-allocate a dedicated container per projection in ReadModelsDB
        //    All projection containers share the 4,000 RU/s autoscale database pool.
        cosmos.Projections.AutoContainerPerProjection(
            database: "ReadModelsDB",
            nameFormatter: type => $"{type.Name}s",
            throughput: ThroughputSettings.Autoscale(maxRu: 4000)
        );

        // 5. Optional: Override a specific high-traffic projection with dedicated throughput
        cosmos.Projections.For<CustomerMetricsProjection>(
            container: "CustomerMetricsDedicated",
            database: "HighThroughputReadModelsDB",
            throughput: ThroughputSettings.Autoscale(maxRu: 10000)
        );
    });

    // Configure Document Schema & High-Cardinality Partition Keys for Read Models
    options.Schema.For<CustomerMetrics>()
        .Identity(m => m.CustomerId)
        .PartitionKey(m => m.CustomerId)   // Critical: Do NOT omit! Prevents single-partition hotspot.
        .UseOptimisticConcurrency(false);

    options.Schema.For<OrderSummary>()
        .Identity(o => o.OrderId)
        .PartitionKey(o => o.Region)
        .UseOptimisticConcurrency(false);

    // Register MultiStream Projections
    options.Projections.Add<CustomerMetricsProjection>(ProjectionLifecycle.Async);
    options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);
});

// Register Change Feed-aware background projection daemon
builder.Services.AddCosmosDaemon(daemonOptions =>
{
    daemonOptions.BatchSize = 250;
    daemonOptions.MaxProjectionConcurrency = 8;
    daemonOptions.MaxEventGroupConcurrency = 16;
    daemonOptions.PollingIntervalMs = 250;
});
```

---

## 4. MultiStreamProjection Implementation

### Example: `CustomerMetricsProjection`

```csharp
using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Projections;

// 1. Read Model Document POCO
public sealed class CustomerMetrics
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalSpend { get; set; }
    public int OrderCount { get; set; }
    public DateTimeOffset LastOrderDate { get; set; }
    public string Tier { get; set; } = "Bronze";
}

// 2. Domain Events
public sealed record OrderPlaced(string OrderId, string CustomerId, decimal Amount, DateTimeOffset PlacedAt);
public sealed record OrderRefunded(string OrderId, string CustomerId, decimal Amount);
public sealed record CustomerDeactivated(string CustomerId);

// 3. MultiStreamProjection Definition
public sealed class CustomerMetricsProjection : MultiStreamProjection<CustomerMetrics, string>
{
    public CustomerMetricsProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    /// <summary>
    /// Routes incoming events from multiple streams (Order streams, Account streams)
    /// to the target CustomerMetrics document ID.
    /// </summary>
    protected override string Identity(IEvent @event) =>
        @event.Data switch
        {
            OrderPlaced e => e.CustomerId,
            OrderRefunded e => e.CustomerId,
            CustomerDeactivated e => e.CustomerId,
            _ => string.Empty
        };

    /// <summary>
    /// Mutates the read model in-place. Return false to delete the document.
    /// </summary>
    public override bool Apply(IEvent @event, CustomerMetrics doc)
    {
        switch (@event.Data)
        {
            case OrderPlaced placed:
                doc.CustomerId = placed.CustomerId;
                doc.TotalSpend += placed.Amount;
                doc.OrderCount++;
                if (placed.PlacedAt > doc.LastOrderDate)
                {
                    doc.LastOrderDate = placed.PlacedAt;
                }
                UpdateTier(doc);
                return true;

            case OrderRefunded refunded:
                doc.TotalSpend -= refunded.Amount;
                UpdateTier(doc);
                return true;

            case CustomerDeactivated:
                // Returning false removes the read model document from Cosmos DB
                return false;

            default:
                return true;
        }
    }

    private static void UpdateTier(CustomerMetrics doc)
    {
        doc.Tier = doc.TotalSpend switch
        {
            >= 10000m => "Platinum",
            >= 5000m => "Gold",
            >= 1000m => "Silver",
            _ => "Bronze"
        };
    }
}
```

---

## 5. Cosmos DB Indexing Policy Optimization

Cosmos DB's default indexing policy indexes every string and number (`/*`), inflating write RU cost on every projection update. 

Since read models are primarily retrieved via point reads or specific dashboard queries, apply a **lean indexing policy** to each projection container:

### Example: `CustomerMetrics` Container Indexing Policy
```json
{
  "indexingMode": "consistent",
  "automatic": true,
  "includedPaths": [
    { "path": "/data/Tier/?" },
    { "path": "/data/TotalSpend/?" },
    { "path": "/data/LastOrderDate/?" }
  ],
  "excludedPaths": [
    { "path": "/*" },
    { "path": "/\"_etag\"/?" }
  ],
  "compositeIndexes": [
    [
      { "path": "/data/Tier", "order": "ascending" },
      { "path": "/data/TotalSpend", "order": "descending" }
    ]
  ]
}
```

### Write RU Cost Comparison
| Document Structure | Default Indexing (`/*`) | Tailored Indexing (Lean) | RU Reduction |
| :--- | :--- | :--- | :--- |
| Simple Document (10 fields) | ~11.5 RUs | ~5.8 RUs | **49.5%** |
| Complex Document (Nested arrays/objects) | ~38.0 RUs | ~12.2 RUs | **67.8%** |

---

## 6. Querying Segregated Projections (1-RU Point Reads)

When querying read models through [`IDocumentSession`](../src/Aquila.Core/Abstractions/IDocumentStore.cs#L59) or [`IQuerySession`](../src/Aquila.Core/Abstractions/IDocumentStore.cs#L35), Aquila's [`CosmosContainerResolver`](../src/Aquila.Cosmos/Storage/CosmosContainerResolver.cs) automatically resolves the correct segregated container:

```csharp
public sealed class CustomerSummaryService
{
    private readonly IDocumentStore _store;

    public CustomerSummaryService(IDocumentStore store)
    {
        _store = store;
    }

    public async Task<CustomerMetrics?> GetMetricsAsync(string customerId, CancellationToken ct = default)
    {
        // 1-RU Point Read directly routed to the CustomerMetrics container
        await using var session = _store.OpenSession(TrackingMode.Lightweight);
        return await session.LoadAsync<CustomerMetrics>(id: customerId, partitionKey: customerId, ct: ct);
    }

    public async Task<IReadOnlyList<CustomerMetrics>> GetTopPlatinumCustomersAsync(CancellationToken ct = default)
    {
        await using var session = _store.OpenSession(TrackingMode.Lightweight);
        return await session.QueryAsync<CustomerMetrics>(
            predicate: m => m.Data.Tier == "Platinum",
            orderBy: m => m.Data.TotalSpend,
            sortOrder: SortOrder.Descending,
            ct: ct
        );
    }
}
```

---

## 7. Operations & Zero-Downtime Projection Rebuilds

When evolving a projection's schema or fixing business logic, trigger a zero-downtime rebuild via [`IProjectionDaemon`](../src/Aquila.Core/Projections/Daemon/IProjectionDaemon.cs):

```csharp
var daemon = app.Services.GetRequiredService<IProjectionDaemon>();

// 1. Replay history and rebuild CustomerMetricsProjection
await daemon.RebuildProjectionAsync<CustomerMetricsProjection>();

// 2. Await full catch-up before switching traffic
await daemon.CatchUpAsync();
```

### Zero-Downtime Rebuild Workflow
1. **Reset Checkpoint**: Resets the checkpoint sequence in `IProjectionCheckpointStore` to `0`.
2. **Purge Old Read Models**: Truncates existing read models in the dedicated container.
3. **Replay Historical Events**: Change feed/daemon replays events from sequence `0` through [`MultiStreamProjection.DispatchBatchAsync`](../src/Aquila.Core/Projections/MultiStreamProjection.cs#L100-L144).
4. **Resumes Live Processing**: Seamlessly resumes processing live incoming events.

---

## 8. Cost Analysis: Standalone Containers vs. Shared Database Throughput

| Deployment Model | Container Count | Provisioning Configuration | Estimated Monthly Cost (Single Region) |
| :--- | :--- | :--- | :--- |
| **Individual Dedicated Containers** | 10 Projections | 10 × 400 RU/s manual minimum | **$233.60 / month** |
| **Individual Autoscale Containers** | 10 Projections | 10 × 1,000 RU/s max autoscale | **$584.00 / month** |
| **Shared Database Throughput (Recommended)** | 10+ Projections | **1 × 4,000 RU/s autoscale on `ReadModelsDB`** | **$116.80 – $233.60 / month** |

> [!TIP]
> **Cost Rule of Thumb**: For 4 or more projections, always configure a shared database throughput pool (`AutoContainerPerProjection("ReadModelsDB")`). This provides full physical container isolation at a fraction of the cost.

---

## 9. Production Readiness Checklist

- [ ] **Partition Key Configured**: Explicitly configured `options.Schema.For<TDoc>().PartitionKey(...)` to avoid falling back to `typeof(TDoc).Name`.
- [ ] **Shared Throughput Pool**: Enabled `AutoContainerPerProjection` inside a shared-throughput database (`ReadModelsDB`).
- [ ] **Lightweight Sessions for Point Reads**: Configured `TrackingMode.Lightweight` for read-only projection queries to bypass Identity Map tracking overhead.
- [ ] **Lean Indexing Policy Applied**: Excluded unqueried paths (`/*`) and configured composite indexes on filtered/sorted fields.
- [ ] **Daemon Parallelism Tuned**: Configured `MaxProjectionConcurrency` and `MaxEventGroupConcurrency` according to CPU and database RU budget.
