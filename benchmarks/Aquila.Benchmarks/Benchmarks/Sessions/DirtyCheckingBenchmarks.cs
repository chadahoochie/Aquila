using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Benchmarks.Benchmarks.Sessions;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class DirtyCheckingBenchmarks
{
    private IDocumentStore _store = null!;
    private CustomerProfileDocument _smallDoc = null!;
    private OrderDocument _largeDoc = null!;

    private List<CustomerProfileDocument> _customers0Pct = null!;
    private List<CustomerProfileDocument> _customers50Pct = null!;
    private List<CustomerProfileDocument> _customers100Pct = null!;

    private List<OrderDocument> _orders0Pct = null!;
    private List<OrderDocument> _orders50Pct = null!;
    private List<OrderDocument> _orders100Pct = null!;

    private IdentityMap _identityMap0Pct = null!;
    private IdentityMap _identityMap50Pct = null!;
    private IdentityMap _identityMap100Pct = null!;

    private const int DocumentCount = 100;

    [GlobalSetup]
    public void Setup()
    {
        _store = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Schema.For<CustomerProfileDocument>()
                .Identity(c => c.Id)
                .PartitionKey(c => c.Region);
            options.Schema.For<OrderDocument>()
                .Identity(o => o.Id)
                .PartitionKey(o => o.Region);
        });

        _smallDoc = BenchmarkDataGenerator.CreateCustomer(1);
        _largeDoc = BenchmarkDataGenerator.CreateOrder(1, itemCount: 5);

        _customers0Pct = BenchmarkDataGenerator.CreateCustomers(DocumentCount);
        _customers50Pct = BenchmarkDataGenerator.CreateCustomers(DocumentCount);
        _customers100Pct = BenchmarkDataGenerator.CreateCustomers(DocumentCount);

        _orders0Pct = BenchmarkDataGenerator.CreateOrders(DocumentCount);
        _orders50Pct = BenchmarkDataGenerator.CreateOrders(DocumentCount);
        _orders100Pct = BenchmarkDataGenerator.CreateOrders(DocumentCount);

        // Populate Identity Maps
        _identityMap0Pct = new IdentityMap();
        _identityMap50Pct = new IdentityMap();
        _identityMap100Pct = new IdentityMap();

        for (int i = 0; i < DocumentCount; i++)
        {
            var cust0 = _customers0Pct[i];
            var env0 = new DocumentEnvelope<CustomerProfileDocument> { Id = cust0.Id, PartitionKey = cust0.Region, Data = cust0 };
            _identityMap0Pct.Track(cust0.Id, cust0, env0, recordSnapshot: true);

            var cust50 = _customers50Pct[i];
            var env50 = new DocumentEnvelope<CustomerProfileDocument> { Id = cust50.Id, PartitionKey = cust50.Region, Data = cust50 };
            _identityMap50Pct.Track(cust50.Id, cust50, env50, recordSnapshot: true);

            var cust100 = _customers100Pct[i];
            var env100 = new DocumentEnvelope<CustomerProfileDocument> { Id = cust100.Id, PartitionKey = cust100.Region, Data = cust100 };
            _identityMap100Pct.Track(cust100.Id, cust100, env100, recordSnapshot: true);
        }

        // Apply mutations
        for (int i = 0; i < DocumentCount; i++)
        {
            if (i % 2 == 0)
            {
                _customers50Pct[i].Name = $"Mutated Name {i}";
                _customers50Pct[i].LoginCount++;
            }

            _customers100Pct[i].Name = $"Mutated Name {i}";
            _customers100Pct[i].LoginCount += 5;
        }
    }

    [Benchmark(Description = "Snapshot Baseline (Small Document UTF8)")]
    public byte[] Snapshot_SmallDocument()
    {
        return JsonSerializer.SerializeToUtf8Bytes(_smallDoc);
    }

    [Benchmark(Description = "Snapshot Baseline (Large Document UTF8)")]
    public byte[] Snapshot_LargeDocument()
    {
        return JsonSerializer.SerializeToUtf8Bytes(_largeDoc);
    }

    [Benchmark(Description = "Dirty Diff Check (100 Tracked Entities, 0% Mutated)")]
    public int DiffCheck_0Percent_Mutated()
    {
        int dirtyCount = 0;
        var trackedList = _identityMap0Pct.GetTrackedEntities();
        for (int i = 0; i < trackedList.Count; i++)
        {
            var item = trackedList[i];
            if (item.Snapshot == null) continue;

            var currentBytes = JsonSerializer.SerializeToUtf8Bytes(item.Entity, item.EntityType);
            if (!currentBytes.AsSpan().SequenceEqual(item.Snapshot.AsSpan()))
            {
                dirtyCount++;
            }
        }
        return dirtyCount;
    }

    [Benchmark(Description = "Dirty Diff Check (100 Tracked Entities, 50% Mutated)")]
    public int DiffCheck_50Percent_Mutated()
    {
        int dirtyCount = 0;
        var trackedList = _identityMap50Pct.GetTrackedEntities();
        for (int i = 0; i < trackedList.Count; i++)
        {
            var item = trackedList[i];
            if (item.Snapshot == null) continue;

            var currentBytes = JsonSerializer.SerializeToUtf8Bytes(item.Entity, item.EntityType);
            if (!currentBytes.AsSpan().SequenceEqual(item.Snapshot.AsSpan()))
            {
                dirtyCount++;
            }
        }
        return dirtyCount;
    }

    [Benchmark(Description = "Dirty Diff Check (100 Tracked Entities, 100% Mutated)")]
    public int DiffCheck_100Percent_Mutated()
    {
        int dirtyCount = 0;
        var trackedList = _identityMap100Pct.GetTrackedEntities();
        for (int i = 0; i < trackedList.Count; i++)
        {
            var item = trackedList[i];
            if (item.Snapshot == null) continue;

            var currentBytes = JsonSerializer.SerializeToUtf8Bytes(item.Entity, item.EntityType);
            if (!currentBytes.AsSpan().SequenceEqual(item.Snapshot.AsSpan()))
            {
                dirtyCount++;
            }
        }
        return dirtyCount;
    }

    [Benchmark(Description = "Session DirtyTracking SaveChangesAsync (0% Mutated)")]
    public async Task Session_DirtyTracking_0Percent_SaveChangesAsync()
    {
        using var session = _store.OpenSession(TrackingMode.DirtyTracking);
        for (int i = 0; i < DocumentCount; i++)
        {
            session.Store(_orders0Pct[i]);
        }
        // No mutations
        await session.SaveChangesAsync();
    }

    [Benchmark(Description = "Session DirtyTracking SaveChangesAsync (50% Mutated)")]
    public async Task Session_DirtyTracking_50Percent_SaveChangesAsync()
    {
        using var session = _store.OpenSession(TrackingMode.DirtyTracking);
        for (int i = 0; i < DocumentCount; i++)
        {
            session.Store(_orders50Pct[i]);
        }

        // Mutate 50%
        for (int i = 0; i < DocumentCount; i += 2)
        {
            _orders50Pct[i].Status = "UpdatedStatus";
            _orders50Pct[i].TotalAmount += 10.0m;
        }

        await session.SaveChangesAsync();
    }

    [Benchmark(Description = "Session DirtyTracking SaveChangesAsync (100% Mutated)")]
    public async Task Session_DirtyTracking_100Percent_SaveChangesAsync()
    {
        using var session = _store.OpenSession(TrackingMode.DirtyTracking);
        for (int i = 0; i < DocumentCount; i++)
        {
            session.Store(_orders100Pct[i]);
        }

        // Mutate 100%
        for (int i = 0; i < DocumentCount; i++)
        {
            _orders100Pct[i].Status = "UpdatedStatus";
            _orders100Pct[i].TotalAmount += 10.0m;
        }

        await session.SaveChangesAsync();
    }
}
