using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Sessions;

namespace Aquila.Benchmarks.Benchmarks.Sessions;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class SessionTrackingModeBenchmarks
{
    private IDocumentStore _store = null!;
    private List<OrderDocument> _preloadedOrders = null!;
    private List<OrderDocument> _batchOrders = null!;

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [Params(TrackingMode.Lightweight, TrackingMode.IdentityMap, TrackingMode.DirtyTracking)]
    public TrackingMode Mode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _store = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Schema.For<OrderDocument>()
                .Identity(o => o.Id)
                .PartitionKey(o => o.Region);
        });

        _preloadedOrders = BenchmarkDataGenerator.CreateOrders(100);
        _batchOrders = BenchmarkDataGenerator.CreateOrders(100);

        // Preload documents for LoadAsync benchmarks
        using var session = _store.OpenSession(TrackingMode.Lightweight);
        foreach (var order in _preloadedOrders)
        {
            session.Store(order);
        }
        await session.SaveChangesAsync();
    }

    [Benchmark(Description = "Store + SaveChangesAsync Batch")]
    public async Task StoreAndSaveChanges_Batch()
    {
        using var session = _store.OpenSession(Mode);
        for (int i = 0; i < BatchSize; i++)
        {
            // Use unique ID per run iteration
            var order = _batchOrders[i];
            session.Store(order);
        }
        await session.SaveChangesAsync();
    }

    [Benchmark(Description = "LoadAsync Cold (From Storage)")]
    public async Task<List<OrderDocument?>> LoadAsync_Cold()
    {
        using var session = _store.OpenSession(Mode);
        var results = new List<OrderDocument?>(BatchSize);
        for (int i = 0; i < BatchSize; i++)
        {
            var order = _preloadedOrders[i];
            var loaded = await session.LoadAsync<OrderDocument>(order.Id, order.Region);
            results.Add(loaded);
        }
        return results;
    }

    [Benchmark(Description = "LoadAsync Warm Repeated (IdentityMap Hit)")]
    public async Task<List<OrderDocument?>> LoadAsync_WarmRepeated()
    {
        using var session = _store.OpenSession(Mode);
        // First load to populate identity map (if enabled)
        for (int i = 0; i < BatchSize; i++)
        {
            var order = _preloadedOrders[i];
            await session.LoadAsync<OrderDocument>(order.Id, order.Region);
        }

        // Second load: measures IdentityMap cache hit vs repeated storage read
        var results = new List<OrderDocument?>(BatchSize);
        for (int i = 0; i < BatchSize; i++)
        {
            var order = _preloadedOrders[i];
            var loaded = await session.LoadAsync<OrderDocument>(order.Id, order.Region);
            results.Add(loaded);
        }
        return results;
    }
}
