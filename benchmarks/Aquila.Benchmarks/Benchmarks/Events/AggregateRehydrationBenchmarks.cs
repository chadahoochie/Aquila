using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Sessions;

namespace Aquila.Benchmarks.Benchmarks.Events;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class AggregateRehydrationBenchmarks
{
    private IDocumentStore _storeNoSnapshots = null!;
    private IDocumentStore _storeWithSnapshots = null!;

    private readonly Dictionary<int, string> _noSnapshotStreamIds = new();
    private readonly Dictionary<int, string> _withSnapshotStreamIds = new();

    [Params(10, 50, 200, 500)]
    public int StreamLength { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _storeNoSnapshots = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
        });

        _storeWithSnapshots = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Events.SnapshotEvery<OrderAggregate>(50);
        });

        var lengths = new[] { 10, 50, 200, 500 };

        foreach (var length in lengths)
        {
            // Seed stream without snapshots
            var noSnapStreamId = $"no-snap-{length}-{Guid.NewGuid():N}";
            _noSnapshotStreamIds[length] = noSnapStreamId;
            var events = BenchmarkDataGenerator.CreateEventSequence(noSnapStreamId, "CUST-001", length);

            using (var session = _storeNoSnapshots.OpenSession(TrackingMode.Lightweight))
            {
                session.Events.StartStream<OrderAggregate>(noSnapStreamId, events.ToArray());
                await session.SaveChangesAsync();
            }

            // Seed stream with snapshots (batches of 10 to trigger snapshot checks if needed, or single batch)
            var withSnapStreamId = $"with-snap-{length}-{Guid.NewGuid():N}";
            _withSnapshotStreamIds[length] = withSnapStreamId;
            var withSnapEvents = BenchmarkDataGenerator.CreateEventSequence(withSnapStreamId, "CUST-001", length);

            using (var session = _storeWithSnapshots.OpenSession(TrackingMode.Lightweight))
            {
                session.Events.StartStream<OrderAggregate>(withSnapStreamId, withSnapEvents.ToArray());
                await session.SaveChangesAsync();
            }
        }
    }

    [Benchmark(Description = "AggregateStreamAsync (Full Replay, No Snapshots)")]
    public async Task<OrderAggregate?> Rehydrate_NoSnapshot()
    {
        using var session = _storeNoSnapshots.OpenSession(TrackingMode.Lightweight);
        var streamId = _noSnapshotStreamIds[StreamLength];
        return await session.Events.AggregateStreamAsync<OrderAggregate>(streamId);
    }

    [Benchmark(Description = "AggregateStreamAsync (Snapshot Accelerated)")]
    public async Task<OrderAggregate?> Rehydrate_WithSnapshot()
    {
        using var session = _storeWithSnapshots.OpenSession(TrackingMode.Lightweight);
        var streamId = _withSnapshotStreamIds[StreamLength];
        return await session.Events.AggregateStreamAsync<OrderAggregate>(streamId);
    }
}
