using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Benchmarks.Benchmarks.Events;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class EventStoreAppendBenchmarks
{
    private IDocumentStore _store = null!;
    private IEventStorageProvider _eventStorage = null!;
    private List<object> _events5 = null!;
    private List<object> _events50 = null!;
    private List<object> _events200 = null!;
    private List<IEvent> _wrappedEvents5 = null!;
    private List<IEvent> _wrappedEvents50 = null!;
    private List<IEvent> _wrappedEvents200 = null!;

    [Params(5, 50, 200)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _store = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
        });

        _eventStorage = _store.Options.EventStorage!;
        var sampleStreamId = "sample-stream";

        _events5 = BenchmarkDataGenerator.CreateEventSequence(sampleStreamId, "CUST-001", 5);
        _events50 = BenchmarkDataGenerator.CreateEventSequence(sampleStreamId, "CUST-001", 50);
        _events200 = BenchmarkDataGenerator.CreateEventSequence(sampleStreamId, "CUST-001", 200);

        _wrappedEvents5 = _events5.Select((e, idx) => (IEvent)new EventEnvelope<object>
        {
            Id = Guid.NewGuid(),
            StreamId = sampleStreamId,
            Version = idx + 1,
            EventType = e.GetType().Name,
            Data = e,
            Timestamp = DateTime.UtcNow
        }).ToList();

        _wrappedEvents50 = _events50.Select((e, idx) => (IEvent)new EventEnvelope<object>
        {
            Id = Guid.NewGuid(),
            StreamId = sampleStreamId,
            Version = idx + 1,
            EventType = e.GetType().Name,
            Data = e,
            Timestamp = DateTime.UtcNow
        }).ToList();

        _wrappedEvents200 = _events200.Select((e, idx) => (IEvent)new EventEnvelope<object>
        {
            Id = Guid.NewGuid(),
            StreamId = sampleStreamId,
            Version = idx + 1,
            EventType = e.GetType().Name,
            Data = e,
            Timestamp = DateTime.UtcNow
        }).ToList();
    }

    private List<object> GetEventsForBatch() => BatchSize switch
    {
        5 => _events5,
        50 => _events50,
        200 => _events200,
        _ => _events5
    };

    private List<IEvent> GetWrappedEventsForBatch() => BatchSize switch
    {
        5 => _wrappedEvents5,
        50 => _wrappedEvents50,
        200 => _wrappedEvents200,
        _ => _wrappedEvents5
    };

    [Benchmark(Description = "StartStream + SaveChangesAsync")]
    public async Task StartStream_SaveChangesAsync()
    {
        var newStreamId = $"new-stream-{Guid.NewGuid():N}";
        var events = GetEventsForBatch();

        using var session = _store.OpenSession(TrackingMode.Lightweight);
        session.Events.StartStream<OrderAggregate>(newStreamId, events.ToArray());
        await session.SaveChangesAsync();
    }

    [Benchmark(Description = "Append to Stream + SaveChangesAsync")]
    public async Task Append_SaveChangesAsync()
    {
        var streamId = $"append-stream-{Guid.NewGuid():N}";
        var events = GetEventsForBatch();

        using var session = _store.OpenSession(TrackingMode.Lightweight);
        // Start stream
        session.Events.StartStream<OrderAggregate>(streamId, events[0]);
        await session.SaveChangesAsync();

        // Append remaining
        if (events.Count > 1)
        {
            using var appendSession = _store.OpenSession(TrackingMode.Lightweight);
            appendSession.Events.Append(streamId, expectedVersion: 1, events.Skip(1).ToArray());
            await appendSession.SaveChangesAsync();
        }
    }

    [Benchmark(Description = "StorageProvider Direct AppendEventsAsync")]
    public async Task StorageProvider_DirectAppend()
    {
        var streamId = $"spi-stream-{Guid.NewGuid():N}";
        var wrappedEvents = GetWrappedEventsForBatch();
        await _eventStorage.AppendEventsAsync(streamId, wrappedEvents, expectedVersion: -1);
    }
}
