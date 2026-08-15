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
public class EventUpcastingBenchmarks
{
    private UpcasterRegistry _registryEmpty = null!;
    private UpcasterRegistry _registrySingleStep = null!;
    private UpcasterRegistry _registryChained = null!;

    private IEvent _envelopeV1 = null!;
    private IEvent _envelopeV2 = null!;
    private IEvent _envelopeV3 = null!;

    private IDocumentStore _storeNoUpcast = null!;
    private IDocumentStore _storeSingleUpcast = null!;
    private IDocumentStore _storeChainedUpcast = null!;

    private string _streamId = null!;

    private const int EventCount = 100;

    [GlobalSetup]
    public async Task Setup()
    {
        _registryEmpty = new UpcasterRegistry();

        _registrySingleStep = new UpcasterRegistry();
        _registrySingleStep.Register(new OrderCreatedV1ToV2Upcaster());

        _registryChained = new UpcasterRegistry();
        _registryChained.Register(new OrderCreatedV1ToV2Upcaster());
        _registryChained.Register(new OrderCreatedV2ToV3Upcaster());

        var v1Data = new OrderCreatedV1("ORD-001", "CUST-001", 99.99m);
        var v2Data = new OrderCreatedV2("ORD-001", "CUST-001", 99.99m, "USD");
        var v3Data = new OrderCreatedV3("ORD-001", "CUST-001", 99.99m, "USD", "Web");

        _envelopeV1 = new EventEnvelope<OrderCreatedV1>
        {
            Id = Guid.NewGuid(),
            StreamId = "stream-001",
            Version = 1,
            EventType = nameof(OrderCreatedV1),
            Data = v1Data,
            Timestamp = DateTime.UtcNow
        };

        _envelopeV2 = new EventEnvelope<OrderCreatedV2>
        {
            Id = Guid.NewGuid(),
            StreamId = "stream-001",
            Version = 1,
            EventType = nameof(OrderCreatedV2),
            Data = v2Data,
            Timestamp = DateTime.UtcNow
        };

        _envelopeV3 = new EventEnvelope<OrderCreatedV3>
        {
            Id = Guid.NewGuid(),
            StreamId = "stream-001",
            Version = 1,
            EventType = nameof(OrderCreatedV3),
            Data = v3Data,
            Timestamp = DateTime.UtcNow
        };

        _streamId = $"upcast-stream-{Guid.NewGuid():N}";

        _storeNoUpcast = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
        });

        _storeSingleUpcast = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Events.RegisterUpcaster<OrderCreatedV1ToV2Upcaster>();
        });

        _storeChainedUpcast = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Events.RegisterUpcaster<OrderCreatedV1ToV2Upcaster>();
            options.Events.RegisterUpcaster<OrderCreatedV2ToV3Upcaster>();
        });

        // Seed 100 V1 events in all 3 stores
        var v1Events = new List<object>(EventCount);
        for (int i = 0; i < EventCount; i++)
        {
            v1Events.Add(new OrderCreatedV1($"ORD-{i:D4}", $"CUST-{i:D4}", 100m + i));
        }

        using (var session = _storeNoUpcast.OpenSession(TrackingMode.Lightweight))
        {
            session.Events.StartStream<OrderAggregate>(_streamId, v1Events.ToArray());
            await session.SaveChangesAsync();
        }

        using (var session = _storeSingleUpcast.OpenSession(TrackingMode.Lightweight))
        {
            session.Events.StartStream<OrderAggregate>(_streamId, v1Events.ToArray());
            await session.SaveChangesAsync();
        }

        using (var session = _storeChainedUpcast.OpenSession(TrackingMode.Lightweight))
        {
            session.Events.StartStream<OrderAggregate>(_streamId, v1Events.ToArray());
            await session.SaveChangesAsync();
        }
    }

    [Benchmark(Description = "Registry Direct Upcast (No-Op Identity)")]
    public IEvent Registry_NoOp()
    {
        return _registryEmpty.Upcast(_envelopeV1);
    }

    [Benchmark(Description = "Registry Direct Upcast (Single Step V1 -> V2)")]
    public IEvent Registry_SingleStep()
    {
        return _registrySingleStep.Upcast(_envelopeV1);
    }

    [Benchmark(Description = "Registry Direct Upcast (Chained V1 -> V2 -> V3)")]
    public IEvent Registry_Chained()
    {
        return _registryChained.Upcast(_envelopeV1);
    }

    [Benchmark(Description = "FetchStream 100 Events (No Upcasting)")]
    public async Task<IReadOnlyList<IEvent>> FetchStream_NoUpcasting()
    {
        using var session = _storeNoUpcast.OpenSession(TrackingMode.Lightweight);
        return await session.Events.FetchStreamAsync(_streamId);
    }

    [Benchmark(Description = "FetchStream 100 Events (Single Step Upcasting V1 -> V2)")]
    public async Task<IReadOnlyList<IEvent>> FetchStream_SingleStepUpcasting()
    {
        using var session = _storeSingleUpcast.OpenSession(TrackingMode.Lightweight);
        return await session.Events.FetchStreamAsync(_streamId);
    }

    [Benchmark(Description = "FetchStream 100 Events (Chained Upcasting V1 -> V2 -> V3)")]
    public async Task<IReadOnlyList<IEvent>> FetchStream_ChainedUpcasting()
    {
        using var session = _storeChainedUpcast.OpenSession(TrackingMode.Lightweight);
        return await session.Events.FetchStreamAsync(_streamId);
    }
}
