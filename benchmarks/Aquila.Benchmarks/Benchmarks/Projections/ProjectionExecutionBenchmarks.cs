using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Benchmarks.Benchmarks.Projections;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class ProjectionExecutionBenchmarks
{
    private OrderSummaryProjection _singleProj = null!;
    private UserOrdersProjection _multiProj = null!;

    private IDocumentStore _store = null!;
    private IEvent _createdEvent = null!;
    private IEvent _itemAddedEvent = null!;
    private IEvent _discountEvent = null!;
    private IEvent _statusEvent = null!;

    private List<IEvent> _eventStream100 = null!;
    private List<IEvent> _multiStreamEvents100 = null!;

    [GlobalSetup]
    public void Setup()
    {
        _singleProj = new OrderSummaryProjection();
        _multiProj = new UserOrdersProjection();

        _store = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Schema.For<UserOrdersSummary>()
                .Identity(u => u.CustomerId)
                .PartitionKey(u => u.CustomerId);
        });

        var created = new OrderCreated("ORD-0001", "CUST-0001", 100m, "US-East", DateTime.UtcNow);
        var itemAdded = new OrderLineItemAdded("ORD-0001", "SKU-001", "Laptop Stand", 2, 29.99m);
        var discount = new OrderDiscountApplied("ORD-0001", 10m, "DISC10");
        var status = new OrderStatusUpdated("ORD-0001", "Shipped");

        _createdEvent = new EventEnvelope<OrderCreated> { StreamId = "ORD-0001", Version = 1, Data = created, EventType = nameof(OrderCreated) };
        _itemAddedEvent = new EventEnvelope<OrderLineItemAdded> { StreamId = "ORD-0001", Version = 2, Data = itemAdded, EventType = nameof(OrderLineItemAdded) };
        _discountEvent = new EventEnvelope<OrderDiscountApplied> { StreamId = "ORD-0001", Version = 3, Data = discount, EventType = nameof(OrderDiscountApplied) };
        _statusEvent = new EventEnvelope<OrderStatusUpdated> { StreamId = "ORD-0001", Version = 4, Data = status, EventType = nameof(OrderStatusUpdated) };

        // 100 single-stream events
        var rawEvents = BenchmarkDataGenerator.CreateEventSequence("ORD-0001", "CUST-0001", 100);
        _eventStream100 = rawEvents.Select((e, idx) => (IEvent)new EventEnvelope<object>
        {
            StreamId = "ORD-0001",
            Version = idx + 1,
            Data = e,
            EventType = e.GetType().Name
        }).ToList();

        // 100 multi-stream events across 10 customers
        _multiStreamEvents100 = new List<IEvent>(100);
        for (int i = 0; i < 100; i++)
        {
            var custId = $"CUST-{(i % 10):D4}";
            var orderId = $"ORD-{i:D4}";
            var evt = new OrderCreated(orderId, custId, 50m + i, "US-East", DateTime.UtcNow);
            _multiStreamEvents100.Add(new EventEnvelope<OrderCreated>
            {
                StreamId = orderId,
                Version = 1,
                Data = evt,
                EventType = nameof(OrderCreated)
            });
        }
    }

    [Benchmark(Description = "SingleStreamProjection ApplyEvent (Single Event)")]
    public OrderAggregate SingleStream_Apply_SingleEvent()
    {
        var agg = new OrderAggregate();
        _singleProj.ApplyEvent(_createdEvent, agg);
        _singleProj.ApplyEvent(_itemAddedEvent, agg);
        _singleProj.ApplyEvent(_discountEvent, agg);
        _singleProj.ApplyEvent(_statusEvent, agg);
        return agg;
    }

    [Benchmark(Description = "SingleStreamProjection Fold (100 Events)")]
    public OrderAggregate SingleStream_Fold_100Events()
    {
        var agg = new OrderAggregate();
        for (int i = 0; i < _eventStream100.Count; i++)
        {
            _singleProj.ApplyEvent(_eventStream100[i], agg);
        }
        return agg;
    }

    [Benchmark(Description = "MultiStreamProjection Apply In-Memory (Single Event)")]
    public UserOrdersSummary MultiStream_Apply_InMemory()
    {
        var summary = new UserOrdersSummary();
        _multiProj.Apply(_createdEvent, summary);
        return summary;
    }

    [Benchmark(Description = "MultiStreamProjection ProcessEventAsync (Full Session Execution)")]
    public async Task MultiStream_ProcessEventAsync()
    {
        using var session = (DocumentSession)_store.OpenSession(TrackingMode.Lightweight);
        await _multiProj.ProcessEventAsync(session, _createdEvent, default);
    }

    [Benchmark(Description = "MultiStreamProjection ProcessEventAsync (100 Events Batch)")]
    public async Task MultiStream_Process_100Events()
    {
        using var session = (DocumentSession)_store.OpenSession(TrackingMode.Lightweight);
        for (int i = 0; i < _multiStreamEvents100.Count; i++)
        {
            await _multiProj.ProcessEventAsync(session, _multiStreamEvents100[i], default);
        }
    }
}
