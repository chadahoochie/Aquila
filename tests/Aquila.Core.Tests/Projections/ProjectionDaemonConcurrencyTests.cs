using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests.Projections;

public sealed record ConcurrencyOrderPlaced(string OrderId, string CustomerId, decimal Amount, int Step);

public sealed class ConcurrencyCustomerSummary
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int OrderCount { get; set; }
    public List<int> Steps { get; set; } = new();
    public List<long> GlobalSequences { get; set; } = new();
}

public sealed class ConcurrencyAsyncMultiStreamProjection : MultiStreamProjection<ConcurrencyCustomerSummary, string>
{
    public ConcurrencyAsyncMultiStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            ConcurrencyOrderPlaced e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, ConcurrencyCustomerSummary document)
    {
        if (@event.Data is ConcurrencyOrderPlaced e)
        {
            document.CustomerId = e.CustomerId;
            document.TotalAmount += e.Amount;
            document.OrderCount++;
            document.Steps.Add(e.Step);
            document.GlobalSequences.Add(@event.GlobalSequence);
            return true;
        }
        return true;
    }
}

public sealed record ConcurrencyBalanceChanged(decimal Delta, int SequenceNumber);

public sealed class ConcurrencyAccountAggregate
{
    public string AccountId { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public List<int> SequenceNumbers { get; set; } = new();
    public List<long> GlobalSequences { get; set; } = new();
}

public sealed class ConcurrencySingleStreamProjection : SingleStreamProjection<ConcurrencyAccountAggregate>
{
    public ConcurrencySingleStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
        ProjectEvent<ConcurrencyBalanceChanged>((e, agg) =>
        {
            agg.Balance += e.Delta;
            agg.SequenceNumbers.Add(e.SequenceNumber);
        });
    }
}

public sealed class FailingProjection : MultiStreamProjection<ConcurrencyCustomerSummary, string>
{
    public FailingProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event) => "failed-cust";

    public override bool Apply(IEvent @event, ConcurrencyCustomerSummary document)
    {
        throw new InvalidOperationException("Simulated projection failure during parallel dispatch.");
    }
}

public sealed class ProjectionDaemonConcurrencyTests
{
    [Fact]
    public void ProjectionDaemonOptions_Default_Values_Are_Expected()
    {
        var options = new ProjectionDaemonOptions();

        options.BatchSize.ShouldBe(1000);
        options.PollingIntervalMs.ShouldBe(100);
        options.IdlePollingIntervalMs.ShouldBe(500);
        options.MaxProjectionConcurrency.ShouldBe(Math.Max(1, Environment.ProcessorCount));
        options.MaxEventGroupConcurrency.ShouldBe(Math.Max(1, Environment.ProcessorCount * 2));
    }

    [Fact]
    public void AddProjectionDaemon_Registers_Options_And_Custom_Configuration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IDocumentStore>(sp => DocumentStore.For(opts => opts.UseInMemoryStorage()));
        services.AddSingleton<IProjectionCheckpointStore, InMemoryProjectionCheckpointStore>();

        services.AddAquilaDaemon(opts =>
        {
            opts.BatchSize = 500;
            opts.PollingIntervalMs = 50;
            opts.MaxProjectionConcurrency = 4;
            opts.MaxEventGroupConcurrency = 8;
        });

        using var provider = services.BuildServiceProvider();
        var daemon = provider.GetRequiredService<IProjectionDaemon>() as ProjectionDaemon;

        daemon.ShouldNotBeNull();
        daemon.Options.BatchSize.ShouldBe(500);
        daemon.Options.PollingIntervalMs.ShouldBe(50);
        daemon.Options.MaxProjectionConcurrency.ShouldBe(4);
        daemon.Options.MaxEventGroupConcurrency.ShouldBe(8);
    }

    [Fact]
    public async Task MultiStream_Bounded_Parallel_Dispatch_Preserves_Intra_Identity_Order()
    {
        var storageProvider = new InMemoryStorageProvider();
        var store = DocumentStore.For(opts =>
        {
            opts.DocumentStorage = storageProvider;
            opts.EventStorage = storageProvider;
            opts.Projections.Add<ConcurrencyAsyncMultiStreamProjection>(ProjectionLifecycle.Async);
        });
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemonOptions = new ProjectionDaemonOptions
        {
            BatchSize = 1000,
            MaxEventGroupConcurrency = 8
        };
        var daemon = new ProjectionDaemon(store, checkpointStore, options: daemonOptions);

        // Generate 30 customers, 10 events each (300 events total), interleaved in global sequence
        int customerCount = 30;
        int eventsPerCustomer = 10;
        var allEvents = new List<IEvent>();
        long globalSeq = 1;

        for (int step = 1; step <= eventsPerCustomer; step++)
        {
            for (int cust = 1; cust <= customerCount; cust++)
            {
                var custId = $"cust-{cust:D3}";
                var payload = new ConcurrencyOrderPlaced($"ord-{globalSeq}", custId, 10m * step, step);
                var evt = new EventEnvelope<ConcurrencyOrderPlaced>
                {
                    Id = Guid.NewGuid(),
                    StreamId = $"stream-{custId}",
                    Version = step,
                    Sequence = step,
                    GlobalSequence = globalSeq++,
                    EventType = typeof(ConcurrencyOrderPlaced).FullName!,
                    Data = payload
                };
                allEvents.Add(evt);
            }
        }

        // Shuffle the append order to simulate concurrent event generation while preserving GlobalSequence
        await storageProvider.AppendEventsAsync("stream-all", allEvents, 0, TestContext.Current.CancellationToken);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Verify checkpoint advanced to 300
        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(ConcurrencyAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(300);

        // Verify all 30 customers have strictly ordered steps 1..10 and strictly ascending GlobalSequences
        using var readSession = store.OpenSession();
        for (int cust = 1; cust <= customerCount; cust++)
        {
            var custId = $"cust-{cust:D3}";
            var doc = await readSession.LoadAsync<ConcurrencyCustomerSummary>(custId, ct: TestContext.Current.CancellationToken);

            doc.ShouldNotBeNull();
            doc.CustomerId.ShouldBe(custId);
            doc.OrderCount.ShouldBe(eventsPerCustomer);
            doc.Steps.Count.ShouldBe(eventsPerCustomer);

            // Steps must be exactly [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            for (int i = 0; i < eventsPerCustomer; i++)
            {
                doc.Steps[i].ShouldBe(i + 1);
            }

            // GlobalSequences must be strictly monotonically increasing
            for (int i = 1; i < doc.GlobalSequences.Count; i++)
            {
                doc.GlobalSequences[i].ShouldBeGreaterThan(doc.GlobalSequences[i - 1]);
            }
        }
    }

    [Fact]
    public async Task SingleStream_Bounded_Parallel_Dispatch_Preserves_Intra_Stream_Order()
    {
        var storageProvider = new InMemoryStorageProvider();
        var store = DocumentStore.For(opts =>
        {
            opts.DocumentStorage = storageProvider;
            opts.EventStorage = storageProvider;
            opts.Projections.Add<ConcurrencySingleStreamProjection>(ProjectionLifecycle.Async);
        });
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemonOptions = new ProjectionDaemonOptions
        {
            BatchSize = 1000,
            MaxEventGroupConcurrency = 8
        };
        var daemon = new ProjectionDaemon(store, checkpointStore, options: daemonOptions);

        int streamCount = 25;
        int eventsPerStream = 8;
        var allEvents = new List<IEvent>();
        long globalSeq = 1;

        for (int seq = 1; seq <= eventsPerStream; seq++)
        {
            for (int s = 1; s <= streamCount; s++)
            {
                var streamId = $"account-{s:D3}";
                var payload = new ConcurrencyBalanceChanged(10m, seq);
                var evt = new EventEnvelope<ConcurrencyBalanceChanged>
                {
                    Id = Guid.NewGuid(),
                    StreamId = streamId,
                    Version = seq,
                    Sequence = seq,
                    GlobalSequence = globalSeq++,
                    EventType = typeof(ConcurrencyBalanceChanged).FullName!,
                    Data = payload
                };
                allEvents.Add(evt);
            }
        }

        await storageProvider.AppendEventsAsync("stream-all", allEvents, 0, TestContext.Current.CancellationToken);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(ConcurrencySingleStreamProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(200);

        using var readSession = store.OpenSession();
        for (int s = 1; s <= streamCount; s++)
        {
            var streamId = $"account-{s:D3}";
            var doc = await readSession.LoadAsync<ConcurrencyAccountAggregate>(streamId, streamId, ct: TestContext.Current.CancellationToken);

            doc.ShouldNotBeNull();
            doc.Balance.ShouldBe(eventsPerStream * 10m);
            doc.SequenceNumbers.Count.ShouldBe(eventsPerStream);

            for (int i = 0; i < eventsPerStream; i++)
            {
                doc.SequenceNumbers[i].ShouldBe(i + 1);
            }
        }
    }

    [Fact]
    public async Task Checkpoint_CrashSafety_Preserved_When_Projection_Fails_In_Parallel_Dispatch()
    {
        var storageProvider = new InMemoryStorageProvider();
        var store = DocumentStore.For(opts =>
        {
            opts.DocumentStorage = storageProvider;
            opts.EventStorage = storageProvider;
            opts.Projections.Add<FailingProjection>(ProjectionLifecycle.Async);
        });
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new ProjectionDaemon(store, checkpointStore);

        var evt = new EventEnvelope<ConcurrencyOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "stream-fail",
            Version = 1,
            Sequence = 1,
            GlobalSequence = 50,
            EventType = typeof(ConcurrencyOrderPlaced).FullName!,
            Data = new ConcurrencyOrderPlaced("ord-1", "failed-cust", 100m, 1)
        };
        await storageProvider.AppendEventsAsync("stream-fail", new[] { evt }, 0, TestContext.Current.CancellationToken);

        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        });

        // Checkpoint must remain 0 and NOT advance to 50
        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(FailingProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(0);
    }
}
