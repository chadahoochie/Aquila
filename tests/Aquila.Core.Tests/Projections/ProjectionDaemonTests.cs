using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record DaemonOrderPlaced(string OrderId, string CustomerId, decimal Amount);

public class DaemonCustomerSummaryReadModel
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int OrderCount { get; set; }
}

public class TestDaemonAsyncProjection : MultiStreamProjection<DaemonCustomerSummaryReadModel, string>
{
    public TestDaemonAsyncProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            DaemonOrderPlaced e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, DaemonCustomerSummaryReadModel document)
    {
        if (@event.Data is DaemonOrderPlaced e)
        {
            document.CustomerId = e.CustomerId;
            document.TotalAmount += e.Amount;
            document.OrderCount++;
            return true;
        }
        return true;
    }
}

public sealed record DaemonAccountOpened(string AccountId, string Owner);

public sealed class DaemonAccountAggregate
{
    public string AccountId { get; set; } = string.Empty;
    public string Owner { get; set; } = string.Empty;
}

public sealed class DaemonSingleStreamAsyncProjection : SingleStreamProjection<DaemonAccountAggregate>
{
    public DaemonSingleStreamAsyncProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
        ProjectEvent<DaemonAccountOpened>((e, agg) =>
        {
            agg.AccountId = e.AccountId;
            agg.Owner = e.Owner;
        });
    }
}

public sealed class ProjectionDaemonTests
{
    [Fact]
    public async Task InMemoryProjectionCheckpointStore_Save_And_Get_Checkpoint()
    {
        IProjectionCheckpointStore store = new InMemoryProjectionCheckpointStore();

        var initialSeq = await store.GetCheckpointAsync("TestProj", TestContext.Current.CancellationToken);
        initialSeq.ShouldBe(0);

        await store.SaveCheckpointAsync("TestProj", 42, TestContext.Current.CancellationToken);

        var updatedSeq = await store.GetCheckpointAsync("TestProj", TestContext.Current.CancellationToken);
        updatedSeq.ShouldBe(42);
    }

    [Fact]
    public async Task DocumentStorageProjectionCheckpointStore_Save_And_Get_Checkpoint()
    {
        var storageProvider = new InMemoryStorageProvider();
        IProjectionCheckpointStore store = new DocumentStorageProjectionCheckpointStore(storageProvider);

        var initialSeq = await store.GetCheckpointAsync("TestProj", TestContext.Current.CancellationToken);
        initialSeq.ShouldBe(0);

        await store.SaveCheckpointAsync("TestProj", 75, TestContext.Current.CancellationToken);

        var updatedSeq = await store.GetCheckpointAsync("TestProj", TestContext.Current.CancellationToken);
        updatedSeq.ShouldBe(75);
    }

    [Fact]
    public async Task Daemon_Processes_Events_Async_And_Updates_Checkpoints()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestDaemonAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();

        using var daemon = new ProjectionDaemon(store, checkpointStore);

        // Append 50 events across 5 streams
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 50; i++)
            {
                var streamId = $"orders/ord-{i}";
                var evt = new DaemonOrderPlaced($"ord-{i}", "cust-1", 10.00m);
                session.Events.StartStream<object>(streamId, evt);
            }
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // CatchUp processing daemon
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(50);

        // Verify projected read model document
        using var readSession = store.OpenSession();
        var readModel = await readSession.LoadAsync<DaemonCustomerSummaryReadModel>("cust-1", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(50);
        readModel.TotalAmount.ShouldBe(500.00m);
    }

    [Fact]
    public async Task RebuildProjectionAsync_Resets_Checkpoint_And_Reprocesses_Events()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestDaemonAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();

        using var daemon = new ProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 20; i++)
            {
                session.Events.StartStream<object>($"orders/ord-{i}", new DaemonOrderPlaced($"ord-{i}", "cust-2", 5.00m));
            }
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(20);

        // Rebuild projection
        await daemon.RebuildProjectionAsync<TestDaemonAsyncProjection>(TestContext.Current.CancellationToken);

        var checkpointAfterRebuild = await checkpointStore.GetCheckpointAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpointAfterRebuild.ShouldBe(20);

        using var readSession = store.OpenSession();
        var readModel = await readSession.LoadAsync<DaemonCustomerSummaryReadModel>("cust-2", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(20);
        readModel.TotalAmount.ShouldBe(100.00m);
    }

    [Fact]
    public async Task StopProjectionAsync_And_StartProjectionAsync_Controls_Processing()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestDaemonAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();

        using var daemon = new ProjectionDaemon(store, checkpointStore);

        // Stop projection before appending events
        await daemon.StopProjectionAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-1", new DaemonOrderPlaced("ord-1", "cust-3", 100.00m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(0);

        // Start projection back up
        await daemon.StartProjectionAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken);
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        (await checkpointStore.GetCheckpointAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(1);
    }

    [Fact]
    public void ServiceCollectionExtensions_AddAquilaDaemon_Registers_Daemon()
    {
        var services = new ServiceCollection();
        var options = new StoreOptions();
        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(new DocumentStore(options));

        services.AddAquilaDaemon();

        var provider = services.BuildServiceProvider();
        var daemon = provider.GetService<IProjectionDaemon>();
        daemon.ShouldNotBeNull();
        daemon.ShouldBeOfType<ProjectionDaemon>();

        var checkpointStore = provider.GetService<IProjectionCheckpointStore>();
        checkpointStore.ShouldNotBeNull();
    }

    [Fact]
    public void StoreOptions_AddAsyncDaemon_Configures_Options()
    {
        var options = new StoreOptions();
        var configured = options.AddAsyncDaemon();
        configured.ShouldBeSameAs(options);
    }

    [Fact]
    public async Task Daemon_Processes_SingleStreamProjection_Branch_And_Updates_Aggregate_Document()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<DaemonSingleStreamAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();

        using var daemon = new ProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("accounts/acc-1", new DaemonAccountOpened("acc-1", "Alice"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(DaemonSingleStreamAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(1);

        var envelope = await storageProvider.Documents.ReadDocumentAsync<object>("accounts/acc-1", "accounts/acc-1", TestContext.Current.CancellationToken);
        envelope.ShouldNotBeNull();
        var aggregate = envelope.Data.ShouldBeOfType<DaemonAccountAggregate>();
        aggregate.AccountId.ShouldBe("acc-1");
        aggregate.Owner.ShouldBe("Alice");
    }

    [Fact]
    public async Task ExecuteAsync_BackgroundService_Loop_Processes_Batch_And_Stops_Cleanly()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestDaemonAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();

        using var daemon = new ProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-1", new DaemonOrderPlaced("ord-1", "cust-9", 20.00m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.StartAsync(TestContext.Current.CancellationToken);

        // give the background polling loop time to process the batch, then hit the idle-delay branch
        await Task.Delay(300, TestContext.Current.CancellationToken);

        await daemon.StopAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(1);
    }

    private sealed class MinimalProjection : IProjection
    {
        public ProjectionLifecycle Lifecycle { get; set; }
        public Type AggregateType => typeof(DaemonAccountAggregate);
        public void ApplyEvent(IEvent @event, object aggregate)
        {
        }
    }

    [Fact]
    public void IProjection_Name_DefaultInterfaceMember_Returns_Implementing_Type_Name()
    {
        IProjection projection = new MinimalProjection();

        projection.Name.ShouldBe(nameof(MinimalProjection));
    }
}
