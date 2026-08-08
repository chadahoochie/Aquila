using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using Shouldly;
using Xunit;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Projections;
using Aquila.Cosmos.Storage;
using Aquila.Cosmos.Extensions;

namespace Aquila.Cosmos.Tests;

public sealed record CosmosDaemonOrderPlaced(string OrderId, string CustomerId, decimal Amount);

public class CosmosDaemonCustomerSummaryReadModel
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int OrderCount { get; set; }
}

public class TestCosmosDaemonAsyncProjection : MultiStreamProjection<CosmosDaemonCustomerSummaryReadModel, string>
{
    public TestCosmosDaemonAsyncProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            CosmosDaemonOrderPlaced e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, CosmosDaemonCustomerSummaryReadModel document)
    {
        if (@event.Data is CosmosDaemonOrderPlaced e)
        {
            document.CustomerId = e.CustomerId;
            document.TotalAmount += e.Amount;
            document.OrderCount++;
            return true;
        }
        return true;
    }
}

public sealed class CosmosDaemonTests
{
    private (IDocumentStore Store, IProjectionCheckpointStore CheckpointStore, CosmosProjectionDaemon Daemon) CreateDaemon()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCosmosDaemonAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        return (store, checkpointStore, daemon);
    }

    [Fact]
    public async Task ProcessChangeFeedBatchAsync_Filters_NonEvent_Documents_And_Dispatches_Events()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        var nonEventDoc1 = new CosmosDocumentEnvelope<object>
        {
            Id = "c1",
            PartitionKey = "c1",
            DocType = "Customer",
            Data = new { Name = "Alice" }
        };

        var nonEventDoc2 = new CosmosDocumentEnvelope<object>
        {
            Id = "$stream_s1",
            PartitionKey = "s1",
            DocType = "$stream_header",
            Data = new { StreamId = "s1", Version = 1 }
        };

        var event1 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s1",
            GlobalSequence = 1,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o1", "cust-1", 100.00m)
        };

        var eventDoc1 = new CosmosDocumentEnvelope<object>
        {
            Id = "$event_s1_v1",
            PartitionKey = "s1",
            DocType = "$event",
            Data = event1
        };

        var event2 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s2",
            GlobalSequence = 2,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o2", "cust-1", 50.00m)
        };

        var eventDoc2 = new CosmosDocumentEnvelope<object>
        {
            Id = "$event_s2_v1",
            PartitionKey = "s2",
            DocType = "$event",
            Data = event2
        };

        var batch = new object[] { nonEventDoc1, nonEventDoc2, eventDoc1, eventDoc2 };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(2);

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<CosmosDaemonCustomerSummaryReadModel>("cust-1", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(2);
        readModel.TotalAmount.ShouldBe(150.00m);
    }

    [Fact]
    public async Task ProcessChangeFeedBatchAsync_Deserializes_Raw_Json_Envelopes()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        var json = @"
        {
            ""id"": ""$event_s1_v1"",
            ""pk"": ""s1"",
            ""_docType"": ""$event"",
            ""_tenantId"": ""default"",
            ""data"": {
                ""Id"": ""3fa85f64-5717-4562-b3fc-2c963f66afa6"",
                ""StreamId"": ""s1"",
                ""Version"": 1,
                ""GlobalSequence"": 5,
                ""EventType"": ""Aquila.Cosmos.Tests.CosmosDaemonOrderPlaced"",
                ""Data"": {
                    ""OrderId"": ""o1"",
                    ""CustomerId"": ""cust-2"",
                    ""Amount"": 75.00
                }
            }
        }";

        var jobject = JObject.Parse(json);
        var batch = new object[] { jobject };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(5);

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<CosmosDaemonCustomerSummaryReadModel>("cust-2", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(1);
        readModel.TotalAmount.ShouldBe(75.00m);
    }

    [Fact]
    public async Task CatchUpAsync_Processes_All_Unprocessed_Events()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 10; i++)
            {
                session.Events.StartStream<object>($"orders/{i}", new CosmosDaemonOrderPlaced($"o{i}", "cust-3", 10.00m));
            }
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(10);

        using var readSession = store.OpenSession();
        var readModel = await readSession.LoadAsync<CosmosDaemonCustomerSummaryReadModel>("cust-3", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(10);
        readModel.TotalAmount.ShouldBe(100.00m);
    }

    [Fact]
    public async Task RebuildProjectionAsync_Resets_Checkpoint_And_Reprocesses_Events()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 5; i++)
            {
                session.Events.StartStream<object>($"orders/{i}", new CosmosDaemonOrderPlaced($"o{i}", "cust-4", 20.00m));
            }
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(5);

        await daemon.RebuildProjectionAsync<TestCosmosDaemonAsyncProjection>(TestContext.Current.CancellationToken);

        var checkpointAfterRebuild = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpointAfterRebuild.ShouldBe(5);

        using var readSession = store.OpenSession();
        var readModel = await readSession.LoadAsync<CosmosDaemonCustomerSummaryReadModel>("cust-4", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(5);
        readModel.TotalAmount.ShouldBe(100.00m);
    }

    [Fact]
    public async Task StopProjectionAsync_And_StartProjectionAsync_Controls_Dispatch()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        await daemon.StopProjectionAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);

        var event1 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s1",
            GlobalSequence = 1,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o1", "cust-5", 50.00m)
        };

        var eventDoc1 = new CosmosDocumentEnvelope<object>
        {
            Id = "$event_s1_v1",
            PartitionKey = "s1",
            DocType = "$event",
            Data = event1
        };

        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc1 }, TestContext.Current.CancellationToken);

        var checkpointStopped = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpointStopped.ShouldBe(0);

        await daemon.StartProjectionAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc1 }, TestContext.Current.CancellationToken);

        var checkpointStarted = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpointStarted.ShouldBe(1);
    }

    [Fact]
    public void AddCosmosDaemon_Registers_Daemon_In_DI()
    {
        var services = new ServiceCollection();
        var options = new StoreOptions();
        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(new DocumentStore(options));

        services.AddCosmosDaemon();

        var provider = services.BuildServiceProvider();
        var daemon = provider.GetService<IProjectionDaemon>();
        daemon.ShouldNotBeNull();
        daemon.ShouldBeOfType<CosmosProjectionDaemon>();

        var checkpointStore = provider.GetService<IProjectionCheckpointStore>();
        checkpointStore.ShouldNotBeNull();
    }
}
