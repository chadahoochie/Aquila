using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Projections;
using Aquila.Cosmos.Storage;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

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

public sealed record CosmosDaemonAccountOpened(string AccountId, string Owner);

public sealed class CosmosDaemonAccountAggregate
{
    public string AccountId { get; set; } = string.Empty;
    public string Owner { get; set; } = string.Empty;
}

public sealed class CosmosDaemonSingleStreamAsyncProjection : SingleStreamProjection<CosmosDaemonAccountAggregate>
{
    public CosmosDaemonSingleStreamAsyncProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
        ProjectEvent<CosmosDaemonAccountOpened>((e, agg) =>
        {
            agg.AccountId = e.AccountId;
            agg.Owner = e.Owner;
        });
    }
}

public sealed class PlainChangeFeedItem
{
    public string DocType { get; set; } = string.Empty;
    public object? Data { get; set; }
}

public sealed class PlainChangeFeedItemWithUnderscoreDocType
{
    public string _docType { get; set; } = string.Empty;
    public object? data { get; set; }
}

public sealed class PlainChangeFeedItemNoDocType
{
    public string OtherProp { get; set; } = "value";
}

public sealed class TestReadOnlyEvent : IEvent
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string StreamId { get; set; } = string.Empty;
    public long Version { get; set; }
    public long Sequence { get; set; }
    public long GlobalSequence { get; set; }
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string EventType { get; set; } = string.Empty;
    public string TenantId { get; set; } = "default";
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public IReadOnlyDictionary<string, object> Headers { get; set; } = new Dictionary<string, object>();
    public object Data { get; }

    public TestReadOnlyEvent(object data)
    {
        Data = data;
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

    [Fact]
    public void AquilaCosmosDaemonGlobalExtensions_AddCosmosDaemon_Overloads_Delegate_To_Extensions_Namespace()
    {
        var services = new ServiceCollection();
        var options = new StoreOptions();
        options.UseStorageProvider(new InMemoryStorageProvider());
        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(new DocumentStore(options));

        var resultServices = Aquila.Cosmos.AquilaCosmosDaemonGlobalExtensions.AddCosmosDaemon(services);
        resultServices.ShouldBeSameAs(services);

        var provider = resultServices.BuildServiceProvider();
        provider.GetService<IProjectionDaemon>().ShouldNotBeNull();

        var resultOptions = Aquila.Cosmos.AquilaCosmosDaemonGlobalExtensions.AddCosmosDaemon(options);
        resultOptions.ShouldBeSameAs(options);
    }

    [Fact]
    public async Task Daemon_Processes_SingleStreamProjection_Branch_And_Updates_Aggregate_Document()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<CosmosDaemonSingleStreamAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("accounts/acc-1", new CosmosDaemonAccountOpened("acc-1", "Alice"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(CosmosDaemonSingleStreamAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(1);

        using var readSession = store.OpenSession();
        var aggregate = await readSession.LoadAsync<CosmosDaemonAccountAggregate>("accounts/acc-1", "accounts/acc-1", TestContext.Current.CancellationToken);
        aggregate.ShouldNotBeNull();
        aggregate.AccountId.ShouldBe("acc-1");
        aggregate.Owner.ShouldBe("Alice");

        var envelope = await storageProvider.Documents.ReadDocumentAsync<CosmosDaemonAccountAggregate>("accounts/acc-1", "accounts/acc-1", TestContext.Current.CancellationToken);
        envelope.ShouldNotBeNull();
        envelope.Data.AccountId.ShouldBe("acc-1");
        envelope.Data.Owner.ShouldBe("Alice");
    }

    [Fact]
    public async Task ProcessChangeFeedBatchAsync_Deserializes_JsonElement_Envelopes()
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
                ""GlobalSequence"": 7,
                ""EventType"": ""Aquila.Cosmos.Tests.CosmosDaemonOrderPlaced"",
                ""Data"": {
                    ""OrderId"": ""o1"",
                    ""CustomerId"": ""cust-6"",
                    ""Amount"": 33.00
                }
            }
        }";

        using var jsonDoc = JsonDocument.Parse(json);
        var batch = new object[] { jsonDoc.RootElement };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(7);

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<CosmosDaemonCustomerSummaryReadModel>("cust-6", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(1);
        readModel.TotalAmount.ShouldBe(33.00m);
    }

    [Fact]
    public async Task ProcessChangeFeedBatchAsync_Recognizes_EventDocument_Via_Reflection_Fallback()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        var evt = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s7",
            GlobalSequence = 3,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o7", "cust-7", 44.00m)
        };

        var item = new PlainChangeFeedItem { DocType = "$event", Data = evt };
        var batch = new object[] { item };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(3);

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<CosmosDaemonCustomerSummaryReadModel>("cust-7", ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(1);
        readModel.TotalAmount.ShouldBe(44.00m);
    }

    [Fact]
    public async Task ExecuteAsync_BackgroundService_Loop_Processes_Batch_And_Stops_Cleanly()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-1", new CosmosDaemonOrderPlaced("ord-1", "cust-8", 15.00m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.StartAsync(TestContext.Current.CancellationToken);

        await Task.Delay(300, TestContext.Current.CancellationToken);

        await daemon.StopAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(1);
    }

    [Fact]
    public void Constructor_Null_DocumentStore_Or_CheckpointStore_Throws_ArgumentNullException()
    {
        var checkpointStore = Substitute.For<IProjectionCheckpointStore>();
        Should.Throw<ArgumentNullException>(() => new CosmosProjectionDaemon((IDocumentStore)null!, checkpointStore));

        var store = Substitute.For<IDocumentStore>();
        Should.Throw<ArgumentNullException>(() => new CosmosProjectionDaemon(store, null!));
    }

    [Fact]
    public void Constructor_Overloads_And_EnsureCosmosStorage_Null_Checks_And_Existing_Provider()
    {
        var options = new StoreOptions();
        var checkpointStore = Substitute.For<IProjectionCheckpointStore>();
        var daemon = new CosmosProjectionDaemon(options, checkpointStore);
        daemon.ShouldNotBeNull();

        Should.Throw<ArgumentNullException>(() => new CosmosProjectionDaemon((Microsoft.Azure.Cosmos.Container)null!, options, checkpointStore));

        Should.Throw<ArgumentNullException>(() => new CosmosProjectionDaemon((Microsoft.Azure.Cosmos.CosmosClient)null!, options, checkpointStore));

        Should.Throw<ArgumentNullException>(() => new CosmosProjectionDaemon((Microsoft.Azure.Cosmos.Container)null!, null!, checkpointStore));
        Should.Throw<ArgumentNullException>(() => new CosmosProjectionDaemon((Microsoft.Azure.Cosmos.CosmosClient)null!, null!, checkpointStore));

        var optionsWithProvider = new StoreOptions();
        var existingProvider = new InMemoryStorageProvider();
        optionsWithProvider.UseStorageProvider(existingProvider);

        var dummyClient = Substitute.For<Microsoft.Azure.Cosmos.CosmosClient>();
        var dummyDatabase = Substitute.For<Microsoft.Azure.Cosmos.Database>();
        var dummyContainer = Substitute.For<Microsoft.Azure.Cosmos.Container>();
        dummyContainer.Database.Returns(dummyDatabase);
        dummyDatabase.Client.Returns(dummyClient);

        var daemonWithContainer = new CosmosProjectionDaemon(dummyContainer, optionsWithProvider, checkpointStore);
        optionsWithProvider.StorageProvider.ShouldBeSameAs(existingProvider);

        var daemonWithClient = new CosmosProjectionDaemon(dummyClient, optionsWithProvider, checkpointStore);
        optionsWithProvider.StorageProvider.ShouldBeSameAs(existingProvider);

        var optionsNullProvider1 = new StoreOptions();
        var daemonContainer = new CosmosProjectionDaemon(dummyContainer, optionsNullProvider1, checkpointStore);
        optionsNullProvider1.StorageProvider.ShouldNotBeNull();

        var optionsNullProvider2 = new StoreOptions();
        var daemonClient = new CosmosProjectionDaemon(dummyClient, optionsNullProvider2, checkpointStore);
        optionsNullProvider2.StorageProvider.ShouldNotBeNull();
    }

    [Fact]
    public async Task ProcessChangeFeedBatchAsync_Null_Or_Empty_Batch_Or_Null_Item_Handled()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        await daemon.ProcessChangeFeedBatchAsync(null!, TestContext.Current.CancellationToken);

        await daemon.ProcessChangeFeedBatchAsync(new object[] { null! }, TestContext.Current.CancellationToken);

        var nonEvent = new PlainChangeFeedItem { DocType = "NonEvent" };
        await daemon.ProcessChangeFeedBatchAsync(new object[] { nonEvent }, TestContext.Current.CancellationToken);

        var event1 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s1",
            GlobalSequence = 1,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o1", "cust-9", 10.00m)
        };
        var eventDoc1 = new CosmosDocumentEnvelope<object>
        {
            Id = "$event_s1_v1",
            PartitionKey = "s1",
            DocType = "$event",
            Data = event1
        };

        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc1 }, TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(1);

        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc1 }, TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(1);
    }

    [Fact]
    public async Task IsEventDocument_Negative_And_Edge_Cases()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        var envNonGenericEvent = new CosmosDocumentEnvelope { DocType = "$event" };
        var envNonGenericOther = new CosmosDocumentEnvelope { DocType = "NotAnEvent" };

        var envNegative = new CosmosDocumentEnvelope<object> { DocType = "NotAnEvent" };

        var jobjNegative1 = JObject.Parse(@"{ ""foo"": ""bar"" }");
        var jobjNegative2 = JObject.Parse(@"{ ""_docType"": ""NotAnEvent"" }");

        var jobjDocType = JObject.Parse(@"{ ""DocType"": ""$event"", ""data"": null }");

        using var docNeg1 = JsonDocument.Parse(@"{ ""foo"": ""bar"" }");
        using var docNeg2 = JsonDocument.Parse(@"{ ""_docType"": ""NotAnEvent"" }");
        using var docPosDocType = JsonDocument.Parse(@"{ ""DocType"": ""$event"", ""Data"": null }");

        var itemUnderscore = new PlainChangeFeedItemWithUnderscoreDocType { _docType = "$event" };

        var itemOtherDocType = new PlainChangeFeedItem { DocType = "NotAnEvent" };

        var itemNoDocType = new PlainChangeFeedItemNoDocType();

        var batch = new object[]
        {
            envNonGenericEvent,
            envNonGenericOther,
            envNegative,
            jobjNegative1,
            jobjNegative2,
            jobjDocType,
            docNeg1.RootElement,
            docNeg2.RootElement,
            docPosDocType.RootElement,
            itemUnderscore,
            itemOtherDocType,
            itemNoDocType,
            123
        };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ExtractEvent_Negative_And_Edge_Cases()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        var evt1 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s10",
            GlobalSequence = 10,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o10", "cust-10", 100m)
        };
        var jobjCapitalData = new JObject
        {
            ["_docType"] = "$event",
            ["Data"] = JObject.FromObject(evt1)
        };

        var evt2 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s11",
            GlobalSequence = 11,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o11", "cust-11", 110m)
        };
        using var jsonDocCapitalData = JsonDocument.Parse($@"{{
            ""_docType"": ""$event"",
            ""Data"": {Newtonsoft.Json.JsonConvert.SerializeObject(evt2)}
        }}");

        var evt3 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s12",
            GlobalSequence = 12,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o12", "cust-12", 120m)
        };
        var reflectionLowercaseData = new PlainChangeFeedItemWithUnderscoreDocType
        {
            _docType = "$event",
            data = evt3
        };

        var evt4Json = Newtonsoft.Json.JsonConvert.SerializeObject(new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s13",
            GlobalSequence = 13,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o13", "cust-13", 130m)
        });
        var stringDataFeedItem = new PlainChangeFeedItem
        {
            DocType = "$event",
            Data = evt4Json
        };

        var evt5 = new EventEnvelope<CosmosDaemonOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "s14",
            GlobalSequence = 14,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
            Data = new CosmosDaemonOrderPlaced("o14", "cust-14", 140m)
        };
        var arbitraryObjectDataFeedItem = new PlainChangeFeedItem
        {
            DocType = "$event",
            Data = evt5
        };

        var nullStringFeedItem = new PlainChangeFeedItem
        {
            DocType = "$event",
            Data = "null"
        };

        var batch = new object[]
        {
            jobjCapitalData,
            jsonDocCapitalData.RootElement,
            reflectionLowercaseData,
            stringDataFeedItem,
            arbitraryObjectDataFeedItem,
            nullStringFeedItem
        };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(14);
    }

    [Fact]
    public async Task EnsureTypedPayload_And_ResolveType_Edge_Cases()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        var nullDataEvent = new CosmosDocumentEnvelope<object>
        {
            DocType = "$event",
            Data = new EventEnvelope<object>
            {
                Id = Guid.NewGuid(),
                StreamId = "s20",
                GlobalSequence = 20,
                Version = 1,
                EventType = typeof(CosmosDaemonOrderPlaced).FullName!,
                Data = null!
            }
        };

        var unknownTypeEvent = new CosmosDocumentEnvelope<object>
        {
            DocType = "$event",
            Data = new EventEnvelope<object>
            {
                Id = Guid.NewGuid(),
                StreamId = "s21",
                GlobalSequence = 21,
                Version = 1,
                EventType = "UnknownNamespace.NonExistentEvent",
                Data = JObject.FromObject(new { Foo = "Bar" })
            }
        };

        var shortTypeNameEvent = new CosmosDocumentEnvelope<object>
        {
            DocType = "$event",
            Data = new EventEnvelope<object>
            {
                Id = Guid.NewGuid(),
                StreamId = "s22",
                GlobalSequence = 22,
                Version = 1,
                EventType = nameof(CosmosDaemonOrderPlaced),
                Data = JObject.FromObject(new CosmosDaemonOrderPlaced("o22", "cust-22", 220m))
            }
        };

        using var jsonDocUnknown = JsonDocument.Parse(@"{
            ""_docType"": ""$event"",
            ""data"": {
                ""Id"": ""3fa85f64-5717-4562-b3fc-2c963f66afa6"",
                ""StreamId"": ""s23"",
                ""Version"": 1,
                ""GlobalSequence"": 23,
                ""EventType"": ""UnknownNamespace.NonExistentEvent"",
                ""Data"": { ""Foo"": ""Bar"" }
            }
        }");

        var readOnlyEvt = new TestReadOnlyEvent(JObject.FromObject(new CosmosDaemonOrderPlaced("o24", "cust-24", 240m)))
        {
            Id = Guid.NewGuid(),
            StreamId = "s24",
            GlobalSequence = 24,
            Version = 1,
            EventType = typeof(CosmosDaemonOrderPlaced).FullName!
        };
        var readOnlyFeedItem = new PlainChangeFeedItem
        {
            DocType = "$event",
            Data = readOnlyEvt
        };

        var batch = new object[]
        {
            nullDataEvent,
            unknownTypeEvent,
            shortTypeNameEvent,
            jsonDocUnknown.RootElement,
            readOnlyFeedItem
        };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(24);
    }

    [Fact]
    public async Task CatchUpAsync_Returns_Early_When_No_Active_Projections()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CatchUpAsync_When_No_Events_In_Storage_Or_Sequence_Not_Higher()
    {
        var (store, checkpointStore, daemon) = CreateDaemon();

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-50", new CosmosDaemonOrderPlaced("ord-50", "cust-50", 50m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(1);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(nameof(TestCosmosDaemonAsyncProjection), TestContext.Current.CancellationToken)).ShouldBe(1);
    }

    [Fact]
    public async Task SingleStreamProjection_Updates_Existing_Aggregate()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<CosmosDaemonSingleStreamAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("accounts/acc-100", new CosmosDaemonAccountOpened("acc-100", "Alice"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        using (var session = store.OpenSession())
        {
            session.Events.Append("accounts/acc-100", new CosmosDaemonAccountOpened("acc-100", "Alice Updated"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var envelope = await storageProvider.Documents.ReadDocumentAsync<CosmosDaemonAccountAggregate>("accounts/acc-100", "accounts/acc-100", TestContext.Current.CancellationToken);
        envelope.ShouldNotBeNull();
        var aggregate = envelope.Data.ShouldBeOfType<CosmosDaemonAccountAggregate>();
        aggregate.Owner.ShouldBe("Alice Updated");
    }

    [Fact]
    public async Task RebuildProjectionAsync_Clears_Existing_ReadModel_Documents()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<CosmosDaemonSingleStreamAsyncProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("accounts/acc-200", new CosmosDaemonAccountOpened("acc-200", "Bob"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        await daemon.RebuildProjectionAsync<CosmosDaemonSingleStreamAsyncProjection>(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(nameof(CosmosDaemonSingleStreamAsyncProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(1);
    }

    [Fact]
    public async Task ExecuteAsync_Handles_No_Active_Projections_Idle_Loop()
    {
        var options = new StoreOptions();
        options.UseStorageProvider(new InMemoryStorageProvider());
        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(200, TestContext.Current.CancellationToken);
        await daemon.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ExecuteAsync_Catches_Exceptions_In_Loop_And_Logs_Error()
    {
        var options = new StoreOptions();
        options.UseStorageProvider(new InMemoryStorageProvider());
        options.Projections.Add<TestCosmosDaemonAsyncProjection>(ProjectionLifecycle.Async);
        var store = new DocumentStore(options);

        var failingCheckpointStore = Substitute.For<IProjectionCheckpointStore>();
        failingCheckpointStore
            .GetCheckpointAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("Simulated checkpoint store error"));

        var logger = Substitute.For<Microsoft.Extensions.Logging.ILogger<CosmosProjectionDaemon>>();
        var daemon = new CosmosProjectionDaemon(store, failingCheckpointStore, logger);

        await daemon.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(300, TestContext.Current.CancellationToken);
        await daemon.StopAsync(TestContext.Current.CancellationToken);
    }
}
