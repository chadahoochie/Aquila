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

namespace Aquila.Cosmos.Tests.Projections;

public sealed record CosmosDirectedOrderPlaced(string OrderId, decimal Amount);
public sealed record CosmosDirectedCustomerRegistered(string CustomerId, string Name);

public sealed class CosmosDirectedOrderSummaryReadModel
{
    public string OrderId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
}

public sealed class CosmosDirectedOrderSummaryProjection : SingleStreamProjection<CosmosDirectedOrderSummaryReadModel>
{
    public CosmosDirectedOrderSummaryProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
        ProjectEvent<CosmosDirectedOrderPlaced>((e, model) =>
        {
            model.OrderId = e.OrderId;
            model.TotalAmount += e.Amount;
            model.ItemCount++;
        });
    }
}

public sealed class CosmosDaemonGarbageRecordsTests
{
    [Fact]
    public async Task CosmosProjectionDaemon_CatchUp_Does_Not_Create_Garbage_Records_For_Unrelated_Streams()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<CosmosDirectedOrderSummaryProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("customers/cust-99", new CosmosDirectedCustomerRegistered("cust-99", "Diana"));
            session.Events.StartStream<object>("orders/ord-99", new CosmosDirectedOrderPlaced("ord-99", 199.99m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        using var readSession = store.OpenSession();

        // Valid order is projected
        var validOrder = await readSession.LoadAsync<CosmosDirectedOrderSummaryReadModel>("orders/ord-99", "orders/ord-99", TestContext.Current.CancellationToken);
        validOrder.ShouldNotBeNull();
        validOrder.OrderId.ShouldBe("ord-99");
        validOrder.TotalAmount.ShouldBe(199.99m);

        // Fixed: CosmosProjectionDaemon does NOT produce blank/garbage records for customer stream
        var customerDoc = await readSession.LoadAsync<CosmosDirectedOrderSummaryReadModel>("customers/cust-99", "customers/cust-99", TestContext.Current.CancellationToken);
        customerDoc.ShouldBeNull("No record should be created for customer stream in OrderSummary storage");
    }

    [Fact]
    public async Task CosmosProjectionDaemon_ChangeFeed_Does_Not_Create_Garbage_Records_For_Unrelated_Streams()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<CosmosDirectedOrderSummaryProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        // Simulate incoming Cosmos DB change feed batch containing an unrelated customer event
        var customerEvent = new EventEnvelope<CosmosDirectedCustomerRegistered>
        {
            Id = Guid.NewGuid(),
            StreamId = "customers/cust-changefeed-1",
            GlobalSequence = 1,
            Version = 1,
            EventType = typeof(CosmosDirectedCustomerRegistered).FullName!,
            Data = new CosmosDirectedCustomerRegistered("cust-changefeed-1", "Eve")
        };

        var changeFeedItem = new CosmosDocumentEnvelope<object>
        {
            Id = "$event_customers/cust-changefeed-1_v1",
            PartitionKey = "customers/cust-changefeed-1",
            DocType = "$event",
            Data = customerEvent
        };

        await daemon.ProcessChangeFeedBatchAsync(new[] { changeFeedItem }, TestContext.Current.CancellationToken);

        using var readSession = store.OpenSession();

        // Fixed: Processing change feed batch does NOT create blank OrderSummary record for customer stream
        var customerDoc = await readSession.LoadAsync<CosmosDirectedOrderSummaryReadModel>("customers/cust-changefeed-1", "customers/cust-changefeed-1", TestContext.Current.CancellationToken);
        customerDoc.ShouldBeNull("No record should be created for customer stream in OrderSummary storage");
    }
}
