using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests.Projections;

public sealed record DirectedOrderPlaced(string OrderId, decimal Amount);
public sealed record DirectedCustomerRegistered(string CustomerId, string Name);
public sealed record DirectedOrderAuditLogged(string OrderId, string Note);

public sealed class DirectedOrderSummaryReadModel
{
    public string OrderId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
}

public sealed class DirectedCustomerSummaryReadModel
{
    public string CustomerId { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

public sealed class DirectedOrderSummaryProjection : SingleStreamProjection<DirectedOrderSummaryReadModel>
{
    public DirectedOrderSummaryProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
        ProjectEvent<DirectedOrderPlaced>((e, model) =>
        {
            model.OrderId = e.OrderId;
            model.TotalAmount += e.Amount;
            model.ItemCount++;
        });
    }
}

public sealed class DirectedCustomerSummaryProjection : SingleStreamProjection<DirectedCustomerSummaryReadModel>
{
    public DirectedCustomerSummaryProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
        ProjectEvent<DirectedCustomerRegistered>((e, model) =>
        {
            model.CustomerId = e.CustomerId;
            model.Name = e.Name;
        });
    }
}

public sealed class ProjectionDaemonGarbageRecordsTests
{
    [Fact]
    public async Task ProjectionDaemon_Does_Not_Create_Blank_Or_Garbage_Records_For_Unrelated_Streams()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<DirectedOrderSummaryProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        using var daemon = new ProjectionDaemon(store, checkpointStore);

        // Act: Save events across different streams:
        // - "customers/cust-1" (CustomerRegistered - NOT handled by DirectedOrderSummaryProjection)
        // - "customers/cust-2" (CustomerRegistered - NOT handled by DirectedOrderSummaryProjection)
        // - "orders/ord-100" (OrderPlaced - handled by DirectedOrderSummaryProjection)
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("customers/cust-1", new DirectedCustomerRegistered("cust-1", "Alice"));
            session.Events.StartStream<object>("customers/cust-2", new DirectedCustomerRegistered("cust-2", "Bob"));
            session.Events.StartStream<object>("orders/ord-100", new DirectedOrderPlaced("ord-100", 250.00m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Run projection daemon catch-up
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Assert:
        // 1. The valid order stream was projected properly
        using var readSession = store.OpenSession();
        var validOrder = await readSession.LoadAsync<DirectedOrderSummaryReadModel>("orders/ord-100", "orders/ord-100", TestContext.Current.CancellationToken);
        validOrder.ShouldNotBeNull();
        validOrder.OrderId.ShouldBe("ord-100");
        validOrder.TotalAmount.ShouldBe(250.00m);
        validOrder.ItemCount.ShouldBe(1);

        // 2. Unrelated customer streams must NOT have documents created in OrderSummary storage
        var customerDoc1 = await readSession.LoadAsync<DirectedOrderSummaryReadModel>("customers/cust-1", "customers/cust-1", TestContext.Current.CancellationToken);
        customerDoc1.ShouldBeNull("No record should be created for unrelated customer stream in OrderSummary storage");

        var customerDoc2 = await readSession.LoadAsync<DirectedOrderSummaryReadModel>("customers/cust-2", "customers/cust-2", TestContext.Current.CancellationToken);
        customerDoc2.ShouldBeNull("No record should be created for unrelated customer stream in OrderSummary storage");

        // 3. Raw storage provider confirms no envelopes exist for customer streams
        var rawEnvelope1 = await storageProvider.ReadDocumentAsync<DirectedOrderSummaryReadModel>("customers/cust-1", "customers/cust-1", TestContext.Current.CancellationToken);
        rawEnvelope1.ShouldBeNull();

        // 4. Querying all OrderSummaries returns ONLY the 1 valid order document
        var allSummaries = await readSession.QueryAsync<DirectedOrderSummaryReadModel>(ct: TestContext.Current.CancellationToken);
        allSummaries.Count.ShouldBe(1);
        allSummaries[0].OrderId.ShouldBe("ord-100");
    }

    [Fact]
    public async Task ProjectionDaemon_Does_Not_Create_Blank_Record_When_Stream_Has_Only_Unhandled_Events()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<DirectedOrderSummaryProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        using var daemon = new ProjectionDaemon(store, checkpointStore);

        // Act: Save an unhandled event (audit log note) on an order stream that never had OrderPlaced
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-999", new DirectedOrderAuditLogged("ord-999", "Initial audit note"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Assert: No blank record is created when no events in the stream were handled
        using var readSession = store.OpenSession();
        var blankDoc = await readSession.LoadAsync<DirectedOrderSummaryReadModel>("orders/ord-999", "orders/ord-999", TestContext.Current.CancellationToken);
        blankDoc.ShouldBeNull("No record should be created when stream has only unhandled events");
    }

    [Fact]
    public async Task ProjectionDaemon_Prevents_Cross_Projection_Contamination()
    {
        // Arrange: Register TWO distinct SingleStreamProjections
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<DirectedOrderSummaryProjection>(ProjectionLifecycle.Async);
        options.Projections.Add<DirectedCustomerSummaryProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        using var daemon = new ProjectionDaemon(store, checkpointStore);

        // Act: Save 1 order event and 1 customer event
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-1", new DirectedOrderPlaced("ord-1", 75.00m));
            session.Events.StartStream<object>("customers/cust-1", new DirectedCustomerRegistered("cust-1", "Charlie"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Assert:
        // 1. OrderSummary store contains ONLY "orders/ord-1", NOT "customers/cust-1"
        using var readSession = store.OpenSession();
        var validOrder = await readSession.LoadAsync<DirectedOrderSummaryReadModel>("orders/ord-1", "orders/ord-1", TestContext.Current.CancellationToken);
        validOrder.ShouldNotBeNull();
        validOrder.OrderId.ShouldBe("ord-1");

        var orderGarbage = await readSession.LoadAsync<DirectedOrderSummaryReadModel>("customers/cust-1", "customers/cust-1", TestContext.Current.CancellationToken);
        orderGarbage.ShouldBeNull("Customer stream must not contaminate OrderSummary store");

        // 2. CustomerSummary store contains ONLY "customers/cust-1", NOT "orders/ord-1"
        var validCustomer = await readSession.LoadAsync<DirectedCustomerSummaryReadModel>("customers/cust-1", "customers/cust-1", TestContext.Current.CancellationToken);
        validCustomer.ShouldNotBeNull();
        validCustomer.CustomerId.ShouldBe("cust-1");

        var customerGarbage = await readSession.LoadAsync<DirectedCustomerSummaryReadModel>("orders/ord-1", "orders/ord-1", TestContext.Current.CancellationToken);
        customerGarbage.ShouldBeNull("Order stream must not contaminate CustomerSummary store");
    }
}
