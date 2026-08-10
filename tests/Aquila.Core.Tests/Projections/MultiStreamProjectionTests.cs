using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record TrackedOrderPlaced(string OrderId, string CustomerId, decimal Amount);
public sealed record TrackedPaymentReceived(string OrderId, string CustomerId, decimal AmountPaid);
public sealed record TrackedOrderShipped(string OrderId, string CustomerId);
public sealed record TrackedCustomerDeactivated(string CustomerId, string Reason);
public sealed record TrackedCustomerReactivated(string CustomerId);
public sealed record TrackedEmptyIdentityEvent(string CustomerId);
public sealed record MultiStreamUnrelatedEvent(string Description);

public class CustomerSummaryReadModel
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public decimal TotalPaid { get; set; }
    public int OrderCount { get; set; }
    public int ShippedCount { get; set; }
    public bool IsActive { get; set; } = true;
}

public class CustomPkReadModel
{
    public string Id { get; set; } = string.Empty;
    public string TenantGroup { get; set; } = string.Empty;
    public decimal Balance { get; set; }
}

public sealed record CustomPkEvent(string Id, string TenantGroup, decimal Amount);

public class TestCustomerMultiStreamProjection : MultiStreamProjection<CustomerSummaryReadModel, string>
{
    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            TrackedOrderPlaced e => e.CustomerId,
            TrackedPaymentReceived e => e.CustomerId,
            TrackedOrderShipped e => e.CustomerId,
            TrackedCustomerDeactivated e => e.CustomerId,
            TrackedCustomerReactivated e => e.CustomerId,
            TrackedEmptyIdentityEvent => "   ",
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, CustomerSummaryReadModel document)
    {
        switch (@event.Data)
        {
            case TrackedOrderPlaced e:
                document.CustomerId = e.CustomerId;
                document.TotalAmount += e.Amount;
                document.OrderCount++;
                document.IsActive = true;
                return true;

            case TrackedPaymentReceived e:
                document.CustomerId = e.CustomerId;
                document.TotalPaid += e.AmountPaid;
                return true;

            case TrackedOrderShipped e:
                document.CustomerId = e.CustomerId;
                document.ShippedCount++;
                return true;

            case TrackedCustomerDeactivated:
                return false; // Tombstone / delete read model

            case TrackedCustomerReactivated e:
                document.CustomerId = e.CustomerId;
                document.IsActive = true;
                return true;

            default:
                return true;
        }
    }
}

public class TestAsyncCustomerMultiStreamProjection : MultiStreamProjection<CustomerSummaryReadModel, string>
{
    public TestAsyncCustomerMultiStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            TrackedOrderPlaced e => e.CustomerId,
            TrackedPaymentReceived e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, CustomerSummaryReadModel document)
    {
        if (@event.Data is TrackedOrderPlaced e)
        {
            document.CustomerId = e.CustomerId;
            document.TotalAmount += e.Amount;
            document.OrderCount++;
            return true;
        }
        if (@event.Data is TrackedPaymentReceived p)
        {
            document.CustomerId = p.CustomerId;
            document.TotalPaid += p.AmountPaid;
            return true;
        }
        return true;
    }
}

public class TestLiveCustomerMultiStreamProjection : MultiStreamProjection<CustomerSummaryReadModel, string>
{
    public TestLiveCustomerMultiStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Live;
    }

    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            TrackedOrderPlaced e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, CustomerSummaryReadModel document)
    {
        if (@event.Data is TrackedOrderPlaced e)
        {
            document.CustomerId = e.CustomerId;
            document.TotalAmount += e.Amount;
            document.OrderCount++;
        }
        return true;
    }
}

public class TestCustomPkMultiStreamProjection : MultiStreamProjection<CustomPkReadModel, string>
{
    protected override string Identity(IEvent @event)
    {
        return @event.Data is CustomPkEvent e ? e.Id : null!;
    }

    public override bool Apply(IEvent @event, CustomPkReadModel document)
    {
        if (@event.Data is CustomPkEvent e)
        {
            document.Id = e.Id;
            document.TenantGroup = e.TenantGroup;
            document.Balance += e.Amount;
        }
        return true;
    }
}

public sealed class MultiStreamProjectionTests
{
    [Fact]
    public void MultiStreamProjection_Contract_Properties_Set_Correctly()
    {
        var projection = new TestCustomerMultiStreamProjection();

        projection.Name.ShouldBe(nameof(TestCustomerMultiStreamProjection));
        projection.ReadModelType.ShouldBe(typeof(CustomerSummaryReadModel));
        projection.AggregateType.ShouldBe(typeof(CustomerSummaryReadModel));
        projection.Lifecycle.ShouldBe(ProjectionLifecycle.Inline);

        projection.Lifecycle = ProjectionLifecycle.Async;
        projection.Lifecycle.ShouldBe(ProjectionLifecycle.Async);
    }

    [Fact]
    public void StoreOptions_Can_Register_MultiStreamProjection_With_Different_Lifecycles()
    {
        var options = new StoreOptions();
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);
        options.Projections.Add<TestAsyncCustomerMultiStreamProjection>(ProjectionLifecycle.Async);
        options.Projections.Add<TestLiveCustomerMultiStreamProjection>(ProjectionLifecycle.Live);

        options.Projections.Projections.Count.ShouldBe(3);
        options.Projections.Projections[0].ShouldBeOfType<TestCustomerMultiStreamProjection>();
        options.Projections.Projections[0].Lifecycle.ShouldBe(ProjectionLifecycle.Inline);
        options.Projections.Projections[1].ShouldBeOfType<TestAsyncCustomerMultiStreamProjection>();
        options.Projections.Projections[1].Lifecycle.ShouldBe(ProjectionLifecycle.Async);
        options.Projections.Projections[2].ShouldBeOfType<TestLiveCustomerMultiStreamProjection>();
        options.Projections.Projections[2].Lifecycle.ShouldBe(ProjectionLifecycle.Live);
    }

    [Fact]
    public async Task MultiStreamProjection_Aggregates_Events_From_Multiple_Streams_Into_Single_ReadModel()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storageProvider, storageProvider, options);

        var orderEvent = new TrackedOrderPlaced("ord-100", "cust-A", 250.00m);
        var paymentEvent = new TrackedPaymentReceived("ord-100", "cust-A", 200.00m);
        var shippingEvent = new TrackedOrderShipped("ord-100", "cust-A");

        // Act: Append events across THREE DIFFERENT streams
        session.Events.StartStream<object>("orders/ord-100", orderEvent);
        session.Events.StartStream<object>("payments/pay-500", paymentEvent);
        session.Events.StartStream<object>("shipping/ship-900", shippingEvent);

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert: Read model document "cust-A" is projected from all three streams
        var doc = await session.LoadAsync<CustomerSummaryReadModel>("cust-A", ct: TestContext.Current.CancellationToken);

        doc.ShouldNotBeNull();
        doc.CustomerId.ShouldBe("cust-A");
        doc.TotalAmount.ShouldBe(250.00m);
        doc.TotalPaid.ShouldBe(200.00m);
        doc.OrderCount.ShouldBe(1);
        doc.ShippedCount.ShouldBe(1);
        doc.IsActive.ShouldBeTrue();
    }

    [Fact]
    public async Task MultiStreamProjection_Handles_Interleaved_Events_For_Multiple_ReadModels()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storageProvider, storageProvider, options);

        // Interleaved stream appends for cust-A and cust-B
        session.Events.StartStream<object>("orders/1", new TrackedOrderPlaced("1", "cust-A", 100m));
        session.Events.StartStream<object>("orders/2", new TrackedOrderPlaced("2", "cust-B", 300m));
        session.Events.StartStream<object>("payments/1", new TrackedPaymentReceived("1", "cust-A", 100m));
        session.Events.StartStream<object>("payments/2", new TrackedPaymentReceived("2", "cust-B", 150m));

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var custA = await session.LoadAsync<CustomerSummaryReadModel>("cust-A", ct: TestContext.Current.CancellationToken);
        var custB = await session.LoadAsync<CustomerSummaryReadModel>("cust-B", ct: TestContext.Current.CancellationToken);

        custA.ShouldNotBeNull();
        custA.CustomerId.ShouldBe("cust-A");
        custA.TotalAmount.ShouldBe(100m);
        custA.TotalPaid.ShouldBe(100m);
        custA.OrderCount.ShouldBe(1);

        custB.ShouldNotBeNull();
        custB.CustomerId.ShouldBe("cust-B");
        custB.TotalAmount.ShouldBe(300m);
        custB.TotalPaid.ShouldBe(150m);
        custB.OrderCount.ShouldBe(1);
    }

    [Fact]
    public async Task MultiStreamProjection_Deletes_Document_When_Apply_Returns_False_Tombstone()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session1 = new DocumentSession(storageProvider, storageProvider, options);

        // Step 1: Create document via order event
        session1.Events.StartStream<object>("orders/ord-101", new TrackedOrderPlaced("ord-101", "cust-B", 100.00m));
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        var docBefore = await session1.LoadAsync<CustomerSummaryReadModel>("cust-B", ct: TestContext.Current.CancellationToken);
        docBefore.ShouldNotBeNull();

        // Step 2: Append tombstone event (TrackedCustomerDeactivated) in session 2
        using var session2 = new DocumentSession(storageProvider, storageProvider, options);
        session2.Events.StartStream<object>("deactivations/deact-1", new TrackedCustomerDeactivated("cust-B", "Account Closed"));
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert: Document "cust-B" is deleted (tombstoned)
        var docAfter = await session2.LoadAsync<CustomerSummaryReadModel>("cust-B", ct: TestContext.Current.CancellationToken);
        docAfter.ShouldBeNull();
    }

    [Fact]
    public async Task MultiStreamProjection_Recreates_Document_After_Tombstone_Deletion()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        // Step 1: Create document
        using (var session1 = new DocumentSession(storageProvider, storageProvider, options))
        {
            session1.Events.StartStream<object>("orders/1", new TrackedOrderPlaced("1", "cust-C", 100m));
            await session1.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Step 2: Deactivate (tombstone)
        using (var session2 = new DocumentSession(storageProvider, storageProvider, options))
        {
            session2.Events.StartStream<object>("deact/1", new TrackedCustomerDeactivated("cust-C", "Closed"));
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Verify deleted
        using (var verifySession = new DocumentSession(storageProvider, storageProvider, options))
        {
            var deletedDoc = await verifySession.LoadAsync<CustomerSummaryReadModel>("cust-C", ct: TestContext.Current.CancellationToken);
            deletedDoc.ShouldBeNull();
        }

        // Step 3: New event for cust-C
        using (var session3 = new DocumentSession(storageProvider, storageProvider, options))
        {
            session3.Events.StartStream<object>("orders/2", new TrackedOrderPlaced("2", "cust-C", 500m));
            await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: Document cust-C is recreated with fresh state
        using (var finalSession = new DocumentSession(storageProvider, storageProvider, options))
        {
            var doc = await finalSession.LoadAsync<CustomerSummaryReadModel>("cust-C", ct: TestContext.Current.CancellationToken);
            doc.ShouldNotBeNull();
            doc.CustomerId.ShouldBe("cust-C");
            doc.TotalAmount.ShouldBe(500m);
            doc.OrderCount.ShouldBe(1);
        }
    }

    [Fact]
    public async Task MultiStreamProjection_Ignores_Events_With_Null_Or_Empty_Identity()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storageProvider, storageProvider, options);

        // Append events that result in null or empty/whitespace identity
        session.Events.StartStream<object>("unrelated/1", new MultiStreamUnrelatedEvent("ignored"));
        session.Events.StartStream<object>("empty/1", new TrackedEmptyIdentityEvent("cust-X"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var queryDocs = await session.QueryAsync<CustomerSummaryReadModel>(ct: TestContext.Current.CancellationToken);
        queryDocs.ShouldBeEmpty();
    }

    [Fact]
    public void MultiStreamProjection_ApplyEvent_Validates_Null_Arguments()
    {
        var projection = new TestCustomerMultiStreamProjection();
        var model = new CustomerSummaryReadModel();
        var envelope = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-1", 50m)
        };

        Should.Throw<ArgumentNullException>(() => projection.ApplyEvent(null!, model));
        Should.Throw<ArgumentNullException>(() => projection.ApplyEvent(envelope, null!));
    }

    [Fact]
    public async Task MultiStreamProjection_ProcessEventAsync_Validates_Null_Arguments()
    {
        var projection = new TestCustomerMultiStreamProjection();
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        using var session = new DocumentSession(storageProvider, storageProvider, options);
        var envelope = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-1", 50m)
        };

        await Should.ThrowAsync<ArgumentNullException>(async () =>
            await projection.ProcessEventAsync(null!, envelope, TestContext.Current.CancellationToken));

        await Should.ThrowAsync<ArgumentNullException>(async () =>
            await projection.ProcessEventAsync(session, null!, TestContext.Current.CancellationToken));
    }

    [Fact]
    public void MultiStreamProjection_ApplyEvent_Handles_Non_TDoc_Aggregate_Gracefully()
    {
        var projection = new TestCustomerMultiStreamProjection();
        var envelope = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-1", 50m)
        };

        var wrongObject = new MultiStreamUnrelatedEvent("wrong");
        // Should not throw or crash when passed object is not TDoc
        projection.ApplyEvent(envelope, wrongObject);
    }

    [Fact]
    public async Task MultiStreamProjection_Lifecycle_Async_Runs_Via_ProjectionDaemon()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestAsyncCustomerMultiStreamProjection>(ProjectionLifecycle.Async);

        var store = new DocumentStore(options);
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        using var daemon = new ProjectionDaemon(store, checkpointStore);

        // Append events in session
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-1", new TrackedOrderPlaced("ord-1", "cust-Async", 150m));
            session.Events.StartStream<object>("payments/pay-1", new TrackedPaymentReceived("ord-1", "cust-Async", 100m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Inline session SaveChangesAsync should NOT have persisted the Async projection
        using (var checkSession = store.OpenSession())
        {
            var docBeforeDaemon = await checkSession.LoadAsync<CustomerSummaryReadModel>("cust-Async", ct: TestContext.Current.CancellationToken);
            docBeforeDaemon.ShouldBeNull();
        }

        // Run daemon catch-up
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Verify document is projected asynchronously
        using (var finalSession = store.OpenSession())
        {
            var docAfterDaemon = await finalSession.LoadAsync<CustomerSummaryReadModel>("cust-Async", ct: TestContext.Current.CancellationToken);
            docAfterDaemon.ShouldNotBeNull();
            docAfterDaemon.CustomerId.ShouldBe("cust-Async");
            docAfterDaemon.TotalAmount.ShouldBe(150m);
            docAfterDaemon.TotalPaid.ShouldBe(100m);
            docAfterDaemon.OrderCount.ShouldBe(1);
        }
    }

    [Fact]
    public async Task MultiStreamProjection_Lifecycle_Live_Does_Not_Persist_Document_During_SaveChangesAsync()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestLiveCustomerMultiStreamProjection>(ProjectionLifecycle.Live);

        using var session = new DocumentSession(storageProvider, storageProvider, options);

        session.Events.StartStream<object>("orders/1", new TrackedOrderPlaced("1", "cust-Live", 200m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Read model should NOT be saved in document storage because Lifecycle is Live
        var doc = await session.LoadAsync<CustomerSummaryReadModel>("cust-Live", ct: TestContext.Current.CancellationToken);
        doc.ShouldBeNull();
    }

    [Fact]
    public async Task MultiStreamProjection_Uses_Custom_PartitionKey_Selector()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Schema.For<CustomPkReadModel>().PartitionKey(x => x.TenantGroup);
        options.Projections.Add<TestCustomPkMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storageProvider, storageProvider, options);

        session.Events.StartStream<object>("events/1", new CustomPkEvent("doc-1", "Group-A", 99.5m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var doc = await session.LoadAsync<CustomPkReadModel>("doc-1", "Group-A", ct: TestContext.Current.CancellationToken);
        doc.ShouldNotBeNull();
        doc.Id.ShouldBe("doc-1");
        doc.TenantGroup.ShouldBe("Group-A");
        doc.Balance.ShouldBe(99.5m);
    }

    [Fact]
    public async Task ProcessEventAsync_WhenIdentityToStringIsWhitespace_ReturnsEarly()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        using var session = new DocumentSession(storageProvider, storageProvider, options);

        var projection = new WhitespaceIdentityProjection();
        var evt = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-1", 10m)
        };

        await Should.NotThrowAsync(() => projection.ProcessEventAsync(session, evt, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ProcessEventAsync_WhenPartitionKeySelectorReturnsEmpty_UsesTypeNameFallback()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        using var session = new DocumentSession(storageProvider, storageProvider, options);

        var projection = new NoPkMultiStreamProjection();
        var evt = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-1", 100m)
        };

        await projection.ProcessEventAsync(session, evt, TestContext.Current.CancellationToken);
        var doc = await session.LoadAsync<NoPkDoc>("cust-1", ct: TestContext.Current.CancellationToken);
        doc.ShouldNotBeNull();
    }

    [Fact]
    public async Task ProcessEventAsync_DirectInvocation_WhenApplyReturnsFalse_DeletesAndUntracks()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        using var session = new DocumentSession(storageProvider, storageProvider, options);

        var projection = new TestCustomerMultiStreamProjection();

        // 1. First upsert document
        var orderEvt = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "orders/1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-deact", 50m)
        };
        await projection.ProcessEventAsync(session, orderEvt, TestContext.Current.CancellationToken);

        // 2. Direct ProcessEventAsync with tombstone event (Apply returns false)
        var deactEvt = new EventEnvelope<TrackedCustomerDeactivated>
        {
            StreamId = "deactivations/1",
            Version = 1,
            Data = new TrackedCustomerDeactivated("cust-deact", "Closed")
        };
        await projection.ProcessEventAsync(session, deactEvt, TestContext.Current.CancellationToken);

        var doc = await session.LoadAsync<CustomerSummaryReadModel>("cust-deact", ct: TestContext.Current.CancellationToken);
        doc.ShouldBeNull();
    }
}

public class WhitespaceIdentityProjection : MultiStreamProjection<CustomerSummaryReadModel, object>
{
    protected override object Identity(IEvent @event) => "   ";
    public override bool Apply(IEvent @event, CustomerSummaryReadModel document) => true;
}

public class NoPkDoc
{
    public string Id { get; set; } = string.Empty;
    public decimal Total { get; set; }
}

public class NoPkMultiStreamProjection : MultiStreamProjection<NoPkDoc, string>
{
    protected override string Identity(IEvent @event) => "cust-1";
    public override bool Apply(IEvent @event, NoPkDoc document)
    {
        if (@event.Data is TrackedOrderPlaced e)
        {
            document.Id = e.CustomerId;
            document.Total += e.Amount;
        }
        return true;
    }
}
