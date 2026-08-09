using System;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record TrackedOrderPlaced(string OrderId, string CustomerId, decimal Amount);
public sealed record TrackedPaymentReceived(string OrderId, string CustomerId, decimal AmountPaid);
public sealed record TrackedCustomerDeactivated(string CustomerId, string Reason);

public class CustomerSummaryReadModel
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public decimal TotalPaid { get; set; }
    public int OrderCount { get; set; }
}

public class TestCustomerMultiStreamProjection : MultiStreamProjection<CustomerSummaryReadModel, string>
{
    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            TrackedOrderPlaced e => e.CustomerId,
            TrackedPaymentReceived e => e.CustomerId,
            TrackedCustomerDeactivated e => e.CustomerId,
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
                return true;

            case TrackedPaymentReceived e:
                document.TotalPaid += e.AmountPaid;
                return true;

            case TrackedCustomerDeactivated:
                return false; // Return false to indicate tombstone / deletion

            default:
                return true;
        }
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
    }

    [Fact]
    public void StoreOptions_Can_Register_MultiStreamProjection()
    {
        var options = new StoreOptions();
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        options.Projections.Projections.Count.ShouldBe(1);
        options.Projections.Projections[0].ShouldBeOfType<TestCustomerMultiStreamProjection>();
        options.Projections.Projections[0].Lifecycle.ShouldBe(ProjectionLifecycle.Inline);
    }

    [Fact]
    public async Task MultiStreamProjection_Aggregates_Events_From_Multiple_Streams_Into_Single_ReadModel()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storageProvider, options);

        var orderEvent = new TrackedOrderPlaced("ord-100", "cust-A", 250.00m);
        var paymentEvent = new TrackedPaymentReceived("ord-100", "cust-A", 200.00m);

        // Act: Append events to TWO DIFFERENT streams
        session.Events.StartStream<object>("orders/ord-100", orderEvent);
        session.Events.StartStream<object>("payments/pay-500", paymentEvent);

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert: Read model document "cust-A" is projected from both streams
        var doc = await session.LoadAsync<CustomerSummaryReadModel>("cust-A", ct: TestContext.Current.CancellationToken);

        doc.ShouldNotBeNull();
        doc.CustomerId.ShouldBe("cust-A");
        doc.TotalAmount.ShouldBe(250.00m);
        doc.TotalPaid.ShouldBe(200.00m);
        doc.OrderCount.ShouldBe(1);
    }

    [Fact]
    public async Task MultiStreamProjection_Deletes_Document_When_Apply_Returns_False_Tombstone()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session1 = new DocumentSession(storageProvider, options);

        // Step 1: Create document via order event
        session1.Events.StartStream<object>("orders/ord-101", new TrackedOrderPlaced("ord-101", "cust-B", 100.00m));
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        var docBefore = await session1.LoadAsync<CustomerSummaryReadModel>("cust-B", ct: TestContext.Current.CancellationToken);
        docBefore.ShouldNotBeNull();

        // Step 2: Append tombstone event (TrackedCustomerDeactivated) in session 2
        using var session2 = new DocumentSession(storageProvider, options);
        session2.Events.StartStream<object>("deactivations/deact-1", new TrackedCustomerDeactivated("cust-B", "Account Closed"));
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert: Document "cust-B" is deleted (tombstoned)
        var docAfter = await session2.LoadAsync<CustomerSummaryReadModel>("cust-B", ct: TestContext.Current.CancellationToken);
        docAfter.ShouldBeNull();
    }

    [Fact]
    public async Task MultiStreamProjection_Ignores_Events_With_Null_Identity()
    {
        // Arrange
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        options.Projections.Add<TestCustomerMultiStreamProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storageProvider, options);

        // Append an event type not mapped by Identity (e.g. UnrelatedEvent)
        session.Events.StartStream<object>("unrelated/1", new UnrelatedEvent("ignored"));
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
    public async Task ProcessEventAsync_WhenIdentityToStringIsWhitespace_ReturnsEarly()
    {
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.UseStorageProvider(storageProvider);
        using var session = new DocumentSession(storageProvider, options);

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
        using var session = new DocumentSession(storageProvider, options);

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
        using var session = new DocumentSession(storageProvider, options);

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

    [Fact]
    public void ApplyEvent_WhenAggregateIsNotTargetDocumentType_ReturnsEarly()
    {
        var projection = new TestCustomerMultiStreamProjection();
        var evt = new EventEnvelope<TrackedOrderPlaced>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new TrackedOrderPlaced("ord-1", "cust-1", 50m)
        };

        Should.NotThrow(() => projection.ApplyEvent(evt, "NotACustomerSummaryReadModel"));
    }

    [Fact]
    public async Task ProcessEventAsync_Throws_On_Null_Arguments()
    {
        var projection = new TestCustomerMultiStreamProjection();
        var storageProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        using var session = new DocumentSession(storageProvider, options);
        var evt = new EventEnvelope<TrackedOrderPlaced> { StreamId = "1", Version = 1, Data = new TrackedOrderPlaced("1", "1", 10m) };

        await Should.ThrowAsync<ArgumentNullException>(() => projection.ProcessEventAsync(null!, evt, CancellationToken.None));
        await Should.ThrowAsync<ArgumentNullException>(() => projection.ProcessEventAsync(session, null!, CancellationToken.None));
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

