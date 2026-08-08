using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Events;
using Aquila.Cosmos.Extensions;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record IntegrationDocument(string Id, string Title, decimal Amount);
public sealed record IntegrationOrderCreatedEvent(Guid OrderId, string CustomerName, decimal TotalAmount);
public sealed record IntegrationItemAddedEvent(Guid OrderId, string ItemName, decimal ItemPrice);

public sealed class IntegrationOrderAggregate
{
    public Guid Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }

    public void Apply(IntegrationOrderCreatedEvent @event)
    {
        Id = @event.OrderId;
        CustomerName = @event.CustomerName;
        TotalAmount = @event.TotalAmount;
    }

    public void Apply(IntegrationItemAddedEvent @event)
    {
        TotalAmount += @event.ItemPrice;
    }
}

[Collection("CosmosIntegration")]
public sealed class CosmosIntegrationTests
{
    private readonly CosmosContainerFixture _fixture;

    public CosmosIntegrationTests(CosmosContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task CosmosStorageProvider_Integration_Document_Crud_And_Batch()
    {
        var provider = new CosmosStorageProvider(_fixture.Client, "IntegrationDb", "DocumentsContainer");
        await provider.InitializeAsync(TestContext.Current.CancellationToken);

        var doc1 = new IntegrationDocument("id-101", "Order 101", 150.00m);
        var envelope1 = new DocumentEnvelope<IntegrationDocument>
        {
            Id = doc1.Id,
            PartitionKey = nameof(IntegrationDocument),
            DocType = nameof(IntegrationDocument),
            TenantId = "tenant-int",
            Data = doc1
        };

        // 1. Upsert & Read
        await provider.Documents.UpsertDocumentAsync(envelope1, TestContext.Current.CancellationToken);

        var loaded = await provider.Documents.ReadDocumentAsync<IntegrationDocument>(doc1.Id, nameof(IntegrationDocument), TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe("id-101");
        loaded.Data.Title.ShouldBe("Order 101");
        loaded.Data.Amount.ShouldBe(150.00m);

        // 2. Query
        var queryResults = await provider.Documents.QueryDocumentsAsync<IntegrationDocument>(
            x => x.TenantId == "tenant-int",
            null,
            TestContext.Current.CancellationToken);

        queryResults.ShouldNotBeEmpty();
        queryResults.ShouldContain(x => x.Id == "id-101");

        // 3. Batch Delete
        var deleteOp = new StorageOperation
        {
            OperationType = StorageOperationType.Delete,
            Id = doc1.Id,
            PartitionKey = nameof(IntegrationDocument),
            DocType = nameof(IntegrationDocument)
        };

        await provider.Documents.ExecuteBatchAsync(new[] { deleteOp }, TestContext.Current.CancellationToken);

        var deletedRead = await provider.Documents.ReadDocumentAsync<IntegrationDocument>(doc1.Id, nameof(IntegrationDocument), TestContext.Current.CancellationToken);
        deletedRead.ShouldBeNull();
    }

    [Fact]
    public async Task CosmosStorageProvider_Integration_EventSourcing_Stream_Appends_And_Concurrency()
    {
        var provider = new CosmosStorageProvider(_fixture.Client, "IntegrationDb", "EventsContainer");
        await provider.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid().ToString();
        var orderCreated = new EventEnvelope<IntegrationOrderCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            TenantId = "tenant-int",
            Data = new IntegrationOrderCreatedEvent(Guid.Parse(streamId), "Acme Corp", 500.00m)
        };

        // 1. Append Event
        await provider.Events.AppendEventsAsync(streamId, new[] { orderCreated }, expectedVersion: 0, ct: TestContext.Current.CancellationToken);

        var header = await provider.Events.GetStreamHeaderAsync(streamId, "tenant-int", TestContext.Current.CancellationToken);
        header.ShouldNotBeNull();
        header.Version.ShouldBe(1);

        // 2. Fetch Events
        var events = await provider.Events.FetchEventsAsync(streamId, "tenant-int", 0, TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);

        // 3. Optimistic Concurrency Failure Check
        var itemAdded = new EventEnvelope<IntegrationItemAddedEvent>
        {
            StreamId = streamId,
            Version = 2,
            TenantId = "tenant-int",
            Data = new IntegrationItemAddedEvent(Guid.Parse(streamId), "Add-on", 50.00m)
        };

        await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            provider.Events.AppendEventsAsync(streamId, new[] { itemAdded }, expectedVersion: 99, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DocumentStore_Integration_DocumentSession_And_EventStore_AggregateRehydration()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "SessionContainer");
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var orderId = Guid.NewGuid();

        using (var session = store.OpenSession())
        {
            var created = new IntegrationOrderCreatedEvent(orderId, "Globex", 1000m);
            var itemAdded = new IntegrationItemAddedEvent(orderId, "Widget", 200m);

            session.Events.StartStream<IntegrationOrderAggregate>(orderId, created);
            session.Events.Append(orderId, itemAdded);

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var session = store.OpenSession())
        {
            var aggregate = await session.Events.AggregateStreamAsync<IntegrationOrderAggregate>(orderId, ct: TestContext.Current.CancellationToken);

            aggregate.ShouldNotBeNull();
            aggregate.Id.ShouldBe(orderId);
            aggregate.CustomerName.ShouldBe("Globex");
            aggregate.TotalAmount.ShouldBe(1200m);
        }
    }
}
