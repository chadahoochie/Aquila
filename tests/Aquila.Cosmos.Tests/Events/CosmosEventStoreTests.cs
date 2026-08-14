using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Events;
using Aquila.Core.Storage;
using Aquila.Cosmos.Events;

namespace Aquila.Cosmos.Tests;

public sealed record CosmosAccountCreatedEvent(Guid AccountId, string Name);
public sealed record CosmosMoneyDepositedEvent(Guid AccountId, decimal Amount);

public sealed class CosmosBankAccountAggregate
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Balance { get; set; }

    public void Apply(CosmosAccountCreatedEvent @event)
    {
        Id = @event.AccountId;
        Name = @event.Name;
    }

    public void Apply(CosmosMoneyDepositedEvent @event)
    {
        Balance += @event.Amount;
    }
}

public sealed class CosmosEventStoreTests
{
    [Fact]
    public async Task CosmosEventStore_Delegates_All_Operations_To_StorageProvider()
    {
        var eventStorage = Substitute.For<IEventStorageProvider>();
        var cosmosEventStore = new CosmosEventStore(eventStorage, "tenant-cosmos");
        var streamId = Guid.NewGuid();
        var evt1 = new CosmosAccountCreatedEvent(streamId, "Cosmos Owner");
        var evt2 = new CosmosMoneyDepositedEvent(streamId, 100m);

        cosmosEventStore.StartStream<CosmosBankAccountAggregate>(streamId, evt1);
        cosmosEventStore.StartStream<CosmosBankAccountAggregate>(streamId.ToString(), evt1);
        cosmosEventStore.Append(streamId, evt2);
        cosmosEventStore.Append(streamId.ToString(), evt2);
        cosmosEventStore.Append(streamId, expectedVersion: 2, evt2);
        cosmosEventStore.Append(streamId.ToString(), expectedVersion: 3, evt2);

        // Generic Append overloads
        cosmosEventStore.Append<CosmosBankAccountAggregate>(streamId, evt2);
        cosmosEventStore.Append<CosmosBankAccountAggregate>(streamId.ToString(), evt2);
        cosmosEventStore.Append<CosmosBankAccountAggregate>(streamId, expectedVersion: 4, evt2);
        cosmosEventStore.Append<CosmosBankAccountAggregate>(streamId.ToString(), expectedVersion: 5, evt2);

        cosmosEventStore.UncommittedEvents.Count.ShouldBe(10);

        var eventsList = new List<IEvent>
        {
            new EventEnvelope<CosmosAccountCreatedEvent> { StreamId = streamId.ToString(), Version = 1, TenantId = "tenant-cosmos", Data = evt1 },
            new EventEnvelope<CosmosMoneyDepositedEvent> { StreamId = streamId.ToString(), Version = 2, TenantId = "tenant-cosmos", Data = evt2 }
        };

        eventStorage.FetchEventsAsync(streamId.ToString(), "tenant-cosmos", 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(eventsList));

        var fetched = await cosmosEventStore.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        fetched.Count.ShouldBe(2);

        var fetchedStr = await cosmosEventStore.FetchStreamAsync(streamId.ToString(), ct: TestContext.Current.CancellationToken);
        fetchedStr.Count.ShouldBe(2);

        var aggregate = await cosmosEventStore.AggregateStreamAsync<CosmosBankAccountAggregate>(streamId, ct: TestContext.Current.CancellationToken);
        aggregate.ShouldNotBeNull();
        aggregate.Name.ShouldBe("Cosmos Owner");
        aggregate.Balance.ShouldBe(100m);

        var aggregateStr = await cosmosEventStore.AggregateStreamAsync<CosmosBankAccountAggregate>(streamId.ToString(), ct: TestContext.Current.CancellationToken);
        aggregateStr.ShouldNotBeNull();

        var aggregateAtVersion = await cosmosEventStore.AggregateStreamAsync<CosmosBankAccountAggregate>(streamId, version: 1, ct: TestContext.Current.CancellationToken);
        aggregateAtVersion.ShouldNotBeNull();
        aggregateAtVersion.Balance.ShouldBe(0m);

        var aggregateStrAtVersion = await cosmosEventStore.AggregateStreamAsync<CosmosBankAccountAggregate>(streamId.ToString(), version: 1, ct: TestContext.Current.CancellationToken);
        aggregateStrAtVersion.ShouldNotBeNull();
        aggregateStrAtVersion.Balance.ShouldBe(0m);

        eventStorage.FetchGlobalEventsAsync(0, 50, "tenant-cosmos", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(eventsList));

        var globalEvents = await cosmosEventStore.FetchGlobalEventsAsync(0, 50, TestContext.Current.CancellationToken);
        globalEvents.Count.ShouldBe(2);

        cosmosEventStore.ClearUncommittedEvents();
        cosmosEventStore.UncommittedEvents.Count.ShouldBe(0);
    }

    [Fact]
    public void CosmosEventStore_Constructor_Validates_Arguments()
    {
        Should.Throw<ArgumentNullException>(() => new CosmosEventStore((IEventStorageProvider)null!, "tenant"));
        Should.Throw<ArgumentException>(() => new CosmosEventStore(Substitute.For<IEventStorageProvider>(), ""));
        Should.Throw<ArgumentException>(() => new CosmosEventStore(Substitute.For<IEventStorageProvider>(), "  "));
    }

    [Fact]
    public void CosmosEventStore_Container_Constructor_Initializes()
    {
        var mockClient = Substitute.For<CosmosClient>();
        var mockDb = Substitute.For<Database>();
        mockDb.Client.Returns(mockClient);
        var mockContainer = Substitute.For<Container>();
        mockContainer.Database.Returns(mockDb);

        var store = new CosmosEventStore(mockContainer, "tenant-1");
        store.ShouldNotBeNull();
    }
}
