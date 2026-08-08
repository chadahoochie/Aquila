using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record AccountCreatedEvent(Guid AccountId, string OwnerName, decimal InitialBalance);
public sealed record MoneyDepositedEvent(Guid AccountId, decimal Amount);

public sealed class BankAccountAggregate
{
    public Guid Id { get; set; }
    public string OwnerName { get; set; } = string.Empty;
    public decimal Balance { get; set; }

    public void Apply(AccountCreatedEvent @event)
    {
        Id = @event.AccountId;
        OwnerName = @event.OwnerName;
        Balance = @event.InitialBalance;
    }

    public void Apply(MoneyDepositedEvent @event)
    {
        Balance += @event.Amount;
    }
}

public sealed class EventStoreTests
{
    [Theory, AutoNSubstituteData]
    public async Task StartStream_Appends_Initial_Events_To_Uncommitted_Queue(
        IAquilaStorageProvider storage,
        Guid accountId, string ownerName)
    {
        // Arrange
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);
        var createdEvent = new AccountCreatedEvent(accountId, ownerName, 500.00m);

        // Act
        session.Events.StartStream<BankAccountAggregate>(accountId, createdEvent);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await storage.Events.Received(1).AppendEventsAsync(
            accountId.ToString(),
            Arg.Is<IEnumerable<IEvent>>(events => System.Linq.Enumerable.Any(events, e => e.Version == 1)),
            -1,
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task Append_Guid_And_String_Overloads_Queue_Events(
        IAquilaStorageProvider storage,
        Guid accountId)
    {
        // Arrange
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);
        var depositEvent = new MoneyDepositedEvent(accountId, 100.00m);

        // Act
        session.Events.Append(accountId, depositEvent);
        session.Events.Append(accountId.ToString(), expectedVersion: 2, depositEvent);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await storage.Events.Received(1).AppendEventsAsync(
            accountId.ToString(),
            Arg.Any<IEnumerable<IEvent>>(),
            Arg.Any<long>(),
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task AggregateStreamAsync_Rehydrates_State_From_Fetched_Events(
        IAquilaStorageProvider storage,
        IEventStorageProvider eventStorage,
        Guid accountId)
    {
        // Arrange
        storage.Events.Returns(eventStorage);
        var options = new StoreOptions { StorageProvider = storage };

        var events = new List<IEvent>
        {
            new EventEnvelope<AccountCreatedEvent>
            {
                StreamId = accountId.ToString(),
                Version = 1,
                Data = new AccountCreatedEvent(accountId, "John Doe", 1000.00m)
            },
            new EventEnvelope<MoneyDepositedEvent>
            {
                StreamId = accountId.ToString(),
                Version = 2,
                Data = new MoneyDepositedEvent(accountId, 250.00m)
            }
        };

        eventStorage.FetchEventsAsync(accountId.ToString(), Arg.Any<string?>(), 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new DocumentSession(storage, options);

        // Act
        var aggregate = await session.Events.AggregateStreamAsync<BankAccountAggregate>(accountId, ct: TestContext.Current.CancellationToken);

        // Assert
        aggregate.ShouldNotBeNull();
        aggregate.Id.ShouldBe(accountId);
        aggregate.OwnerName.ShouldBe("John Doe");
        aggregate.Balance.ShouldBe(1250.00m);
    }

    [Theory, AutoNSubstituteData]
    public async Task AggregateStreamAsync_WithVersionLimit_StopsAtSpecifiedVersion(
        IAquilaStorageProvider storage,
        IEventStorageProvider eventStorage,
        Guid accountId)
    {
        // Arrange
        storage.Events.Returns(eventStorage);
        var options = new StoreOptions { StorageProvider = storage };

        var events = new List<IEvent>
        {
            new EventEnvelope<AccountCreatedEvent>
            {
                StreamId = accountId.ToString(),
                Version = 1,
                Data = new AccountCreatedEvent(accountId, "Jane Doe", 500.00m)
            },
            new EventEnvelope<MoneyDepositedEvent>
            {
                StreamId = accountId.ToString(),
                Version = 2,
                Data = new MoneyDepositedEvent(accountId, 250.00m)
            }
        };

        eventStorage.FetchEventsAsync(accountId.ToString(), Arg.Any<string?>(), 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new DocumentSession(storage, options);

        // Act
        var aggregate = await session.Events.AggregateStreamAsync<BankAccountAggregate>(accountId, version: 1, ct: TestContext.Current.CancellationToken);

        // Assert
        aggregate.ShouldNotBeNull();
        aggregate.Balance.ShouldBe(500.00m);
    }

    [Theory, AutoNSubstituteData]
    public void EventStore_InputValidation_ThrowsExceptions_OnInvalidParameters(
        IAquilaStorageProvider storage)
    {
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        Should.Throw<ArgumentException>(() => session.Events.StartStream<BankAccountAggregate>("", new object()));
        Should.Throw<ArgumentException>(() => session.Events.StartStream<BankAccountAggregate>("   ", new object()));
        Should.Throw<ArgumentNullException>(() => session.Events.StartStream<BankAccountAggregate>("stream1", null!));

        Should.Throw<ArgumentException>(() => session.Events.Append("", new object()));
        Should.Throw<ArgumentException>(() => session.Events.Append("   ", 1, new object()));
        Should.Throw<ArgumentNullException>(() => session.Events.Append("stream1", null!));

        Should.ThrowAsync<ArgumentException>(() => session.Events.FetchStreamAsync(""));
        Should.ThrowAsync<ArgumentException>(() => session.Events.FetchStreamAsync("   "));

        Should.ThrowAsync<ArgumentException>(() => session.Events.AggregateStreamAsync<BankAccountAggregate>(""));
        Should.ThrowAsync<ArgumentException>(() => session.Events.AggregateStreamAsync<BankAccountAggregate>("   "));
    }
}
