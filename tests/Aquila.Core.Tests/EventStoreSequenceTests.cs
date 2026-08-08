using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed class EventStoreSequenceTests
{
    [Fact]
    public void EventEnvelope_Has_GlobalSequence_Property()
    {
        var envelope = new EventEnvelope<AccountCreatedEvent>
        {
            GlobalSequence = 42
        };

        envelope.GlobalSequence.ShouldBe(42);
        ((IEvent)envelope).GlobalSequence.ShouldBe(42);
    }

    [Fact]
    public async Task InMemoryStorage_Assigns_Monotonically_Increasing_GlobalSequence_Across_Multiple_Streams()
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        session.Events.StartStream<BankAccountAggregate>(stream1, new AccountCreatedEvent(stream1, "Alice", 100m));
        session.Events.StartStream<BankAccountAggregate>(stream2, new AccountCreatedEvent(stream2, "Bob", 200m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Events.Append(stream1, new MoneyDepositedEvent(stream1, 50m));
        session.Events.Append(stream2, new MoneyDepositedEvent(stream2, 25m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var stream1Events = await session.Events.FetchStreamAsync(stream1, ct: TestContext.Current.CancellationToken);
        var stream2Events = await session.Events.FetchStreamAsync(stream2, ct: TestContext.Current.CancellationToken);

        stream1Events.Count.ShouldBe(2);
        stream2Events.Count.ShouldBe(2);

        stream1Events[0].GlobalSequence.ShouldBe(1);
        stream2Events[0].GlobalSequence.ShouldBe(2);
        stream1Events[1].GlobalSequence.ShouldBe(3);
        stream2Events[1].GlobalSequence.ShouldBe(4);
    }

    [Fact]
    public async Task FetchGlobalEventsAsync_Returns_Events_After_FromGlobalSequence_Ordered_Ascending()
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        session.Events.StartStream<BankAccountAggregate>(stream1, new AccountCreatedEvent(stream1, "Alice", 100m));
        session.Events.StartStream<BankAccountAggregate>(stream2, new AccountCreatedEvent(stream2, "Bob", 200m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var globalEventsFromStart = await session.Events.FetchGlobalEventsAsync(0, ct: TestContext.Current.CancellationToken);
        globalEventsFromStart.Count.ShouldBe(2);
        globalEventsFromStart[0].GlobalSequence.ShouldBe(1);
        globalEventsFromStart[1].GlobalSequence.ShouldBe(2);

        var globalEventsAfterFirst = await session.Events.FetchGlobalEventsAsync(1, ct: TestContext.Current.CancellationToken);
        globalEventsAfterFirst.Count.ShouldBe(1);
        globalEventsAfterFirst[0].GlobalSequence.ShouldBe(2);
    }

    [Fact]
    public async Task FetchGlobalEventsAsync_Respects_BatchSize()
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var stream1 = Guid.NewGuid();

        session.Events.StartStream<BankAccountAggregate>(stream1, 
            new AccountCreatedEvent(stream1, "Alice", 100m),
            new MoneyDepositedEvent(stream1, 50m),
            new MoneyDepositedEvent(stream1, 25m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var batch = await session.Events.FetchGlobalEventsAsync(0, batchSize: 2, ct: TestContext.Current.CancellationToken);
        batch.Count.ShouldBe(2);
        batch[0].GlobalSequence.ShouldBe(1);
        batch[1].GlobalSequence.ShouldBe(2);
    }

    [Fact]
    public async Task StorageProvider_FetchGlobalEventsAsync_Filters_By_TenantId()
    {
        var storage = new InMemoryStorageProvider();

        var evt1 = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = "s1",
            Version = 1,
            TenantId = "tenant-a",
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m)
        };
        var evt2 = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = "s2",
            Version = 1,
            TenantId = "tenant-b",
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Bob", 200m)
        };

        await storage.Events.AppendEventsAsync("s1", new[] { evt1 }, 0, TestContext.Current.CancellationToken);
        await storage.Events.AppendEventsAsync("s2", new[] { evt2 }, 0, TestContext.Current.CancellationToken);

        var tenantAEvents = await storage.Events.FetchGlobalEventsAsync(0, 1000, "tenant-a", TestContext.Current.CancellationToken);
        tenantAEvents.Count.ShouldBe(1);
        tenantAEvents[0].TenantId.ShouldBe("tenant-a");

        var allEvents = await storage.Events.FetchGlobalEventsAsync(0, 1000, null, TestContext.Current.CancellationToken);
        allEvents.Count.ShouldBe(2);
    }
}
