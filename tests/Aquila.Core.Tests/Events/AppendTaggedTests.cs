using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed class AppendTaggedTests
{
    [Fact]
    public async Task AppendTagged_ProducesEnvelopesWithSuppliedTags()
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { DocumentStorage = storage, EventStorage = storage };
        using var session = new DocumentSession(storage, storage, options);

        var streamId = Guid.NewGuid();
        var createdEvent = new AccountCreatedEvent(streamId, "Alice", 500m);

        session.Events.AppendTagged(streamId, new[] { new TaggedEvent(createdEvent, new HashSet<string> { "AccountCreated", "Account" }) });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var events = await storage.FetchEventsAsync(streamId.ToString(), ct: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);
        events[0].Tags.ShouldContain("AccountCreated");
        events[0].Tags.ShouldContain("Account");
    }

    [Fact]
    public async Task Append_ParamsObjectOverload_StillProducesEmptyTags()
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { DocumentStorage = storage, EventStorage = storage };
        using var session = new DocumentSession(storage, storage, options);

        var streamId = Guid.NewGuid();
        var createdEvent = new AccountCreatedEvent(streamId, "Bob", 100m);

        session.Events.Append(streamId, createdEvent);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var events = await storage.FetchEventsAsync(streamId.ToString(), ct: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);
        events[0].Tags.ShouldBeEmpty();
    }
}
