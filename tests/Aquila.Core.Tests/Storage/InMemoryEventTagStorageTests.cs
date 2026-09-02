using Shouldly;
using Aquila.Core.Events;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed class InMemoryEventTagStorageTests
{
    private static EventEnvelope<AccountCreatedEvent> CreateTaggedEvent(string streamId, long version, IReadOnlySet<string>? tags = null, string tenantId = "default") =>
        new()
        {
            StreamId = streamId,
            Version = version,
            TenantId = tenantId,
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m),
            Tags = tags ?? new HashSet<string>()
        };

    [Fact]
    public async Task FetchEventsByTagAsync_ReturnsOnlyEventsWithMatchingTag()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-tags-1";

        var patientCreated = CreateTaggedEvent(streamId, 1, new HashSet<string> { "PatientCreated", "Patient" });
        var patientUpdated = CreateTaggedEvent(streamId, 2, new HashSet<string> { "Patient" });
        var untagged = CreateTaggedEvent(streamId, 3);

        await provider.AppendEventsAsync(streamId, new IEvent[] { patientCreated, patientUpdated, untagged }, 0, TestContext.Current.CancellationToken);

        var createdOnly = await provider.FetchEventsByTagAsync("PatientCreated", ct: TestContext.Current.CancellationToken);
        createdOnly.Count.ShouldBe(1);
        createdOnly[0].Version.ShouldBe(1);

        var allPatient = await provider.FetchEventsByTagAsync("Patient", ct: TestContext.Current.CancellationToken);
        allPatient.Count.ShouldBe(2);
    }

    [Fact]
    public async Task FetchEventsByTagAsync_UntaggedLegacyEvent_NeverMatchesAnyTag_AndDoesNotThrow()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-tags-legacy";

        var legacyEvent = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m)
        };

        legacyEvent.Tags.ShouldBeEmpty();

        await provider.AppendEventsAsync(streamId, new[] { legacyEvent }, 0, TestContext.Current.CancellationToken);

        var result = await provider.FetchEventsByTagAsync("AnyTag", ct: TestContext.Current.CancellationToken);
        result.ShouldBeEmpty();
    }

    [Fact]
    public async Task FetchEventsByTagAsync_RespectsTenantIdFilter()
    {
        var provider = new InMemoryStorageProvider();

        var tenantAEvent = CreateTaggedEvent("stream-a", 1, new HashSet<string> { "Patient" }, tenantId: "tenant-a");
        var tenantBEvent = CreateTaggedEvent("stream-b", 1, new HashSet<string> { "Patient" }, tenantId: "tenant-b");

        await provider.AppendEventsAsync("stream-a", new IEvent[] { tenantAEvent }, 0, TestContext.Current.CancellationToken);
        await provider.AppendEventsAsync("stream-b", new IEvent[] { tenantBEvent }, 0, TestContext.Current.CancellationToken);

        var tenantAResults = await provider.FetchEventsByTagAsync("Patient", tenantId: "tenant-a", ct: TestContext.Current.CancellationToken);
        tenantAResults.Count.ShouldBe(1);
        tenantAResults[0].TenantId.ShouldBe("tenant-a");
    }

    [Fact]
    public async Task FetchEventsByTagAsync_RespectsFromGlobalSequenceExclusiveLowerBound()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-tags-seq";

        var first = CreateTaggedEvent(streamId, 1, new HashSet<string> { "Patient" });
        var second = CreateTaggedEvent(streamId, 2, new HashSet<string> { "Patient" });

        await provider.AppendEventsAsync(streamId, new IEvent[] { first, second }, 0, TestContext.Current.CancellationToken);

        var fromFirst = await provider.FetchEventsByTagAsync("Patient", fromGlobalSequence: first.GlobalSequence, ct: TestContext.Current.CancellationToken);
        fromFirst.Count.ShouldBe(1);
        fromFirst[0].GlobalSequence.ShouldBe(second.GlobalSequence);
    }

    [Fact]
    public async Task FetchEventsByTagAsync_RespectsBatchSize()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-tags-batch";

        var events = Enumerable.Range(1, 5)
            .Select(i => (IEvent)CreateTaggedEvent(streamId, i, new HashSet<string> { "Patient" }))
            .ToArray();

        await provider.AppendEventsAsync(streamId, events, 0, TestContext.Current.CancellationToken);

        var page = await provider.FetchEventsByTagAsync("Patient", batchSize: 2, ct: TestContext.Current.CancellationToken);
        page.Count.ShouldBe(2);
        page[0].Version.ShouldBe(1);
        page[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task FetchEventsByTagAsync_BatchSizeZeroOrNegative_ReturnsEmpty()
    {
        var provider = new InMemoryStorageProvider();

        var result = await provider.FetchEventsByTagAsync("Patient", batchSize: 0, ct: TestContext.Current.CancellationToken);
        result.ShouldBeEmpty();
    }

    [Fact]
    public async Task FetchEventsByTagAsync_UnknownTag_ReturnsEmpty()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-tags-unknown";

        var tagged = CreateTaggedEvent(streamId, 1, new HashSet<string> { "Patient" });
        await provider.AppendEventsAsync(streamId, new IEvent[] { tagged }, 0, TestContext.Current.CancellationToken);

        var result = await provider.FetchEventsByTagAsync("NoSuchTag", ct: TestContext.Current.CancellationToken);
        result.ShouldBeEmpty();
    }

    [Fact]
    public async Task FetchEventsByTagAsync_NullOrWhitespaceTag_Throws()
    {
        var provider = new InMemoryStorageProvider();

        await Should.ThrowAsync<ArgumentException>(() => provider.FetchEventsByTagAsync("   "));
    }
}
