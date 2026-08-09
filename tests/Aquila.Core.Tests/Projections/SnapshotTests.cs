using NSubstitute;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record SnapshotTestEvent(int Amount);

public sealed class SnapshotTestAggregate
{
    public int TotalAmount { get; set; }

    public void Apply(SnapshotTestEvent @event)
    {
        TotalAmount += @event.Amount;
    }
}

public sealed class SnapshotTests
{
    [Fact]
    public void DefaultSnapshotStrategy_ShouldSnapshot_ReturnsTrue_WhenThresholdReached()
    {
        var strategy = new DefaultSnapshotStrategy<SnapshotTestAggregate>(threshold: 50);

        strategy.ShouldSnapshot(currentVersion: 50, eventsSinceLastSnapshot: 49).ShouldBeFalse();
        strategy.ShouldSnapshot(currentVersion: 50, eventsSinceLastSnapshot: 50).ShouldBeTrue();
        strategy.ShouldSnapshot(currentVersion: 60, eventsSinceLastSnapshot: 55).ShouldBeTrue();
    }

    [Theory, AutoNSubstituteData]
    public async Task AggregateStreamAsync_WithSnapshot_RehydratesFromSnapshotAndFetchesOnlyRemainingEvents(
        IAquilaStorageProvider storage,
        IEventStorageProvider eventStorage)
    {
        // Arrange
        var streamId = Guid.NewGuid().ToString();
        storage.Events.Returns(eventStorage);
        var options = new StoreOptions { StorageProvider = storage };

        var snapshotState = new SnapshotTestAggregate { TotalAmount = 500 };
        long snapshotVersion = 50;

        eventStorage.GetSnapshotAsync<SnapshotTestAggregate>(streamId, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<(SnapshotTestAggregate?, long)>((snapshotState, snapshotVersion)));

        var remainingEvents = new List<IEvent>
        {
            new EventEnvelope<SnapshotTestEvent>
            {
                StreamId = streamId,
                Version = 51,
                Data = new SnapshotTestEvent(50)
            },
            new EventEnvelope<SnapshotTestEvent>
            {
                StreamId = streamId,
                Version = 52,
                Data = new SnapshotTestEvent(25)
            }
        };

        eventStorage.FetchEventsAsync(streamId, Arg.Any<string?>(), 51, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(remainingEvents));

        using var session = new DocumentSession(storage, options);

        // Act
        var aggregate = await session.Events.AggregateStreamAsync<SnapshotTestAggregate>(streamId, ct: TestContext.Current.CancellationToken);

        // Assert
        aggregate.ShouldNotBeNull();
        aggregate.TotalAmount.ShouldBe(575); // 500 (snapshot) + 50 + 25

        await eventStorage.Received(1).FetchEventsAsync(streamId, Arg.Any<string?>(), 51, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InMemoryStorageProvider_SaveSnapshotAsync_And_GetSnapshotAsync_PersistsAndRetrievesSnapshot()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = Guid.NewGuid().ToString();
        var snapshot = new SnapshotTestAggregate { TotalAmount = 300 };

        await provider.SaveSnapshotAsync(streamId, 30, snapshot, tenantId: "tenant1", ct: TestContext.Current.CancellationToken);

        var (retrievedSnapshot, version) = await provider.GetSnapshotAsync<SnapshotTestAggregate>(streamId, tenantId: "tenant1", ct: TestContext.Current.CancellationToken);
        retrievedSnapshot.ShouldNotBeNull();
        retrievedSnapshot.TotalAmount.ShouldBe(300);
        version.ShouldBe(30);

        // Verify tenant isolation
        var (otherTenantSnapshot, otherVersion) = await provider.GetSnapshotAsync<SnapshotTestAggregate>(streamId, tenantId: "tenant2", ct: TestContext.Current.CancellationToken);
        otherTenantSnapshot.ShouldBeNull();
        otherVersion.ShouldBe(0);
    }
}
