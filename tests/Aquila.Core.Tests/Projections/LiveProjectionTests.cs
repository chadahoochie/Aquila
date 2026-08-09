using NSubstitute;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed class LiveProjectionTests
{
    [Fact]
    public async Task LiveStreamAsync_Evaluates_Registered_SingleStreamProjection_Without_Persisting_Document()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var docStorage = NSubstitute.Substitute.For<IDocumentStorageProvider>();
        var eventStorage = NSubstitute.Substitute.For<IEventStorageProvider>();
        storageProvider.Documents.Returns(docStorage);
        storageProvider.Events.Returns(eventStorage);

        var options = new StoreOptions { StorageProvider = storageProvider };
        options.Projections.Add<UserProjection>(ProjectionLifecycle.Live);

        var streamId = Guid.NewGuid().ToString();
        var userId = Guid.NewGuid();

        var events = new IEvent[]
        {
            new EventEnvelope<UserRegisteredEvent>
            {
                StreamId = streamId,
                Version = 1,
                Data = new UserRegisteredEvent(userId, "Alice Smith", "alice@example.com")
            },
            new EventEnvelope<UserEmailUpdatedEvent>
            {
                StreamId = streamId,
                Version = 2,
                Data = new UserEmailUpdatedEvent(userId, "alice.smith@example.com")
            }
        };

        eventStorage.FetchEventsAsync(streamId, "default", 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new QuerySession(storageProvider, options);

        // Act
        var result = await session.LiveStreamAsync<UserAggregate>(streamId, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe(userId);
        result.FullName.ShouldBe("Alice Smith");
        result.Email.ShouldBe("alice.smith@example.com");
        result.RevisionCount.ShouldBe(2);

        // Ensure NO storage write operations were executed
        await docStorage.DidNotReceiveWithAnyArgs().UpsertDocumentAsync<UserAggregate>(default!, Arg.Any<CancellationToken>());
        await docStorage.DidNotReceiveWithAnyArgs().ExecuteBatchAsync(default!, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task LiveStreamAsync_Falls_Back_To_SingleStream_Aggregate_Convention_When_No_Projection_Registered()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var docStorage = NSubstitute.Substitute.For<IDocumentStorageProvider>();
        var eventStorage = NSubstitute.Substitute.For<IEventStorageProvider>();
        storageProvider.Documents.Returns(docStorage);
        storageProvider.Events.Returns(eventStorage);

        var options = new StoreOptions { StorageProvider = storageProvider };
        // Note: No projection registered in options.Projections

        var streamId = Guid.NewGuid().ToString();
        var userId = Guid.NewGuid();

        var events = new IEvent[]
        {
            new EventEnvelope<UserRegisteredEvent>
            {
                StreamId = streamId,
                Version = 1,
                Data = new UserRegisteredEvent(userId, "Bob Builder", "bob@example.com")
            },
            new EventEnvelope<UserEmailUpdatedEvent>
            {
                StreamId = streamId,
                Version = 2,
                Data = new UserEmailUpdatedEvent(userId, "bob.builder@example.com")
            }
        };

        eventStorage.FetchEventsAsync(streamId, "default", 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new QuerySession(storageProvider, options);

        // Act
        var result = await session.LiveStreamAsync<UserAggregate>(streamId, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe(userId);
        result.FullName.ShouldBe("Bob Builder");
        result.Email.ShouldBe("bob.builder@example.com");
        result.RevisionCount.ShouldBe(2);

        // Verify no persistence occurred
        await docStorage.DidNotReceiveWithAnyArgs().UpsertDocumentAsync<UserAggregate>(default!, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task LiveStreamAsync_Returns_Null_When_Stream_Has_No_Events()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var eventStorage = NSubstitute.Substitute.For<IEventStorageProvider>();
        storageProvider.Events.Returns(eventStorage);

        var options = new StoreOptions { StorageProvider = storageProvider };
        var streamId = "empty-stream";

        eventStorage.FetchEventsAsync(streamId, "default", 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(Array.Empty<IEvent>()));

        using var session = new QuerySession(storageProvider, options);

        // Act
        var result = await session.LiveStreamAsync<UserAggregate>(streamId, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldBeNull();
    }

    [Fact]
    public async Task LiveStreamAsync_Supports_Guid_StreamId_Overload()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var eventStorage = NSubstitute.Substitute.For<IEventStorageProvider>();
        storageProvider.Events.Returns(eventStorage);

        var options = new StoreOptions { StorageProvider = storageProvider };
        var streamIdGuid = Guid.NewGuid();
        var userId = Guid.NewGuid();

        var events = new IEvent[]
        {
            new EventEnvelope<UserRegisteredEvent>
            {
                StreamId = streamIdGuid.ToString(),
                Version = 1,
                Data = new UserRegisteredEvent(userId, "Charlie Brown", "charlie@example.com")
            }
        };

        eventStorage.FetchEventsAsync(streamIdGuid.ToString(), "default", 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new QuerySession(storageProvider, options);

        // Act
        var result = await session.LiveStreamAsync<UserAggregate>(streamIdGuid, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.FullName.ShouldBe("Charlie Brown");
    }

    [Fact]
    public async Task LiveStreamAsync_Supports_TenantId_Override_Parameter()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var eventStorage = NSubstitute.Substitute.For<IEventStorageProvider>();
        storageProvider.Events.Returns(eventStorage);

        var options = new StoreOptions { StorageProvider = storageProvider };
        var streamId = "tenant-stream-1";
        var customTenant = "tenant-xyz";
        var userId = Guid.NewGuid();

        var events = new IEvent[]
        {
            new EventEnvelope<UserRegisteredEvent>
            {
                StreamId = streamId,
                TenantId = customTenant,
                Version = 1,
                Data = new UserRegisteredEvent(userId, "Dave Tenant", "dave@xyz.com")
            }
        };

        eventStorage.FetchEventsAsync(streamId, customTenant, 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new QuerySession(storageProvider, options);

        // Act
        var result = await session.LiveStreamAsync<UserAggregate>(streamId, customTenant, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.FullName.ShouldBe("Dave Tenant");
    }

    [Fact]
    public async Task LiveStreamAsync_Throws_ArgumentException_On_NullOrWhiteSpace_StreamId()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var options = new StoreOptions { StorageProvider = storageProvider };
        using var session = new QuerySession(storageProvider, options);

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(() => session.LiveStreamAsync<UserAggregate>("", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => session.LiveStreamAsync<UserAggregate>("   ", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => session.LiveStreamAsync<UserAggregate>((string)null!, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task QuerySession_TrackingMode_Constructor_Overload_Evaluates_LiveStream()
    {
        // Arrange
        var storageProvider = NSubstitute.Substitute.For<IAquilaStorageProvider>();
        var docStorage = NSubstitute.Substitute.For<IDocumentStorageProvider>();
        var eventStorage = NSubstitute.Substitute.For<IEventStorageProvider>();
        storageProvider.Documents.Returns(docStorage);
        storageProvider.Events.Returns(eventStorage);

        var options = new StoreOptions { StorageProvider = storageProvider };
        options.Projections.Add<UserProjection>(ProjectionLifecycle.Live);

        var streamId = Guid.NewGuid().ToString();
        var userId = Guid.NewGuid();

        var events = new IEvent[]
        {
            new EventEnvelope<UserRegisteredEvent>
            {
                StreamId = streamId,
                Version = 1,
                Data = new UserRegisteredEvent(userId, "Lightweight Larry", "larry@example.com")
            }
        };

        eventStorage.FetchEventsAsync(streamId, "default", 0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<IEvent>>(events));

        using var session = new QuerySession(storageProvider, options, TrackingMode.Lightweight);

        // Act
        var result = await session.LiveStreamAsync<UserAggregate>(streamId, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.FullName.ShouldBe("Lightweight Larry");
    }
}
