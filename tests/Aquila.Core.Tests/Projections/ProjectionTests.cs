using NSubstitute;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record UserRegisteredEvent(Guid UserId, string FullName, string Email);
public sealed record UserEmailUpdatedEvent(Guid UserId, string NewEmail);
public sealed record UnrelatedEvent(string Message);

public sealed class UserAggregate
{
    public Guid Id { get; set; }
    public string FullName { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public int RevisionCount { get; set; }

    public void Apply(UserRegisteredEvent @event)
    {
        Id = @event.UserId;
        FullName = @event.FullName;
        Email = @event.Email;
        RevisionCount = 1;
    }

    public void Apply(UserEmailUpdatedEvent @event)
    {
        Email = @event.NewEmail;
        RevisionCount++;
    }
}

public sealed class UserProjection : SingleStreamProjection<UserAggregate>
{
    public UserProjection()
    {
        CreateEvent<UserRegisteredEvent>(e => new UserAggregate
        {
            Id = e.UserId,
            FullName = e.FullName,
            Email = e.Email,
            RevisionCount = 1
        });

        ProjectEvent<UserEmailUpdatedEvent>((e, aggregate) =>
        {
            aggregate.Email = e.NewEmail;
            aggregate.RevisionCount++;
        });
    }
}

public sealed class ProjectionTests
{
    [Theory, AutoNSubstituteData]
    public void SingleStreamProjection_Initializes_Aggregate_Correctly(
        Guid userId, string fullName, string email)
    {
        // Arrange
        var projection = new UserProjection();
        var aggregate = new UserAggregate();
        var @event = new EventEnvelope<UserRegisteredEvent>
        {
            StreamId = userId.ToString(),
            Version = 1,
            Data = new UserRegisteredEvent(userId, fullName, email)
        };

        // Act
        projection.ApplyEvent(@event, aggregate);

        // Assert
        aggregate.Id.ShouldBe(userId);
        aggregate.FullName.ShouldBe(fullName);
        aggregate.Email.ShouldBe(email);
        aggregate.RevisionCount.ShouldBe(1);
    }

    [Theory, AutoNSubstituteData]
    public void SingleStreamProjection_Applies_Multiple_Mutating_Events(
        Guid userId, string fullName, string initialEmail, string updatedEmail)
    {
        // Arrange
        var projection = new UserProjection();
        var aggregate = new UserAggregate();

        var registeredEvent = new EventEnvelope<UserRegisteredEvent>
        {
            StreamId = userId.ToString(),
            Version = 1,
            Data = new UserRegisteredEvent(userId, fullName, initialEmail)
        };

        var emailUpdatedEvent = new EventEnvelope<UserEmailUpdatedEvent>
        {
            StreamId = userId.ToString(),
            Version = 2,
            Data = new UserEmailUpdatedEvent(userId, updatedEmail)
        };

        // Act
        projection.ApplyEvent(registeredEvent, aggregate);
        projection.ApplyEvent(emailUpdatedEvent, aggregate);

        // Assert
        aggregate.Id.ShouldBe(userId);
        aggregate.FullName.ShouldBe(fullName);
        aggregate.Email.ShouldBe(updatedEmail);
        aggregate.RevisionCount.ShouldBe(2);
    }

    [Theory, AutoNSubstituteData]
    public void Projection_InputValidation_ThrowsExceptions_OnNullArguments(
        Guid userId)
    {
        var projection = new UserProjection();
        var aggregate = new UserAggregate();
        var @event = new EventEnvelope<UserRegisteredEvent>
        {
            StreamId = userId.ToString(),
            Version = 1,
            Data = new UserRegisteredEvent(userId, "Test", "test@test.com")
        };

        Should.Throw<ArgumentNullException>(() => projection.ApplyEvent(null!, aggregate));
        Should.Throw<ArgumentNullException>(() => projection.ApplyEvent(@event, null!));
    }

    [Theory, AutoNSubstituteData]
    public void Projection_Ignores_Unregistered_Event_Types(
        Guid userId)
    {
        // Arrange
        var projection = new UserProjection();
        var aggregate = new UserAggregate { FullName = "Original" };
        var @event = new EventEnvelope<UnrelatedEvent>
        {
            StreamId = userId.ToString(),
            Version = 1,
            Data = new UnrelatedEvent("Hello")
        };

        // Act
        projection.ApplyEvent(@event, aggregate);

        // Assert
        aggregate.FullName.ShouldBe("Original");
    }

    [Theory, AutoNSubstituteData]
    public async Task Inline_Projection_Runs_During_SaveChangesAsync(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        Guid userId)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        options.Projections.Add<UserProjection>(ProjectionLifecycle.Inline);

        using var session = new DocumentSession(storage, options);
        var registeredEvent = new UserRegisteredEvent(userId, "Jane Doe", "jane@doe.com");

        // Act
        session.Events.StartStream<UserAggregate>(userId, registeredEvent);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).UpsertDocumentAsync(
            Arg.Is<DocumentEnvelope<UserAggregate>>(env => env.Id == userId.ToString() && env.DocType == nameof(UserAggregate)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public void PropertyCopier_Fallback_Handles_Aggregates_Without_Writable_Properties()
    {
        var projection = new EmptyAggregateProjection();
        var aggregate = new EmptyAggregate();
        var @event = new EventEnvelope<UserRegisteredEvent>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new UserRegisteredEvent(Guid.NewGuid(), "Name", "email@test.com")
        };

        Should.NotThrow(() => projection.ApplyEvent(@event, aggregate));
    }

    [Fact]
    public void ApplyEvent_WhenAggregateIsNotTargetType_ReturnsEarly()
    {
        var projection = new UserProjection();
        var @event = new EventEnvelope<UserRegisteredEvent>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = new UserRegisteredEvent(Guid.NewGuid(), "Name", "email@test.com")
        };

        Should.NotThrow(() => projection.ApplyEvent(@event, "InvalidAggregateType"));
    }

    [Fact]
    public void ApplyEvent_WhenEventDataIsNull_ReturnsEarly()
    {
        var projection = new UserProjection();
        var aggregate = new UserAggregate();
        var @event = new EventEnvelope<UserRegisteredEvent>
        {
            StreamId = "stream-1",
            Version = 1,
            Data = null!
        };

        Should.NotThrow(() => projection.ApplyEvent(@event, aggregate));
    }

    [Fact]
    public void Registration_Methods_Throw_On_Null_Arguments()
    {
        var projection = new EmptyAggregateProjection();
        Should.Throw<ArgumentNullException>(() => projection.ExposeCreateNull());
        Should.Throw<ArgumentNullException>(() => projection.ExposeProjectNull());
    }
}

public sealed class EmptyAggregate
{
    public int ReadOnlyProp => 42;
}

public sealed class EmptyAggregateProjection : SingleStreamProjection<EmptyAggregate>
{
    public EmptyAggregateProjection()
    {
        CreateEvent<UserRegisteredEvent>(e => new EmptyAggregate());
    }

    public void ExposeCreateNull() => CreateEvent<UserRegisteredEvent>(null!);
    public void ExposeProjectNull() => ProjectEvent<UserRegisteredEvent>(null!);
}

