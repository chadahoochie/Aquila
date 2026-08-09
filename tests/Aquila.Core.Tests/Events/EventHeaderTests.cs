using Newtonsoft.Json;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record HeaderTestEvent(string Message, decimal Amount);
public sealed record HeaderTestUpcastEvent(string Message, decimal Amount, string UpcastedBy);

public sealed class HeaderTestEventUpcaster : EventUpcaster<HeaderTestEvent, HeaderTestUpcastEvent>
{
    public override HeaderTestUpcastEvent Upcast(HeaderTestEvent oldEvent)
    {
        return new HeaderTestUpcastEvent(oldEvent.Message, oldEvent.Amount, "SystemUpcaster");
    }
}

public sealed class HeaderTestAggregate
{
    public string Message { get; set; } = string.Empty;
    public decimal Total { get; set; }

    public void Apply(HeaderTestEvent @event)
    {
        Message = @event.Message;
        Total += @event.Amount;
    }
}

public sealed class EventHeaderTests
{
    [Fact]
    public async Task Session_Propagates_CorrelationId_CausationId_And_Headers_To_Appended_Events()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var streamId = Guid.NewGuid().ToString();
        var correlationId = "corr-abc-123";
        var causationId = "cause-xyz-789";

        session.CorrelationId = correlationId;
        session.CausationId = causationId;
        session.SetHeader("UserId", "user-42");
        session.SetHeader("TenantContext", "enterprise-us");

        var evt = new HeaderTestEvent("Test Message", 100m);

        // Act
        session.Events.Append(streamId, evt);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert - Fetch stream events and verify headers were attached
        var fetchedEvents = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        fetchedEvents.Count.ShouldBe(1);

        var fetchedEvent = fetchedEvents[0];
        fetchedEvent.CorrelationId.ShouldBe(correlationId);
        fetchedEvent.CausationId.ShouldBe(causationId);
        fetchedEvent.Headers.ShouldNotBeNull();
        fetchedEvent.Headers["UserId"].ShouldBe("user-42");
        fetchedEvent.Headers["TenantContext"].ShouldBe("enterprise-us");
    }

    [Fact]
    public async Task Session_Propagates_Headers_When_StartStream_Is_Called()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var streamId = Guid.NewGuid().ToString();
        session.CorrelationId = "corr-start-stream";
        session.CausationId = "cause-start-stream";
        session.SetHeader("AppVersion", "v1.2.3");

        var evt = new HeaderTestEvent("Stream Started", 50m);

        // Act
        session.Events.StartStream<HeaderTestAggregate>(streamId, evt);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        var fetchedEvents = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        fetchedEvents.Count.ShouldBe(1);

        var fetchedEvent = fetchedEvents[0];
        fetchedEvent.CorrelationId.ShouldBe("corr-start-stream");
        fetchedEvent.CausationId.ShouldBe("cause-start-stream");
        fetchedEvent.Headers["AppVersion"].ShouldBe("v1.2.3");
    }

    [Fact]
    public async Task Headers_Survive_Aggregate_Rehydration()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var streamId = Guid.NewGuid().ToString();
        session.CorrelationId = "corr-aggregate-1";
        session.CausationId = "cause-aggregate-1";
        session.SetHeader("TraceId", "trace-001");

        session.Events.StartStream<HeaderTestAggregate>(streamId, new HeaderTestEvent("Initial", 200m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Act
        var aggregate = await session.Events.AggregateStreamAsync<HeaderTestAggregate>(streamId, ct: TestContext.Current.CancellationToken);

        // Assert aggregate state
        aggregate.ShouldNotBeNull();
        aggregate.Message.ShouldBe("Initial");
        aggregate.Total.ShouldBe(200m);

        // Assert that underlying stream events retain their correlation and header metadata
        var events = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        events[0].CorrelationId.ShouldBe("corr-aggregate-1");
        events[0].CausationId.ShouldBe("cause-aggregate-1");
        events[0].Headers["TraceId"].ShouldBe("trace-001");
    }

    [Fact]
    public async Task Upcasting_Preserves_Event_CorrelationId_CausationId_And_Headers()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        options.Events.RegisterUpcaster<HeaderTestEventUpcaster>();

        using var session = new DocumentSession(storage, options);

        var streamId = Guid.NewGuid().ToString();
        session.CorrelationId = "corr-upcast-100";
        session.CausationId = "cause-upcast-200";
        session.SetHeader("Source", "LegacySystem");

        session.Events.Append(streamId, new HeaderTestEvent("Old Version", 75m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Act
        var events = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);

        // Assert
        events.Count.ShouldBe(1);
        var upcastEvt = events[0];
        upcastEvt.Data.ShouldBeOfType<HeaderTestUpcastEvent>();
        upcastEvt.CorrelationId.ShouldBe("corr-upcast-100");
        upcastEvt.CausationId.ShouldBe("cause-upcast-200");
        upcastEvt.Headers["Source"].ShouldBe("LegacySystem");
    }

    [Fact]
    public void EventEnvelope_Json_Serialization_Preserves_Headers()
    {
        // Arrange
        var envelope = new EventEnvelope<HeaderTestEvent>
        {
            StreamId = "stream-json",
            Version = 1,
            CorrelationId = "corr-json-999",
            CausationId = "cause-json-888",
            Headers = new Dictionary<string, object>
            {
                ["CustomMeta"] = "MetaValue",
                ["NumericCode"] = 12345L
            },
            Data = new HeaderTestEvent("JsonPayload", 300m)
        };

        // Act
        var json = JsonConvert.SerializeObject(envelope);
        var deserialized = JsonConvert.DeserializeObject<EventEnvelope<HeaderTestEvent>>(json);

        // Assert
        deserialized.ShouldNotBeNull();
        deserialized.CorrelationId.ShouldBe("corr-json-999");
        deserialized.CausationId.ShouldBe("cause-json-888");
        deserialized.Headers.ShouldNotBeNull();
        deserialized.Headers["CustomMeta"].ToString().ShouldBe("MetaValue");
    }

    [Fact]
    public void EventEnvelope_Defaults_To_Empty_ReadOnlyDictionary_Headers()
    {
        var envelope = new EventEnvelope<HeaderTestEvent>();
        envelope.CorrelationId.ShouldBeNull();
        envelope.CausationId.ShouldBeNull();
        envelope.Headers.ShouldNotBeNull();
        envelope.Headers.Count.ShouldBe(0);
    }

    [Fact]
    public void SetHeader_InputValidation_Throws_On_Invalid_Keys()
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        Should.Throw<ArgumentException>(() => session.SetHeader("", "val"));
        Should.Throw<ArgumentException>(() => session.SetHeader("   ", "val"));
        Should.Throw<ArgumentNullException>(() => session.SetHeader(null!, "val"));
        Should.Throw<ArgumentNullException>(() => session.SetHeader("Key", null!));
    }
}
