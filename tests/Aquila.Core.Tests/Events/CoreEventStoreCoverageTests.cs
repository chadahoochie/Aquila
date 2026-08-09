using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Shouldly;
using Xunit;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests.Events;

public class CoreEventStoreCoverageTests
{
    public class SimpleAggregate
    {
        public int Count { get; set; }
        public string Text { get; set; } = string.Empty;

        public void Apply(SimpleEvent e)
        {
            Count += e.Amount;
            Text = e.Text;
        }
    }

    public record SimpleEvent(int Amount, string Text);
    public record OtherEvent(string Note);

    public class NoApplyAggregate
    {
        public int Value { get; set; }
    }

    [Fact]
    public async Task AggregateStreamAsync_SnapshotScenarios()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var eventStorage = Substitute.For<IEventStorageProvider>();
        storage.Events.Returns(eventStorage);

        var eventStore = new CoreEventStore(storage, "tenant1");
        var streamId = "stream-snapshot";

        // Scenario 1: Snapshot exists, version > 0, requested version == 0 -> uses snapshot, fetches events from snapshotVersion + 1
        var snapshot1 = new SimpleAggregate { Count = 10, Text = "Snap" };
        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamId, "tenant1", Arg.Any<CancellationToken>())
            .Returns((snapshot1, 5L));
        eventStorage.FetchEventsAsync(streamId, "tenant1", 6L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent>
            {
                new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 6, Data = new SimpleEvent(2, "Snap2") }
            });

        var result1 = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamId, ct: TestContext.Current.CancellationToken);
        result1.ShouldNotBeNull();
        result1.Count.ShouldBe(12);
        result1.Text.ShouldBe("Snap2");

        // Scenario 2: Snapshot exists (version 5), requested version = 4 (< snapshotVersion) -> ignores snapshot, fetches from version 0
        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamId, "tenant1", Arg.Any<CancellationToken>())
            .Returns((snapshot1, 5L));
        eventStorage.FetchEventsAsync(streamId, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent>
            {
                new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 1, Data = new SimpleEvent(3, "V1") },
                new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 2, Data = new SimpleEvent(4, "V2") },
                new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 5, Data = new SimpleEvent(5, "V5") }
            });

        var result2 = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamId, version: 4, ct: TestContext.Current.CancellationToken);
        result2.ShouldNotBeNull();
        result2.Count.ShouldBe(7); // V1 (3) + V2 (4) = 7, stops before V5
        result2.Text.ShouldBe("V2");

        // Scenario 3: Snapshot exists with snapshotVersion = 0 -> ignores snapshot
        var snapshot0 = new SimpleAggregate { Count = 99 };
        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamId, "tenant1", Arg.Any<CancellationToken>())
            .Returns((snapshot0, 0L));
        eventStorage.FetchEventsAsync(streamId, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent>());

        var result3 = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamId, ct: TestContext.Current.CancellationToken);
        result3.ShouldBeNull(); // No events and invalid snapshot -> returns null
    }

    [Fact]
    public async Task AggregateStreamAsync_StreamNotFound_ReturnsNull()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var eventStorage = Substitute.For<IEventStorageProvider>();
        storage.Events.Returns(eventStorage);

        var eventStore = new CoreEventStore(storage, "tenant1");
        var streamId = "non-existent-stream";

        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamId, "tenant1", Arg.Any<CancellationToken>())
            .Returns(((SimpleAggregate?)null, 0L));
        eventStorage.FetchEventsAsync(streamId, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent>());

        var result = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamId, ct: TestContext.Current.CancellationToken);
        result.ShouldBeNull();
    }

    [Fact]
    public async Task AggregateStreamAsync_MultiBranch_ReplayConditions()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var eventStorage = Substitute.For<IEventStorageProvider>();
        storage.Events.Returns(eventStorage);

        var eventStore = new CoreEventStore(storage, "tenant1");
        var streamId = "stream-multi";

        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamId, "tenant1", Arg.Any<CancellationToken>())
            .Returns(((SimpleAggregate?)null, 0L));

        var events = new List<IEvent>
        {
            new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 1, Data = new SimpleEvent(10, "E1") },
            new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 2, Data = new SimpleEvent(20, "E2") },
            new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 3, Data = new SimpleEvent(30, "E3") },
            new EventEnvelope<SimpleEvent> { StreamId = streamId, Version = 4, Data = new SimpleEvent(40, "E4") }
        };

        eventStorage.FetchEventsAsync(streamId, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(events);

        // Requested version = 2 (breaks loop when version 3 > 2 is encountered)
        var result = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamId, version: 2, ct: TestContext.Current.CancellationToken);
        result.ShouldNotBeNull();
        result.Count.ShouldBe(30); // 10 + 20
        result.Text.ShouldBe("E2");
    }

    [Fact]
    public async Task AggregateStreamAsync_Applies_JObject_And_JsonString_And_IgnoresUnmatchedEvents()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var eventStorage = Substitute.For<IEventStorageProvider>();
        storage.Events.Returns(eventStorage);

        var eventStore = new CoreEventStore(storage, "tenant1");

        // 1. JObject payload matching string event type name
        var streamJObj = "stream-jobj";
        var jobj = JObject.FromObject(new SimpleEvent(15, "JObj"));
        var envJObj = new EventEnvelope<object>
        {
            StreamId = streamJObj,
            Version = 1,
            EventType = typeof(SimpleEvent).FullName!,
            Data = jobj
        };
        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamJObj, "tenant1", Arg.Any<CancellationToken>())
            .Returns(((SimpleAggregate?)null, 0L));
        eventStorage.FetchEventsAsync(streamJObj, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent> { envJObj });

        var aggJObj = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamJObj, ct: TestContext.Current.CancellationToken);
        aggJObj.ShouldNotBeNull();
        aggJObj.Count.ShouldBe(15);
        aggJObj.Text.ShouldBe("JObj");

        // 2. String JSON payload matching string event type name
        var streamJsonStr = "stream-jsonstr";
        var jsonStr = Newtonsoft.Json.JsonConvert.SerializeObject(new SimpleEvent(25, "JsonStr"));
        var envJsonStr = new EventEnvelope<string>
        {
            StreamId = streamJsonStr,
            Version = 1,
            EventType = typeof(SimpleEvent).FullName!,
            Data = jsonStr
        };
        eventStorage.GetSnapshotAsync<SimpleAggregate>(streamJsonStr, "tenant1", Arg.Any<CancellationToken>())
            .Returns(((SimpleAggregate?)null, 0L));
        eventStorage.FetchEventsAsync(streamJsonStr, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent> { envJsonStr });

        var aggJsonStr = await eventStore.AggregateStreamAsync<SimpleAggregate>(streamJsonStr, ct: TestContext.Current.CancellationToken);
        aggJsonStr.ShouldNotBeNull();
        aggJsonStr.Count.ShouldBe(25);
        aggJsonStr.Text.ShouldBe("JsonStr");

        // 3. Payload with no matching Apply method on aggregate
        var streamNoMatch = "stream-nomatch";
        var envOther = new EventEnvelope<OtherEvent>
        {
            StreamId = streamNoMatch,
            Version = 1,
            EventType = typeof(OtherEvent).FullName!,
            Data = new OtherEvent("ignored")
        };
        eventStorage.GetSnapshotAsync<NoApplyAggregate>(streamNoMatch, "tenant1", Arg.Any<CancellationToken>())
            .Returns(((NoApplyAggregate?)null, 0L));
        eventStorage.FetchEventsAsync(streamNoMatch, "tenant1", 0L, Arg.Any<CancellationToken>())
            .Returns(new List<IEvent> { envOther });

        var aggNoMatch = await eventStore.AggregateStreamAsync<NoApplyAggregate>(streamNoMatch, ct: TestContext.Current.CancellationToken);
        aggNoMatch.ShouldNotBeNull();
        aggNoMatch.Value.ShouldBe(0);
    }

    [Fact]
    public void Append_MultipleAppends_And_HeaderMerging()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();

        var headerProvider = new Func<(string?, string?, IReadOnlyDictionary<string, object>)>(() =>
            ("corr-provider", "cause-provider", new Dictionary<string, object> { ["H1"] = "V1", ["H2"] = "ProviderV2" }));

        var eventStore = new CoreEventStore(storage, "tenant1", headerProvider: headerProvider);
        var streamId = "stream-appends";

        // Pre-existing event with headers
        var existingEvent = new EventEnvelope<SimpleEvent>
        {
            CorrelationId = "corr-orig",
            CausationId = "cause-orig",
            Headers = new Dictionary<string, object> { ["H2"] = "OrigV2", ["H3"] = "OrigV3" },
            Data = new SimpleEvent(5, "Evt1")
        };

        // First append: adds streamId to _streamExpectedVersions
        eventStore.Append(streamId, expectedVersion: 1, existingEvent);
        eventStore.StreamExpectedVersions[streamId].ShouldBe(1);

        // Second append: _streamExpectedVersions already contains streamId (hits ContainsKey branch)
        eventStore.Append(streamId, expectedVersion: 2, new SimpleEvent(10, "Evt2"));

        eventStore.UncommittedEvents.Count.ShouldBe(2);

        var env1 = eventStore.UncommittedEvents[0];
        env1.CorrelationId.ShouldBe("corr-provider");
        env1.CausationId.ShouldBe("cause-provider");
        env1.Headers["H1"].ShouldBe("V1");
        env1.Headers["H2"].ShouldBe("ProviderV2"); // provider takes precedence
        env1.Headers["H3"].ShouldBe("OrigV3");     // merged from existing event
    }

    [Fact]
    public void ApplyHeaders_WithoutHeaderProvider_UsesExistingEventHeaders()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var eventStore = new CoreEventStore(storage, "tenant1");

        var existingEvt = new EventEnvelope<SimpleEvent>
        {
            CorrelationId = "corr-existing",
            CausationId = "cause-existing",
            Headers = new Dictionary<string, object> { ["Tag"] = "Val" },
            Data = new SimpleEvent(1, "Test")
        };

        eventStore.Append("stream-no-provider", existingEvt);

        var env = eventStore.UncommittedEvents[0];
        env.CorrelationId.ShouldBe("corr-existing");
        env.CausationId.ShouldBe("cause-existing");
        env.Headers["Tag"].ShouldBe("Val");
    }

    [Fact]
    public void ClearUncommittedEvents_ClearsEventsAndExpectedVersions()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var eventStore = new CoreEventStore(storage, "tenant1");

        eventStore.Append("stream-1", 1, new SimpleEvent(1, "E1"));
        eventStore.UncommittedEvents.Count.ShouldBe(1);
        eventStore.StreamExpectedVersions.Count.ShouldBe(1);

        eventStore.ClearUncommittedEvents();

        eventStore.UncommittedEvents.Count.ShouldBe(0);
        eventStore.StreamExpectedVersions.Count.ShouldBe(0);
    }

    [Fact]
    public async Task CoreEventStore_Constructors_And_Overloads()
    {
        var storage = Substitute.For<IAquilaStorageProvider>();
        var options = new StoreOptions { StorageProvider = storage };

        var store1 = new CoreEventStore(storage, options, "tenant1");
        store1.UncommittedEvents.Count.ShouldBe(0);

        var guidStreamId = Guid.NewGuid();
        store1.Append(guidStreamId, new SimpleEvent(1, "G1"));
        store1.UncommittedEvents.Count.ShouldBe(1);

        var fetched = await store1.FetchStreamAsync(guidStreamId, ct: TestContext.Current.CancellationToken);
        fetched.ShouldNotBeNull();
    }
}
