using System.Collections.ObjectModel;
using Shouldly;
using Aquila.Core.Events;

namespace Aquila.Core.Tests.Events;

public class EventExtensionsTests
{
    private class ReadOnlyGlobalSequenceEvent : IEvent
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string StreamId { get; set; } = "stream";
        public long Version { get; set; } = 1;
        public long Sequence { get; set; } = 1;
        public long GlobalSequence => 10; // Read-only property (CanWrite == false)
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
        public string EventType { get; set; } = nameof(ReadOnlyGlobalSequenceEvent);
        public object Data { get; set; } = new object();
        public string TenantId { get; set; } = "default";
        public string? CorrelationId { get; set; }
        public string? CausationId { get; set; }
        public IReadOnlyDictionary<string, object> Headers { get; set; } = ReadOnlyDictionary<string, object>.Empty;
    }

    private class SamplePayload { }

    [Fact]
    public void SetGlobalSequence_NullEvent_DoesNotThrow()
    {
        IEvent? evt = null;
        Should.NotThrow(() => evt!.SetGlobalSequence(100));
    }

    [Fact]
    public void SetGlobalSequence_WritableProperty_UpdatesValue()
    {
        IEvent evt = new EventEnvelope<SamplePayload>
        {
            GlobalSequence = 5
        };

        evt.SetGlobalSequence(42);
        evt.GlobalSequence.ShouldBe(42);
    }

    [Fact]
    public void SetGlobalSequence_ReadOnlyProperty_FallbackNoOp_DoesNotThrow()
    {
        IEvent evt = new ReadOnlyGlobalSequenceEvent();

        Should.NotThrow(() => evt.SetGlobalSequence(99));
        evt.GlobalSequence.ShouldBe(10); // Unchanged because it's read-only
    }
}
