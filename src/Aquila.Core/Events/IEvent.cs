using System;

namespace Aquila.Core.Events;

/// <summary>
/// Represents metadata for an event stored in Aquila.
/// </summary>
public interface IEvent
{
    Guid Id { get; }
    string StreamId { get; }
    long Version { get; }
    long Sequence { get; }
    DateTimeOffset Timestamp { get; }
    string EventType { get; }
    object Data { get; }
    string TenantId { get; }
}

/// <summary>
/// Strongly typed event wrapper with metadata.
/// </summary>
public interface IEvent<out T> : IEvent where T : class
{
    new T Data { get; }
}

/// <summary>
/// Default implementation of IEvent.
/// </summary>
public sealed class EventEnvelope<T> : IEvent<T> where T : class
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string StreamId { get; set; } = string.Empty;
    public long Version { get; set; }
    public long Sequence { get; set; }
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string EventType { get; set; } = typeof(T).Name;
    public T Data { get; set; } = default!;

    object IEvent.Data => Data;
    public string TenantId { get; set; } = "default";
}

/// <summary>
/// Header metadata for an event stream.
/// </summary>
public sealed class EventStreamHeader
{
    public string StreamId { get; set; } = string.Empty;
    public string AggregateType { get; set; } = string.Empty;
    public long Version { get; set; }
    public string TenantId { get; set; } = "default";
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset LastModified { get; set; } = DateTimeOffset.UtcNow;
}
