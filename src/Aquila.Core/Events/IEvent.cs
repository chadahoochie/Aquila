using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

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
    long GlobalSequence { get; }
    DateTimeOffset Timestamp { get; }
    string EventType { get; }
    object Data { get; }
    string TenantId { get; }
    string? CorrelationId { get; set; }
    string? CausationId { get; set; }
    IReadOnlyDictionary<string, object> Headers { get; set; }
}

/// <summary>
/// Strongly typed event wrapper with metadata.
/// </summary>
public interface IEvent<out T> : IEvent where T : class
{
    new T Data { get; }
}

/// <summary>
/// Default implementation of an event envelope.
/// </summary>
public class EventEnvelope<T> : IEvent<T> where T : class
{
    private IReadOnlyDictionary<string, object>? _headers;

    public Guid Id { get; set; } = Guid.NewGuid();
    public string StreamId { get; set; } = string.Empty;
    public long Version { get; set; }
    public long Sequence { get; set; }
    public long GlobalSequence { get; set; }
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string EventType { get; set; } = typeof(T).FullName ?? typeof(T).Name;
    public T Data { get; set; } = default!;

    object IEvent.Data => Data;
    public string TenantId { get; set; } = "default";
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public IReadOnlyDictionary<string, object> Headers
    {
        get => _headers ?? ReadOnlyDictionary<string, object>.Empty;
        set => _headers = value;
    }
}

/// <summary>
/// Internal extensions for manipulating IEvent objects.
/// </summary>
public static class EventExtensions
{
    private static readonly ConcurrentDictionary<Type, Action<IEvent, long>> _globalSequenceSetters = new();

    public static void SetGlobalSequence(this IEvent evt, long globalSequence)
    {
        if (evt is null) return;
        var setter = _globalSequenceSetters.GetOrAdd(evt.GetType(), t =>
        {
            var prop = t.GetProperty(nameof(IEvent.GlobalSequence));
            if (prop != null && prop.CanWrite && prop.SetMethod != null)
            {
                var instanceParam = Expression.Parameter(typeof(IEvent), "evt");
                var valueParam = Expression.Parameter(typeof(long), "val");
                var castInstance = Expression.Convert(instanceParam, t);
                var call = Expression.Call(castInstance, prop.SetMethod, valueParam);
                return Expression.Lambda<Action<IEvent, long>>(call, instanceParam, valueParam).Compile();
            }
            return (_, _) => { };
        });

        setter(evt, globalSequence);
    }
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
