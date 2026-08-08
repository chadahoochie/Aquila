using System;
using System.Collections.Concurrent;
using Aquila.Core.Storage;

namespace Aquila.Core.Sessions;

public interface IIdentityMap
{
    bool TryGet<T>(string id, out T? entity) where T : class;
    void Track<T>(string id, T entity, DocumentEnvelope<T> envelope) where T : class;
    DocumentEnvelope<T>? GetEnvelope<T>(string id) where T : class;
    void Clear();
}

public sealed class IdentityMap : IIdentityMap
{
    private readonly ConcurrentDictionary<(Type Type, string Id), TrackedItem> _map = new();

    private sealed record TrackedItem(object Entity, object Envelope);

    public bool TryGet<T>(string id, out T? entity) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        if (_map.TryGetValue((typeof(T), id), out var item))
        {
            entity = (T)item.Entity;
            return true;
        }

        entity = null;
        return false;
    }

    public void Track<T>(string id, T entity, DocumentEnvelope<T> envelope) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(envelope);

        _map[(typeof(T), id)] = new TrackedItem(entity, envelope);
    }

    public DocumentEnvelope<T>? GetEnvelope<T>(string id) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        if (_map.TryGetValue((typeof(T), id), out var item))
        {
            return (DocumentEnvelope<T>)item.Envelope;
        }

        return null;
    }

    public void Clear()
    {
        _map.Clear();
    }
}
