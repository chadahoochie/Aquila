using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Aquila.Core.Storage;

namespace Aquila.Core.Sessions;

public sealed record TrackedEntity(
    string Id,
    Type EntityType,
    object Entity,
    object Envelope,
    byte[]? Snapshot);

public interface IIdentityMap
{
    bool TryGet<T>(string id, out T? entity) where T : class;
    void Track<T>(string id, T entity, DocumentEnvelope<T> envelope) where T : class;
    void Track<T>(string id, T entity, DocumentEnvelope<T> envelope, byte[]? snapshot) where T : class;
    void Track<T>(string id, T entity, DocumentEnvelope<T> envelope, bool recordSnapshot) where T : class;
    void Untrack<T>(string id) where T : class;
    DocumentEnvelope<T>? GetEnvelope<T>(string id) where T : class;
    IReadOnlyList<TrackedEntity> GetTrackedEntities();
    void UpdateSnapshot(Type entityType, string id, byte[] snapshot);
    void Clear();
}

public sealed class IdentityMap : IIdentityMap
{
    private readonly ConcurrentDictionary<(Type Type, string Id), TrackedItem> _map = new();

    private sealed record TrackedItem(string Id, Type EntityType, object Entity, object Envelope, byte[]? Snapshot);

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
        Track<T>(id, entity, envelope, snapshot: null);
    }

    public void Track<T>(string id, T entity, DocumentEnvelope<T> envelope, bool recordSnapshot) where T : class
    {
        byte[]? snapshot = recordSnapshot ? JsonSerializer.SerializeToUtf8Bytes(entity) : null;
        Track<T>(id, entity, envelope, snapshot);
    }

    public void Track<T>(string id, T entity, DocumentEnvelope<T> envelope, byte[]? snapshot) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(envelope);

        _map[(typeof(T), id)] = new TrackedItem(id, typeof(T), entity, envelope, snapshot);
    }

    public void Untrack<T>(string id) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        _map.TryRemove((typeof(T), id), out _);
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

    public IReadOnlyList<TrackedEntity> GetTrackedEntities()
    {
        return _map.Values
            .Select(x => new TrackedEntity(x.Id, x.EntityType, x.Entity, x.Envelope, x.Snapshot))
            .ToList();
    }

    public void UpdateSnapshot(Type entityType, string id, byte[] snapshot)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(snapshot);

        if (_map.TryGetValue((entityType, id), out var item))
        {
            _map[(entityType, id)] = item with { Snapshot = snapshot };
        }
    }

    public void Clear()
    {
        _map.Clear();
    }
}

public sealed class NoIdentityMap : IIdentityMap
{
    public static readonly NoIdentityMap Instance = new();

    private NoIdentityMap() { }

    public bool TryGet<T>(string id, out T? entity) where T : class
    {
        entity = null;
        return false;
    }

    public void Track<T>(string id, T entity, DocumentEnvelope<T> envelope) where T : class { }
    public void Track<T>(string id, T entity, DocumentEnvelope<T> envelope, byte[]? snapshot) where T : class { }
    public void Track<T>(string id, T entity, DocumentEnvelope<T> envelope, bool recordSnapshot) where T : class { }
    public void Untrack<T>(string id) where T : class { }
    public DocumentEnvelope<T>? GetEnvelope<T>(string id) where T : class => null;
    public IReadOnlyList<TrackedEntity> GetTrackedEntities() => Array.Empty<TrackedEntity>();
    public void UpdateSnapshot(Type entityType, string id, byte[] snapshot) { }
    public void Clear() { }
}
