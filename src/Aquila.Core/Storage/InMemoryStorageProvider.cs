using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Newtonsoft.Json;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;

namespace Aquila.Core.Storage;

public sealed class InMemoryStorageProvider : IAquilaStorageProvider, IDocumentStorageProvider, IEventStorageProvider
{
    private readonly ConcurrentDictionary<string, object> _documents = new();
    private readonly ConcurrentDictionary<string, EventStreamHeader> _streamHeaders = new();
    private readonly ConcurrentDictionary<string, List<IEvent>> _eventStreams = new();
    private readonly ConcurrentDictionary<string, (string Json, long SnapshotVersion, string TenantId)> _snapshots = new();
    private long _globalSequence;

    public string ProviderName => "InMemory";
    public IDocumentStorageProvider Documents => this;
    public IEventStorageProvider Events => this;

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

    // --- DocumentStorageProvider Implementation ---

    public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        var key = $"{typeof(T).Name}:{partitionKey}:{id}";
        if (_documents.TryGetValue(key, out var raw) && raw is DocumentEnvelope<T> env && !env.IsDeleted)
        {
            return Task.FromResult<DocumentEnvelope<T>?>(env);
        }
        return Task.FromResult<DocumentEnvelope<T>?>(null);
    }

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        IEnumerable<DocumentEnvelope<T>> query = _documents.Values
            .OfType<DocumentEnvelope<T>>()
            .Where(env => !env.IsDeleted);

        if (!string.IsNullOrEmpty(options?.PartitionKey))
        {
            query = query.Where(env => env.PartitionKey == options.PartitionKey);
        }

        if (predicate != null)
        {
            var compiled = predicate.Compile();
            query = query.Where(compiled);
        }

        if (options != null && options.MaxItemCount.HasValue && options.MaxItemCount.Value > 0)
        {
            query = query.Take(options.MaxItemCount.Value);
        }

        return Task.FromResult<IReadOnlyList<DocumentEnvelope<T>>>(query.ToList());
    }

    public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.Id);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.PartitionKey);

        var key = $"{typeof(T).Name}:{envelope.PartitionKey}:{envelope.Id}";
        _documents[key] = envelope;
        return Task.CompletedTask;
    }

    public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        var key = $"{typeof(T).Name}:{partitionKey}:{id}";
        _documents.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operations);

        foreach (var op in operations)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(op.Id);
            ArgumentException.ThrowIfNullOrWhiteSpace(op.PartitionKey);

            var key = $"{op.DocType}:{op.PartitionKey}:{op.Id}";
            if (op.OperationType == StorageOperationType.Upsert)
            {
                _documents[key] = op.Document;
            }
            else if (op.OperationType == StorageOperationType.Delete)
            {
                _documents.TryRemove(key, out _);
            }
            else if (op.OperationType == StorageOperationType.Patch)
            {
                if (_documents.TryGetValue(key, out var rawEnvelope) && rawEnvelope != null)
                {
                    ApplyPatchOperations(rawEnvelope, op.PatchOperations);
                }
            }
        }
        return Task.CompletedTask;
    }

    private static void ApplyPatchOperations(object rawEnvelope, List<PatchOperationData> patchOperations)
    {
        if (patchOperations == null || patchOperations.Count == 0) return;

        var envType = rawEnvelope.GetType();
        var versionProp = envType.GetProperty("Version");
        if (versionProp != null && versionProp.CanWrite)
        {
            versionProp.SetValue(rawEnvelope, Guid.NewGuid().ToString());
        }

        foreach (var patch in patchOperations)
        {
            ApplySinglePatch(rawEnvelope, patch);
        }
    }

    private static void ApplySinglePatch(object rawEnvelope, PatchOperationData patch)
    {
        if (string.IsNullOrWhiteSpace(patch.Path)) return;

        var parts = patch.Path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return;

        object targetObject = rawEnvelope;
        PropertyInfo? propInfo = null;

        for (int i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            var targetType = targetObject.GetType();
            propInfo = targetType.GetProperty(part, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

            if (propInfo == null)
            {
                return;
            }

            if (i < parts.Length - 1)
            {
                var nextObj = propInfo.GetValue(targetObject);
                if (nextObj == null)
                {
                    if (!propInfo.CanWrite) return;
                    nextObj = Activator.CreateInstance(propInfo.PropertyType);
                    if (nextObj == null) return;
                    propInfo.SetValue(targetObject, nextObj);
                }
                targetObject = nextObj;
            }
        }

        if (propInfo == null || !propInfo.CanWrite) return;

        switch (patch.Action)
        {
            case PatchAction.Set:
                SetPropertyValue(targetObject, propInfo, patch.Value);
                break;

            case PatchAction.Increment:
                IncrementPropertyValue(targetObject, propInfo, patch.Value);
                break;

            case PatchAction.Append:
                AppendPropertyValue(targetObject, propInfo, patch.Value);
                break;

            case PatchAction.Remove:
                RemovePropertyValue(targetObject, propInfo, patch.Value);
                break;
        }
    }

    private static void SetPropertyValue(object target, PropertyInfo prop, object? value)
    {
        if (value == null)
        {
            prop.SetValue(target, null);
            return;
        }

        var propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
        if (propType.IsInstanceOfType(value))
        {
            prop.SetValue(target, value);
        }
        else if (propType.IsEnum)
        {
            var enumVal = value is string s ? Enum.Parse(propType, s, true) : Enum.ToObject(propType, value);
            prop.SetValue(target, enumVal);
        }
        else
        {
            var converted = Convert.ChangeType(value, propType);
            prop.SetValue(target, converted);
        }
    }

    private static void IncrementPropertyValue(object target, PropertyInfo prop, object? value)
    {
        var current = prop.GetValue(target);
        var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

        long currentLong = current != null ? Convert.ToInt64(current) : 0;
        long incLong = value != null ? Convert.ToInt64(value) : 1;
        long newLong = currentLong + incLong;

        var converted = Convert.ChangeType(newLong, targetType);
        prop.SetValue(target, converted);
    }

    private static void AppendPropertyValue(object target, PropertyInfo prop, object? element)
    {
        var collection = prop.GetValue(target);

        if (collection == null)
        {
            if (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
            {
                collection = Activator.CreateInstance(prop.PropertyType);
                prop.SetValue(target, collection);
            }
            else if (prop.PropertyType.IsGenericType)
            {
                var elemType = prop.PropertyType.GetGenericArguments()[0];
                var listType = typeof(List<>).MakeGenericType(elemType);
                collection = Activator.CreateInstance(listType);
                prop.SetValue(target, collection);
            }
            else if (typeof(System.Collections.IList).IsAssignableFrom(prop.PropertyType))
            {
                collection = new List<object>();
                prop.SetValue(target, collection);
            }
        }

        if (collection is System.Collections.IList list)
        {
            list.Add(element);
            return;
        }

        var addMethod = prop.PropertyType.GetMethod("Add");
        if (addMethod != null && collection != null)
        {
            addMethod.Invoke(collection, new[] { element });
        }
    }

    private static void RemovePropertyValue(object target, PropertyInfo prop, object? element)
    {
        var collection = prop.GetValue(target);
        if (collection == null) return;

        if (collection is System.Collections.IList list)
        {
            list.Remove(element);
            return;
        }

        var removeMethod = prop.PropertyType.GetMethod("Remove");
        if (removeMethod != null)
        {
            removeMethod.Invoke(collection, new[] { element });
        }
    }

    // --- EventStorageProvider Implementation ---

    public Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);

        var eventList = events.ToList();
        if (!eventList.Any()) return Task.CompletedTask;

        var stream = _eventStreams.GetOrAdd(streamId, _ => new List<IEvent>());
        var header = _streamHeaders.GetOrAdd(streamId, id => new EventStreamHeader
        {
            StreamId = id,
            Version = 0,
            TenantId = eventList.FirstOrDefault()?.TenantId ?? "default",
            CreatedAt = DateTimeOffset.UtcNow
        });

        lock (stream)
        {
            if (expectedVersion >= 0 && header.Version != expectedVersion)
            {
                throw new AquilaConcurrencyException(streamId, expectedVersion.ToString(), header.Version.ToString());
            }

            foreach (var @evt in eventList)
            {
                header.Version++;
                if (@evt.GlobalSequence == 0)
                {
                    @evt.SetGlobalSequence(Interlocked.Increment(ref _globalSequence));
                }
                stream.Add(@evt);
            }

            header.LastModified = DateTimeOffset.UtcNow;
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        if (_eventStreams.TryGetValue(streamId, out var stream))
        {
            lock (stream)
            {
                var filtered = stream
                    .Where(e => (string.IsNullOrEmpty(tenantId) || e.TenantId == tenantId) && e.Version >= fromVersion)
                    .OrderBy(e => e.Version)
                    .ToList();
                return Task.FromResult<IReadOnlyList<IEvent>>(filtered);
            }
        }
        return Task.FromResult<IReadOnlyList<IEvent>>(Array.Empty<IEvent>());
    }

    public Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default)
    {
        if (batchSize <= 0)
        {
            return Task.FromResult<IReadOnlyList<IEvent>>(Array.Empty<IEvent>());
        }

        List<IEvent> allEvents;
        lock (_eventStreams)
        {
            allEvents = _eventStreams.Values
                .SelectMany(s =>
                {
                    lock (s) { return s.ToList(); }
                })
                .Where(e => (string.IsNullOrEmpty(tenantId) || e.TenantId == tenantId) && e.GlobalSequence > fromGlobalSequence)
                .OrderBy(e => e.GlobalSequence)
                .Take(batchSize)
                .ToList();
        }

        return Task.FromResult<IReadOnlyList<IEvent>>(allEvents);
    }

    public Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        _streamHeaders.TryGetValue(streamId, out var header);
        if (header != null && !string.IsNullOrEmpty(tenantId) && header.TenantId != tenantId)
        {
            return Task.FromResult<EventStreamHeader?>(null);
        }
        return Task.FromResult(header);
    }

    public Task SaveSnapshotAsync<TAggregate>(string streamId, long version, TAggregate snapshot, string tenantId = "default", CancellationToken ct = default) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(snapshot);

        var key = $"{tenantId}:{typeof(TAggregate).FullName}:{streamId}";
        var json = JsonConvert.SerializeObject(snapshot);
        _snapshots[key] = (json, version, tenantId);
        return Task.CompletedTask;
    }

    public Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var key = $"{tenantId}:{typeof(TAggregate).FullName}:{streamId}";
        if (_snapshots.TryGetValue(key, out var entry) && entry.TenantId == tenantId)
        {
            var snapshot = JsonConvert.DeserializeObject<TAggregate>(entry.Json);
            return Task.FromResult<(TAggregate?, long)>((snapshot, entry.SnapshotVersion));
        }

        return Task.FromResult<(TAggregate?, long)>((null, 0));
    }

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
