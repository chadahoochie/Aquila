using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;

namespace Aquila.Core.Storage;

public sealed class InMemoryStorageProvider : IAquilaStorageProvider, IDocumentStorageProvider, IEventStorageProvider
{
    private readonly ConcurrentDictionary<string, object> _documents = new();
    private readonly ConcurrentDictionary<string, EventStreamHeader> _streamHeaders = new();
    private readonly ConcurrentDictionary<string, List<IEvent>> _eventStreams = new();
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

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var compiled = predicate.Compile();
        var results = _documents.Values
            .OfType<DocumentEnvelope<T>>()
            .Where(compiled)
            .ToList();

        return Task.FromResult<IReadOnlyList<DocumentEnvelope<T>>>(results);
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
        }
        return Task.CompletedTask;
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

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
