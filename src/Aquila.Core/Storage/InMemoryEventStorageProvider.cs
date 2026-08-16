using System.Collections.Concurrent;
using Newtonsoft.Json;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Serialization;

namespace Aquila.Core.Storage;

/// <summary>
/// In-memory implementation of IEventStorageProvider.
/// </summary>
public sealed class InMemoryEventStorageProvider : IEventStorageProvider
{
    private readonly ConcurrentDictionary<string, EventStreamHeader> _streamHeaders = new();
    private readonly ConcurrentDictionary<string, List<IEvent>> _eventStreams = new();
    private readonly ConcurrentDictionary<string, (string Json, long SnapshotVersion, string TenantId)> _snapshots = new();
    private long _globalSequence;

    public string ProviderName => "InMemoryEvents";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

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
                @evt.SetVersion(header.Version);
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
        var json = JsonConvert.SerializeObject(snapshot, PrivateConstructorContractResolver.Settings);
        _snapshots[key] = (json, version, tenantId);
        return Task.CompletedTask;
    }

    public Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var key = $"{tenantId}:{typeof(TAggregate).FullName}:{streamId}";
        if (_snapshots.TryGetValue(key, out var entry) && entry.TenantId == tenantId)
        {
            var snapshot = JsonConvert.DeserializeObject<TAggregate>(entry.Json, PrivateConstructorContractResolver.Settings);
            return Task.FromResult<(TAggregate?, long)>((snapshot, entry.SnapshotVersion));
        }

        return Task.FromResult<(TAggregate?, long)>((null, 0));
    }

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
