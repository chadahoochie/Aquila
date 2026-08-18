using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Queries;

namespace Aquila.Core.Storage;

/// <summary>
/// Composite in-memory storage provider implementing IDocumentStorageProvider, IEventStorageProvider, and IProjectionStorageProvider.
/// Delegates to dedicated InMemoryDocumentStorageProvider and InMemoryEventStorageProvider instances.
/// </summary>
public sealed class InMemoryStorageProvider : IDocumentStorageProvider, IEventStorageProvider, IProjectionStorageProvider
{
    private readonly InMemoryDocumentStorageProvider _documents;
    private readonly InMemoryEventStorageProvider _events;

    public InMemoryStorageProvider()
    {
        _documents = new InMemoryDocumentStorageProvider();
        _events = new InMemoryEventStorageProvider();
    }

    public string ProviderName => "InMemory";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await _documents.InitializeAsync(ct).ConfigureAwait(false);
        await _events.InitializeAsync(ct).ConfigureAwait(false);
    }

    // --- IDocumentStorageProvider Delegation ---

    public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _documents.ReadDocumentAsync<T>(id, partitionKey, ct);

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class =>
        _documents.QueryDocumentsAsync(predicate, options, ct);

    public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class =>
        _documents.QueryPagedDocumentsAsync(predicate, options, ct);

    public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
        _documents.UpsertDocumentAsync(envelope, ct);

    public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _documents.DeleteDocumentAsync<T>(id, partitionKey, ct);

    public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) =>
        _documents.ExecuteBatchAsync(operations, ct);

    // --- IProjectionStorageProvider Delegation ---

    public Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(readModelType);
        return _documents.PurgeDocumentsByTypeAsync(readModelType, ct);
    }

    // --- IEventStorageProvider Delegation ---

    public Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default) =>
        _events.AppendEventsAsync(streamId, events, expectedVersion, ct);

    public Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default) =>
        _events.FetchEventsAsync(streamId, tenantId, fromVersion, ct);

    public Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default) =>
        _events.FetchGlobalEventsAsync(fromGlobalSequence, batchSize, tenantId, ct);

    public Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default) =>
        _events.GetStreamHeaderAsync(streamId, tenantId, ct);

    public Task SaveSnapshotAsync<TAggregate>(string streamId, long version, TAggregate snapshot, string tenantId = "default", CancellationToken ct = default) where TAggregate : class =>
        _events.SaveSnapshotAsync(streamId, version, snapshot, tenantId, ct);

    public Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class =>
        _events.GetSnapshotAsync<TAggregate>(streamId, tenantId, ct);

    public void Dispose()
    {
        _documents.Dispose();
        _events.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _documents.DisposeAsync().ConfigureAwait(false);
        await _events.DisposeAsync().ConfigureAwait(false);
    }
}
