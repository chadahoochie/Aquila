using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Aquila.Core.Events;

namespace Aquila.Core.Storage;

/// <summary>
/// Universal document envelope format across all Aquila storage providers.
/// </summary>
public sealed class DocumentEnvelope<T>
{
    public string Id { get; set; } = string.Empty;
    public string PartitionKey { get; set; } = string.Empty;
    public string DocType { get; set; } = typeof(T).Name;
    public string TenantId { get; set; } = "default";
    public bool IsDeleted { get; set; }
    public string Version { get; set; } = Guid.NewGuid().ToString();
    public string? ETag { get; set; }
    public T Data { get; set; } = default!;
}

/// <summary>
/// Types of transactional storage mutations.
/// </summary>
public enum StorageOperationType
{
    Upsert,
    Delete
}

/// <summary>
/// Atomic operation definition passed to storage providers.
/// </summary>
public sealed class StorageOperation
{
    public StorageOperationType OperationType { get; set; }
    public string Id { get; set; } = string.Empty;
    public string PartitionKey { get; set; } = string.Empty;
    public string DocType { get; set; } = string.Empty;
    public object Document { get; set; } = default!;
}

/// <summary>
/// Provider interface for underlying document database persistence.
/// </summary>
public interface IDocumentStorageProvider
{
    Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class;
    Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate, CancellationToken ct = default) where T : class;
    Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class;
    Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class;
    Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default);
}

/// <summary>
/// Provider interface for underlying event store stream persistence.
/// </summary>
public interface IEventStorageProvider
{
    Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default);
    Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default);
    Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default);
    Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default);
}

/// <summary>
/// Combined pluggable storage provider interface for Aquila.
/// </summary>
public interface IAquilaStorageProvider : IDisposable, IAsyncDisposable
{
    string ProviderName { get; }
    IDocumentStorageProvider Documents { get; }
    IEventStorageProvider Events { get; }
    Task InitializeAsync(CancellationToken ct = default);
}
