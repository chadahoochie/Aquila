using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Queries;

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
    Delete,
    Patch
}

/// <summary>
/// Patch operations supported for partial document updates.
/// </summary>
public enum PatchAction
{
    Set,
    Increment,
    Append,
    Remove
}

/// <summary>
/// Represents a single patch operation with target path, action, and value.
/// </summary>
public sealed class PatchOperationData
{
    public string Path { get; set; } = string.Empty;
    public PatchAction Action { get; set; }
    public object? Value { get; set; }
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
    public List<PatchOperationData> PatchOperations { get; set; } = new();
    public Type? ModelType { get; set; }
}

/// <summary>
/// Options for configuring document queries.
/// </summary>
public sealed class QueryOptions
{
    public string? PartitionKey { get; set; }
    public int? MaxItemCount { get; set; }
    public string? ContinuationToken { get; set; }
    public int? Skip { get; set; }
    public List<SortDescriptor> Orderings { get; set; } = new();

    public QueryOptions OrderBy(LambdaExpression keySelector, SortOrder direction = SortOrder.Ascending)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        Orderings.Add(new SortDescriptor(keySelector, direction));
        return this;
    }

    public QueryOptions OrderByDescending(LambdaExpression keySelector) =>
        OrderBy(keySelector, SortOrder.Descending);

    public QueryOptions ThenBy(LambdaExpression keySelector, SortOrder direction = SortOrder.Ascending) =>
        OrderBy(keySelector, direction);

    public QueryOptions ThenByDescending(LambdaExpression keySelector) =>
        OrderBy(keySelector, SortOrder.Descending);

    public QueryOptions OrderBy<T>(Expression<Func<DocumentEnvelope<T>, object?>> keySelector, SortOrder direction = SortOrder.Ascending)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        Orderings.Add(new SortDescriptor(keySelector, direction));
        return this;
    }

    public QueryOptions OrderByDescending<T>(Expression<Func<DocumentEnvelope<T>, object?>> keySelector) =>
        OrderBy<T>(keySelector, SortOrder.Descending);

    public QueryOptions ThenBy<T>(Expression<Func<DocumentEnvelope<T>, object?>> keySelector, SortOrder direction = SortOrder.Ascending) =>
        OrderBy<T>(keySelector, direction);

    public QueryOptions ThenByDescending<T>(Expression<Func<DocumentEnvelope<T>, object?>> keySelector) =>
        OrderBy<T>(keySelector, SortOrder.Descending);
}

/// <summary>
/// Represents the result of a storage-level paged query execution.
/// </summary>
public sealed class StorageQueryResult<T>
{
    public IReadOnlyList<DocumentEnvelope<T>> Documents { get; init; } = Array.Empty<DocumentEnvelope<T>>();
    public string? ContinuationToken { get; init; }
    public int? TotalCount { get; init; }
    public double RequestCharge { get; init; }

    public StorageQueryResult() { }

    public StorageQueryResult(IReadOnlyList<DocumentEnvelope<T>> documents, string? continuationToken = null, int? totalCount = null, double requestCharge = 0.0)
    {
        Documents = documents ?? Array.Empty<DocumentEnvelope<T>>();
        ContinuationToken = string.IsNullOrWhiteSpace(continuationToken) ? null : continuationToken;
        TotalCount = totalCount;
        RequestCharge = requestCharge;
    }
}

/// <summary>
/// Provider interface for underlying document database persistence.
/// </summary>
public interface IDocumentStorageProvider : IDisposable, IAsyncDisposable
{
    string ProviderName { get; }
    double LastRequestCharge => 0.0;
    double CumulativeRequestCharge => 0.0;
    Task InitializeAsync(CancellationToken ct = default);
    Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class;
    Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class;
    Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class;
    Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class;
    Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class;
    Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default);
}

/// <summary>
/// Provider interface for underlying projection read-model persistence.
/// Extends <see cref="IDocumentStorageProvider"/> with projection-lifecycle operations.
/// </summary>
public interface IProjectionStorageProvider : IDocumentStorageProvider
{
    /// <summary>
    /// Purges all materialized read-model documents for the specified projection, enabling instantaneous rebuilds.
    /// </summary>
    Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default);
}

/// <summary>
/// Provider interface for underlying event store stream persistence.
/// </summary>
public interface IEventStorageProvider : IDisposable, IAsyncDisposable
{
    string ProviderName { get; }
    double LastRequestCharge => 0.0;
    double CumulativeRequestCharge => 0.0;
    Task InitializeAsync(CancellationToken ct = default);
    Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default);
    Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default);
    Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default);
    Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default);
    Task SaveSnapshotAsync<TAggregate>(string streamId, long version, TAggregate snapshot, string tenantId = "default", CancellationToken ct = default) where TAggregate : class;
    Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class;
}
