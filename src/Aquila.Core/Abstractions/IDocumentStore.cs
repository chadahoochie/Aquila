using System.Linq.Expressions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Patching;
using Aquila.Core.Queries;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Abstractions;

/// <summary>
/// Event store subsystem interface for appending, fetching, and aggregating streams.
/// </summary>
public interface IEventStore
{
    void StartStream<TAggregate>(Guid streamId, params object[] events) where TAggregate : class;
    void StartStream<TAggregate>(string streamId, params object[] events) where TAggregate : class;

    void Append(Guid streamId, params object[] events);
    void Append(string streamId, params object[] events);
    void Append(Guid streamId, long expectedVersion, params object[] events);
    void Append(string streamId, long expectedVersion, params object[] events);

    Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long fromVersion = 0, CancellationToken ct = default);
    Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamId, long fromVersion = 0, CancellationToken ct = default);
    Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, CancellationToken ct = default);

    Task<TAggregate?> AggregateStreamAsync<TAggregate>(Guid streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new();
    Task<TAggregate?> AggregateStreamAsync<TAggregate>(string streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new();
}

/// <summary>
/// Read-only document session interface.
/// </summary>
public interface IQuerySession : IDisposable, IAsyncDisposable
{
    string TenantId { get; }
    TrackingMode TrackingMode { get; }
    IEventStore Events { get; }
    IIdentityMap IdentityMap { get; }

    Task<T?> LoadAsync<T>(string id, string? partitionKey = null, CancellationToken ct = default) where T : class;
    Task<T?> LoadAsync<T>(Guid id, string? partitionKey = null, CancellationToken ct = default) where T : class;
    Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<string> ids, CancellationToken ct = default) where T : class;

    IQueryable<T> Query<T>() where T : class;
    Task<IReadOnlyList<T>> QueryAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, CancellationToken ct = default) where T : class;
    Task<TResult> QueryAsync<TDoc, TResult>(ICompiledQuery<TDoc, TResult> query, CancellationToken ct = default) where TDoc : class;

    Task<TDoc?> LiveStreamAsync<TDoc>(string streamId, CancellationToken ct = default) where TDoc : class, new();
    Task<TDoc?> LiveStreamAsync<TDoc>(string streamId, string? tenantId, CancellationToken ct = default) where TDoc : class, new();
    Task<TDoc?> LiveStreamAsync<TDoc>(Guid streamId, CancellationToken ct = default) where TDoc : class, new();
    Task<TDoc?> LiveStreamAsync<TDoc>(Guid streamId, string? tenantId, CancellationToken ct = default) where TDoc : class, new();
}

/// <summary>
/// Unit-of-work document session interface for mutations and event appends.
/// </summary>
public interface IDocumentSession : IQuerySession
{
    string? CorrelationId { get; set; }
    string? CausationId { get; set; }
    IReadOnlyDictionary<string, object> Headers { get; }
    void SetHeader(string key, object value);

    void Store<T>(T document, string? partitionKey = null) where T : class;
    void Store<T>(IEnumerable<T> documents) where T : class;

    void Delete<T>(T document) where T : class;
    void Delete<T>(string id, string? partitionKey = null) where T : class;
    void Delete<T>(Guid id, string? partitionKey = null) where T : class;

    void SoftDelete<T>(T document) where T : class;
    void SoftDelete<T>(string id, string? partitionKey = null) where T : class;
    Task SoftDeleteAsync<T>(T document, CancellationToken ct = default) where T : class;
    Task SoftDeleteAsync<T>(string id, string? partitionKey = null, CancellationToken ct = default) where T : class;

    IPatchExpression<T> Patch<T>(string id, string? partitionKey = null) where T : class;

    Task SaveChangesAsync(CancellationToken ct = default);
}

/// <summary>
/// Thread-safe singleton root document store.
/// </summary>
public interface IDocumentStore : IDisposable, IAsyncDisposable
{
    StoreOptions Options { get; }
    IStoreMetadata Metadata { get; }
    Task InitializeAsync(CancellationToken ct = default);
    IQuerySession QuerySession(string? tenantId = null);
    IDocumentSession OpenSession(TrackingMode trackingMode = TrackingMode.DirtyTracking, string? tenantId = null);
    IDocumentSession OpenSession(string? tenantId);
    IDocumentSession LightweightSession(string? tenantId = null);
}
