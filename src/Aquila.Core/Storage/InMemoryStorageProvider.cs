using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Queries;

namespace Aquila.Core.Storage;

/// <summary>
/// Composite in-memory storage provider implementing both IDocumentStorageProvider and IEventStorageProvider.
/// Delegates to dedicated InMemoryDocumentStorageProvider and InMemoryEventStorageProvider instances.
/// </summary>
public sealed class InMemoryStorageProvider : IDocumentStorageProvider, IEventStorageProvider
{
    private readonly InMemoryDocumentStorageProvider _documents;
    private readonly InMemoryEventStorageProvider _events;

    public InMemoryStorageProvider()
    {
        _documents = new InMemoryDocumentStorageProvider();
        _events = new InMemoryEventStorageProvider();
    }

    // Performance Optimization: Cache compiled predicate and sort key selector delegates
    // to eliminate repetitive Expression.Compile() overhead on high-frequency query executions.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<LambdaExpression, object> _compiledPredicateCache = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<LambdaExpression, object> _compiledKeySelectorCache = new();

    private static Func<DocumentEnvelope<T>, bool> CompilePredicate<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate) where T : class
    {
        return (Func<DocumentEnvelope<T>, bool>)_compiledPredicateCache.GetOrAdd(predicate, static p => ((Expression<Func<DocumentEnvelope<T>, bool>>)p).Compile());
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
            var compiled = CompilePredicate(predicate);
            query = query.Where(compiled);
        }

        // Only sort if explicit orderings are supplied (avoid unrequested OrderBy on standard queries)
        if (options?.Orderings != null && options.Orderings.Count > 0)
        {
            query = ApplyOrdering(query, options.Orderings);
        }

        if (options != null && options.MaxItemCount.HasValue && options.MaxItemCount.Value > 0)
        {
            query = query.Take(options.MaxItemCount.Value);
        }

        return Task.FromResult<IReadOnlyList<DocumentEnvelope<T>>>(query.ToList());
    }

    public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(
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
            var compiled = CompilePredicate(predicate);
            query = query.Where(compiled);
        }

        if (options?.Orderings != null && options.Orderings.Count > 0)
        {
            query = ApplyOrdering(query, options.Orderings);
        }
        else
        {
            // Default deterministic ordering by Id for pagination stability
            query = query.OrderBy(env => env.Id, StringComparer.Ordinal);
        }

        var allItems = query.ToList();
        int totalCount = allItems.Count;

        // Offset-based pagination
        if (options != null && options.Skip.HasValue && options.Skip.Value >= 0)
        {
            int skip = options.Skip.Value;
            int take = (options.MaxItemCount.HasValue && options.MaxItemCount.Value > 0)
                ? options.MaxItemCount.Value
                : totalCount;

            var pageItems = allItems.Skip(skip).Take(take).ToList();
            return Task.FromResult(new StorageQueryResult<T>(pageItems, continuationToken: null, totalCount: totalCount));
        }

        // Continuation-token based pagination
        int startIndex = 0;
        if (!string.IsNullOrWhiteSpace(options?.ContinuationToken))
        {
            if (TryParseContinuationToken(options.ContinuationToken, out int parsedIndex))
            {
                startIndex = parsedIndex;
            }
        }

        int maxItems = (options != null && options.MaxItemCount.HasValue && options.MaxItemCount.Value > 0)
            ? options.MaxItemCount.Value
            : totalCount;

        var items = allItems.Skip(startIndex).Take(maxItems).ToList();
        int nextIndex = startIndex + items.Count;

        string? nextContinuationToken = null;
        if (nextIndex < totalCount && items.Count > 0)
        {
            nextContinuationToken = CreateContinuationToken(nextIndex);
        }

        return Task.FromResult(new StorageQueryResult<T>(items, nextContinuationToken, totalCount));
    }

    private static IEnumerable<DocumentEnvelope<T>> ApplyOrdering<T>(
        IEnumerable<DocumentEnvelope<T>> query,
        IReadOnlyList<SortDescriptor>? orderings)
    {
        if (orderings == null || orderings.Count == 0)
        {
            return query;
        }

        IOrderedEnumerable<DocumentEnvelope<T>>? ordered = null;

        for (int i = 0; i < orderings.Count; i++)
        {
            var descriptor = orderings[i];
            if (descriptor?.KeySelector == null) continue;

            var compiled = CompileKeySelector<T>(descriptor.KeySelector);

            if (ordered == null)
            {
                ordered = descriptor.Direction == SortOrder.Ascending
                    ? query.OrderBy(compiled, NullSafeComparer.Instance)
                    : query.OrderByDescending(compiled, NullSafeComparer.Instance);
            }
            else
            {
                ordered = descriptor.Direction == SortOrder.Ascending
                    ? ordered.ThenBy(compiled, NullSafeComparer.Instance)
                    : ordered.ThenByDescending(compiled, NullSafeComparer.Instance);
            }
        }

        return ordered ?? query;
    }

    private static Func<DocumentEnvelope<T>, object?> CompileKeySelector<T>(LambdaExpression expression)
    {
        return (Func<DocumentEnvelope<T>, object?>)_compiledKeySelectorCache.GetOrAdd(expression, static expr =>
        {
            if (expr is Expression<Func<DocumentEnvelope<T>, object?>> typedExpr)
            {
                return typedExpr.Compile();
            }

            var param = expr.Parameters[0];
            if (param.Type == typeof(DocumentEnvelope<T>))
            {
                var body = expr.Body;
                if (body.Type != typeof(object))
                {
                    body = Expression.Convert(body, typeof(object));
                }
                return Expression.Lambda<Func<DocumentEnvelope<T>, object?>>(body, param).Compile();
            }
            else
            {
                var newParam = Expression.Parameter(typeof(DocumentEnvelope<T>), "env");
                var visitor = new ParameterReplaceVisitor(param, newParam);
                var rewrittenBody = visitor.Visit(expr.Body);
                if (rewrittenBody.Type != typeof(object))
                {
                    rewrittenBody = Expression.Convert(rewrittenBody, typeof(object));
                }
                return Expression.Lambda<Func<DocumentEnvelope<T>, object?>>(rewrittenBody, newParam).Compile();
            }
        });
    }

    private sealed class ParameterReplaceVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _from;
        private readonly ParameterExpression _to;

        public ParameterReplaceVisitor(ParameterExpression from, ParameterExpression to)
        {
            _from = from;
            _to = to;
        }

        protected override Expression VisitParameter(ParameterExpression node) =>
            node == _from ? _to : base.VisitParameter(node);
    }

    internal sealed class NullSafeComparer : IComparer<object?>
    {
        public static readonly NullSafeComparer Instance = new();

        public int Compare(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            if (x is IComparable compX)
            {
                if (y is IComparable)
                {
                    try
                    {
                        if (x.GetType() == y.GetType())
                        {
                            return compX.CompareTo(y);
                        }
                        var convertedY = Convert.ChangeType(y, x.GetType());
                        return compX.CompareTo(convertedY);
                    }
                    catch
                    {
                    }
                }
            }

            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }
    }

    private static string CreateContinuationToken(int index)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes($"offset:{index}");
        return Convert.ToBase64String(bytes);
    }

    private static bool TryParseContinuationToken(string token, out int index)
    {
        index = 0;
        try
        {
            var bytes = Convert.FromBase64String(token);
            var text = System.Text.Encoding.UTF8.GetString(bytes);
            if (text.StartsWith("offset:", StringComparison.Ordinal) &&
                int.TryParse(text.AsSpan("offset:".Length), out int parsed))
            {
                index = parsed;
                return true;
            }
        }
        catch
        {
            // If token is invalid/unparseable, fallback to 0
        }
        return false;
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
