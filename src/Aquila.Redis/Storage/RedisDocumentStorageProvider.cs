using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using StackExchange.Redis;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Aquila.Redis.Configuration;

namespace Aquila.Redis.Storage;

/// <summary>
/// Redis-backed implementation of <see cref="IDocumentStorageProvider"/> leveraging pipelined batches and UTF-8 byte serialization.
/// </summary>
public sealed class RedisDocumentStorageProvider : IDocumentStorageProvider
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly RedisStorageOptions _options;

    public string ProviderName => "Redis";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    private static readonly ConcurrentDictionary<LambdaExpression, object> _compiledPredicateCache = new();
    private static readonly ConcurrentDictionary<LambdaExpression, object> _compiledKeySelectorCache = new();

    public RedisDocumentStorageProvider(IConnectionMultiplexer multiplexer, RedisStorageOptions? options = null)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _options = options ?? new RedisStorageOptions();
    }

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

    public async Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        var pk = string.IsNullOrWhiteSpace(partitionKey) ? typeof(T).Name : partitionKey;

        var db = _multiplexer.GetDatabase(_options.Database);
        var key = _options.BuildKey("default", typeof(T).Name, pk, id);

        byte[]? bytes = await db.StringGetAsync(key).ConfigureAwait(false);
        if (bytes == null || bytes.Length == 0) return null;

        var env = JsonSerializer.Deserialize<DocumentEnvelope<T>>(bytes, _options.SerializerOptions);
        if (env == null || env.IsDeleted) return null;

        return env;
    }

    public async Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.Id);

        var pk = string.IsNullOrWhiteSpace(envelope.PartitionKey) ? typeof(T).Name : envelope.PartitionKey;
        var db = _multiplexer.GetDatabase(_options.Database);
        var key = _options.BuildKey(envelope.TenantId, envelope.DocType, pk, envelope.Id);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(envelope, _options.SerializerOptions);
        await db.StringSetAsync(key, bytes).ConfigureAwait(false);
    }

    public async Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        var pk = string.IsNullOrWhiteSpace(partitionKey) ? typeof(T).Name : partitionKey;

        var db = _multiplexer.GetDatabase(_options.Database);
        var key = _options.BuildKey("default", typeof(T).Name, pk, id);
        await db.KeyDeleteAsync(key).ConfigureAwait(false);
    }

    public async Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var opList = operations.ToList();
        if (opList.Count == 0) return;

        var db = _multiplexer.GetDatabase(_options.Database);
        var batch = db.CreateBatch();
        var tasks = new List<Task>(opList.Count);

        foreach (var op in opList)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(op.Id);
            var pk = string.IsNullOrWhiteSpace(op.PartitionKey) ? op.DocType : op.PartitionKey;
            var key = _options.BuildKey("default", op.DocType, pk, op.Id);

            if (op.OperationType == StorageOperationType.Upsert)
            {
                var bytes = JsonSerializer.SerializeToUtf8Bytes(op.Document, _options.SerializerOptions);
                tasks.Add(batch.StringSetAsync(key, bytes));
            }
            else if (op.OperationType == StorageOperationType.Delete)
            {
                tasks.Add(batch.KeyDeleteAsync(key));
            }
            else if (op.OperationType == StorageOperationType.Patch)
            {
                // Patch support in batch: apply changes or unlink/upsert
                var bytes = JsonSerializer.SerializeToUtf8Bytes(op.Document, _options.SerializerOptions);
                tasks.Add(batch.StringSetAsync(key, bytes));
            }
        }

        batch.Execute();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        var result = await QueryPagedDocumentsAsync(predicate, options, ct).ConfigureAwait(false);
        return result.Documents;
    }

    public async Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        var db = _multiplexer.GetDatabase(_options.Database);
        var endpoints = _multiplexer.GetEndPoints();
        var pattern = _options.BuildTypePattern(typeof(T).Name);
        var compiled = predicate != null ? CompilePredicate(predicate) : null;

        var documents = new List<DocumentEnvelope<T>>();

        foreach (var endpoint in endpoints)
        {
            var server = _multiplexer.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica) continue;

            await foreach (var key in server.KeysAsync(_options.Database, pattern).WithCancellation(ct).ConfigureAwait(false))
            {
                byte[]? bytes = await db.StringGetAsync(key).ConfigureAwait(false);
                if (bytes == null || bytes.Length == 0) continue;

                var env = JsonSerializer.Deserialize<DocumentEnvelope<T>>(bytes, _options.SerializerOptions);
                if (env != null && !env.IsDeleted)
                {
                    if (!string.IsNullOrEmpty(options?.PartitionKey) && env.PartitionKey != options.PartitionKey)
                    {
                        continue;
                    }

                    if (compiled == null || compiled(env))
                    {
                        documents.Add(env);
                    }
                }
            }
        }

        IEnumerable<DocumentEnvelope<T>> query = documents;
        if (options?.Orderings != null && options.Orderings.Count > 0)
        {
            query = ApplyOrdering(query, options.Orderings);
        }
        else
        {
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
            return new StorageQueryResult<T>(pageItems, continuationToken: null, totalCount: totalCount);
        }

        // Continuation token-based pagination
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

        return new StorageQueryResult<T>(items, nextContinuationToken, totalCount);
    }

    private static Func<DocumentEnvelope<T>, bool> CompilePredicate<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate) where T : class
    {
        return (Func<DocumentEnvelope<T>, bool>)_compiledPredicateCache.GetOrAdd(predicate, static p => ((Expression<Func<DocumentEnvelope<T>, bool>>)p).Compile());
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
                var dataAccess = Expression.Property(newParam, nameof(DocumentEnvelope<T>.Data));
                var visitor = new ParameterReplaceVisitor(param, dataAccess);
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
        private readonly Expression _to;

        public ParameterReplaceVisitor(ParameterExpression from, Expression to)
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
        }
        return false;
    }

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
