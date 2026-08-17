using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Aquila.Core.Queries;

namespace Aquila.Core.Storage;

/// <summary>
/// In-memory implementation of IDocumentStorageProvider.
/// </summary>
public sealed class InMemoryDocumentStorageProvider : IDocumentStorageProvider
{
    private readonly ConcurrentDictionary<string, object> _documents = new();

    public string ProviderName => "InMemoryDocuments";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

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

    public async Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        var result = await QueryPagedDocumentsAsync(predicate, options, ct).ConfigureAwait(false);
        return result.Documents;
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
            var compiled = predicate.Compile();
            query = query.Where(compiled);
        }

        var allItems = ApplyOrdering(query, options?.Orderings).ToList();
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
            return query.OrderBy(env => env.Id, StringComparer.Ordinal);
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

        return ordered ?? query.OrderBy(env => env.Id, StringComparer.Ordinal);
    }

    private static Func<DocumentEnvelope<T>, object?> CompileKeySelector<T>(LambdaExpression expression)
    {
        if (expression is Expression<Func<DocumentEnvelope<T>, object?>> typedExpr)
        {
            return typedExpr.Compile();
        }

        var param = expression.Parameters[0];
        if (param.Type == typeof(DocumentEnvelope<T>))
        {
            var body = expression.Body;
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
            var rewrittenBody = visitor.Visit(expression.Body);
            if (rewrittenBody.Type != typeof(object))
            {
                rewrittenBody = Expression.Convert(rewrittenBody, typeof(object));
            }
            return Expression.Lambda<Func<DocumentEnvelope<T>, object?>>(rewrittenBody, newParam).Compile();
        }
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

    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
