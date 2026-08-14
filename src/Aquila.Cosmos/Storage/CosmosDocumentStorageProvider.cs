using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosDocumentStorageProvider : IDocumentStorageProvider
{
    private readonly Func<Container>? _containerProvider;
    private readonly Func<Type, Container>? _typeContainerResolver;

    public CosmosDocumentStorageProvider(Func<Container> containerProvider)
    {
        ArgumentNullException.ThrowIfNull(containerProvider);
        _containerProvider = containerProvider;
    }

    public CosmosDocumentStorageProvider(Container container)
    {
        ArgumentNullException.ThrowIfNull(container);
        _containerProvider = () => container;
    }

    public CosmosDocumentStorageProvider(Func<Type, Container> typeContainerResolver)
    {
        ArgumentNullException.ThrowIfNull(typeContainerResolver);
        _typeContainerResolver = typeContainerResolver;
    }

    private Container GetContainer<T>() => _typeContainerResolver != null ? _typeContainerResolver(typeof(T)) : _containerProvider!();
    private Container GetContainer(Type type) => _typeContainerResolver != null ? _typeContainerResolver(type) : _containerProvider!();

    public string ProviderName => "AzureCosmosDB";
    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;
    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        if (id.Contains('/'))
        {
            return await QuerySingleDocumentAsync<T>(id, partitionKey, ct);
        }

        try
        {
            var container = GetContainer<T>();
            var response = await container.ReadItemAsync<CosmosDocumentEnvelope<T>>(
                id,
                CosmosPartitionKeyHelper.CreatePartitionKey(partitionKey),
                cancellationToken: ct);

            if (response?.Resource == null || response.Resource.IsDeleted) return null;

            return MapToEnvelope(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private async Task<DocumentEnvelope<T>?> QuerySingleDocumentAsync<T>(string id, string partitionKey, CancellationToken ct) where T : class
    {
        var container = GetContainer<T>();
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", id);

        using var iterator = container.GetItemQueryIterator<CosmosDocumentEnvelope<T>>(
            queryDef, requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(partitionKey) });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct);
            foreach (var item in response)
            {
                if (item != null && !item.IsDeleted)
                {
                    return MapToEnvelope(item);
                }
            }
        }

        return null;
    }

    public async Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        QueryOptions? options = null,
        CancellationToken ct = default) where T : class
    {
        var container = GetContainer<T>();
        var requestOptions = new QueryRequestOptions();
        if (options != null)
        {
            if (!string.IsNullOrEmpty(options.PartitionKey))
            {
                requestOptions.PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(options.PartitionKey);
            }
            if (options.MaxItemCount.HasValue)
            {
                requestOptions.MaxItemCount = options.MaxItemCount.Value;
            }
        }

        var docType = typeof(T).Name;
        IQueryable<CosmosDocumentEnvelope<T>>? queryable = container.GetItemLinqQueryable<CosmosDocumentEnvelope<T>>(
            false,
            options?.ContinuationToken,
            requestOptions);

        if (queryable == null || queryable.Provider == null)
        {
            return Array.Empty<DocumentEnvelope<T>>();
        }

        queryable = queryable.Where(x => x.DocType == docType && !x.IsDeleted);

        if (predicate != null)
        {
            var rewritten = CosmosExpressionRewriter.Rewrite(predicate);
            if (rewritten != null)
            {
                queryable = queryable.Where(rewritten);
            }
        }

        var results = new List<DocumentEnvelope<T>>();

        try
        {
            var queryDef = queryable.ToQueryDefinition();
            var sql = queryDef.QueryText;

            if (sql.StartsWith("SELECT VALUE root"))
            {
                // Only swap the projection clause, not the "root" alias — the WHERE/JOIN clauses
                // the Cosmos LINQ provider generates for any predicate (e.g. the DocType/IsDeleted
                // filter above) reference "root" throughout, not just in the FROM clause. Renaming
                // the alias to "c" here left those later references dangling, so every query with
                // a WHERE clause failed with "Identifier 'root' could not be resolved" (SC2001).
                sql = "SELECT *" + sql.Substring("SELECT VALUE root".Length);
                var rewrittenDef = new QueryDefinition(sql);
                foreach (var (name, value) in queryDef.GetQueryParameters())
                {
                    rewrittenDef = rewrittenDef.WithParameter(name, value);
                }
                queryDef = rewrittenDef;
            }

            using var iterator = container.GetItemQueryIterator<CosmosDocumentEnvelope<T>>(
                queryDef,
                continuationToken: options?.ContinuationToken,
                requestOptions: requestOptions);

            while (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync(ct);
                foreach (var item in response)
                {
                    results.Add(MapToEnvelope(item));
                }
            }
        }
        catch (Exception ex) when (ex is InvalidOperationException || ex is ArgumentOutOfRangeException || ex is ArgumentException)
        {
            foreach (var item in queryable.ToList())
            {
                results.Add(MapToEnvelope(item));
            }
        }

        return results;
    }

    public async Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.Id);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelope.PartitionKey);

        var cosmosEnvelope = new CosmosDocumentEnvelope<T>
        {
            Id = envelope.Id,
            PartitionKey = envelope.PartitionKey,
            DocType = envelope.DocType,
            TenantId = envelope.TenantId,
            IsDeleted = envelope.IsDeleted,
            Version = envelope.Version,
            ETag = envelope.ETag,
            Data = envelope.Data
        };

        var container = GetContainer<T>();
        await container.UpsertItemAsync(cosmosEnvelope, CosmosPartitionKeyHelper.CreatePartitionKey(envelope.PartitionKey), cancellationToken: ct);
    }

    public async Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        try
        {
            var container = GetContainer<T>();
            await container.DeleteItemAsync<CosmosDocumentEnvelope<T>>(id, CosmosPartitionKeyHelper.CreatePartitionKey(partitionKey), cancellationToken: ct);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound || ex.StatusCode == System.Net.HttpStatusCode.BadRequest)
        {
        }
    }

    public async Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operations);

        foreach (var op in operations)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(op.Id);
            ArgumentException.ThrowIfNullOrWhiteSpace(op.PartitionKey);

            var pk = CosmosPartitionKeyHelper.CreatePartitionKey(op.PartitionKey);

            if (op.OperationType == StorageOperationType.Upsert)
            {
                Type? docDataType = null;
                object itemToUpsert = op.Document!;

                if (op.Document != null)
                {
                    var docType = op.Document.GetType();
                    if (docType.IsGenericType && docType.GetGenericTypeDefinition() == typeof(DocumentEnvelope<>))
                    {
                        docDataType = docType.GetGenericArguments()[0];
                        var dataProp = docType.GetProperty("Data")?.GetValue(op.Document);
                        var versionProp = docType.GetProperty("Version")?.GetValue(op.Document)?.ToString() ?? "1";
                        var isDeletedProp = (bool)(docType.GetProperty("IsDeleted")?.GetValue(op.Document) ?? false);
                        var tenantIdProp = docType.GetProperty("TenantId")?.GetValue(op.Document)?.ToString() ?? "default";
                        var etagProp = docType.GetProperty("ETag")?.GetValue(op.Document)?.ToString();

                        itemToUpsert = new CosmosDocumentEnvelope<object>
                        {
                            Id = op.Id,
                            PartitionKey = op.PartitionKey,
                            DocType = op.DocType,
                            TenantId = tenantIdProp,
                            IsDeleted = isDeletedProp,
                            Version = versionProp,
                            ETag = etagProp,
                            Data = dataProp!
                        };
                    }
                    else if (op.Document is not CosmosDocumentEnvelope<object>)
                    {
                        itemToUpsert = new CosmosDocumentEnvelope<object>
                        {
                            Id = op.Id,
                            PartitionKey = op.PartitionKey,
                            DocType = op.DocType,
                            Data = op.Document
                        };
                    }
                }

                var container = docDataType != null
                    ? GetContainer(docDataType)
                    : (op.Document != null ? GetContainer(op.Document.GetType()) : (_containerProvider != null ? _containerProvider() : GetContainer(typeof(object))));

                await container.UpsertItemAsync(itemToUpsert, pk, cancellationToken: ct);
            }
            else if (op.OperationType == StorageOperationType.Delete)
            {
                var container = _containerProvider != null ? _containerProvider() : GetContainer(typeof(object));
                await container.DeleteItemAsync<object>(op.Id, pk, cancellationToken: ct);
            }
            else if (op.OperationType == StorageOperationType.Patch)
            {
                if (op.PatchOperations == null || op.PatchOperations.Count == 0)
                {
                    continue;
                }

                var container = _containerProvider != null ? _containerProvider() : GetContainer(typeof(object));
                var cosmosPatchOperations = op.PatchOperations.Select(BuildCosmosPatchOperation).ToList();
                await container.PatchItemAsync<CosmosDocumentEnvelope<object>>(op.Id, pk, cosmosPatchOperations, cancellationToken: ct);
            }
        }
    }

    private static PatchOperation BuildCosmosPatchOperation(PatchOperationData patchData)
    {
        return patchData.Action switch
        {
            PatchAction.Set => PatchOperation.Replace(patchData.Path, patchData.Value),
            PatchAction.Increment => PatchOperation.Increment(patchData.Path, Convert.ToInt64(patchData.Value)),
            PatchAction.Remove => PatchOperation.Remove(patchData.Path),
            PatchAction.Append => PatchOperation.Add($"{patchData.Path}/-", patchData.Value),
            _ => throw new NotSupportedException($"Patch action '{patchData.Action}' is not supported.")
        };
    }

    private static DocumentEnvelope<T> MapToEnvelope<T>(CosmosDocumentEnvelope<T> item)
    {
        return new DocumentEnvelope<T>
        {
            Id = item.Id,
            PartitionKey = item.PartitionKey,
            DocType = item.DocType,
            TenantId = item.TenantId,
            IsDeleted = item.IsDeleted,
            Version = item.Version,
            ETag = item.ETag,
            Data = item.Data
        };
    }
}
