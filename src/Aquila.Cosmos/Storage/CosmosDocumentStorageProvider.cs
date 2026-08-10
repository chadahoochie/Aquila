using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

public sealed class CosmosDocumentStorageProvider : IDocumentStorageProvider
{
    private readonly Func<Container> _containerProvider;

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

    private Container Container => _containerProvider();

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
            var response = await Container.ReadItemAsync<CosmosDocumentEnvelope<T>>(
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
        var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
            .WithParameter("@id", id);

        using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<T>>(
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
        IQueryable<CosmosDocumentEnvelope<T>>? queryable = Container.GetItemLinqQueryable<CosmosDocumentEnvelope<T>>(
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

            if (sql.StartsWith("SELECT VALUE root FROM root"))
            {
                sql = "SELECT * FROM c" + sql.Substring("SELECT VALUE root FROM root".Length);
                queryDef = new QueryDefinition(sql);
            }

            using var iterator = Container.GetItemQueryIterator<CosmosDocumentEnvelope<T>>(
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

        await Container.UpsertItemAsync(cosmosEnvelope, CosmosPartitionKeyHelper.CreatePartitionKey(envelope.PartitionKey), cancellationToken: ct);
    }

    public async Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        try
        {
            await Container.DeleteItemAsync<CosmosDocumentEnvelope<T>>(id, CosmosPartitionKeyHelper.CreatePartitionKey(partitionKey), cancellationToken: ct);
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
                await Container.UpsertItemAsync(op.Document, pk, cancellationToken: ct);
            }
            else if (op.OperationType == StorageOperationType.Delete)
            {
                await Container.DeleteItemAsync<object>(op.Id, pk, cancellationToken: ct);
            }
            else if (op.OperationType == StorageOperationType.Patch)
            {
                if (op.PatchOperations == null || op.PatchOperations.Count == 0)
                {
                    continue;
                }

                var cosmosPatchOperations = op.PatchOperations.Select(BuildCosmosPatchOperation).ToList();
                await Container.PatchItemAsync<CosmosDocumentEnvelope<object>>(op.Id, pk, cosmosPatchOperations, cancellationToken: ct);
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
