using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using Aquila.Core.Queries;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

/// <summary>
/// Cosmos DB implementation of <see cref="IProjectionStorageProvider"/> for materialized read models and projections.
/// </summary>
public sealed class CosmosProjectionStorageProvider : IProjectionStorageProvider
{
    private readonly CosmosDocumentStorageProvider _inner;

    public string ProviderName => "AzureCosmosDB";
    public double LastRequestCharge => _inner.LastRequestCharge;
    public double CumulativeRequestCharge => _inner.CumulativeRequestCharge;

    public CosmosProjectionStorageProvider(CosmosDocumentStorageProvider inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public CosmosProjectionStorageProvider(Func<Container> containerProvider)
    {
        _inner = new CosmosDocumentStorageProvider(containerProvider);
    }

    public CosmosProjectionStorageProvider(Container container)
    {
        _inner = new CosmosDocumentStorageProvider(container);
    }

    public CosmosProjectionStorageProvider(Func<Type, Container> typeContainerResolver)
    {
        _inner = new CosmosDocumentStorageProvider(typeContainerResolver);
    }

    public Task InitializeAsync(CancellationToken ct = default) => _inner.InitializeAsync(ct);

    public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _inner.ReadDocumentAsync<T>(id, partitionKey, ct);

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _inner.QueryDocumentsAsync(predicate, options, ct);

    public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _inner.QueryPagedDocumentsAsync(predicate, options, ct);

    public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
        _inner.UpsertDocumentAsync(envelope, ct);

    public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _inner.DeleteDocumentAsync<T>(id, partitionKey, ct);

    public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) =>
        _inner.ExecuteBatchAsync(operations, ct);

    public Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default) =>
        _inner.PurgeDocumentsByTypeAsync(readModelType, ct);

    public void Dispose() => _inner.Dispose();
    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
