using System.Linq.Expressions;
using StackExchange.Redis;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Aquila.Redis.Configuration;

namespace Aquila.Redis.Storage;

/// <summary>
/// High-performance Redis projection storage provider implementing non-blocking asynchronous UNLINK purges.
/// </summary>
public sealed class RedisProjectionStorageProvider : IProjectionStorageProvider
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly RedisStorageOptions _options;
    private readonly RedisDocumentStorageProvider _innerDocumentProvider;

    public string ProviderName => "Redis";
    public double LastRequestCharge => 0.0;
    public double CumulativeRequestCharge => 0.0;

    public RedisProjectionStorageProvider(IConnectionMultiplexer multiplexer, RedisStorageOptions? options = null)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _options = options ?? new RedisStorageOptions();
        _innerDocumentProvider = new RedisDocumentStorageProvider(multiplexer, _options);
    }

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.ReadDocumentAsync<T>(id, partitionKey, ct);

    public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.QueryDocumentsAsync(predicate, options, ct);

    public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.QueryPagedDocumentsAsync(predicate, options, ct);

    public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.UpsertDocumentAsync(envelope, ct);

    public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
        _innerDocumentProvider.DeleteDocumentAsync<T>(id, partitionKey, ct);

    public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) =>
        _innerDocumentProvider.ExecuteBatchAsync(operations, ct);

    /// <summary>
    /// Purges all projection read models asynchronously using non-blocking streaming UNLINK batches.
    /// </summary>
    public async Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        ArgumentNullException.ThrowIfNull(readModelType);

        var db = _multiplexer.GetDatabase(_options.Database);
        var endpoints = _multiplexer.GetEndPoints();
        var pattern = _options.BuildTypePattern(readModelType.Name);

        foreach (var endpoint in endpoints)
        {
            var server = _multiplexer.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica) continue;

            var batch = new List<RedisKey>(_options.BatchChunkSize);
            await foreach (var key in server.KeysAsync(_options.Database, pattern).WithCancellation(ct).ConfigureAwait(false))
            {
                batch.Add(key);
                if (batch.Count >= _options.BatchChunkSize)
                {
                    await db.KeyDeleteAsync(batch.ToArray()).ConfigureAwait(false);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await db.KeyDeleteAsync(batch.ToArray()).ConfigureAwait(false);
            }
        }
    }

    public void Dispose() => _innerDocumentProvider.Dispose();
    public ValueTask DisposeAsync() => _innerDocumentProvider.DisposeAsync();
}
