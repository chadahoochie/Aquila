using System.Collections.Concurrent;
using System.Linq.Expressions;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Queries;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Storage;

public class TripartiteStorageRoutingTests
{
    public class OrderPlaced
    {
        public string OrderId { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    public class CustomerDocument
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    public class OrderSummaryReadModel
    {
        public string Id { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
    }

    public class OrderSummaryProjection : SingleStreamProjection<OrderSummaryReadModel>
    {
        public OrderSummaryProjection()
        {
            Lifecycle = ProjectionLifecycle.Async;
            CreateEvent<OrderPlaced>(e => new OrderSummaryReadModel { Id = e.OrderId, TotalAmount = e.Amount });
        }
    }

    private sealed class TrackingStorageProvider : IDocumentStorageProvider, IProjectionStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();
        public readonly List<string> ReadCalls = new();
        public readonly List<string> QueryCalls = new();
        public readonly List<StorageOperation> ExecutedBatchOperations = new();

        public string ProviderName { get; }

        public TrackingStorageProvider(string name) => ProviderName = name;

        public Task InitializeAsync(CancellationToken ct = default) => _inner.InitializeAsync(ct);

        public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class
        {
            ReadCalls.Add($"{typeof(T).Name}:{id}");
            return _inner.ReadDocumentAsync<T>(id, partitionKey, ct);
        }

        public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class
        {
            QueryCalls.Add(typeof(T).Name);
            return _inner.QueryDocumentsAsync(predicate, options, ct);
        }

        public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class
        {
            QueryCalls.Add(typeof(T).Name);
            return _inner.QueryPagedDocumentsAsync(predicate, options, ct);
        }

        public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
            _inner.UpsertDocumentAsync(envelope, ct);

        public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
            _inner.DeleteDocumentAsync<T>(id, partitionKey, ct);

        public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default)
        {
            var ops = operations.ToList();
            ExecutedBatchOperations.AddRange(ops);
            return _inner.ExecuteBatchAsync(ops, ct);
        }

        public Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default) =>
            _inner.PurgeProjectionAsync(projectionName, readModelType, ct);

        public void Dispose() => _inner.Dispose();
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    [Fact]
    public async Task LoadAsync_RoutesToProjectionStorage_ForRegisteredReadModels_AndDocumentStorage_ForDocuments()
    {
        var docStorage = new TrackingStorageProvider("CosmosDocs");
        var projStorage = new TrackingStorageProvider("RedisProjections");
        var eventStorage = new InMemoryStorageProvider();

        var options = new StoreOptions();
        options.DocumentStorage = docStorage;
        options.ProjectionStorage = projStorage;
        options.EventStorage = eventStorage;
        options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);

        using var store = new DocumentStore(options);
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        using var session = store.OpenSession();

        // 1. Load domain document -> routes to DocumentStorage
        await session.LoadAsync<CustomerDocument>("C-1", partitionKey: null, TestContext.Current.CancellationToken);
        docStorage.ReadCalls.ShouldContain("CustomerDocument:C-1");
        projStorage.ReadCalls.ShouldNotContain("CustomerDocument:C-1");

        // 2. Load projection read model -> routes to ProjectionStorage
        await session.LoadAsync<OrderSummaryReadModel>("ORD-1", partitionKey: null, TestContext.Current.CancellationToken);
        projStorage.ReadCalls.ShouldContain("OrderSummaryReadModel:ORD-1");
        docStorage.ReadCalls.ShouldNotContain("OrderSummaryReadModel:ORD-1");
    }

    [Fact]
    public async Task QueryPagedAsync_RoutesToCorrectStorageProvider()
    {
        var docStorage = new TrackingStorageProvider("CosmosDocs");
        var projStorage = new TrackingStorageProvider("RedisProjections");
        var eventStorage = new InMemoryStorageProvider();

        var options = new StoreOptions();
        options.DocumentStorage = docStorage;
        options.ProjectionStorage = projStorage;
        options.EventStorage = eventStorage;
        options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);

        using var store = new DocumentStore(options);
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        using var session = store.OpenSession();

        await session.QueryPagedAsync<CustomerDocument>(ct: TestContext.Current.CancellationToken);
        docStorage.QueryCalls.ShouldContain("CustomerDocument");
        projStorage.QueryCalls.ShouldNotContain("CustomerDocument");

        await session.QueryPagedAsync<OrderSummaryReadModel>(ct: TestContext.Current.CancellationToken);
        projStorage.QueryCalls.ShouldContain("OrderSummaryReadModel");
        docStorage.QueryCalls.ShouldNotContain("OrderSummaryReadModel");
    }

    [Fact]
    public async Task SaveChangesAsync_SplitsPendingOperations_BetweenDocumentStorage_AndProjectionStorage()
    {
        var docStorage = new TrackingStorageProvider("CosmosDocs");
        var projStorage = new TrackingStorageProvider("RedisProjections");
        var eventStorage = new InMemoryStorageProvider();

        var options = new StoreOptions();
        options.DocumentStorage = docStorage;
        options.ProjectionStorage = projStorage;
        options.EventStorage = eventStorage;
        options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);

        using var store = new DocumentStore(options);
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        using var session = store.OpenSession();
        session.Store(new CustomerDocument { Id = "CUST-100", Name = "Alice" });
        session.Store(new OrderSummaryReadModel { Id = "ORD-200", TotalAmount = 99.95m });

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        docStorage.ExecutedBatchOperations.Count.ShouldBe(1);
        docStorage.ExecutedBatchOperations[0].Id.ShouldBe("CUST-100");
        docStorage.ExecutedBatchOperations[0].DocType.ShouldBe(nameof(CustomerDocument));

        projStorage.ExecutedBatchOperations.Count.ShouldBe(1);
        projStorage.ExecutedBatchOperations[0].Id.ShouldBe("ORD-200");
        projStorage.ExecutedBatchOperations[0].DocType.ShouldBe(nameof(OrderSummaryReadModel));
    }
}
