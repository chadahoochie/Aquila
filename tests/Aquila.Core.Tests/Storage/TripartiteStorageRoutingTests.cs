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

    public class RunningTotalProjection : SingleStreamProjection<OrderSummaryReadModel>
    {
        public RunningTotalProjection()
        {
            Lifecycle = ProjectionLifecycle.Async;
            CreateEvent<OrderPlaced>(e => new OrderSummaryReadModel { Id = e.OrderId, TotalAmount = e.Amount });
            ProjectEvent<OrderPlaced>((e, doc) =>
            {
                doc.Id = e.OrderId;
                doc.TotalAmount += e.Amount;
            });
        }
    }

    private sealed class TrackingStorageProvider : IDocumentStorageProvider, IProjectionStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();
        public readonly List<string> ReadCalls = new();
        public readonly List<string> QueryCalls = new();
        public readonly List<StorageOperation> ExecutedBatchOperations = new();
        public readonly List<string> ExecutedUpserts = new();

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

        public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class
        {
            ExecutedUpserts.Add($"{typeof(T).Name}:{envelope.Id}");
            return _inner.UpsertDocumentAsync(envelope, ct);
        }

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

    [Fact]
    public async Task AsyncProjection_WritesAndReadsThroughTheSameStore_WhenInitializeAsyncWasNeverCalled()
    {
        // Regression for the routing gap: the documented AddAquila setup never calls
        // InitializeAsync, which used to be the only caller of Freeze(). With an empty routing
        // registry, LoadAsync resolved to DocumentStorage while the projection writer targeted
        // ProjectionStorage unconditionally -- so the read model was written to one store and
        // read from another, and LoadAsync returned null forever.
        var docStorage = new TrackingStorageProvider("CosmosDocs");
        var projStorage = new TrackingStorageProvider("RedisProjections");
        var eventStorage = new InMemoryStorageProvider();

        var options = new StoreOptions();
        options.DocumentStorage = docStorage;
        options.ProjectionStorage = projStorage;
        options.EventStorage = eventStorage;
        options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);

        // Deliberately no InitializeAsync() -- this is what AddAquila does.
        using var store = new DocumentStore(options);

        var projection = (OrderSummaryProjection)options.Projections.Projections.Single();
        await projection.DispatchBatchAsync(
            store,
            new IEvent[]
            {
                new EventEnvelope<OrderPlaced>
                {
                    StreamId = "ORD-77",
                    Version = 1,
                    GlobalSequence = 1,
                    Data = new OrderPlaced { OrderId = "ORD-77", Amount = 42.50m }
                }
            },
            maxConcurrency: 1,
            TestContext.Current.CancellationToken);

        projStorage.ExecutedUpserts.ShouldContain("OrderSummaryReadModel:ORD-77");
        docStorage.ExecutedUpserts.ShouldNotContain("OrderSummaryReadModel:ORD-77");

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<OrderSummaryReadModel>("ORD-77", "ORD-77", TestContext.Current.CancellationToken);

        readModel.ShouldNotBeNull();
        readModel.TotalAmount.ShouldBe(42.50m);
    }

    [Fact]
    public async Task AsyncProjection_AccumulatesAcrossBatches_RatherThanRestartingFromAnEmptyReadModel()
    {
        // The consequence of the split above: SingleStreamProjection loads the prior aggregate
        // through the session before folding in new events. Reading the wrong store yielded a
        // fresh instance every batch, so accumulating projections silently reset each cycle.
        var docStorage = new TrackingStorageProvider("CosmosDocs");
        var projStorage = new TrackingStorageProvider("RedisProjections");

        var options = new StoreOptions();
        options.DocumentStorage = docStorage;
        options.ProjectionStorage = projStorage;
        options.EventStorage = new InMemoryStorageProvider();
        options.Projections.Add<RunningTotalProjection>(ProjectionLifecycle.Async);

        using var store = new DocumentStore(options);
        var projection = (RunningTotalProjection)options.Projections.Projections.Single();

        for (int i = 1; i <= 3; i++)
        {
            await projection.DispatchBatchAsync(
                store,
                new IEvent[]
                {
                    new EventEnvelope<OrderPlaced>
                    {
                        StreamId = "ORD-88",
                        Version = i,
                        GlobalSequence = i,
                        Data = new OrderPlaced { OrderId = "ORD-88", Amount = 10m }
                    }
                },
                maxConcurrency: 1,
                TestContext.Current.CancellationToken);
        }

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<OrderSummaryReadModel>("ORD-88", "ORD-88", TestContext.Current.CancellationToken);

        readModel.ShouldNotBeNull();
        readModel.TotalAmount.ShouldBe(30m, "three batches of 10 must accumulate, not overwrite");
    }
}
