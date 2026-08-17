using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Storage;

public sealed class StorageContractsTests
{
    private record TestDocument(string Name, int Age, decimal Balance);

    // ==========================================
    // 1. DocumentEnvelope<T> Tests
    // ==========================================

    [Fact]
    public void DocumentEnvelope_DefaultValues_AreCorrect()
    {
        var envelope = new DocumentEnvelope<TestDocument>();

        envelope.Id.ShouldBe(string.Empty);
        envelope.PartitionKey.ShouldBe(string.Empty);
        envelope.DocType.ShouldBe(nameof(TestDocument));
        envelope.TenantId.ShouldBe("default");
        envelope.IsDeleted.ShouldBeFalse();
        envelope.Version.ShouldNotBeNullOrWhiteSpace();
        Guid.TryParse(envelope.Version, out _).ShouldBeTrue();
        envelope.ETag.ShouldBeNull();
        envelope.Data.ShouldBeNull();
    }

    [Fact]
    public void DocumentEnvelope_CustomValues_CanBeAssignedAndRetrieved()
    {
        var doc = new TestDocument("Alice", 30, 100.50m);
        var envelope = new DocumentEnvelope<TestDocument>
        {
            Id = "doc-123",
            PartitionKey = "pk-456",
            DocType = "CustomDocType",
            TenantId = "tenant-xyz",
            IsDeleted = true,
            Version = "v-1.0",
            ETag = "\"etag-abc\"",
            Data = doc
        };

        envelope.Id.ShouldBe("doc-123");
        envelope.PartitionKey.ShouldBe("pk-456");
        envelope.DocType.ShouldBe("CustomDocType");
        envelope.TenantId.ShouldBe("tenant-xyz");
        envelope.IsDeleted.ShouldBeTrue();
        envelope.Version.ShouldBe("v-1.0");
        envelope.ETag.ShouldBe("\"etag-abc\"");
        envelope.Data.ShouldBe(doc);
        envelope.Data.Name.ShouldBe("Alice");
        envelope.Data.Age.ShouldBe(30);
        envelope.Data.Balance.ShouldBe(100.50m);
    }

    // ==========================================
    // 2. StorageOperationType & PatchAction Enums
    // ==========================================

    [Fact]
    public void StorageOperationType_ContainsExpectedValues()
    {
        Enum.GetValues<StorageOperationType>().ShouldBe(new[]
        {
            StorageOperationType.Upsert,
            StorageOperationType.Delete,
            StorageOperationType.Patch
        });
    }

    [Fact]
    public void PatchAction_ContainsExpectedValues()
    {
        Enum.GetValues<PatchAction>().ShouldBe(new[]
        {
            PatchAction.Set,
            PatchAction.Increment,
            PatchAction.Append,
            PatchAction.Remove
        });
    }

    // ==========================================
    // 3. PatchOperationData Tests
    // ==========================================

    [Fact]
    public void PatchOperationData_DefaultValues_AreCorrect()
    {
        var data = new PatchOperationData();

        data.Path.ShouldBe(string.Empty);
        data.Action.ShouldBe(PatchAction.Set);
        data.Value.ShouldBeNull();
    }

    [Fact]
    public void PatchOperationData_CustomValues_CanBeSet()
    {
        var data = new PatchOperationData
        {
            Path = "/details/score",
            Action = PatchAction.Increment,
            Value = 42
        };

        data.Path.ShouldBe("/details/score");
        data.Action.ShouldBe(PatchAction.Increment);
        data.Value.ShouldBe(42);
    }

    // ==========================================
    // 4. StorageOperation Tests
    // ==========================================

    [Fact]
    public void StorageOperation_DefaultValues_AreCorrect()
    {
        var op = new StorageOperation();

        op.OperationType.ShouldBe(StorageOperationType.Upsert);
        op.Id.ShouldBe(string.Empty);
        op.PartitionKey.ShouldBe(string.Empty);
        op.DocType.ShouldBe(string.Empty);
        op.Document.ShouldBeNull();
        op.PatchOperations.ShouldNotBeNull();
        op.PatchOperations.ShouldBeEmpty();
    }

    [Fact]
    public void StorageOperation_CustomValues_CanBeAssigned()
    {
        var patch1 = new PatchOperationData { Path = "/status", Action = PatchAction.Set, Value = "Active" };
        var patch2 = new PatchOperationData { Path = "/loginCount", Action = PatchAction.Increment, Value = 1 };

        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "op-id-1",
            PartitionKey = "op-pk-1",
            DocType = "User",
            Document = new { Id = "op-id-1" },
            PatchOperations = new List<PatchOperationData> { patch1, patch2 }
        };

        op.OperationType.ShouldBe(StorageOperationType.Patch);
        op.Id.ShouldBe("op-id-1");
        op.PartitionKey.ShouldBe("op-pk-1");
        op.DocType.ShouldBe("User");
        op.Document.ShouldNotBeNull();
        op.PatchOperations.Count.ShouldBe(2);
        op.PatchOperations[0].Path.ShouldBe("/status");
        op.PatchOperations[1].Action.ShouldBe(PatchAction.Increment);
    }

    // ==========================================
    // 5. QueryOptions Tests
    // ==========================================

    [Fact]
    public void QueryOptions_DefaultValues_AreCorrect()
    {
        var options = new QueryOptions();

        options.PartitionKey.ShouldBeNull();
        options.MaxItemCount.ShouldBeNull();
        options.ContinuationToken.ShouldBeNull();
        options.Skip.ShouldBeNull();
        options.Orderings.ShouldNotBeNull();
        options.Orderings.ShouldBeEmpty();
    }

    [Fact]
    public void QueryOptions_Properties_CanBeSetDirectly()
    {
        var options = new QueryOptions
        {
            PartitionKey = "pk-test",
            MaxItemCount = 50,
            ContinuationToken = "token-123",
            Skip = 10
        };

        options.PartitionKey.ShouldBe("pk-test");
        options.MaxItemCount.ShouldBe(50);
        options.ContinuationToken.ShouldBe("token-123");
        options.Skip.ShouldBe(10);
    }

    [Fact]
    public void QueryOptions_UntypedLambda_OrderBy_AddsDescriptors()
    {
        var options = new QueryOptions();
        LambdaExpression expr1 = (Expression<Func<DocumentEnvelope<TestDocument>, object?>>)(e => e.Data.Name);
        LambdaExpression expr2 = (Expression<Func<DocumentEnvelope<TestDocument>, object?>>)(e => e.Data.Age);

        options.OrderBy(expr1).ShouldBe(options);
        options.ThenByDescending(expr2).ShouldBe(options);

        options.Orderings.Count.ShouldBe(2);
        options.Orderings[0].KeySelector.ShouldBe(expr1);
        options.Orderings[0].Direction.ShouldBe(SortOrder.Ascending);
        options.Orderings[1].KeySelector.ShouldBe(expr2);
        options.Orderings[1].Direction.ShouldBe(SortOrder.Descending);
    }

    [Fact]
    public void QueryOptions_UntypedLambda_OrderByDescending_And_ThenBy_AddsDescriptors()
    {
        var options = new QueryOptions();
        LambdaExpression expr1 = (Expression<Func<DocumentEnvelope<TestDocument>, object?>>)(e => e.Data.Balance);
        LambdaExpression expr2 = (Expression<Func<DocumentEnvelope<TestDocument>, object?>>)(e => e.Data.Name);

        options.OrderByDescending(expr1).ShouldBe(options);
        options.ThenBy(expr2, SortOrder.Ascending).ShouldBe(options);

        options.Orderings.Count.ShouldBe(2);
        options.Orderings[0].KeySelector.ShouldBe(expr1);
        options.Orderings[0].Direction.ShouldBe(SortOrder.Descending);
        options.Orderings[1].KeySelector.ShouldBe(expr2);
        options.Orderings[1].Direction.ShouldBe(SortOrder.Ascending);
    }

    [Fact]
    public void QueryOptions_UntypedLambda_NullKeySelector_ThrowsArgumentNullException()
    {
        var options = new QueryOptions();
        LambdaExpression nullExpr = null!;

        Should.Throw<ArgumentNullException>(() => options.OrderBy(nullExpr));
        Should.Throw<ArgumentNullException>(() => options.OrderByDescending(nullExpr));
        Should.Throw<ArgumentNullException>(() => options.ThenBy(nullExpr));
        Should.Throw<ArgumentNullException>(() => options.ThenByDescending(nullExpr));
    }

    [Fact]
    public void QueryOptions_TypedGeneric_OrderBy_FluentChaining()
    {
        var options = new QueryOptions();

        options.OrderBy<TestDocument>(e => e.Data.Name)
            .ThenByDescending<TestDocument>(e => e.Data.Age)
            .ThenBy<TestDocument>(e => e.Data.Balance, SortOrder.Ascending);

        options.Orderings.Count.ShouldBe(3);
        options.Orderings[0].Direction.ShouldBe(SortOrder.Ascending);
        options.Orderings[1].Direction.ShouldBe(SortOrder.Descending);
        options.Orderings[2].Direction.ShouldBe(SortOrder.Ascending);
    }

    [Fact]
    public void QueryOptions_TypedGeneric_OrderByDescending_FluentChaining()
    {
        var options = new QueryOptions();

        options.OrderByDescending<TestDocument>(e => e.Data.Balance)
            .ThenByDescending<TestDocument>(e => e.Data.Name);

        options.Orderings.Count.ShouldBe(2);
        options.Orderings[0].Direction.ShouldBe(SortOrder.Descending);
        options.Orderings[1].Direction.ShouldBe(SortOrder.Descending);
    }

    [Fact]
    public void QueryOptions_TypedGeneric_NullKeySelector_ThrowsArgumentNullException()
    {
        var options = new QueryOptions();
        Expression<Func<DocumentEnvelope<TestDocument>, object?>> nullExpr = null!;

        Should.Throw<ArgumentNullException>(() => options.OrderBy(nullExpr));
        Should.Throw<ArgumentNullException>(() => options.OrderByDescending(nullExpr));
        Should.Throw<ArgumentNullException>(() => options.ThenBy(nullExpr));
        Should.Throw<ArgumentNullException>(() => options.ThenByDescending(nullExpr));
    }

    // ==========================================
    // 6. StorageQueryResult<T> Tests
    // ==========================================

    [Fact]
    public void StorageQueryResult_DefaultConstructor_HasExpectedDefaults()
    {
        var result = new StorageQueryResult<TestDocument>();

        result.Documents.ShouldNotBeNull();
        result.Documents.ShouldBeEmpty();
        result.ContinuationToken.ShouldBeNull();
        result.TotalCount.ShouldBeNull();
        result.RequestCharge.ShouldBe(0.0);
    }

    [Fact]
    public void StorageQueryResult_ParameterizedConstructor_SetsAllProperties()
    {
        var env1 = new DocumentEnvelope<TestDocument> { Id = "1", Data = new TestDocument("A", 20, 10m) };
        var env2 = new DocumentEnvelope<TestDocument> { Id = "2", Data = new TestDocument("B", 30, 20m) };
        var list = new List<DocumentEnvelope<TestDocument>> { env1, env2 };

        var result = new StorageQueryResult<TestDocument>(list, continuationToken: "token-abc", totalCount: 100, requestCharge: 4.75);

        result.Documents.Count.ShouldBe(2);
        result.Documents[0].Id.ShouldBe("1");
        result.Documents[1].Id.ShouldBe("2");
        result.ContinuationToken.ShouldBe("token-abc");
        result.TotalCount.ShouldBe(100);
        result.RequestCharge.ShouldBe(4.75);
    }

    [Fact]
    public void StorageQueryResult_ParameterizedConstructor_HandlesNullDocumentsAndWhitespaceTokens()
    {
        var resultNullDocs = new StorageQueryResult<TestDocument>(null!, continuationToken: null, totalCount: null);
        resultNullDocs.Documents.ShouldNotBeNull();
        resultNullDocs.Documents.ShouldBeEmpty();
        resultNullDocs.ContinuationToken.ShouldBeNull();
        resultNullDocs.TotalCount.ShouldBeNull();
        resultNullDocs.RequestCharge.ShouldBe(0.0);

        var resultEmptyToken = new StorageQueryResult<TestDocument>(Array.Empty<DocumentEnvelope<TestDocument>>(), continuationToken: "");
        resultEmptyToken.ContinuationToken.ShouldBeNull();

        var resultWhitespaceToken = new StorageQueryResult<TestDocument>(Array.Empty<DocumentEnvelope<TestDocument>>(), continuationToken: "   \t\n  ");
        resultWhitespaceToken.ContinuationToken.ShouldBeNull();
    }

    [Fact]
    public void StorageQueryResult_InitProperties_WorkProperly()
    {
        var env = new DocumentEnvelope<TestDocument> { Id = "doc-99" };
        var result = new StorageQueryResult<TestDocument>
        {
            Documents = new[] { env },
            ContinuationToken = "custom-token",
            TotalCount = 5,
            RequestCharge = 1.23
        };

        result.Documents.Count.ShouldBe(1);
        result.Documents[0].Id.ShouldBe("doc-99");
        result.ContinuationToken.ShouldBe("custom-token");
        result.TotalCount.ShouldBe(5);
        result.RequestCharge.ShouldBe(1.23);
    }

    // ==========================================
    // 7. IDocumentStorageProvider & IEventStorageProvider Default Implementations
    // ==========================================

    private sealed class MinimalDocumentStorageProviderStub : IDocumentStorageProvider
    {
        public string ProviderName => "Stub";
        public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;
        public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class => Task.FromResult<DocumentEnvelope<T>?>(null);
        public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class => Task.FromResult<IReadOnlyList<DocumentEnvelope<T>>>(Array.Empty<DocumentEnvelope<T>>());
        public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class => Task.FromResult(new StorageQueryResult<T>());
        public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class => Task.CompletedTask;
        public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class => Task.CompletedTask;
        public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) => Task.CompletedTask;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class MinimalEventStorageProviderStub : IEventStorageProvider
    {
        public string ProviderName => "StubEvents";
        public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;
        public Task AppendEventsAsync(string streamId, IEnumerable<IEvent> events, long expectedVersion, CancellationToken ct = default) => Task.CompletedTask;
        public Task<IReadOnlyList<IEvent>> FetchEventsAsync(string streamId, string? tenantId = null, long fromVersion = 0, CancellationToken ct = default) => Task.FromResult<IReadOnlyList<IEvent>>(Array.Empty<IEvent>());
        public Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, string? tenantId = null, CancellationToken ct = default) => Task.FromResult<IReadOnlyList<IEvent>>(Array.Empty<IEvent>());
        public Task<EventStreamHeader?> GetStreamHeaderAsync(string streamId, string? tenantId = null, CancellationToken ct = default) => Task.FromResult<EventStreamHeader?>(null);
        public Task SaveSnapshotAsync<TAggregate>(string streamId, long version, TAggregate snapshot, string tenantId = "default", CancellationToken ct = default) where TAggregate : class => Task.CompletedTask;
        public Task<(TAggregate? Snapshot, long SnapshotVersion)> GetSnapshotAsync<TAggregate>(string streamId, string tenantId = "default", CancellationToken ct = default) where TAggregate : class => Task.FromResult<(TAggregate?, long)>((null, 0));
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public void IDocumentStorageProvider_DefaultInterfaceProperties_ReturnZero()
    {
        IDocumentStorageProvider provider = new MinimalDocumentStorageProviderStub();

        provider.ProviderName.ShouldBe("Stub");
        provider.LastRequestCharge.ShouldBe(0.0);
        provider.CumulativeRequestCharge.ShouldBe(0.0);
    }

    [Fact]
    public void IEventStorageProvider_DefaultInterfaceProperties_ReturnZero()
    {
        IEventStorageProvider provider = new MinimalEventStorageProviderStub();

        provider.ProviderName.ShouldBe("StubEvents");
        provider.LastRequestCharge.ShouldBe(0.0);
        provider.CumulativeRequestCharge.ShouldBe(0.0);
    }
}
