using System.Net;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Exceptions;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests.Storage;

public sealed record DocTestEntity(string Id, string Title, decimal Price);
public sealed record AnotherDocEntity(string Id, string Description);

public sealed class CosmosDocumentStorageProviderTests
{
    private readonly Container _mockContainer;
    private readonly CosmosDocumentStorageProvider _provider;

    public CosmosDocumentStorageProviderTests()
    {
        _mockContainer = Substitute.For<Container>();
        _provider = new CosmosDocumentStorageProvider(_mockContainer);
    }

    // ==========================================
    // 1. Constructor & Lifecycle Tests
    // ==========================================

    [Fact]
    public void Constructors_NullArguments_ThrowArgumentNullException()
    {
        Func<Container> nullContainerProvider = null!;
        Container nullContainer = null!;
        Func<Type, Container> nullTypeResolver = null!;

        Should.Throw<ArgumentNullException>(() => new CosmosDocumentStorageProvider(nullContainerProvider));
        Should.Throw<ArgumentNullException>(() => new CosmosDocumentStorageProvider(nullContainer));
        Should.Throw<ArgumentNullException>(() => new CosmosDocumentStorageProvider(nullTypeResolver));
    }

    [Fact]
    public async Task ProviderMetadata_And_Lifecycle_WorkAsExpected()
    {
        _provider.ProviderName.ShouldBe("AzureCosmosDB");
        _provider.LastRequestCharge.ShouldBe(0.0);
        _provider.CumulativeRequestCharge.ShouldBe(0.0);

        await _provider.InitializeAsync(TestContext.Current.CancellationToken);
        _provider.Dispose();
        await _provider.DisposeAsync();
    }

    // ==========================================
    // 2. ReadDocumentAsync Tests
    // ==========================================

    [Fact]
    public async Task ReadDocumentAsync_InvalidArguments_ThrowArgumentException()
    {
        await Should.ThrowAsync<ArgumentException>(() => _provider.ReadDocumentAsync<DocTestEntity>("", "pk", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.ReadDocumentAsync<DocTestEntity>("   ", "pk", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.ReadDocumentAsync<DocTestEntity>("id-1", "", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.ReadDocumentAsync<DocTestEntity>("id-1", "   ", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ReadDocumentAsync_PointRead_ReturnsEnvelope_WhenFound()
    {
        var entity = new DocTestEntity("doc-1", "Item 1", 19.99m);
        var envelope = new CosmosDocumentEnvelope<DocTestEntity>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(DocTestEntity),
            TenantId = "tenant-1",
            IsDeleted = false,
            Version = "v-1",
            ETag = "\"etag-1\"",
            Data = entity
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        response.Resource.Returns(envelope);
        response.RequestCharge.Returns(1.5);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<DocTestEntity>>(
            "doc-1",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var result = await _provider.ReadDocumentAsync<DocTestEntity>("doc-1", "pk-1", TestContext.Current.CancellationToken);

        result.ShouldNotBeNull();
        result.Id.ShouldBe("doc-1");
        result.PartitionKey.ShouldBe("pk-1");
        result.TenantId.ShouldBe("tenant-1");
        result.ETag.ShouldBe("\"etag-1\"");
        result.Data.ShouldNotBeNull();
        result.Data.Title.ShouldBe("Item 1");
        result.Data.Price.ShouldBe(19.99m);

        _provider.LastRequestCharge.ShouldBe(1.5);
        _provider.CumulativeRequestCharge.ShouldBe(1.5);
    }

    [Fact]
    public async Task ReadDocumentAsync_PointRead_ReturnsNull_WhenDeletedOrResourceNull()
    {
        var deletedEnvelope = new CosmosDocumentEnvelope<DocTestEntity>
        {
            Id = "doc-del",
            PartitionKey = "pk-1",
            IsDeleted = true,
            Data = new DocTestEntity("doc-del", "Deleted", 0m)
        };

        var deletedResponse = Substitute.For<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        deletedResponse.Resource.Returns(deletedEnvelope);
        deletedResponse.RequestCharge.Returns(1.0);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<DocTestEntity>>(
            "doc-del",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(deletedResponse));

        var resultDeleted = await _provider.ReadDocumentAsync<DocTestEntity>("doc-del", "pk-1", TestContext.Current.CancellationToken);
        resultDeleted.ShouldBeNull();

        var nullResponse = Substitute.For<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        nullResponse.Resource.Returns((CosmosDocumentEnvelope<DocTestEntity>?)null);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<DocTestEntity>>(
            "doc-null",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(nullResponse));

        var resultNull = await _provider.ReadDocumentAsync<DocTestEntity>("doc-null", "pk-1", TestContext.Current.CancellationToken);
        resultNull.ShouldBeNull();
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.InternalServerError)]
    public async Task ReadDocumentAsync_CosmosException_HandledStatusCodes_ReturnNull(HttpStatusCode statusCode)
    {
        var ex = new CosmosException("Error", statusCode, 0, "act-1", 1.0);
        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<DocTestEntity>>(
            "doc-err",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>(ex));

        var result = await _provider.ReadDocumentAsync<DocTestEntity>("doc-err", "pk-1", TestContext.Current.CancellationToken);
        result.ShouldBeNull();
    }

    [Fact]
    public async Task ReadDocumentAsync_SlashedId_QueriesSingleDocument()
    {
        var slashedId = "orders/2026/01/100";
        var entity = new DocTestEntity(slashedId, "Slashed Item", 55.0m);
        var envelope = new CosmosDocumentEnvelope<DocTestEntity>
        {
            Id = slashedId,
            PartitionKey = "pk-slash",
            DocType = nameof(DocTestEntity),
            Data = entity
        };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<DocTestEntity>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        page.GetEnumerator().Returns(new List<CosmosDocumentEnvelope<DocTestEntity>> { envelope }.GetEnumerator());
        page.RequestCharge.Returns(2.5);

        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<DocTestEntity>>(
            Arg.Is<QueryDefinition>(q => q.QueryText.Contains("SELECT * FROM c WHERE c.id = @id")),
            requestOptions: Arg.Any<QueryRequestOptions>())
            .Returns(iterator);

        var result = await _provider.ReadDocumentAsync<DocTestEntity>(slashedId, "pk-slash", TestContext.Current.CancellationToken);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(slashedId);
        result.Data.Title.ShouldBe("Slashed Item");
        _provider.LastRequestCharge.ShouldBe(2.5);
    }

    [Fact]
    public async Task ReadDocumentAsync_SlashedId_ReturnsNull_WhenEmptyQueryResults()
    {
        var slashedId = "orders/missing/999";

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<DocTestEntity>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        page.GetEnumerator().Returns(new List<CosmosDocumentEnvelope<DocTestEntity>>().GetEnumerator());
        page.RequestCharge.Returns(2.0);

        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<DocTestEntity>>(
            Arg.Any<QueryDefinition>(),
            requestOptions: Arg.Any<QueryRequestOptions>())
            .Returns(iterator);

        var result = await _provider.ReadDocumentAsync<DocTestEntity>(slashedId, "pk-slash", TestContext.Current.CancellationToken);
        result.ShouldBeNull();
        _provider.LastRequestCharge.ShouldBe(2.0);
    }

    // ==========================================
    // 3. UpsertDocumentAsync Tests
    // ==========================================

    [Fact]
    public async Task UpsertDocumentAsync_InvalidArguments_ThrowException()
    {
        await Should.ThrowAsync<ArgumentNullException>(() => _provider.UpsertDocumentAsync<DocTestEntity>(null!, TestContext.Current.CancellationToken));

        var invalidIdEnvelope = new DocumentEnvelope<DocTestEntity> { Id = "", PartitionKey = "pk" };
        await Should.ThrowAsync<ArgumentException>(() => _provider.UpsertDocumentAsync(invalidIdEnvelope, TestContext.Current.CancellationToken));

        var invalidPkEnvelope = new DocumentEnvelope<DocTestEntity> { Id = "id", PartitionKey = "" };
        await Should.ThrowAsync<ArgumentException>(() => _provider.UpsertDocumentAsync(invalidPkEnvelope, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UpsertDocumentAsync_UpsertsItem_WithETagMatching()
    {
        var envelope = new DocumentEnvelope<DocTestEntity>
        {
            Id = "doc-upsert",
            PartitionKey = "pk-upsert",
            DocType = nameof(DocTestEntity),
            TenantId = "tenant-1",
            ETag = "\"etag-val-123\"",
            Data = new DocTestEntity("doc-upsert", "Title", 10m)
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        response.RequestCharge.Returns(6.0);

        _mockContainer.UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<DocTestEntity>>(e => e.Id == "doc-upsert" && e.ETag == "\"etag-val-123\""),
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-upsert"),
            Arg.Is<ItemRequestOptions>(opts => opts.IfMatchEtag == "\"etag-val-123\""),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        await _provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        _provider.LastRequestCharge.ShouldBe(6.0);
    }

    [Fact]
    public async Task UpsertDocumentAsync_PreconditionFailed_ThrowsAquilaConcurrencyException()
    {
        var envelope = new DocumentEnvelope<DocTestEntity>
        {
            Id = "doc-conflict",
            PartitionKey = "pk-1",
            ETag = "\"old-etag\"",
            Data = new DocTestEntity("doc-conflict", "Title", 10m)
        };

        var ex = new CosmosException("Precondition Failed", HttpStatusCode.PreconditionFailed, 0, "act-1", 1.0);
        _mockContainer.UpsertItemAsync(
            Arg.Any<CosmosDocumentEnvelope<DocTestEntity>>(),
            Arg.Any<PartitionKey>(),
            Arg.Any<ItemRequestOptions>(),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>(ex));

        var thrown = await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            _provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken));

        thrown.DocumentId.ShouldBe("doc-conflict");
    }

    // ==========================================
    // 4. DeleteDocumentAsync Tests
    // ==========================================

    [Fact]
    public async Task DeleteDocumentAsync_InvalidArguments_ThrowArgumentException()
    {
        await Should.ThrowAsync<ArgumentException>(() => _provider.DeleteDocumentAsync<DocTestEntity>("", "pk", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.DeleteDocumentAsync<DocTestEntity>("id", "", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeleteDocumentAsync_Calls_DeleteItemStreamAsync_Success()
    {
        var response = Substitute.For<ResponseMessage>();
        response.IsSuccessStatusCode.Returns(true);
        var headers = Substitute.For<Headers>();
        headers.RequestCharge.Returns(2.4);
        response.Headers.Returns(headers);

        _mockContainer.DeleteItemStreamAsync(
            "doc-del-1",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-del"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        await _provider.DeleteDocumentAsync<DocTestEntity>("doc-del-1", "pk-del", TestContext.Current.CancellationToken);

        _provider.LastRequestCharge.ShouldBe(2.4);
    }

    [Fact]
    public async Task DeleteDocumentAsync_FallsBackToSoftDelete_WhenDeleteItemStreamFails()
    {
        var response = Substitute.For<ResponseMessage>();
        response.IsSuccessStatusCode.Returns(false);
        response.StatusCode.Returns(HttpStatusCode.MethodNotAllowed);

        _mockContainer.DeleteItemStreamAsync(
            "doc-del-fallback",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-del"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var upsertResponse = Substitute.For<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        upsertResponse.RequestCharge.Returns(3.2);

        _mockContainer.UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<DocTestEntity>>(e => e.Id == "doc-del-fallback" && e.IsDeleted),
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-del"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(upsertResponse));

        await _provider.DeleteDocumentAsync<DocTestEntity>("doc-del-fallback", "pk-del", TestContext.Current.CancellationToken);

        _provider.LastRequestCharge.ShouldBe(3.2);
    }

    [Fact]
    public async Task DeleteDocumentAsync_Handles_CosmosException_NotFound()
    {
        var ex = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "act-1", 1.2);

        _mockContainer.DeleteItemStreamAsync(
            "doc-del-notfound",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-del"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ResponseMessage>(ex));

        await _provider.DeleteDocumentAsync<DocTestEntity>("doc-del-notfound", "pk-del", TestContext.Current.CancellationToken);

        _provider.LastRequestCharge.ShouldBe(1.2);
    }

    // ==========================================
    // 5. ExecuteBatchAsync Tests
    // ==========================================

    [Fact]
    public async Task ExecuteBatchAsync_EmptyOrNullOperations_HandlesProperly()
    {
        await Should.ThrowAsync<ArgumentNullException>(() => _provider.ExecuteBatchAsync(null!, TestContext.Current.CancellationToken));
        await _provider.ExecuteBatchAsync(Enumerable.Empty<StorageOperation>(), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ExecuteBatchAsync_Validates_Operation_Id_And_PartitionKey()
    {
        var opInvalidId = new StorageOperation { Id = "", PartitionKey = "pk", OperationType = StorageOperationType.Upsert };
        await Should.ThrowAsync<ArgumentException>(() => _provider.ExecuteBatchAsync(new[] { opInvalidId }, TestContext.Current.CancellationToken));

        var opInvalidPk = new StorageOperation { Id = "id", PartitionKey = "", OperationType = StorageOperationType.Upsert };
        await Should.ThrowAsync<ArgumentException>(() => _provider.ExecuteBatchAsync(new[] { opInvalidPk }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExecuteBatchAsync_ExecutesTransactionalBatch_Success()
    {
        var batch = Substitute.For<TransactionalBatch>();
        var batchResponse = Substitute.For<TransactionalBatchResponse>();
        batchResponse.IsSuccessStatusCode.Returns(true);
        batchResponse.RequestCharge.Returns(12.5);

        batch.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(batchResponse));

        _mockContainer.CreateTransactionalBatch(Arg.Any<PartitionKey>()).Returns(batch);

        var docEnvelope = new DocumentEnvelope<DocTestEntity>
        {
            Id = "b-1",
            PartitionKey = "pk-b",
            DocType = nameof(DocTestEntity),
            Data = new DocTestEntity("b-1", "Batch 1", 100m)
        };

        var ops = new List<StorageOperation>
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = "b-1",
                PartitionKey = "pk-b",
                DocType = nameof(DocTestEntity),
                Document = docEnvelope
            },
            new StorageOperation
            {
                OperationType = StorageOperationType.Delete,
                Id = "b-2",
                PartitionKey = "pk-b",
                DocType = nameof(DocTestEntity)
            },
            new StorageOperation
            {
                OperationType = StorageOperationType.Patch,
                Id = "b-3",
                PartitionKey = "pk-b",
                DocType = nameof(DocTestEntity),
                PatchOperations = new List<PatchOperationData>
                {
                    new() { Path = "/Title", Action = PatchAction.Set, Value = "New Title" },
                    new() { Path = "/Price", Action = PatchAction.Increment, Value = 5 },
                    new() { Path = "/Tags", Action = PatchAction.Append, Value = "Tag1" },
                    new() { Path = "/Tags/0", Action = PatchAction.Remove }
                }
            }
        };

        await _provider.ExecuteBatchAsync(ops, TestContext.Current.CancellationToken);

        batch.Received(1).UpsertItem(Arg.Any<object>(), Arg.Any<TransactionalBatchItemRequestOptions>());
        batch.Received(1).DeleteItem("b-2", Arg.Any<TransactionalBatchItemRequestOptions>());
        batch.Received(1).PatchItem("b-3", Arg.Any<IReadOnlyList<Microsoft.Azure.Cosmos.PatchOperation>>(), Arg.Any<TransactionalBatchPatchItemRequestOptions>());
        _provider.LastRequestCharge.ShouldBe(12.5);
    }

    [Fact]
    public async Task ExecuteBatchAsync_TransactionalBatch_ConcurrencyException_OnConflict()
    {
        var batch = Substitute.For<TransactionalBatch>();
        var batchResponse = Substitute.For<TransactionalBatchResponse>();
        batchResponse.IsSuccessStatusCode.Returns(false);
        batchResponse.StatusCode.Returns(HttpStatusCode.Conflict);

        batch.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(batchResponse));
        _mockContainer.CreateTransactionalBatch(Arg.Any<PartitionKey>()).Returns(batch);

        var ops = new List<StorageOperation>
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = "b-conflict",
                PartitionKey = "pk-b",
                Document = new DocumentEnvelope<DocTestEntity> { Id = "b-conflict", PartitionKey = "pk-b", ETag = "\"old-etag\"" }
            }
        };

        var ex = await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            _provider.ExecuteBatchAsync(ops, TestContext.Current.CancellationToken));

        ex.DocumentId.ShouldBe("b-conflict");
    }

    // ==========================================
    // 6. Type-Container Resolver Tests
    // ==========================================

    [Fact]
    public async Task TypeContainerResolver_Routes_Operations_To_Specific_Containers()
    {
        var containerDoc1 = Substitute.For<Container>();
        var containerDoc2 = Substitute.For<Container>();

        var provider = new CosmosDocumentStorageProvider(type =>
        {
            if (type == typeof(DocTestEntity)) return containerDoc1;
            if (type == typeof(AnotherDocEntity)) return containerDoc2;
            return _mockContainer;
        });

        var resp1 = Substitute.For<ItemResponse<CosmosDocumentEnvelope<DocTestEntity>>>();
        resp1.Resource.Returns(new CosmosDocumentEnvelope<DocTestEntity> { Id = "d1", PartitionKey = "pk", Data = new DocTestEntity("d1", "T", 1) });
        containerDoc1.ReadItemAsync<CosmosDocumentEnvelope<DocTestEntity>>("d1", Arg.Any<PartitionKey>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(resp1));

        var resp2 = Substitute.For<ItemResponse<CosmosDocumentEnvelope<AnotherDocEntity>>>();
        resp2.Resource.Returns(new CosmosDocumentEnvelope<AnotherDocEntity> { Id = "d2", PartitionKey = "pk", Data = new AnotherDocEntity("d2", "Desc") });
        containerDoc2.ReadItemAsync<CosmosDocumentEnvelope<AnotherDocEntity>>("d2", Arg.Any<PartitionKey>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(resp2));

        var item1 = await provider.ReadDocumentAsync<DocTestEntity>("d1", "pk", TestContext.Current.CancellationToken);
        var item2 = await provider.ReadDocumentAsync<AnotherDocEntity>("d2", "pk", TestContext.Current.CancellationToken);

        item1.ShouldNotBeNull();
        item1.Data.Title.ShouldBe("T");
        item2.ShouldNotBeNull();
        item2.Data.Description.ShouldBe("Desc");
    }
}
