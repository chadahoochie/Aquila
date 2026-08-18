using System.Net;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests.Storage;

public sealed record SampleProjectionModel(string Id, string Title, decimal Score);

public sealed class CosmosProjectionStorageProviderTests
{
    private readonly Container _mockContainer;
    private readonly CosmosDocumentStorageProvider _mockDocProvider;

    public CosmosProjectionStorageProviderTests()
    {
        _mockContainer = Substitute.For<Container>();
        _mockDocProvider = new CosmosDocumentStorageProvider(() => _mockContainer);
    }

    [Fact]
    public void Constructors_NullChecks_And_Overloads()
    {
        CosmosDocumentStorageProvider nullDocProvider = null!;
        Func<Container> nullFunc = null!;
        Container nullContainer = null!;
        Func<Type, Container> nullTypeFunc = null!;

        Should.Throw<ArgumentNullException>(() => new CosmosProjectionStorageProvider(nullDocProvider));
        Should.Throw<ArgumentNullException>(() => new CosmosProjectionStorageProvider(nullFunc));
        Should.Throw<ArgumentNullException>(() => new CosmosProjectionStorageProvider(nullContainer));
        Should.Throw<ArgumentNullException>(() => new CosmosProjectionStorageProvider(nullTypeFunc));

        // Valid overloads
        var p1 = new CosmosProjectionStorageProvider(_mockDocProvider);
        p1.ShouldNotBeNull();
        p1.ProviderName.ShouldBe("AzureCosmosDB");

        var p2 = new CosmosProjectionStorageProvider(() => _mockContainer);
        p2.ShouldNotBeNull();

        var p3 = new CosmosProjectionStorageProvider(_mockContainer);
        p3.ShouldNotBeNull();

        var p4 = new CosmosProjectionStorageProvider(t => _mockContainer);
        p4.ShouldNotBeNull();
    }

    [Fact]
    public async Task Metadata_And_Lifecycle_DelegateProperly()
    {
        var provider = new CosmosProjectionStorageProvider(_mockDocProvider);

        provider.ProviderName.ShouldBe("AzureCosmosDB");
        provider.LastRequestCharge.ShouldBe(0.0);
        provider.CumulativeRequestCharge.ShouldBe(0.0);

        await provider.InitializeAsync(TestContext.Current.CancellationToken);
        provider.Dispose();
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task ReadDocumentAsync_And_UpsertDocumentAsync_Delegate()
    {
        var provider = new CosmosProjectionStorageProvider(_mockContainer);
        var envelope = new DocumentEnvelope<SampleProjectionModel>
        {
            Id = "proj-1",
            PartitionKey = "pk-1",
            DocType = nameof(SampleProjectionModel),
            Data = new SampleProjectionModel("proj-1", "Test Read Model", 99.5m)
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<SampleProjectionModel>>>();
        response.Resource.Returns(new CosmosDocumentEnvelope<SampleProjectionModel>
        {
            Id = "proj-1",
            PartitionKey = "pk-1",
            DocType = nameof(SampleProjectionModel),
            Data = envelope.Data
        });
        response.RequestCharge.Returns(2.0);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<SampleProjectionModel>>(
            "proj-1",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var loaded = await provider.ReadDocumentAsync<SampleProjectionModel>("proj-1", "pk-1", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe("proj-1");
        loaded.Data.Title.ShouldBe("Test Read Model");
        provider.LastRequestCharge.ShouldBe(2.0);

        // Upsert
        var upsertResponse = Substitute.For<ItemResponse<CosmosDocumentEnvelope<SampleProjectionModel>>>();
        upsertResponse.RequestCharge.Returns(3.0);
        _mockContainer.UpsertItemAsync(
            Arg.Any<CosmosDocumentEnvelope<SampleProjectionModel>>(),
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(upsertResponse));

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);
        provider.LastRequestCharge.ShouldBe(3.0);
    }

    [Fact]
    public async Task DeleteDocumentAsync_And_ExecuteBatchAsync_Delegate()
    {
        var provider = new CosmosProjectionStorageProvider(_mockContainer);

        var delResponse = new ResponseMessage(HttpStatusCode.OK);
        _mockContainer.DeleteItemStreamAsync(
            "proj-del",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-del"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(delResponse));

        await provider.DeleteDocumentAsync<SampleProjectionModel>("proj-del", "pk-del", TestContext.Current.CancellationToken);

        // Batch execution
        var batch = Substitute.For<TransactionalBatch>();
        var batchResponse = Substitute.For<TransactionalBatchResponse>();
        batchResponse.IsSuccessStatusCode.Returns(true);
        batchResponse.RequestCharge.Returns(5.0);
        batch.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(batchResponse));
        _mockContainer.CreateTransactionalBatch(Arg.Any<PartitionKey>()).Returns(batch);

        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Upsert,
            Id = "batch-1",
            PartitionKey = "pk-b",
            DocType = nameof(SampleProjectionModel),
            Document = new DocumentEnvelope<SampleProjectionModel>
            {
                Id = "batch-1",
                PartitionKey = "pk-b",
                DocType = nameof(SampleProjectionModel),
                Data = new SampleProjectionModel("batch-1", "Batch Title", 10m)
            }
        };

        await provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);
        provider.LastRequestCharge.ShouldBe(5.0);
    }

    [Fact]
    public async Task QueryDocumentsAsync_And_QueryPagedDocumentsAsync_Delegate()
    {
        var provider = new CosmosProjectionStorageProvider(_mockContainer);

        var fakeList = new List<CosmosDocumentEnvelope<SampleProjectionModel>>().AsQueryable() as IOrderedQueryable<CosmosDocumentEnvelope<SampleProjectionModel>>;
        _mockContainer.GetItemLinqQueryable<CosmosDocumentEnvelope<SampleProjectionModel>>(
            Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<QueryRequestOptions>(), Arg.Any<CosmosLinqSerializerOptions>())
            .Returns(fakeList);

        var queryDocs = await provider.QueryDocumentsAsync<SampleProjectionModel>(e => e.Data.Score > 20, new QueryOptions { PartitionKey = "pk-q" }, TestContext.Current.CancellationToken);
        queryDocs.ShouldNotBeNull();

        var paged = await provider.QueryPagedDocumentsAsync<SampleProjectionModel>(e => e.Data.Score > 20, new QueryOptions { PartitionKey = "pk-q", MaxItemCount = 10 }, TestContext.Current.CancellationToken);
        paged.ShouldNotBeNull();
    }

    [Fact]
    public async Task PurgeProjectionAsync_DelegatesToPurgeDocumentsByType()
    {
        var provider = new CosmosProjectionStorageProvider(_mockContainer);

        var item = new CosmosDocumentEnvelope
        {
            Id = "proj-purge-1",
            PartitionKey = "pk-purge",
            DocType = nameof(SampleProjectionModel)
        };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope>>();
        page.GetEnumerator().Returns(new List<CosmosDocumentEnvelope> { item }.GetEnumerator());
        page.RequestCharge.Returns(1.0);

        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope>(
            Arg.Any<QueryDefinition>(),
            Arg.Any<string>(),
            Arg.Any<QueryRequestOptions>())
            .Returns(iterator);

        var delResponse = new ResponseMessage(HttpStatusCode.OK);
        _mockContainer.DeleteItemStreamAsync(
            "proj-purge-1",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-purge"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(delResponse));

        await provider.PurgeProjectionAsync("SampleProjection", typeof(SampleProjectionModel), TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).DeleteItemStreamAsync(
            "proj-purge-1",
            CosmosPartitionKeyHelper.CreatePartitionKey("pk-purge"),
            cancellationToken: Arg.Any<CancellationToken>());
    }
}
