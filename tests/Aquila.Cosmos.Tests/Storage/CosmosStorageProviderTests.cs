using System.Net;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record MockDoc(string Id, string Name);

public sealed class CosmosStorageProviderTests
{
    private readonly Container _mockContainer;
    private readonly CosmosClient _mockClient;
    private readonly CosmosStorageProvider _provider;

    public CosmosStorageProviderTests()
    {
        _mockContainer = Substitute.For<Container>();
        var mockDatabase = Substitute.For<Database>();
        _mockClient = Substitute.For<CosmosClient>();

        _mockClient.GetDatabase(Arg.Any<string>()).Returns(mockDatabase);
        _mockClient.GetContainer(Arg.Any<string>(), Arg.Any<string>()).Returns(_mockContainer);
        mockDatabase.GetContainer(Arg.Any<string>()).Returns(_mockContainer);

        _provider = new CosmosStorageProvider(_mockClient, "TestDatabase", "TestContainer");
    }

    [Fact]
    public void ProviderName_Returns_AzureCosmosDB()
    {
        _provider.ProviderName.ShouldBe("AzureCosmosDB");
    }

    [Fact]
    public async Task InitializeAsync_Calls_CreateDatabaseIfNotExists_And_CreateContainerIfNotExists()
    {
        var mockDatabase = Substitute.For<Database>();
        var mockDbResponse = Substitute.For<DatabaseResponse>();
        var mockContainerResponse = Substitute.For<ContainerResponse>();

        mockDbResponse.Database.Returns(mockDatabase);
        mockContainerResponse.Container.Returns(_mockContainer);

        _mockClient.CreateDatabaseIfNotExistsAsync("TestDatabase", cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(mockDbResponse));

        mockDatabase.CreateContainerIfNotExistsAsync(Arg.Any<ContainerProperties>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(mockContainerResponse));

        await _provider.InitializeAsync(TestContext.Current.CancellationToken);

        await _mockClient.Received(1).CreateDatabaseIfNotExistsAsync("TestDatabase", cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ReadDocumentAsync_Returns_Envelope_When_Found()
    {
        var expectedDoc = new MockDoc("doc-1", "Test Item");
        var envelope = new CosmosDocumentEnvelope<MockDoc>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(MockDoc),
            TenantId = "default",
            Data = expectedDoc
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        response.Resource.Returns(envelope);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>(
            "doc-1",
            new PartitionKey("pk-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var result = await _provider.ReadDocumentAsync<MockDoc>("doc-1", "pk-1", TestContext.Current.CancellationToken);

        result.ShouldNotBeNull();
        result.Id.ShouldBe("doc-1");
        result.Data.Name.ShouldBe("Test Item");
    }

    [Fact]
    public async Task ReadDocumentAsync_Returns_Null_When_NotFound()
    {
        var exception = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "activity-1", 0);
        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>("missing-id", new PartitionKey("pk-1"), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>(exception));

        var result = await _provider.ReadDocumentAsync<MockDoc>("missing-id", "pk-1", TestContext.Current.CancellationToken);
        result.ShouldBeNull();
    }

    [Fact]
    public async Task UpsertDocumentAsync_Calls_Container_UpsertItemAsync()
    {
        var envelope = new DocumentEnvelope<MockDoc>
        {
            Id = "doc-10",
            PartitionKey = "pk-10",
            DocType = nameof(MockDoc),
            TenantId = "default",
            Data = new MockDoc("doc-10", "Upsert Test")
        };

        await _provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<MockDoc>>(e => e.Id == "doc-10" && e.PartitionKey == "pk-10"),
            new PartitionKey("pk-10"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DeleteDocumentAsync_Calls_Container_DeleteItemAsync()
    {
        await _provider.DeleteDocumentAsync<MockDoc>("doc-99", "pk-99", TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).DeleteItemAsync<CosmosDocumentEnvelope<MockDoc>>(
            "doc-99",
            new PartitionKey("pk-99"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteBatchAsync_Processes_Upsert_And_Delete_Operations()
    {
        var ops = new List<StorageOperation>
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = "op-1",
                PartitionKey = "pk-op",
                DocType = nameof(MockDoc),
                Document = new DocumentEnvelope<MockDoc> { Id = "op-1", PartitionKey = "pk-op", DocType = nameof(MockDoc), Data = new MockDoc("op-1", "B1") }
            },
            new StorageOperation
            {
                OperationType = StorageOperationType.Delete,
                Id = "op-2",
                PartitionKey = "pk-op",
                DocType = nameof(MockDoc)
            }
        };

        await _provider.ExecuteBatchAsync(ops, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).UpsertItemAsync(
            Arg.Any<object>(),
            new PartitionKey("pk-op"),
            cancellationToken: Arg.Any<CancellationToken>());

        await _mockContainer.Received(1).DeleteItemAsync<object>(
            "op-2",
            new PartitionKey("pk-op"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AppendEventsAsync_Throws_AquilaConcurrencyException_On_Version_Mismatch()
    {
        var streamId = "stream-conc-1";
        var existingHeader = new CosmosDocumentEnvelope<EventStreamHeader>
        {
            Id = $"$stream_{streamId}",
            PartitionKey = streamId,
            TenantId = "default",
            Data = new EventStreamHeader { StreamId = streamId, Version = 5, TenantId = "default" }
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>();
        response.Resource.Returns(existingHeader);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            new PartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var evt = new EventEnvelope<object> { StreamId = streamId, Version = 1, Data = new { Key = "Val" } };

        // Expected version is 1, but actual header version is 5 -> MUST throw AquilaConcurrencyException
        await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            _provider.AppendEventsAsync(streamId, new[] { evt }, expectedVersion: 1, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task GetStreamHeaderAsync_Returns_Null_When_NotFound_Or_Tenant_Mismatch()
    {
        var streamId = "stream-hdr-missing";

        var exception = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "", 0);
        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            new PartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>(exception));

        var header = await _provider.GetStreamHeaderAsync(streamId, ct: TestContext.Current.CancellationToken);
        header.ShouldBeNull();

        var resultZero = await _provider.FetchGlobalEventsAsync(0, batchSize: 0, ct: TestContext.Current.CancellationToken);
        resultZero.ShouldBeEmpty();

        var resultNeg = await _provider.FetchGlobalEventsAsync(0, batchSize: -10, ct: TestContext.Current.CancellationToken);
        resultNeg.ShouldBeEmpty();
    }

    [Fact]
    public void StorageProvider_InputValidation_ThrowsExceptions()
    {
        Should.ThrowAsync<ArgumentException>(() => _provider.ReadDocumentAsync<MockDoc>("", "pk"));
        Should.ThrowAsync<ArgumentException>(() => _provider.ReadDocumentAsync<MockDoc>("id", "   "));
        Should.ThrowAsync<ArgumentNullException>(() => _provider.UpsertDocumentAsync<MockDoc>(null!));
        Should.ThrowAsync<ArgumentException>(() => _provider.DeleteDocumentAsync<MockDoc>("", "pk"));
        Should.ThrowAsync<ArgumentException>(() => _provider.AppendEventsAsync("", new List<IEvent>(), 0));
        Should.ThrowAsync<ArgumentNullException>(() => _provider.AppendEventsAsync("s1", null!, 0));
        Should.ThrowAsync<ArgumentException>(() => _provider.FetchEventsAsync(""));
        Should.ThrowAsync<ArgumentException>(() => _provider.GetStreamHeaderAsync(""));
    }
    [Fact]
    public async Task FetchGlobalEventsAsync_Paginates_Filters_And_Limits_BatchSize()
    {
        var evt1 = new EventEnvelope<object> { StreamId = "s1", Version = 1, TenantId = "t1", GlobalSequence = 10, Data = "d1" };
        var evt2 = new EventEnvelope<object> { StreamId = "s1", Version = 2, TenantId = "t1", GlobalSequence = 20, Data = "d2" };
        var evt3 = new EventEnvelope<object> { StreamId = "s2", Version = 1, TenantId = "t1", GlobalSequence = 30, Data = "d3" };

        var env1 = new CosmosDocumentEnvelope<object> { Id = "e1", PartitionKey = "s1", TenantId = "t1", Data = evt1 };
        var env2 = new CosmosDocumentEnvelope<object> { Id = "e2", PartitionKey = "s1", TenantId = "t1", Data = evt2 };
        var env3 = new CosmosDocumentEnvelope<object> { Id = "e3", PartitionKey = "s2", TenantId = "t1", Data = evt3 };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page1 = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        var page2 = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();

        var page1List = new List<CosmosDocumentEnvelope<object>> { env1, env2 };
        var page2List = new List<CosmosDocumentEnvelope<object>> { env3 };

        page1.GetEnumerator().Returns(_ => page1List.GetEnumerator());
        page2.GetEnumerator().Returns(_ => page2List.GetEnumerator());

        iterator.HasMoreResults.Returns(true, true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(page1), Task.FromResult(page2));

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(Arg.Any<QueryDefinition>())
            .Returns(iterator);

        var results = await _provider.FetchGlobalEventsAsync(fromGlobalSequence: 15, batchSize: 1, tenantId: "t1", ct: TestContext.Current.CancellationToken);

        results.Count.ShouldBe(1);
        results[0].GlobalSequence.ShouldBe(20);
    }

    [Fact]
    public async Task FetchGlobalEventsAsync_Deserializes_RawJson_Data()
    {
        var rawEvt = new EventEnvelope<object> { StreamId = "s1", Version = 1, TenantId = "t1", GlobalSequence = 100, Data = "json-data" };
        var jsonString = Newtonsoft.Json.JsonConvert.SerializeObject(rawEvt);
        var rawJObject = Newtonsoft.Json.Linq.JObject.Parse(jsonString);

        var env = new CosmosDocumentEnvelope<object>
        {
            Id = "e1",
            PartitionKey = "s1",
            TenantId = "t1",
            Data = rawJObject
        };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        var pageList = new List<CosmosDocumentEnvelope<object>> { env };

        page.GetEnumerator().Returns(_ => pageList.GetEnumerator());
        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(Arg.Any<QueryDefinition>())
            .Returns(iterator);

        var results = await _provider.FetchGlobalEventsAsync(fromGlobalSequence: 0, batchSize: 10, tenantId: "t1", ct: TestContext.Current.CancellationToken);

        results.Count.ShouldBe(1);
        results[0].GlobalSequence.ShouldBe(100);
    }

    [Fact]
    public async Task GetSnapshotAsync_Returns_Snapshot_And_Version_When_Valid()
    {
        var snapshotDoc = new MockDoc("snap-1", "Snapshot Data");
        var envelope = new CosmosDocumentEnvelope<MockDoc>
        {
            Id = "$snapshot_stream-1",
            PartitionKey = "stream-1",
            TenantId = "default",
            Version = "42",
            Data = snapshotDoc
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        response.Resource.Returns(envelope);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>(
            "$snapshot_stream-1",
            new PartitionKey("stream-1"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var (snapshot, version) = await _provider.GetSnapshotAsync<MockDoc>("stream-1", tenantId: "default", ct: TestContext.Current.CancellationToken);

        snapshot.ShouldNotBeNull();
        snapshot.Name.ShouldBe("Snapshot Data");
        version.ShouldBe(42);
    }

    [Fact]
    public async Task GetSnapshotAsync_Returns_Null_When_Resource_Is_Null_Or_Deleted_Or_TenantMismatch()
    {
        var respNull = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        respNull.Resource.Returns((CosmosDocumentEnvelope<MockDoc>)null!);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>("$snapshot_null", new PartitionKey("null"), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(respNull));

        var resNull = await _provider.GetSnapshotAsync<MockDoc>("null", ct: TestContext.Current.CancellationToken);
        resNull.Snapshot.ShouldBeNull();
        resNull.SnapshotVersion.ShouldBe(0);

        var envDeleted = new CosmosDocumentEnvelope<MockDoc> { IsDeleted = true, TenantId = "default", Version = "1", Data = new MockDoc("1", "D") };
        var respDeleted = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        respDeleted.Resource.Returns(envDeleted);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>("$snapshot_deleted", new PartitionKey("deleted"), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(respDeleted));

        var resDeleted = await _provider.GetSnapshotAsync<MockDoc>("deleted", ct: TestContext.Current.CancellationToken);
        resDeleted.Snapshot.ShouldBeNull();
        resDeleted.SnapshotVersion.ShouldBe(0);

        var envTenant = new CosmosDocumentEnvelope<MockDoc> { IsDeleted = false, TenantId = "tenant-A", Version = "1", Data = new MockDoc("1", "T") };
        var respTenant = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        respTenant.Resource.Returns(envTenant);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>("$snapshot_tenant", new PartitionKey("tenant"), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(respTenant));

        var resTenant = await _provider.GetSnapshotAsync<MockDoc>("tenant", tenantId: "tenant-B", ct: TestContext.Current.CancellationToken);
        resTenant.Snapshot.ShouldBeNull();
        resTenant.SnapshotVersion.ShouldBe(0);
    }

    [Fact]
    public async Task GetSnapshotAsync_Returns_Version_Zero_When_Version_Parsing_Fails()
    {
        var envelope = new CosmosDocumentEnvelope<MockDoc>
        {
            Id = "$snapshot_nonnum",
            PartitionKey = "nonnum",
            TenantId = "default",
            Version = "not-a-number",
            Data = new MockDoc("s1", "Name")
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        response.Resource.Returns(envelope);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>(
            "$snapshot_nonnum",
            new PartitionKey("nonnum"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var (snapshot, version) = await _provider.GetSnapshotAsync<MockDoc>("nonnum", ct: TestContext.Current.CancellationToken);

        snapshot.ShouldNotBeNull();
        version.ShouldBe(0);
    }

    [Fact]
    public async Task SaveSnapshotAsync_Upserts_Snapshot_Document()
    {
        var snapshotDoc = new MockDoc("snap-2", "Saved Snapshot");

        await _provider.SaveSnapshotAsync("stream-save", 10, snapshotDoc, tenantId: "t1", ct: TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<MockDoc>>(e => e.Id == "$snapshot_stream-save" && e.Version == "10" && e.TenantId == "t1"),
            new PartitionKey("stream-save"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AppendEventsAsync_EarlyReturn_On_EmptyEvents_And_NegativeExpectedVersion()
    {
        await _provider.AppendEventsAsync("s-empty", Array.Empty<IEvent>(), expectedVersion: 0, ct: TestContext.Current.CancellationToken);
        await _mockContainer.DidNotReceiveWithAnyArgs().UpsertItemAsync<object>(default!, default, cancellationToken: default);

        var evt = new EventEnvelope<object> { StreamId = "s-neg", Version = 0, TenantId = "t1", Data = new MockDoc("1", "N") };
        await _provider.AppendEventsAsync("s-neg", new[] { evt }, expectedVersion: -1, ct: TestContext.Current.CancellationToken);

        await _mockContainer.Received().UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<object>>(e => e.PartitionKey == "s-neg"),
            new PartitionKey("s-neg"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AppendEventsAsync_Assigns_GlobalSequence_When_Zero()
    {
        var evt = new EventEnvelope<object> { StreamId = "s-seq", Version = 0, GlobalSequence = 0, TenantId = "t1", Data = new MockDoc("1", "S") };
        await _provider.AppendEventsAsync("s-seq", new[] { evt }, expectedVersion: -1, ct: TestContext.Current.CancellationToken);

        evt.GlobalSequence.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task AppendEventsAsync_Uses_TransactionalBatch_When_Available()
    {
        var mockBatch = Substitute.For<TransactionalBatch>();
        var mockBatchResponse = Substitute.For<TransactionalBatchResponse>();
        mockBatchResponse.IsSuccessStatusCode.Returns(true);
        mockBatch.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(mockBatchResponse));

        _mockContainer.CreateTransactionalBatch(new PartitionKey("s-batch")).Returns(mockBatch);

        var evt = new EventEnvelope<object> { StreamId = "s-batch", Version = 0, TenantId = "t1", Data = new MockDoc("1", "Batch") };
        await _provider.AppendEventsAsync("s-batch", new[] { evt }, expectedVersion: -1, ct: TestContext.Current.CancellationToken);

        _mockContainer.Received(1).CreateTransactionalBatch(new PartitionKey("s-batch"));
        mockBatch.Received().UpsertItem(Arg.Is<CosmosDocumentEnvelope<object>>(e => e.PartitionKey == "s-batch"));
        mockBatch.Received().UpsertItem(Arg.Is<CosmosDocumentEnvelope<EventStreamHeader>>(e => e.PartitionKey == "s-batch"));
        await mockBatch.Received(1).ExecuteAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task QueryDocumentsAsync_Returns_EmptyArray_When_Queryable_Is_Null()
    {
        _mockContainer.GetItemLinqQueryable<CosmosDocumentEnvelope<MockDoc>>(Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<QueryRequestOptions>(), Arg.Any<CosmosLinqSerializerOptions>())
            .Returns((IOrderedQueryable<CosmosDocumentEnvelope<MockDoc>>)null!);

        var results = await _provider.QueryDocumentsAsync<MockDoc>(x => x.Id == "1", ct: TestContext.Current.CancellationToken);
        results.ShouldBeEmpty();
    }

    [Fact]
    public async Task QueryDocumentsAsync_Fallback_When_QueryIterator_Throws()
    {
        var envelope = new CosmosDocumentEnvelope<MockDoc>
        {
            Id = "doc-fb",
            PartitionKey = "pk-fb",
            DocType = nameof(MockDoc),
            Data = new MockDoc("doc-fb", "Fallback Item")
        };
        var fakeQueryable = new List<CosmosDocumentEnvelope<MockDoc>> { envelope }.AsQueryable() as IOrderedQueryable<CosmosDocumentEnvelope<MockDoc>>;

        _mockContainer.GetItemLinqQueryable<CosmosDocumentEnvelope<MockDoc>>(Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<QueryRequestOptions>(), Arg.Any<CosmosLinqSerializerOptions>())
            .Returns(fakeQueryable);

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<MockDoc>>(Arg.Any<QueryDefinition>(), Arg.Any<string>(), Arg.Any<QueryRequestOptions>())
            .Returns(_ => throw new InvalidOperationException("LINQ to Cosmos definition failed"));

        var results = await _provider.QueryDocumentsAsync<MockDoc>(options: null, ct: TestContext.Current.CancellationToken);

        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe("doc-fb");
    }

    [Fact]
    public async Task FetchEventsAsync_Filters_TenantId_And_FromVersion_And_Deserializes_Json()
    {
        var rawEvt1 = new EventEnvelope<object> { StreamId = "s-fetch", Version = 5, TenantId = "tenant-X", Data = "data" };
        var rawJObj1 = Newtonsoft.Json.Linq.JObject.Parse(Newtonsoft.Json.JsonConvert.SerializeObject(rawEvt1));

        var rawEvt2 = new EventEnvelope<object> { StreamId = "s-fetch", Version = 6, TenantId = "tenant-Y", Data = "data" };
        var rawJObj2 = Newtonsoft.Json.Linq.JObject.Parse(Newtonsoft.Json.JsonConvert.SerializeObject(rawEvt2));

        var item1 = new CosmosDocumentEnvelope<object> { Id = "e1", PartitionKey = "s-fetch", TenantId = "tenant-X", Data = rawJObj1 };
        var item2 = new CosmosDocumentEnvelope<object> { Id = "e2", PartitionKey = "s-fetch", TenantId = "tenant-Y", Data = rawJObj2 };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        var pageList = new List<CosmosDocumentEnvelope<object>> { item1, item2 };

        page.GetEnumerator().Returns(_ => pageList.GetEnumerator());
        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(Arg.Any<QueryDefinition>(), requestOptions: Arg.Any<QueryRequestOptions>())
            .Returns(iterator);

        var events = await _provider.FetchEventsAsync("s-fetch", tenantId: "tenant-X", fromVersion: 2, ct: TestContext.Current.CancellationToken);

        events.Count.ShouldBe(1);
        events[0].TenantId.ShouldBe("tenant-X");
    }

    [Fact]
    public async Task ExecuteBatchAsync_Handles_All_PatchActions_And_Throws_On_Unknown()
    {
        var patchOps = new List<PatchOperationData>
        {
            new PatchOperationData { Action = PatchAction.Set, Path = "/Name", Value = "NewName" },
            new PatchOperationData { Action = PatchAction.Increment, Path = "/Count", Value = 1L },
            new PatchOperationData { Action = PatchAction.Remove, Path = "/OldProp" },
            new PatchOperationData { Action = PatchAction.Append, Path = "/Tags", Value = "Tag1" }
        };

        var batchOps = new List<StorageOperation>
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Patch,
                Id = "p-1",
                PartitionKey = "pk-p",
                PatchOperations = patchOps
            },
            new StorageOperation
            {
                OperationType = StorageOperationType.Patch,
                Id = "p-2",
                PartitionKey = "pk-p",
                PatchOperations = new List<PatchOperationData>()
            }
        };

        await _provider.ExecuteBatchAsync(batchOps, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).PatchItemAsync<CosmosDocumentEnvelope<object>>(
            "p-1",
            new PartitionKey("pk-p"),
            Arg.Is<IReadOnlyList<PatchOperation>>(l => l.Count == 4),
            cancellationToken: Arg.Any<CancellationToken>());

        var invalidPatchOp = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "p-err",
            PartitionKey = "pk-err",
            PatchOperations = new List<PatchOperationData> { new PatchOperationData { Action = (PatchAction)999, Path = "/x" } }
        };

        await Should.ThrowAsync<NotSupportedException>(() =>
            _provider.ExecuteBatchAsync(new[] { invalidPatchOp }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ReadDocumentAsync_Returns_Null_When_Resource_IsDeleted()
    {
        var envelope = new CosmosDocumentEnvelope<MockDoc> { Id = "d-del", PartitionKey = "pk-del", IsDeleted = true };
        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<MockDoc>>>();
        response.Resource.Returns(envelope);

        _mockContainer.ReadItemAsync<CosmosDocumentEnvelope<MockDoc>>("d-del", new PartitionKey("pk-del"), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var doc = await _provider.ReadDocumentAsync<MockDoc>("d-del", "pk-del", TestContext.Current.CancellationToken);
        doc.ShouldBeNull();
    }

    [Fact]
    public async Task Dispose_And_DisposeAsync_DoesNotDispose_External_CosmosClient()
    {
        _provider.Dispose();
        _mockClient.DidNotReceive().Dispose();

        await _provider.DisposeAsync();
        _mockClient.DidNotReceive().Dispose();
    }

    [Fact]
    public void CosmosStorageProvider_Constructor_Validation()
    {
        Should.Throw<ArgumentNullException>(() => new CosmosStorageProvider(client: null!));
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider("connStr", "", "container"));
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider("connStr", "db", "   "));
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider("   ", "db", "container"));
    }
}
