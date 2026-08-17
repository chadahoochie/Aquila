using System.Net;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests.Storage;

public sealed record TestEventPayload(string OrderId, decimal Amount);
public sealed class TestAggregateState
{
    public string Id { get; set; } = string.Empty;
    public decimal Balance { get; set; }
}

public sealed class CosmosEventStorageProviderTests
{
    private readonly Container _mockEventContainer;
    private readonly Container _mockSnapshotContainer;
    private readonly CosmosEventStorageProvider _provider;

    public CosmosEventStorageProviderTests()
    {
        _mockEventContainer = Substitute.For<Container>();
        _mockSnapshotContainer = Substitute.For<Container>();
        _provider = new CosmosEventStorageProvider(() => _mockEventContainer, () => _mockSnapshotContainer);
    }

    // ==========================================
    // 1. Constructor & Lifecycle Tests
    // ==========================================

    [Fact]
    public void Constructors_NullArguments_ThrowArgumentNullException()
    {
        Func<Container> nullProvider = null!;
        Container nullContainer = null!;

        Should.Throw<ArgumentNullException>(() => new CosmosEventStorageProvider(nullProvider));
        Should.Throw<ArgumentNullException>(() => new CosmosEventStorageProvider(nullContainer));
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
    // 2. AppendEventsAsync Tests
    // ==========================================

    [Fact]
    public async Task AppendEventsAsync_InvalidArguments_ThrowException()
    {
        await Should.ThrowAsync<ArgumentException>(() => _provider.AppendEventsAsync("", new List<IEvent>(), 0, TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.AppendEventsAsync("   ", new List<IEvent>(), 0, TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentNullException>(() => _provider.AppendEventsAsync("stream-1", null!, 0, TestContext.Current.CancellationToken));

        // Empty list returns immediately without throwing
        await _provider.AppendEventsAsync("stream-1", Enumerable.Empty<IEvent>(), 0, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AppendEventsAsync_Throws_AquilaConcurrencyException_OnVersionMismatch()
    {
        var streamId = "stream-conc-test";
        var header = new EventStreamHeader { StreamId = streamId, Version = 10, TenantId = "default" };
        var headerEnvelope = new CosmosDocumentEnvelope<EventStreamHeader>
        {
            Id = $"$stream_{streamId}",
            PartitionKey = streamId,
            Data = header
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>();
        response.Resource.Returns(headerEnvelope);

        _mockEventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var evt = new EventEnvelope<TestEventPayload>
        {
            StreamId = streamId,
            Data = new TestEventPayload("ord-1", 50m)
        };

        // Expected version is 5, but current version in header is 10
        var ex = await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            _provider.AppendEventsAsync(streamId, new[] { evt }, expectedVersion: 5, ct: TestContext.Current.CancellationToken));

        ex.DocumentId.ShouldBe(streamId);
        ex.ExpectedVersion.ShouldBe("5");
        ex.ActualVersion.ShouldBe("10");
    }

    [Fact]
    public async Task AppendEventsAsync_ExecutesTransactionalBatch_Success()
    {
        var streamId = "stream-batch-ok";
        var notFoundEx = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "act-1", 0);

        _mockEventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>(notFoundEx));

        var batch = Substitute.For<TransactionalBatch>();
        var batchResponse = Substitute.For<TransactionalBatchResponse>();
        batchResponse.IsSuccessStatusCode.Returns(true);
        batchResponse.RequestCharge.Returns(8.5);

        batch.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(batchResponse));
        _mockEventContainer.CreateTransactionalBatch(Arg.Any<PartitionKey>()).Returns(batch);

        var evt1 = new EventEnvelope<TestEventPayload> { StreamId = streamId, Data = new TestEventPayload("ord-1", 10m) };
        var evt2 = new EventEnvelope<TestEventPayload> { StreamId = streamId, Data = new TestEventPayload("ord-2", 20m) };

        await _provider.AppendEventsAsync(streamId, new[] { evt1, evt2 }, expectedVersion: 0, ct: TestContext.Current.CancellationToken);

        evt1.Version.ShouldBe(1);
        evt2.Version.ShouldBe(2);
        evt1.GlobalSequence.ShouldBeGreaterThan(0);
        evt2.GlobalSequence.ShouldBeGreaterThan(evt1.GlobalSequence);

        batch.Received(2).UpsertItem(Arg.Is<CosmosDocumentEnvelope<object>>(e => e.DocType == "$event"));
        batch.Received(1).UpsertItem(Arg.Is<CosmosDocumentEnvelope<EventStreamHeader>>(e => e.DocType == "$stream_header"));
        _provider.LastRequestCharge.ShouldBe(8.5);
    }

    [Fact]
    public async Task AppendEventsAsync_TransactionalBatch_ConcurrencyException_OnConflict()
    {
        var streamId = "stream-conflict";
        var notFoundEx = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "act-1", 0);

        _mockEventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>(notFoundEx));

        var batch = Substitute.For<TransactionalBatch>();
        var batchResponse = Substitute.For<TransactionalBatchResponse>();
        batchResponse.IsSuccessStatusCode.Returns(false);
        batchResponse.StatusCode.Returns(HttpStatusCode.Conflict);

        batch.ExecuteAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(batchResponse));
        _mockEventContainer.CreateTransactionalBatch(Arg.Any<PartitionKey>()).Returns(batch);

        var evt = new EventEnvelope<TestEventPayload> { StreamId = streamId, Data = new TestEventPayload("ord-1", 10m) };

        await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            _provider.AppendEventsAsync(streamId, new[] { evt }, expectedVersion: 0, ct: TestContext.Current.CancellationToken));
    }

    // ==========================================
    // 3. FetchEventsAsync Tests
    // ==========================================

    [Fact]
    public async Task FetchEventsAsync_InvalidArguments_ThrowArgumentException()
    {
        await Should.ThrowAsync<ArgumentException>(() => _provider.FetchEventsAsync("", ct: TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.FetchEventsAsync("   ", ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task FetchEventsAsync_QueriesAndDeserializesEvents()
    {
        var streamId = "stream-fetch-1";
        var evt = new EventEnvelope<TestEventPayload>
        {
            StreamId = streamId,
            Version = 1,
            TenantId = "tenant-fetch",
            GlobalSequence = 100,
            Data = new TestEventPayload("ord-fetch", 99.99m)
        };

        var docEnvelope = new CosmosDocumentEnvelope<object>
        {
            Id = $"$event_{streamId}_v1",
            PartitionKey = streamId,
            DocType = "$event",
            TenantId = "tenant-fetch",
            Data = evt
        };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        page.GetEnumerator().Returns(new List<CosmosDocumentEnvelope<object>> { docEnvelope }.GetEnumerator());
        page.RequestCharge.Returns(3.5);

        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockEventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            Arg.Any<QueryDefinition>(),
            requestOptions: Arg.Any<QueryRequestOptions>())
            .Returns(iterator);

        var events = await _provider.FetchEventsAsync(streamId, tenantId: "tenant-fetch", fromVersion: 1, ct: TestContext.Current.CancellationToken);

        events.Count.ShouldBe(1);
        events[0].StreamId.ShouldBe(streamId);
        events[0].Version.ShouldBe(1);
        _provider.LastRequestCharge.ShouldBe(3.5);
    }

    // ==========================================
    // 4. FetchGlobalEventsAsync Tests
    // ==========================================

    [Fact]
    public async Task FetchGlobalEventsAsync_BatchSizeZeroOrNegative_ReturnsEmpty()
    {
        var resultZero = await _provider.FetchGlobalEventsAsync(0, batchSize: 0, ct: TestContext.Current.CancellationToken);
        resultZero.ShouldBeEmpty();

        var resultNeg = await _provider.FetchGlobalEventsAsync(0, batchSize: -5, ct: TestContext.Current.CancellationToken);
        resultNeg.ShouldBeEmpty();
    }

    [Fact]
    public async Task FetchGlobalEventsAsync_QueriesGlobalEvents_WithPagination()
    {
        var evt1 = new EventEnvelope<TestEventPayload> { StreamId = "s1", Version = 1, TenantId = "t1", GlobalSequence = 10, Data = new TestEventPayload("o1", 10m) };
        var evt2 = new EventEnvelope<TestEventPayload> { StreamId = "s2", Version = 1, TenantId = "t1", GlobalSequence = 20, Data = new TestEventPayload("o2", 20m) };

        var env1 = new CosmosDocumentEnvelope<object> { Id = "e1", PartitionKey = "s1", TenantId = "t1", Data = evt1 };
        var env2 = new CosmosDocumentEnvelope<object> { Id = "e2", PartitionKey = "s2", TenantId = "t1", Data = evt2 };

        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        page.GetEnumerator().Returns(new List<CosmosDocumentEnvelope<object>> { env1, env2 }.GetEnumerator());
        page.RequestCharge.Returns(4.0);

        iterator.HasMoreResults.Returns(true, false);
        iterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(page));

        _mockEventContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            Arg.Any<QueryDefinition>(),
            requestOptions: Arg.Any<QueryRequestOptions>())
            .Returns(iterator);

        var events = await _provider.FetchGlobalEventsAsync(fromGlobalSequence: 5, batchSize: 10, tenantId: "t1", ct: TestContext.Current.CancellationToken);

        events.Count.ShouldBe(2);
        events[0].GlobalSequence.ShouldBe(10);
        events[1].GlobalSequence.ShouldBe(20);
        _provider.LastRequestCharge.ShouldBe(4.0);
    }

    // ==========================================
    // 5. GetStreamHeaderAsync Tests
    // ==========================================

    [Fact]
    public async Task GetStreamHeaderAsync_InvalidArguments_ThrowArgumentException()
    {
        await Should.ThrowAsync<ArgumentException>(() => _provider.GetStreamHeaderAsync("", ct: TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.GetStreamHeaderAsync("   ", ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task GetStreamHeaderAsync_ReturnsHeader_WhenFound()
    {
        var streamId = "stream-header-1";
        var expectedHeader = new EventStreamHeader
        {
            StreamId = streamId,
            Version = 4,
            TenantId = "tenant-h",
            CreatedAt = DateTimeOffset.UtcNow
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>();
        response.Resource.Returns(new CosmosDocumentEnvelope<EventStreamHeader> { TenantId = "tenant-h", Data = expectedHeader });
        response.RequestCharge.Returns(1.0);

        _mockEventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var header = await _provider.GetStreamHeaderAsync(streamId, tenantId: "tenant-h", ct: TestContext.Current.CancellationToken);

        header.ShouldNotBeNull();
        header.StreamId.ShouldBe(streamId);
        header.Version.ShouldBe(4);
        _provider.LastRequestCharge.ShouldBe(1.0);
    }

    [Fact]
    public async Task GetStreamHeaderAsync_ReturnsNull_WhenTenantMismatchOrNotFound()
    {
        var streamId = "stream-header-mismatch";
        var header = new EventStreamHeader { StreamId = streamId, Version = 1, TenantId = "tenant-A" };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>();
        response.Resource.Returns(new CosmosDocumentEnvelope<EventStreamHeader> { TenantId = "tenant-A", Data = header });

        _mockEventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        // Requested tenant-B, but actual is tenant-A
        var resultMismatch = await _provider.GetStreamHeaderAsync(streamId, tenantId: "tenant-B", ct: TestContext.Current.CancellationToken);
        resultMismatch.ShouldBeNull();

        var notFoundEx = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "act-1", 0);
        _mockEventContainer.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_missing",
            CosmosPartitionKeyHelper.CreatePartitionKey("missing"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>(notFoundEx));

        var resultMissing = await _provider.GetStreamHeaderAsync("missing", ct: TestContext.Current.CancellationToken);
        resultMissing.ShouldBeNull();
    }

    // ==========================================
    // 6. Snapshot Operations Tests
    // ==========================================

    [Fact]
    public async Task SaveSnapshotAsync_And_GetSnapshotAsync_InvalidArguments_ThrowException()
    {
        await Should.ThrowAsync<ArgumentException>(() => _provider.SaveSnapshotAsync<TestAggregateState>("", 1, new TestAggregateState(), ct: TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentNullException>(() => _provider.SaveSnapshotAsync<TestAggregateState>("s1", 1, null!, ct: TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => _provider.GetSnapshotAsync<TestAggregateState>("", ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SaveSnapshotAsync_UpsertsSnapshot_ToSnapshotContainer()
    {
        var streamId = "stream-snap-1";
        var state = new TestAggregateState { Id = streamId, Balance = 250m };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<TestAggregateState>>>();
        response.RequestCharge.Returns(5.0);

        _mockSnapshotContainer.UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<TestAggregateState>>(e => e.Id == $"$snapshot_{streamId}" && e.Data.Balance == 250m),
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        await _provider.SaveSnapshotAsync(streamId, version: 10, snapshot: state, tenantId: "tenant-snap", ct: TestContext.Current.CancellationToken);

        _provider.LastRequestCharge.ShouldBe(5.0);
    }

    [Fact]
    public async Task GetSnapshotAsync_ReturnsSnapshot_WhenFound()
    {
        var streamId = "stream-snap-2";
        var state = new TestAggregateState { Id = streamId, Balance = 500m };
        var envelope = new CosmosDocumentEnvelope<TestAggregateState>
        {
            Id = $"$snapshot_{streamId}",
            PartitionKey = streamId,
            TenantId = "tenant-snap",
            Version = "25",
            Data = state
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<TestAggregateState>>>();
        response.Resource.Returns(envelope);
        response.RequestCharge.Returns(1.5);

        _mockSnapshotContainer.ReadItemAsync<CosmosDocumentEnvelope<TestAggregateState>>(
            $"$snapshot_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var (snapshot, version) = await _provider.GetSnapshotAsync<TestAggregateState>(streamId, tenantId: "tenant-snap", ct: TestContext.Current.CancellationToken);

        snapshot.ShouldNotBeNull();
        snapshot.Balance.ShouldBe(500m);
        version.ShouldBe(25);
        _provider.LastRequestCharge.ShouldBe(1.5);
    }

    [Fact]
    public async Task GetSnapshotAsync_ReturnsDefault_WhenNotFoundOrTenantMismatch()
    {
        var streamId = "stream-snap-3";
        var state = new TestAggregateState { Id = streamId, Balance = 100m };
        var envelope = new CosmosDocumentEnvelope<TestAggregateState>
        {
            Id = $"$snapshot_{streamId}",
            PartitionKey = streamId,
            TenantId = "tenant-A",
            Version = "5",
            Data = state
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<TestAggregateState>>>();
        response.Resource.Returns(envelope);

        _mockSnapshotContainer.ReadItemAsync<CosmosDocumentEnvelope<TestAggregateState>>(
            $"$snapshot_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        // Tenant mismatch (requested tenant-B, but snapshot is tenant-A)
        var (snapshotMismatch, versionMismatch) = await _provider.GetSnapshotAsync<TestAggregateState>(streamId, tenantId: "tenant-B", ct: TestContext.Current.CancellationToken);
        snapshotMismatch.ShouldBeNull();
        versionMismatch.ShouldBe(0);

        var notFoundEx = new CosmosException("Not Found", HttpStatusCode.NotFound, 0, "act-1", 0);
        _mockSnapshotContainer.ReadItemAsync<CosmosDocumentEnvelope<TestAggregateState>>(
            "$snapshot_missing",
            CosmosPartitionKeyHelper.CreatePartitionKey("missing"),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ItemResponse<CosmosDocumentEnvelope<TestAggregateState>>>(notFoundEx));

        var (snapshotMissing, versionMissing) = await _provider.GetSnapshotAsync<TestAggregateState>("missing", ct: TestContext.Current.CancellationToken);
        snapshotMissing.ShouldBeNull();
        versionMissing.ShouldBe(0);
    }
}
