using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Xunit;
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

        var result = await _provider.Documents.ReadDocumentAsync<MockDoc>("doc-1", "pk-1", TestContext.Current.CancellationToken);

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

        var result = await _provider.Documents.ReadDocumentAsync<MockDoc>("missing-id", "pk-1", TestContext.Current.CancellationToken);
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

        await _provider.Documents.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<MockDoc>>(e => e.Id == "doc-10" && e.PartitionKey == "pk-10"),
            new PartitionKey("pk-10"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DeleteDocumentAsync_Calls_Container_DeleteItemAsync()
    {
        await _provider.Documents.DeleteDocumentAsync<MockDoc>("doc-99", "pk-99", TestContext.Current.CancellationToken);

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

        await _provider.Documents.ExecuteBatchAsync(ops, TestContext.Current.CancellationToken);

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
            _provider.Events.AppendEventsAsync(streamId, new[] { evt }, expectedVersion: 1, ct: TestContext.Current.CancellationToken));
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

        var header = await _provider.Events.GetStreamHeaderAsync(streamId, ct: TestContext.Current.CancellationToken);
        header.ShouldBeNull();
    }

    [Fact]
    public async Task FetchGlobalEventsAsync_Returns_EmptyList_When_BatchSize_Is_Zero_Or_Negative()
    {
        var resultZero = await _provider.Events.FetchGlobalEventsAsync(0, batchSize: 0, ct: TestContext.Current.CancellationToken);
        resultZero.ShouldBeEmpty();

        var resultNeg = await _provider.Events.FetchGlobalEventsAsync(0, batchSize: -10, ct: TestContext.Current.CancellationToken);
        resultNeg.ShouldBeEmpty();
    }

    [Fact]
    public void StorageProvider_InputValidation_ThrowsExceptions()
    {
        Should.ThrowAsync<ArgumentException>(() => _provider.Documents.ReadDocumentAsync<MockDoc>("", "pk"));
        Should.ThrowAsync<ArgumentException>(() => _provider.Documents.ReadDocumentAsync<MockDoc>("id", "   "));
        Should.ThrowAsync<ArgumentNullException>(() => _provider.Documents.UpsertDocumentAsync<MockDoc>(null!));
        Should.ThrowAsync<ArgumentException>(() => _provider.Documents.DeleteDocumentAsync<MockDoc>("", "pk"));
        Should.ThrowAsync<ArgumentException>(() => _provider.Events.AppendEventsAsync("", new List<IEvent>(), 0));
        Should.ThrowAsync<ArgumentNullException>(() => _provider.Events.AppendEventsAsync("s1", null!, 0));
        Should.ThrowAsync<ArgumentException>(() => _provider.Events.FetchEventsAsync(""));
        Should.ThrowAsync<ArgumentException>(() => _provider.Events.GetStreamHeaderAsync(""));
    }
}
