using Microsoft.Azure.Cosmos;
using NSubstitute;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed class CosmosPatchTests
{
    private readonly Container _mockContainer;
    private readonly CosmosClient _mockClient;
    private readonly CosmosStorageProvider _provider;

    public CosmosPatchTests()
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
    public async Task ExecuteBatchAsync_PatchOperation_Set_MapsToReplace()
    {
        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "doc-1",
            PartitionKey = "pk-1",
            PatchOperations = new List<PatchOperationData>
            {
                new PatchOperationData { Path = "/Data/Status", Action = PatchAction.Set, Value = "Active" }
            }
        };

        await _provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).PatchItemAsync<CosmosDocumentEnvelope<object>>(
            "doc-1",
            new PartitionKey("pk-1"),
            Arg.Is<IReadOnlyList<PatchOperation>>(patchOps =>
                patchOps.Count == 1 &&
                patchOps[0].OperationType == PatchOperationType.Replace &&
                patchOps[0].Path == "/Data/Status"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteBatchAsync_PatchOperation_Increment_MapsToIncrement()
    {
        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "doc-2",
            PartitionKey = "pk-2",
            PatchOperations = new List<PatchOperationData>
            {
                new PatchOperationData { Path = "/Data/Count", Action = PatchAction.Increment, Value = 5 }
            }
        };

        await _provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).PatchItemAsync<CosmosDocumentEnvelope<object>>(
            "doc-2",
            new PartitionKey("pk-2"),
            Arg.Is<IReadOnlyList<PatchOperation>>(patchOps =>
                patchOps.Count == 1 &&
                patchOps[0].OperationType == PatchOperationType.Increment &&
                patchOps[0].Path == "/Data/Count"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteBatchAsync_PatchOperation_Remove_MapsToRemove()
    {
        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "doc-3",
            PartitionKey = "pk-3",
            PatchOperations = new List<PatchOperationData>
            {
                new PatchOperationData { Path = "/Data/OldField", Action = PatchAction.Remove }
            }
        };

        await _provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).PatchItemAsync<CosmosDocumentEnvelope<object>>(
            "doc-3",
            new PartitionKey("pk-3"),
            Arg.Is<IReadOnlyList<PatchOperation>>(patchOps =>
                patchOps.Count == 1 &&
                patchOps[0].OperationType == PatchOperationType.Remove &&
                patchOps[0].Path == "/Data/OldField"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteBatchAsync_PatchOperation_Append_MapsToAddWithSlashDash()
    {
        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "doc-4",
            PartitionKey = "pk-4",
            PatchOperations = new List<PatchOperationData>
            {
                new PatchOperationData { Path = "/Data/Tags", Action = PatchAction.Append, Value = "item1" }
            }
        };

        await _provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).PatchItemAsync<CosmosDocumentEnvelope<object>>(
            "doc-4",
            new PartitionKey("pk-4"),
            Arg.Is<IReadOnlyList<PatchOperation>>(patchOps =>
                patchOps.Count == 1 &&
                patchOps[0].OperationType == PatchOperationType.Add &&
                patchOps[0].Path == "/Data/Tags/-"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteBatchAsync_PatchOperation_MultipleOperations_DispatchesAll()
    {
        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "doc-5",
            PartitionKey = "pk-5",
            PatchOperations = new List<PatchOperationData>
            {
                new PatchOperationData { Path = "/Data/Status", Action = PatchAction.Set, Value = "Active" },
                new PatchOperationData { Path = "/Data/Count", Action = PatchAction.Increment, Value = 10 },
                new PatchOperationData { Path = "/Data/Unwanted", Action = PatchAction.Remove },
                new PatchOperationData { Path = "/Data/Items", Action = PatchAction.Append, Value = "newItem" }
            }
        };

        await _provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);

        await _mockContainer.Received(1).PatchItemAsync<CosmosDocumentEnvelope<object>>(
            "doc-5",
            new PartitionKey("pk-5"),
            Arg.Is<IReadOnlyList<PatchOperation>>(patchOps =>
                patchOps.Count == 4 &&
                patchOps[0].OperationType == PatchOperationType.Replace && patchOps[0].Path == "/Data/Status" &&
                patchOps[1].OperationType == PatchOperationType.Increment && patchOps[1].Path == "/Data/Count" &&
                patchOps[2].OperationType == PatchOperationType.Remove && patchOps[2].Path == "/Data/Unwanted" &&
                patchOps[3].OperationType == PatchOperationType.Add && patchOps[3].Path == "/Data/Items/-"),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteBatchAsync_PatchOperation_EmptyOperationsList_DoesNotCallPatchItemAsync()
    {
        var op = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "doc-6",
            PartitionKey = "pk-6",
            PatchOperations = new List<PatchOperationData>()
        };

        await _provider.ExecuteBatchAsync(new[] { op }, TestContext.Current.CancellationToken);

        await _mockContainer.DidNotReceiveWithAnyArgs().PatchItemAsync<CosmosDocumentEnvelope<object>>(
            default!, default, default, cancellationToken: TestContext.Current.CancellationToken);
    }
}
