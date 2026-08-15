using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Storage;
using Aquila.Cosmos.Configuration;
using Aquila.Cosmos.Projections;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests.Projections;

public sealed record OptimOrderPlaced(string OrderId, string CustomerId, decimal Amount);

public sealed class OptimCustomerSummary
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
}

public sealed class OptimAsyncMultiStreamProjection : MultiStreamProjection<OptimCustomerSummary, string>
{
    public OptimAsyncMultiStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event) => @event.Data is OptimOrderPlaced e ? e.CustomerId : null!;

    public override bool Apply(IEvent @event, OptimCustomerSummary document)
    {
        if (@event.Data is OptimOrderPlaced e)
        {
            document.CustomerId = e.CustomerId;
            document.TotalAmount += e.Amount;
            return true;
        }
        return true;
    }
}

public sealed class CosmosProjectionThroughputOptimizationTests
{
    [Fact]
    public async Task FetchGlobalEventsAsync_Issues_ServerSide_Filtered_Sorted_Query_Without_Tenant()
    {
        var mockContainer = Substitute.For<Container>();
        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        var pageList = new List<CosmosDocumentEnvelope<object>>();

        page.GetEnumerator().Returns(_ => pageList.GetEnumerator());
        iterator.HasMoreResults.Returns(false);

        QueryDefinition? capturedQueryDef = null;
        QueryRequestOptions? capturedOptions = null;

        mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            Arg.Do<QueryDefinition>(q => capturedQueryDef = q),
            Arg.Any<string>(),
            Arg.Do<QueryRequestOptions>(o => capturedOptions = o))
            .Returns(iterator);

        var provider = new CosmosEventStorageProvider(() => mockContainer, () => mockContainer);

        await provider.FetchGlobalEventsAsync(fromGlobalSequence: 100, batchSize: 500, tenantId: null, ct: TestContext.Current.CancellationToken);

        capturedQueryDef.ShouldNotBeNull();
        capturedQueryDef.QueryText.ShouldBe("SELECT * FROM c WHERE c._docType = '$event' AND c.data.GlobalSequence > @fromGlobalSequence ORDER BY c.data.GlobalSequence");

        var paramDict = capturedQueryDef.GetQueryParameters().ToDictionary(p => p.Name, p => p.Value);
        paramDict["@fromGlobalSequence"].ShouldBe(100L);
        paramDict.ContainsKey("@tenantId").ShouldBeFalse();

        capturedOptions.ShouldNotBeNull();
        capturedOptions.MaxItemCount.ShouldBe(500);
    }

    [Fact]
    public async Task FetchGlobalEventsAsync_Issues_ServerSide_Filtered_Sorted_Query_With_Tenant()
    {
        var mockContainer = Substitute.For<Container>();
        var iterator = Substitute.For<FeedIterator<CosmosDocumentEnvelope<object>>>();
        var page = Substitute.For<FeedResponse<CosmosDocumentEnvelope<object>>>();
        var pageList = new List<CosmosDocumentEnvelope<object>>();

        page.GetEnumerator().Returns(_ => pageList.GetEnumerator());
        iterator.HasMoreResults.Returns(false);

        QueryDefinition? capturedQueryDef = null;
        QueryRequestOptions? capturedOptions = null;

        mockContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            Arg.Do<QueryDefinition>(q => capturedQueryDef = q),
            Arg.Any<string>(),
            Arg.Do<QueryRequestOptions>(o => capturedOptions = o))
            .Returns(iterator);

        var provider = new CosmosEventStorageProvider(() => mockContainer, () => mockContainer);

        await provider.FetchGlobalEventsAsync(fromGlobalSequence: 250, batchSize: 200, tenantId: "tenant-abc", ct: TestContext.Current.CancellationToken);

        capturedQueryDef.ShouldNotBeNull();
        capturedQueryDef.QueryText.ShouldBe("SELECT * FROM c WHERE c._docType = '$event' AND c._tenantId = @tenantId AND c.data.GlobalSequence > @fromGlobalSequence ORDER BY c.data.GlobalSequence");

        var paramDict = capturedQueryDef.GetQueryParameters().ToDictionary(p => p.Name, p => p.Value);
        paramDict["@fromGlobalSequence"].ShouldBe(250L);
        paramDict["@tenantId"].ShouldBe("tenant-abc");

        capturedOptions.ShouldNotBeNull();
        capturedOptions.MaxItemCount.ShouldBe(200);
    }

    [Fact]
    public void StorageLocationOptions_ThroughputSettings_Manual_And_Autoscale_Builders()
    {
        var manualLoc = new StorageLocationOptions("Docs", "DB").WithManualThroughput(1200);
        manualLoc.Throughput.ShouldNotBeNull();
        manualLoc.Throughput.IsAutoscale.ShouldBeFalse();
        manualLoc.Throughput.ManualThroughput.ShouldBe(1200);
        manualLoc.Throughput.ToThroughputProperties()!.Throughput.ShouldBe(1200);

        var autoscaleLoc = new StorageLocationOptions("Docs", "DB").WithAutoscaleThroughput(4000);
        autoscaleLoc.Throughput.ShouldNotBeNull();
        autoscaleLoc.Throughput.IsAutoscale.ShouldBeTrue();
        autoscaleLoc.Throughput.AutoscaleMaxThroughput.ShouldBe(4000);
        autoscaleLoc.Throughput.ToThroughputProperties()!.AutoscaleMaxThroughput.ShouldBe(4000);
    }

    [Fact]
    public void ProjectionStorageOptions_Dedicated_And_AutoContainer_Throughput_Builders()
    {
        var projOptions = new ProjectionStorageOptions();

        projOptions.ToContainer("Projections", "CustomDB", ThroughputSettings.Manual(800));
        projOptions.Throughput.ShouldNotBeNull();
        projOptions.Throughput.IsAutoscale.ShouldBeFalse();
        projOptions.Throughput.ManualThroughput.ShouldBe(800);

        projOptions.ToContainer("ProjectionsAuto", "CustomDB", ThroughputSettings.Autoscale(6000));
        projOptions.Throughput.ShouldNotBeNull();
        projOptions.Throughput.IsAutoscale.ShouldBeTrue();
        projOptions.Throughput.AutoscaleMaxThroughput.ShouldBe(6000);

        projOptions.AutoContainerPerProjection("CustomDB", throughput: ThroughputSettings.Manual(1000));
        projOptions.Throughput.ShouldNotBeNull();
        projOptions.Throughput.ManualThroughput.ShouldBe(1000);

        projOptions.AutoContainerPerProjection("CustomDB", throughput: ThroughputSettings.Autoscale(8000));
        projOptions.Throughput.ShouldNotBeNull();
        projOptions.Throughput.AutoscaleMaxThroughput.ShouldBe(8000);

        projOptions.For<OptimAsyncMultiStreamProjection>("SpecialContainer", throughput: ThroughputSettings.Manual(2000));
        var mapping = projOptions.Overrides[typeof(OptimAsyncMultiStreamProjection)];
        mapping.Throughput.ShouldNotBeNull();
        mapping.Throughput.ManualThroughput.ShouldBe(2000);

        projOptions.For<OptimAsyncMultiStreamProjection>("SpecialContainerAuto", throughput: ThroughputSettings.Autoscale(10000));
        var autoMapping = projOptions.Overrides[typeof(OptimAsyncMultiStreamProjection)];
        autoMapping.Throughput.ShouldNotBeNull();
        autoMapping.Throughput.AutoscaleMaxThroughput.ShouldBe(10000);
    }

    [Fact]
    public void CosmosContainerResolver_GetAllConfiguredContainers_Resolves_Throughput()
    {
        var cosmosOptions = new CosmosStorageOptions { DefaultDatabase = "MainDB" };
        cosmosOptions.ConfigureEvents("Events", "MainDB");
        cosmosOptions.Events.WithManualThroughput(1500);

        cosmosOptions.ConfigureSnapshots("Snapshots", "MainDB");
        cosmosOptions.Snapshots.WithAutoscaleThroughput(4000);

        cosmosOptions.ConfigureDocuments("Docs", "MainDB");
        cosmosOptions.Documents.WithManualThroughput(2500);

        cosmosOptions.Projections.ToContainer("Projections", "MainDB", ThroughputSettings.Manual(3000));

        var mockClient = Substitute.For<CosmosClient>();
        var resolver = new CosmosContainerResolver(mockClient, cosmosOptions);

        var containers = resolver.GetAllConfiguredContainers();

        var eventsEntry = containers.Single(c => c.Container == "Events");
        eventsEntry.Throughput.ShouldNotBeNull();
        eventsEntry.Throughput.Throughput.ShouldBe(1500);

        var snapshotsEntry = containers.Single(c => c.Container == "Snapshots");
        snapshotsEntry.Throughput.ShouldNotBeNull();
        snapshotsEntry.Throughput.AutoscaleMaxThroughput.ShouldBe(4000);

        var docsEntry = containers.Single(c => c.Container == "Docs");
        docsEntry.Throughput.ShouldNotBeNull();
        docsEntry.Throughput.Throughput.ShouldBe(2500);

        var projEntry = containers.Single(c => c.Container == "Projections");
        projEntry.Throughput.ShouldNotBeNull();
        projEntry.Throughput.Throughput.ShouldBe(3000);
    }

    [Fact]
    public void CreateDefaultContainerProperties_Has_Expected_Indexing_And_Composite_Indexes()
    {
        var props = CosmosStorageProvider.CreateDefaultEventsContainerProperties("EventsContainer");

        props.IndexingPolicy.IncludedPaths.Any(p => p.Path == "/*").ShouldBeTrue();
        props.IndexingPolicy.ExcludedPaths.Any(p => p.Path == "/data/*").ShouldBeTrue();
        props.IndexingPolicy.IncludedPaths.Any(p => p.Path == "/_docType/?").ShouldBeTrue();
        props.IndexingPolicy.IncludedPaths.Any(p => p.Path == "/_tenantId/?").ShouldBeTrue();
        props.IndexingPolicy.IncludedPaths.Any(p => p.Path == "/data/GlobalSequence/?").ShouldBeTrue();
        props.IndexingPolicy.IncludedPaths.Any(p => p.Path == "/pk/?").ShouldBeTrue();
        props.IndexingPolicy.IncludedPaths.Any(p => p.Path == "/id/?").ShouldBeTrue();

        props.IndexingPolicy.CompositeIndexes.Count.ShouldBe(2);

        var composite1 = props.IndexingPolicy.CompositeIndexes[0];
        composite1[0].Path.ShouldBe("/_docType");
        composite1[1].Path.ShouldBe("/_tenantId");

        var composite2 = props.IndexingPolicy.CompositeIndexes[1];
        composite2[0].Path.ShouldBe("/_docType");
        composite2[1].Path.ShouldBe("/data/GlobalSequence");
    }

    [Fact]
    public void CreateDefaultClientOptions_Configures_DirectMode_And_BulkExecution()
    {
        var clientOptions = CosmosStorageProvider.CreateDefaultClientOptions();

        clientOptions.ConnectionMode.ShouldBe(ConnectionMode.Direct);
        clientOptions.AllowBulkExecution.ShouldBeTrue();
        clientOptions.MaxRetryAttemptsOnRateLimitedRequests.ShouldBe(9);
        clientOptions.MaxRetryWaitTimeOnRateLimitedRequests.ShouldBe(TimeSpan.FromSeconds(30));
        clientOptions.Serializer.ShouldBeOfType<AquilaCosmosJsonSerializer>();
    }

    [Fact]
    public async Task ExecuteBatchAsync_Groups_By_PartitionKey_And_Processes_Chunks()
    {
        var mockContainer = Substitute.For<Container>();

        var operations = new List<StorageOperation>();
        // Add 150 operations for pk1 (which should be split into 2 chunks: 100 and 50) and 20 operations for pk2
        for (int i = 0; i < 150; i++)
        {
            operations.Add(new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = $"doc1-{i}",
                PartitionKey = "pk-1",
                DocType = "MockDoc",
                Document = new OptimCustomerSummary { CustomerId = $"cust-{i}" }
            });
        }
        for (int i = 0; i < 20; i++)
        {
            operations.Add(new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = $"doc2-{i}",
                PartitionKey = "pk-2",
                DocType = "MockDoc",
                Document = new OptimCustomerSummary { CustomerId = $"cust2-{i}" }
            });
        }

        var provider = new CosmosDocumentStorageProvider(() => mockContainer);

        // Execute batch against mock container (fallback sequential path executes mock container UpsertItemAsync)
        await provider.ExecuteBatchAsync(operations, TestContext.Current.CancellationToken);

        // 170 total upserts should have executed
        await mockContainer.Received(170).UpsertItemAsync(
            Arg.Any<object>(),
            Arg.Any<PartitionKey>(),
            Arg.Any<ItemRequestOptions>(),
            Arg.Any<CancellationToken>());
    }
}
