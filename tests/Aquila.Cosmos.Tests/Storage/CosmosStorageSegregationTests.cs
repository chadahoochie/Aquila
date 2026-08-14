using System.Net;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Storage;
using Aquila.Cosmos.Configuration;
using Aquila.Cosmos.Extensions;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record SegregationItem(string Id, string Description);
public sealed record SegregationEvent(string OrderId, decimal Amount);
public sealed class SegregationOrderAggregate
{
    public string Id { get; set; } = string.Empty;
    public decimal Total { get; set; }

    public void Apply(SegregationEvent @event)
    {
        Id = @event.OrderId;
        Total += @event.Amount;
    }
}

public sealed class SegregationOrderProjection : SingleStreamProjection<SegregationOrderAggregate>
{
    public SegregationOrderProjection()
    {
        ProjectEvent<SegregationEvent>((e, agg) =>
        {
            agg.Id = e.OrderId;
            agg.Total += e.Amount;
        });
    }
}

public sealed class SegregationSummaryReadModel
{
    public string Id { get; set; } = string.Empty;
    public int Count { get; set; }
}

public sealed class SegregationSummaryProjection : MultiStreamProjection<SegregationSummaryReadModel, string>
{
    protected override string Identity(IEvent @event) => "summary";

    public override bool Apply(IEvent @event, SegregationSummaryReadModel document)
    {
        document.Id = "summary";
        document.Count++;
        return true;
    }
}

public sealed class CosmosStorageSegregationTests
{
    private readonly CosmosClient _mockClient;
    private readonly Container _eventsContainer;
    private readonly Container _snapshotsContainer;
    private readonly Container _documentsContainer;
    private readonly Container _projectionsContainer;
    private readonly Container _autoProjContainer;

    public CosmosStorageSegregationTests()
    {
        _mockClient = Substitute.For<CosmosClient>();
        _eventsContainer = Substitute.For<Container>();
        _snapshotsContainer = Substitute.For<Container>();
        _documentsContainer = Substitute.For<Container>();
        _projectionsContainer = Substitute.For<Container>();
        _autoProjContainer = Substitute.For<Container>();

        _mockClient.GetContainer("EventsDB", "EventStore").Returns(_eventsContainer);
        _mockClient.GetContainer("SnapshotsDB", "SnapshotStore").Returns(_snapshotsContainer);
        _mockClient.GetContainer("DocsDB", "DocumentStore").Returns(_documentsContainer);
        _mockClient.GetContainer("ReadDB", "ProjectionsStore").Returns(_projectionsContainer);
        _mockClient.GetContainer("ReadDB", "SegregationOrderProjection").Returns(_autoProjContainer);
        _mockClient.GetContainer("CustomDB", "CustomProjectionStore").Returns(_projectionsContainer);
    }

    [Fact]
    public void CosmosStorageOptions_DefaultConfiguration_HasSensibleDefaults()
    {
        var options = new CosmosStorageOptions();

        options.DefaultDatabase.ShouldBe("AquilaDB");
        options.Events.Container.ShouldBe("Events");
        options.Events.Database.ShouldBeNull();
        options.Snapshots.Container.ShouldBe("Snapshots");
        options.Snapshots.Database.ShouldBeNull();
        options.Documents.Container.ShouldBe("Documents");
        options.Documents.Database.ShouldBeNull();
        options.Projections.Mode.ShouldBe(ProjectionStorageMode.InheritDocuments);
    }

    [Fact]
    public void CosmosStorageOptions_FluentConfiguration_SetsCoordinatesCorrectly()
    {
        var options = new CosmosStorageOptions
        {
            DefaultDatabase = "MainDB"
        };

        options.ConfigureEvents("EventsContainer", "EventsDB");
        options.ConfigureSnapshots("SnapshotsContainer", "SnapshotsDB");
        options.ConfigureDocuments("DocsContainer", "DocsDB");
        options.Projections.ToContainer("ProjContainer", "ProjDB");

        options.Events.Container.ShouldBe("EventsContainer");
        options.Events.Database.ShouldBe("EventsDB");
        options.Snapshots.Container.ShouldBe("SnapshotsContainer");
        options.Snapshots.Database.ShouldBe("SnapshotsDB");
        options.Documents.Container.ShouldBe("DocsContainer");
        options.Documents.Database.ShouldBe("DocsDB");
        options.Projections.Mode.ShouldBe(ProjectionStorageMode.DedicatedContainer);
        options.Projections.Container.ShouldBe("ProjContainer");
        options.Projections.Database.ShouldBe("ProjDB");
    }

    [Fact]
    public void CosmosContainerResolver_Routes_Events_Snapshots_And_Documents()
    {
        var cosmosOptions = new CosmosStorageOptions
        {
            DefaultDatabase = "DefaultDB"
        };
        cosmosOptions.ConfigureEvents("EventStore", "EventsDB");
        cosmosOptions.ConfigureSnapshots("SnapshotStore", "SnapshotsDB");
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");

        var resolver = new CosmosContainerResolver(_mockClient, cosmosOptions);

        resolver.GetEventsContainer().ShouldBe(_eventsContainer);
        resolver.GetSnapshotsContainer().ShouldBe(_snapshotsContainer);
        resolver.GetDocumentsContainer().ShouldBe(_documentsContainer);
        resolver.GetContainerForDocumentType(typeof(SegregationItem)).ShouldBe(_documentsContainer);
    }

    [Fact]
    public void CosmosContainerResolver_InheritDocuments_Routes_Projections_To_Documents_Container()
    {
        var cosmosOptions = new CosmosStorageOptions();
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");
        cosmosOptions.Projections.Mode = ProjectionStorageMode.InheritDocuments;

        var storeOptions = new StoreOptions();
        storeOptions.Projections.Add<SegregationOrderProjection>();

        var resolver = new CosmosContainerResolver(_mockClient, cosmosOptions, storeOptions);

        resolver.GetContainerForDocumentType(typeof(SegregationOrderAggregate)).ShouldBe(_documentsContainer);
    }

    [Fact]
    public void CosmosContainerResolver_DedicatedContainer_Routes_Projections_To_Projections_Container()
    {
        var cosmosOptions = new CosmosStorageOptions();
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");
        cosmosOptions.Projections.ToContainer("ProjectionsStore", "ReadDB");

        var storeOptions = new StoreOptions();
        storeOptions.Projections.Add<SegregationOrderProjection>();

        var resolver = new CosmosContainerResolver(_mockClient, cosmosOptions, storeOptions);

        resolver.GetContainerForDocumentType(typeof(SegregationOrderAggregate)).ShouldBe(_projectionsContainer);
        resolver.GetContainerForDocumentType(typeof(SegregationItem)).ShouldBe(_documentsContainer);
    }

    [Fact]
    public void CosmosContainerResolver_AutoContainerPerProjection_Routes_Dynamically()
    {
        var cosmosOptions = new CosmosStorageOptions();
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");
        cosmosOptions.Projections.AutoContainerPerProjection("ReadDB", type => type.Name);

        var storeOptions = new StoreOptions();
        storeOptions.Projections.Add<SegregationOrderProjection>();

        var resolver = new CosmosContainerResolver(_mockClient, cosmosOptions, storeOptions);

        resolver.GetContainerForDocumentType(typeof(SegregationOrderAggregate)).ShouldBe(_autoProjContainer);
        resolver.GetContainerForDocumentType(typeof(SegregationItem)).ShouldBe(_documentsContainer);
    }

    [Fact]
    public void CosmosContainerResolver_ProjectionOverrides_TakesPrecedence()
    {
        var cosmosOptions = new CosmosStorageOptions();
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");
        cosmosOptions.Projections.For<SegregationOrderProjection>("CustomProjectionStore", "CustomDB");

        var storeOptions = new StoreOptions();
        storeOptions.Projections.Add<SegregationOrderProjection>();

        var resolver = new CosmosContainerResolver(_mockClient, cosmosOptions, storeOptions);

        resolver.GetContainerForDocumentType(typeof(SegregationOrderAggregate)).ShouldBe(_projectionsContainer);
    }

    [Fact]
    public async Task InitializeAsync_Creates_All_Segregated_Databases_And_Containers()
    {
        var eventsDb = Substitute.For<Database>();
        var snapshotsDb = Substitute.For<Database>();
        var docsDb = Substitute.For<Database>();

        var evDbResp = Substitute.For<DatabaseResponse>();
        evDbResp.Database.Returns(eventsDb);
        var snapDbResp = Substitute.For<DatabaseResponse>();
        snapDbResp.Database.Returns(snapshotsDb);
        var docDbResp = Substitute.For<DatabaseResponse>();
        docDbResp.Database.Returns(docsDb);

        _mockClient.CreateDatabaseIfNotExistsAsync("EventsDB", cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(evDbResp));
        _mockClient.CreateDatabaseIfNotExistsAsync("SnapshotsDB", cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(snapDbResp));
        _mockClient.CreateDatabaseIfNotExistsAsync("DocsDB", cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(docDbResp));

        eventsDb.CreateContainerIfNotExistsAsync(Arg.Any<ContainerProperties>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Substitute.For<ContainerResponse>()));
        snapshotsDb.CreateContainerIfNotExistsAsync(Arg.Any<ContainerProperties>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Substitute.For<ContainerResponse>()));
        docsDb.CreateContainerIfNotExistsAsync(Arg.Any<ContainerProperties>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Substitute.For<ContainerResponse>()));

        var cosmosOptions = new CosmosStorageOptions();
        cosmosOptions.ConfigureEvents("EventStore", "EventsDB");
        cosmosOptions.ConfigureSnapshots("SnapshotStore", "SnapshotsDB");
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");

        var provider = new CosmosStorageProvider(_mockClient, cosmosOptions);

        await provider.InitializeAsync(TestContext.Current.CancellationToken);

        await _mockClient.Received(1).CreateDatabaseIfNotExistsAsync("EventsDB", cancellationToken: Arg.Any<CancellationToken>());
        await _mockClient.Received(1).CreateDatabaseIfNotExistsAsync("SnapshotsDB", cancellationToken: Arg.Any<CancellationToken>());
        await _mockClient.Received(1).CreateDatabaseIfNotExistsAsync("DocsDB", cancellationToken: Arg.Any<CancellationToken>());

        await eventsDb.Received(1).CreateContainerIfNotExistsAsync(Arg.Is<ContainerProperties>(p => p.Id == "EventStore"), cancellationToken: Arg.Any<CancellationToken>());
        await snapshotsDb.Received(1).CreateContainerIfNotExistsAsync(Arg.Is<ContainerProperties>(p => p.Id == "SnapshotStore"), cancellationToken: Arg.Any<CancellationToken>());
        await docsDb.Received(1).CreateContainerIfNotExistsAsync(Arg.Is<ContainerProperties>(p => p.Id == "DocumentStore"), cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Snapshots_ArePersisted_To_SnapshotsContainer_WhenSegregated()
    {
        var cosmosOptions = new CosmosStorageOptions();
        cosmosOptions.ConfigureEvents("EventStore", "EventsDB");
        cosmosOptions.ConfigureSnapshots("SnapshotStore", "SnapshotsDB");
        cosmosOptions.ConfigureDocuments("DocumentStore", "DocsDB");

        var provider = new CosmosStorageProvider(_mockClient, cosmosOptions);
        var snapshot = new SegregationOrderAggregate { Id = "order-1", Total = 150m };

        await provider.SaveSnapshotAsync("order-1", 10, snapshot, tenantId: "tenant1", ct: TestContext.Current.CancellationToken);

        await _snapshotsContainer.Received(1).UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<SegregationOrderAggregate>>(e => e.Id == "$snapshot_order-1" && e.Data.Total == 150m),
            Arg.Any<PartitionKey>(),
            cancellationToken: Arg.Any<CancellationToken>());

        // Verify events container was not called for snapshot save
        await _eventsContainer.DidNotReceive().UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<SegregationOrderAggregate>>(e => e.Id == "$snapshot_order-1"),
            Arg.Any<PartitionKey>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public void UseCosmos_FluentAction_ConfiguresSegregationInStoreOptions()
    {
        var options = new StoreOptions();
        options.UseCosmos(_mockClient, cosmos =>
        {
            cosmos.DefaultDatabase = "AppDB";
            cosmos.ConfigureEvents("Events_v1", "EventsDB");
            cosmos.ConfigureSnapshots("Snapshots_v1", "SnapshotsDB");
            cosmos.ConfigureDocuments("Docs_v1", "DocsDB");
            cosmos.Projections.ToContainer("Projections_v1", "ReadDB");
        });

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        options.EventStorage.ShouldBeOfType<CosmosStorageProvider>();

        var cosmosProvider = (CosmosStorageProvider)options.DocumentStorage;
        cosmosProvider.Options.Events.Container.ShouldBe("Events_v1");
        cosmosProvider.Options.Events.Database.ShouldBe("EventsDB");
        cosmosProvider.Options.Snapshots.Container.ShouldBe("Snapshots_v1");
        cosmosProvider.Options.Snapshots.Database.ShouldBe("SnapshotsDB");
        cosmosProvider.Options.Documents.Container.ShouldBe("Docs_v1");
        cosmosProvider.Options.Documents.Database.ShouldBe("DocsDB");
        cosmosProvider.Options.Projections.Container.ShouldBe("Projections_v1");
        cosmosProvider.Options.Projections.Database.ShouldBe("ReadDB");
    }

    [Fact]
    public void StorageLocationOptions_Setters_And_Validation()
    {
        Should.Throw<ArgumentException>(() => new StorageLocationOptions(""));
        Should.Throw<ArgumentException>(() => new StorageLocationOptions("  "));

        var loc = new StorageLocationOptions("InitialContainer", "InitialDB");
        loc.Container.ShouldBe("InitialContainer");
        loc.Database.ShouldBe("InitialDB");

        loc.SetContainer("NewContainer");
        loc.Container.ShouldBe("NewContainer");
        Should.Throw<ArgumentException>(() => loc.SetContainer(""));
        Should.Throw<ArgumentException>(() => loc.SetContainer(" "));

        loc.SetDatabase("NewDB");
        loc.Database.ShouldBe("NewDB");
        loc.SetDatabase(null);
        loc.Database.ShouldBeNull();

        // Resolve fallback
        loc.Resolve("FallbackDB").ShouldBe(("FallbackDB", "NewContainer"));

        loc.SetDatabase("ExplicitDB");
        loc.Resolve("FallbackDB").ShouldBe(("ExplicitDB", "NewContainer"));

        loc.SetDatabase("   ");
        loc.Resolve("FallbackDB").ShouldBe(("FallbackDB", "NewContainer"));
    }

    [Fact]
    public void ProjectionStorageOptions_ForType_And_Validation()
    {
        var options = new ProjectionStorageOptions();

        options.For(typeof(SegregationOrderProjection), "OrderProjections", "OrderDb");
        options.Overrides.ContainsKey(typeof(SegregationOrderProjection)).ShouldBeTrue();
        options.Overrides[typeof(SegregationOrderProjection)].Container.ShouldBe("OrderProjections");
        options.Overrides[typeof(SegregationOrderProjection)].Database.ShouldBe("OrderDb");

        Should.Throw<ArgumentNullException>(() => options.For(null!, "Cont"));
        Should.Throw<ArgumentException>(() => options.For(typeof(SegregationOrderProjection), ""));
        Should.Throw<ArgumentException>(() => options.For<SegregationOrderProjection>(""));
        Should.Throw<ArgumentException>(() => options.ToContainer(""));
    }
}
