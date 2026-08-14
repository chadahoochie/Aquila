using Microsoft.Azure.Cosmos;
using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Sessions;
using Aquila.Cosmos.Configuration;
using Aquila.Cosmos.Extensions;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

// ─── Domain Models for Integration Tests ───────────────────────────────────

public sealed record SegregatedCustomerDoc(string Id, string Name, string Email);

public sealed record SegregatedOrderPlacedEvent(string OrderId, string CustomerId, decimal Amount);
public sealed record SegregatedOrderItemAddedEvent(string OrderId, string ItemName, decimal Price);

public sealed class SegregatedOrderAggregate
{
    public string Id { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }

    public void Apply(SegregatedOrderPlacedEvent @event)
    {
        Id = @event.OrderId;
        CustomerId = @event.CustomerId;
        TotalAmount = @event.Amount;
        ItemCount = 1;
    }

    public void Apply(SegregatedOrderItemAddedEvent @event)
    {
        TotalAmount += @event.Price;
        ItemCount++;
    }
}

// Projection 1: Single Stream Read Model
public sealed class SegregatedOrderSummary
{
    public string Id { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
}

public sealed class SegregatedOrderSummaryProjection : SingleStreamProjection<SegregatedOrderSummary>
{
    public SegregatedOrderSummaryProjection()
    {
        ProjectEvent<SegregatedOrderPlacedEvent>((e, doc) =>
        {
            doc.Id = e.OrderId;
            doc.TotalAmount = e.Amount;
            doc.ItemCount = 1;
        });

        ProjectEvent<SegregatedOrderItemAddedEvent>((e, doc) =>
        {
            doc.TotalAmount += e.Price;
            doc.ItemCount++;
        });
    }
}

// Projection 2: Multi Stream Read Model
public sealed class SegregatedCustomerMetrics
{
    public string Id { get; set; } = string.Empty;
    public decimal TotalSpend { get; set; }
    public int OrderCount { get; set; }
}

public sealed class SegregatedCustomerMetricsProjection : MultiStreamProjection<SegregatedCustomerMetrics, string>
{
    protected override string Identity(IEvent @event)
    {
        if (@event.Data is SegregatedOrderPlacedEvent placed)
        {
            return placed.CustomerId;
        }
        return string.Empty;
    }

    public override bool Apply(IEvent @event, SegregatedCustomerMetrics document)
    {
        if (@event.Data is SegregatedOrderPlacedEvent placed)
        {
            document.Id = placed.CustomerId;
            document.TotalSpend += placed.Amount;
            document.OrderCount++;
            return true;
        }
        return false;
    }
}

// Projection 3: Standard Projection (for override test)
public sealed class SegregatedStandardReadModel
{
    public string Id { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public sealed class SegregatedStandardProjection : SingleStreamProjection<SegregatedStandardReadModel>
{
    public SegregatedStandardProjection()
    {
        ProjectEvent<SegregatedOrderPlacedEvent>((e, doc) =>
        {
            doc.Id = e.OrderId;
            doc.Amount = e.Amount;
        });
    }
}

// Projection 4: Special Projection (for override test)
public sealed class SegregatedSpecialReadModel
{
    public string Id { get; set; } = string.Empty;
    public decimal SpecialAmount { get; set; }
}

public sealed class SegregatedSpecialProjection : SingleStreamProjection<SegregatedSpecialReadModel>
{
    public SegregatedSpecialProjection()
    {
        ProjectEvent<SegregatedOrderPlacedEvent>((e, doc) =>
        {
            doc.Id = e.OrderId;
            doc.SpecialAmount = e.Amount * 2;
        });
    }
}

// ─── Integration Tests ─────────────────────────────────────────────────────

[Collection("CosmosIntegration")]
public sealed class CosmosStorageSegregationIntegrationTests
{
    private readonly CosmosContainerFixture _fixture;

    public CosmosStorageSegregationIntegrationTests(CosmosContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Event_And_Snapshot_Segregation_Persists_To_Separate_Containers_And_Databases()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var eventsDbName = $"EventsDb_{suffix}";
        var snapshotsDbName = $"SnapshotsDb_{suffix}";
        var docsDbName = $"DocsDb_{suffix}";

        var eventsContainerName = "EventsJournal";
        var snapshotsContainerName = "SnapshotsStore";
        var docsContainerName = "DocumentsStore";

        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, cosmos =>
            {
                cosmos.DefaultDatabase = docsDbName;
                cosmos.ConfigureEvents(eventsContainerName, eventsDbName);
                cosmos.ConfigureSnapshots(snapshotsContainerName, snapshotsDbName);
                cosmos.ConfigureDocuments(docsContainerName, docsDbName);
            });

            options.Events.SnapshotEvery<SegregatedOrderAggregate>(threshold: 2);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = $"ord_{Guid.NewGuid():N}";
        var customerId = $"cust_{Guid.NewGuid():N}";

        // 1. Store a document in Documents DB
        var docId = $"doc_{Guid.NewGuid():N}";
        using (var session = store.OpenSession())
        {
            session.Store(new SegregatedCustomerDoc(docId, "Alice", "alice@example.com"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // 2. Append 2 events to hit the snapshot threshold (2)
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderPlacedEvent(streamId, customerId, 100.00m));
            session.Events.Append<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderItemAddedEvent(streamId, "Book", 25.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // 3. Direct inspection on Events Container
        var eventsContainer = _fixture.Client.GetContainer(eventsDbName, eventsContainerName);
        var eventQuery = new QueryDefinition("SELECT * FROM c WHERE c.pk = @streamId")
            .WithParameter("@streamId", streamId);

        using var eventIterator = eventsContainer.GetItemQueryIterator<CosmosDocumentEnvelope<object>>(
            eventQuery,
            requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });

        var eventDocs = new List<CosmosDocumentEnvelope<object>>();
        while (eventIterator.HasMoreResults)
        {
            var resp = await eventIterator.ReadNextAsync(TestContext.Current.CancellationToken);
            eventDocs.AddRange(resp);
        }

        // Events container should contain 2 $event documents and 1 $stream_header document, but NO $snapshot document
        eventDocs.Count(d => d.DocType == "$event").ShouldBe(2);
        eventDocs.Count(d => d.DocType == "$stream_header").ShouldBe(1);
        eventDocs.Any(d => d.DocType == "$snapshot").ShouldBeFalse();

        // 4. Direct inspection on Snapshots Container
        var snapshotsContainer = _fixture.Client.GetContainer(snapshotsDbName, snapshotsContainerName);
        var snapshotQuery = new QueryDefinition("SELECT * FROM c WHERE c.pk = @streamId")
            .WithParameter("@streamId", streamId);

        using var snapshotIterator = snapshotsContainer.GetItemQueryIterator<CosmosDocumentEnvelope<SegregatedOrderAggregate>>(
            snapshotQuery,
            requestOptions: new QueryRequestOptions { PartitionKey = CosmosPartitionKeyHelper.CreatePartitionKey(streamId) });

        var snapshotDocs = new List<CosmosDocumentEnvelope<SegregatedOrderAggregate>>();
        while (snapshotIterator.HasMoreResults)
        {
            var resp = await snapshotIterator.ReadNextAsync(TestContext.Current.CancellationToken);
            snapshotDocs.AddRange(resp);
        }

        // Snapshots container should contain the $snapshot document, but NO $event or $stream_header documents
        snapshotDocs.Count.ShouldBe(1);
        var snapshotDoc = snapshotDocs[0];
        snapshotDoc.DocType.ShouldBe("$snapshot");
        snapshotDoc.Version.ShouldBe("2");
        snapshotDoc.Data.ShouldNotBeNull();
        snapshotDoc.Data.TotalAmount.ShouldBe(125.00m);
        snapshotDoc.Data.ItemCount.ShouldBe(2);

        // 5. Direct inspection on Documents Container
        var docsContainer = _fixture.Client.GetContainer(docsDbName, docsContainerName);
        var docResp = await docsContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedCustomerDoc>>(
            docId,
            CosmosPartitionKeyHelper.CreatePartitionKey(nameof(SegregatedCustomerDoc)),
            cancellationToken: TestContext.Current.CancellationToken);

        docResp.Resource.ShouldNotBeNull();
        docResp.Resource.Data.Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task Snapshot_Threshold_Automatically_Takes_Snapshot_Only_When_Threshold_Is_Reached()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var dbName = $"SnapThresholdDb_{suffix}";
        var snapshotsContainerName = "Snapshots";

        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, cosmos =>
            {
                cosmos.DefaultDatabase = dbName;
                cosmos.ConfigureEvents("Events", dbName);
                cosmos.ConfigureSnapshots(snapshotsContainerName, dbName);
                cosmos.ConfigureDocuments("Documents", dbName);
            });

            // Set snapshot threshold to 3
            options.Events.SnapshotEvery<SegregatedOrderAggregate>(threshold: 3);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = $"ord_{Guid.NewGuid():N}";
        var customerId = $"cust_{Guid.NewGuid():N}";
        var snapshotsContainer = _fixture.Client.GetContainer(dbName, snapshotsContainerName);

        // Stage 1: Append 2 events (threshold 3 NOT reached)
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderPlacedEvent(streamId, customerId, 50.00m));
            session.Events.Append<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderItemAddedEvent(streamId, "Item 1", 10.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Direct check: Snapshot should NOT exist yet
        try
        {
            var snapResp = await snapshotsContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedOrderAggregate>>(
                $"$snapshot_{streamId}",
                CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
                cancellationToken: TestContext.Current.CancellationToken);
            snapResp.Resource.ShouldBeNull();
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Expected: not found
        }

        // Stage 2: Append 1 more event (Total 3 events -> threshold 3 reached!)
        using (var session = store.OpenSession())
        {
            session.Events.Append<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderItemAddedEvent(streamId, "Item 2", 15.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Direct check: Snapshot should now exist at Version 3
        var snapV3 = await snapshotsContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedOrderAggregate>>(
            $"$snapshot_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: TestContext.Current.CancellationToken);

        snapV3.Resource.ShouldNotBeNull();
        snapV3.Resource.Version.ShouldBe("3");
        snapV3.Resource.Data.TotalAmount.ShouldBe(75.00m); // 50 + 10 + 15
        snapV3.Resource.Data.ItemCount.ShouldBe(3);

        // Stage 3: Append 2 more events (Total 5 events, delta since last snapshot = 2 < 3)
        using (var session = store.OpenSession())
        {
            session.Events.Append<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderItemAddedEvent(streamId, "Item 3", 5.00m),
                new SegregatedOrderItemAddedEvent(streamId, "Item 4", 5.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Direct check: Snapshot should STILL be at Version 3
        var snapStillV3 = await snapshotsContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedOrderAggregate>>(
            $"$snapshot_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: TestContext.Current.CancellationToken);

        snapStillV3.Resource.Version.ShouldBe("3");
        snapStillV3.Resource.Data.TotalAmount.ShouldBe(75.00m);

        // Stage 4: Append 1 more event (Total 6 events, delta = 3 -> threshold reached!)
        using (var session = store.OpenSession())
        {
            session.Events.Append<SegregatedOrderAggregate>(
                streamId,
                new SegregatedOrderItemAddedEvent(streamId, "Item 5", 20.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Direct check: Snapshot should now be updated to Version 6
        var snapV6 = await snapshotsContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedOrderAggregate>>(
            $"$snapshot_{streamId}",
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: TestContext.Current.CancellationToken);

        snapV6.Resource.Version.ShouldBe("6");
        snapV6.Resource.Data.TotalAmount.ShouldBe(105.00m); // 75 + 5 + 5 + 20
        snapV6.Resource.Data.ItemCount.ShouldBe(6);

        // Stage 5: Rehydrate aggregate seamlessly from snapshot + delta
        using (var session = store.OpenSession())
        {
            var rehydrated = await session.Events.AggregateStreamAsync<SegregatedOrderAggregate>(
                streamId,
                ct: TestContext.Current.CancellationToken);

            rehydrated.ShouldNotBeNull();
            rehydrated.TotalAmount.ShouldBe(105.00m);
            rehydrated.ItemCount.ShouldBe(6);
        }
    }

    [Fact]
    public async Task Auto_Container_Per_Projection_Creates_And_Routes_Each_Projection_To_Its_Own_Container()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var projDbName = $"AutoProjDb_{suffix}";

        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, cosmos =>
            {
                cosmos.DefaultDatabase = projDbName;
                cosmos.ConfigureEvents("Events", projDbName);
                cosmos.ConfigureSnapshots("Snapshots", projDbName);
                cosmos.ConfigureDocuments("Documents", projDbName);

                // Auto-container per projection in dedicated DB
                cosmos.Projections.AutoContainerPerProjection(projDbName);
            });

            options.Projections.Add<SegregatedOrderSummaryProjection>(ProjectionLifecycle.Inline);
            options.Projections.Add<SegregatedCustomerMetricsProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = $"ord_{Guid.NewGuid():N}";
        var customerId = $"cust_{Guid.NewGuid():N}";

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<SegregatedOrderSummary>(
                streamId,
                new SegregatedOrderPlacedEvent(streamId, customerId, 200.00m));
            session.Events.Append(
                streamId,
                new SegregatedOrderItemAddedEvent(streamId, "Item A", 50.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // 1. Direct inspection on Auto-Container for SingleStreamProjection: SegregatedOrderSummaryProjection
        var summaryContainer = _fixture.Client.GetContainer(projDbName, nameof(SegregatedOrderSummaryProjection));
        var summaryResp = await summaryContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedOrderSummary>>(
            streamId,
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: TestContext.Current.CancellationToken);

        summaryResp.Resource.ShouldNotBeNull();
        summaryResp.Resource.Data.TotalAmount.ShouldBe(250.00m);
        summaryResp.Resource.Data.ItemCount.ShouldBe(2);

        // 2. Direct inspection on Auto-Container for MultiStreamProjection: SegregatedCustomerMetricsProjection
        var metricsContainer = _fixture.Client.GetContainer(projDbName, nameof(SegregatedCustomerMetricsProjection));
        var metricsResp = await metricsContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedCustomerMetrics>>(
            customerId,
            CosmosPartitionKeyHelper.CreatePartitionKey(nameof(SegregatedCustomerMetrics)),
            cancellationToken: TestContext.Current.CancellationToken);

        metricsResp.Resource.ShouldNotBeNull();
        metricsResp.Resource.Data.TotalSpend.ShouldBe(200.00m);
        metricsResp.Resource.Data.OrderCount.ShouldBe(1);

        // 3. Load via DocumentSession to verify seamless read routing
        using (var session = store.OpenSession())
        {
            var loadedSummary = await session.LoadAsync<SegregatedOrderSummary>(streamId, partitionKey: streamId, ct: TestContext.Current.CancellationToken);
            loadedSummary.ShouldNotBeNull();
            loadedSummary.TotalAmount.ShouldBe(250.00m);

            var loadedMetrics = await session.LoadAsync<SegregatedCustomerMetrics>(customerId, ct: TestContext.Current.CancellationToken);
            loadedMetrics.ShouldNotBeNull();
            loadedMetrics.TotalSpend.ShouldBe(200.00m);
        }
    }

    [Fact]
    public async Task Separate_Db_And_Container_For_Specific_Projections_Routes_Correctly()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var mainDbName = $"MainDb_{suffix}";
        var specialDbName = $"SpecialDb_{suffix}";

        var standardContainerName = "StandardProjectionsContainer";
        var specialContainerName = "SpecialProjectionsContainer";

        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, cosmos =>
            {
                cosmos.DefaultDatabase = mainDbName;
                cosmos.ConfigureEvents("Events", mainDbName);
                cosmos.ConfigureSnapshots("Snapshots", mainDbName);
                cosmos.ConfigureDocuments("Documents", mainDbName);

                // Default projections location
                cosmos.Projections.ToContainer(standardContainerName, mainDbName);

                // Specific projection override to a separate DB and Container
                cosmos.Projections.For<SegregatedSpecialProjection>(specialContainerName, specialDbName);
            });

            options.Projections.Add<SegregatedStandardProjection>(ProjectionLifecycle.Inline);
            options.Projections.Add<SegregatedSpecialProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = $"ord_{Guid.NewGuid():N}";
        var customerId = $"cust_{Guid.NewGuid():N}";

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<SegregatedStandardReadModel>(
                streamId,
                new SegregatedOrderPlacedEvent(streamId, customerId, 300.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // 1. Direct inspection on MainDb / StandardProjectionsContainer
        var standardContainer = _fixture.Client.GetContainer(mainDbName, standardContainerName);
        var standardResp = await standardContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedStandardReadModel>>(
            streamId,
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: TestContext.Current.CancellationToken);

        standardResp.Resource.ShouldNotBeNull();
        standardResp.Resource.Data.Amount.ShouldBe(300.00m);

        // 2. Direct inspection on SpecialDb / SpecialProjectionsContainer
        var specialContainer = _fixture.Client.GetContainer(specialDbName, specialContainerName);
        var specialResp = await specialContainer.ReadItemAsync<CosmosDocumentEnvelope<SegregatedSpecialReadModel>>(
            streamId,
            CosmosPartitionKeyHelper.CreatePartitionKey(streamId),
            cancellationToken: TestContext.Current.CancellationToken);

        specialResp.Resource.ShouldNotBeNull();
        specialResp.Resource.Data.SpecialAmount.ShouldBe(600.00m); // 300 * 2

        // 3. Load via DocumentSession to verify seamless read routing for both models
        using (var session = store.OpenSession())
        {
            var loadedStandard = await session.LoadAsync<SegregatedStandardReadModel>(streamId, partitionKey: streamId, ct: TestContext.Current.CancellationToken);
            loadedStandard.ShouldNotBeNull();
            loadedStandard.Amount.ShouldBe(300.00m);

            var loadedSpecial = await session.LoadAsync<SegregatedSpecialReadModel>(streamId, partitionKey: streamId, ct: TestContext.Current.CancellationToken);
            loadedSpecial.ShouldNotBeNull();
            loadedSpecial.SpecialAmount.ShouldBe(600.00m);
        }
    }
}
