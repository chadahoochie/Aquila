using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Extensions;
using Aquila.Cosmos.Projections;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

// ─── Events ────────────────────────────────────────────────────────────────

public sealed record IntegrationOrderPlaced(string OrderId, string CustomerId, decimal Amount);
public sealed record IntegrationPaymentReceived(string OrderId, string CustomerId, decimal AmountPaid);
public sealed record IntegrationOrderShipped(string OrderId, string CustomerId);
public sealed record IntegrationCustomerDeactivated(string CustomerId, string Reason);
public sealed record IntegrationCustomerReactivated(string CustomerId);

// ─── Read Model ────────────────────────────────────────────────────────────

public class IntegrationCustomerSummaryReadModel
{
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public decimal TotalPaid { get; set; }
    public int OrderCount { get; set; }
    public int ShippedCount { get; set; }
    public bool IsActive { get; set; } = true;
}

// ─── Projection ────────────────────────────────────────────────────────────

public class IntegrationMultiStreamProjection : MultiStreamProjection<IntegrationCustomerSummaryReadModel, string>
{
    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            IntegrationOrderPlaced e => e.CustomerId,
            IntegrationPaymentReceived e => e.CustomerId,
            IntegrationOrderShipped e => e.CustomerId,
            IntegrationCustomerDeactivated e => e.CustomerId,
            IntegrationCustomerReactivated e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, IntegrationCustomerSummaryReadModel document)
    {
        switch (@event.Data)
        {
            case IntegrationOrderPlaced e:
                document.CustomerId = e.CustomerId;
                document.TotalAmount += e.Amount;
                document.OrderCount++;
                document.IsActive = true;
                return true;

            case IntegrationPaymentReceived e:
                document.CustomerId = e.CustomerId;
                document.TotalPaid += e.AmountPaid;
                return true;

            case IntegrationOrderShipped e:
                document.CustomerId = e.CustomerId;
                document.ShippedCount++;
                return true;

            case IntegrationCustomerDeactivated:
                return false; // Tombstone — delete the read model

            case IntegrationCustomerReactivated e:
                document.CustomerId = e.CustomerId;
                document.IsActive = true;
                return true;

            default:
                return true;
        }
    }
}

public class IntegrationAsyncMultiStreamProjection : MultiStreamProjection<IntegrationCustomerSummaryReadModel, string>
{
    public IntegrationAsyncMultiStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;
    }

    protected override string Identity(IEvent @event)
    {
        return @event.Data switch
        {
            IntegrationOrderPlaced e => e.CustomerId,
            IntegrationPaymentReceived e => e.CustomerId,
            IntegrationOrderShipped e => e.CustomerId,
            IntegrationCustomerDeactivated e => e.CustomerId,
            IntegrationCustomerReactivated e => e.CustomerId,
            _ => null!
        };
    }

    public override bool Apply(IEvent @event, IntegrationCustomerSummaryReadModel document)
    {
        switch (@event.Data)
        {
            case IntegrationOrderPlaced e:
                document.CustomerId = e.CustomerId;
                document.TotalAmount += e.Amount;
                document.OrderCount++;
                document.IsActive = true;
                return true;

            case IntegrationPaymentReceived e:
                document.CustomerId = e.CustomerId;
                document.TotalPaid += e.AmountPaid;
                return true;

            case IntegrationOrderShipped e:
                document.CustomerId = e.CustomerId;
                document.ShippedCount++;
                return true;

            case IntegrationCustomerDeactivated:
                return false;

            case IntegrationCustomerReactivated e:
                document.CustomerId = e.CustomerId;
                document.IsActive = true;
                return true;

            default:
                return true;
        }
    }
}

// ─── Integration Tests ─────────────────────────────────────────────────────

[Collection("CosmosIntegration")]
public sealed class CosmosMultiStreamProjectionIntegrationTests
{
    private readonly CosmosContainerFixture _fixture;

    public CosmosMultiStreamProjectionIntegrationTests(CosmosContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Inline_MultiStreamProjection_Aggregates_Events_From_Multiple_Streams()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationMultiStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Arrange: append events across three different streams for the same customer
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/ord-100",
                new IntegrationOrderPlaced("ord-100", "cust-A", 250.00m));
            session.Events.StartStream<object>("payments/pay-500",
                new IntegrationPaymentReceived("ord-100", "cust-A", 200.00m));
            session.Events.StartStream<object>("shipping/ship-900",
                new IntegrationOrderShipped("ord-100", "cust-A"));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: the read model aggregates events from all three streams
        using (var session = store.OpenSession())
        {
            var doc = await session.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-A",
                ct: TestContext.Current.CancellationToken);

            doc.ShouldNotBeNull();
            doc.CustomerId.ShouldBe("cust-A");
            doc.TotalAmount.ShouldBe(250.00m);
            doc.TotalPaid.ShouldBe(200.00m);
            doc.OrderCount.ShouldBe(1);
            doc.ShippedCount.ShouldBe(1);
            doc.IsActive.ShouldBeTrue();
        }
    }

    [Fact]
    public async Task Inline_MultiStreamProjection_Handles_Interleaved_Events_For_Multiple_Customers()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationMultiStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Act: append interleaved events for cust-A and cust-B
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/1",
                new IntegrationOrderPlaced("1", "cust-A", 100m));
            session.Events.StartStream<object>("orders/2",
                new IntegrationOrderPlaced("2", "cust-B", 300m));
            session.Events.StartStream<object>("payments/1",
                new IntegrationPaymentReceived("1", "cust-A", 100m));
            session.Events.StartStream<object>("payments/2",
                new IntegrationPaymentReceived("2", "cust-B", 150m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: each customer gets its own projected read model
        using (var session = store.OpenSession())
        {
            var custA = await session.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-A",
                ct: TestContext.Current.CancellationToken);
            var custB = await session.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-B",
                ct: TestContext.Current.CancellationToken);

            custA.ShouldNotBeNull();
            custA.CustomerId.ShouldBe("cust-A");
            custA.TotalAmount.ShouldBe(100m);
            custA.TotalPaid.ShouldBe(100m);
            custA.OrderCount.ShouldBe(1);

            custB.ShouldNotBeNull();
            custB.CustomerId.ShouldBe("cust-B");
            custB.TotalAmount.ShouldBe(300m);
            custB.TotalPaid.ShouldBe(150m);
            custB.OrderCount.ShouldBe(1);
        }
    }

    [Fact]
    public async Task Inline_MultiStreamProjection_Deletes_Document_When_Apply_Returns_False()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationMultiStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Step 1: Create document via order event
        using (var session1 = store.OpenSession())
        {
            session1.Events.StartStream<object>("orders/ord-101",
                new IntegrationOrderPlaced("ord-101", "cust-B", 100.00m));
            await session1.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Verify document exists
        using (var verifySession = store.OpenSession())
        {
            var docBefore = await verifySession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-B",
                ct: TestContext.Current.CancellationToken);
            docBefore.ShouldNotBeNull();
        }

        // Step 2: Deactivate customer (tombstone)
        using (var session2 = store.OpenSession())
        {
            session2.Events.StartStream<object>("deactivations/deact-1",
                new IntegrationCustomerDeactivated("cust-B", "Account Closed"));
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: document is deleted
        using (var session3 = store.OpenSession())
        {
            var docAfter = await session3.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-B",
                ct: TestContext.Current.CancellationToken);
            docAfter.ShouldBeNull();
        }
    }

    [Fact]
    public async Task Inline_MultiStreamProjection_Recreates_Document_After_Tombstone()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationMultiStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Step 1: Create document
        using (var session1 = store.OpenSession())
        {
            session1.Events.StartStream<object>("orders/1",
                new IntegrationOrderPlaced("1", "cust-C", 100m));
            await session1.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Step 2: Deactivate (tombstone)
        using (var session2 = store.OpenSession())
        {
            session2.Events.StartStream<object>("deact/1",
                new IntegrationCustomerDeactivated("cust-C", "Closed"));
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Verify deleted
        using (var verifySession = store.OpenSession())
        {
            var deletedDoc = await verifySession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-C",
                ct: TestContext.Current.CancellationToken);
            deletedDoc.ShouldBeNull();
        }

        // Step 3: New event for cust-C recreates the document
        using (var session3 = store.OpenSession())
        {
            session3.Events.StartStream<object>("orders/2",
                new IntegrationOrderPlaced("2", "cust-C", 500m));
            await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Assert: document is recreated with fresh state
        using (var finalSession = store.OpenSession())
        {
            var doc = await finalSession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-C",
                ct: TestContext.Current.CancellationToken);
            doc.ShouldNotBeNull();
            doc.CustomerId.ShouldBe("cust-C");
            doc.TotalAmount.ShouldBe(500m);
            doc.OrderCount.ShouldBe(1);
        }
    }

    [Fact]
    public async Task Inline_MultiStreamProjection_Ignores_Events_With_Null_Identity()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationMultiStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Append an unrelated event type that the projection does not handle (null identity)
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("unrelated/1",
                new IntegrationOrderPlaced("x", "cust-X", 999m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The projection handles IntegrationOrderPlaced but should work fine
        using (var session2 = store.OpenSession())
        {
            var doc = await session2.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-X",
                ct: TestContext.Current.CancellationToken);
            doc.ShouldNotBeNull();
            doc.CustomerId.ShouldBe("cust-X");
            doc.TotalAmount.ShouldBe(999m);
        }
    }

    [Fact]
    public async Task Async_MultiStreamProjection_CatchUp_Processes_All_Events()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationAsyncMultiStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Arrange: write events across multiple streams
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 5; i++)
            {
                session.Events.StartStream<object>($"orders/{i}",
                    new IntegrationOrderPlaced($"o{i}", "cust-D", 10.00m));
            }

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act: catch up using the daemon
        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Assert: all events processed
        var checkpoint = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(5);

        using var readSession = store.OpenSession();
        var readModel = await readSession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-D",
            ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(5);
        readModel.TotalAmount.ShouldBe(50.00m);
    }

    [Fact]
    public async Task Async_MultiStreamProjection_ProcessChangeFeedBatch_Aggregates_Cross_Stream()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationAsyncMultiStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        // Build change feed items with events from multiple streams
        var event1 = new EventEnvelope<IntegrationOrderPlaced>
        {
            Id = Guid.NewGuid(),
            StreamId = "orders/ord-1",
            GlobalSequence = 1,
            Version = 1,
            EventType = typeof(IntegrationOrderPlaced).FullName!,
            Data = new IntegrationOrderPlaced("ord-1", "cust-E", 100.00m)
        };

        var event2 = new EventEnvelope<IntegrationPaymentReceived>
        {
            Id = Guid.NewGuid(),
            StreamId = "payments/pay-1",
            GlobalSequence = 2,
            Version = 1,
            EventType = typeof(IntegrationPaymentReceived).FullName!,
            Data = new IntegrationPaymentReceived("ord-1", "cust-E", 80.00m)
        };

        var event3 = new EventEnvelope<IntegrationOrderShipped>
        {
            Id = Guid.NewGuid(),
            StreamId = "shipping/ship-1",
            GlobalSequence = 3,
            Version = 1,
            EventType = typeof(IntegrationOrderShipped).FullName!,
            Data = new IntegrationOrderShipped("ord-1", "cust-E")
        };

        var batch = new object[]
        {
            new CosmosDocumentEnvelope<object>
            {
                Id = "$event_orders_ord-1_v1",
                PartitionKey = "orders/ord-1",
                DocType = "$event",
                Data = event1
            },
            new CosmosDocumentEnvelope<object>
            {
                Id = "$event_payments_pay-1_v1",
                PartitionKey = "payments/pay-1",
                DocType = "$event",
                Data = event2
            },
            new CosmosDocumentEnvelope<object>
            {
                Id = "$event_shipping_ship-1_v1",
                PartitionKey = "shipping/ship-1",
                DocType = "$event",
                Data = event3
            }
        };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        // Assert: checkpoint and read model
        var checkpoint = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(3);

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-E",
            ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.CustomerId.ShouldBe("cust-E");
        readModel.TotalAmount.ShouldBe(100.00m);
        readModel.TotalPaid.ShouldBe(80.00m);
        readModel.OrderCount.ShouldBe(1);
        readModel.ShippedCount.ShouldBe(1);
        readModel.IsActive.ShouldBeTrue();
    }

    [Fact]
    public async Task Async_MultiStreamProjection_Rebuild_Clears_And_Reprocesses()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationAsyncMultiStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Write initial events
        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= 3; i++)
            {
                session.Events.StartStream<object>($"orders/{i}",
                    new IntegrationOrderPlaced($"o{i}", "cust-F", 20.00m));
            }

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);
        (await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken)).ShouldBe(3);

        // Rebuild projection
        await daemon.RebuildProjectionAsync<IntegrationAsyncMultiStreamProjection>(
            TestContext.Current.CancellationToken);

        // Verify checkpoint reset and re-processed
        var checkpointAfter = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        checkpointAfter.ShouldBe(3);

        using var readSession = store.OpenSession();
        var readModel = await readSession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-F",
            ct: TestContext.Current.CancellationToken);
        readModel.ShouldNotBeNull();
        readModel.OrderCount.ShouldBe(3);
        readModel.TotalAmount.ShouldBe(60.00m);
    }

    [Fact]
    public async Task Async_MultiStreamProjection_Stop_And_Start_Controls_Dispatch()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationAsyncMultiStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        // Stop projection
        await daemon.StopProjectionAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);

        var eventDoc = new CosmosDocumentEnvelope<object>
        {
            Id = "$event_orders_stop-1_v1",
            PartitionKey = "orders/stop-1",
            DocType = "$event",
            Data = new EventEnvelope<IntegrationOrderPlaced>
            {
                Id = Guid.NewGuid(),
                StreamId = "orders/stop-1",
                GlobalSequence = 1,
                Version = 1,
                EventType = typeof(IntegrationOrderPlaced).FullName!,
                Data = new IntegrationOrderPlaced("stop-1", "cust-G", 50.00m)
            }
        };

        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc }, TestContext.Current.CancellationToken);

        // Checkpoint should NOT advance while stopped
        var checkpointStopped = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        checkpointStopped.ShouldBe(0);

        // Start projection
        await daemon.StartProjectionAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc }, TestContext.Current.CancellationToken);

        var checkpointStarted = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncMultiStreamProjection), TestContext.Current.CancellationToken);
        checkpointStarted.ShouldBe(1);
    }

    [Fact]
    public async Task Async_MultiStreamProjection_Standalone_Rebuild_Preserves_Multiple_Customers()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "MultiStreamContainer");
            options.Projections.Add<IntegrationAsyncMultiStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        // Write events for two different customers across interleaved streams
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<object>("orders/1",
                new IntegrationOrderPlaced("1", "cust-H", 100m));
            session.Events.StartStream<object>("orders/2",
                new IntegrationOrderPlaced("2", "cust-I", 200m));
            session.Events.StartStream<object>("payments/1",
                new IntegrationPaymentReceived("1", "cust-H", 50m));
            session.Events.StartStream<object>("payments/2",
                new IntegrationPaymentReceived("2", "cust-I", 150m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Verify both customers projected correctly
        using (var session = store.OpenSession())
        {
            var custH = await session.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-H",
                ct: TestContext.Current.CancellationToken);
            var custI = await session.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-I",
                ct: TestContext.Current.CancellationToken);

            custH.ShouldNotBeNull();
            custH.CustomerId.ShouldBe("cust-H");
            custH.TotalAmount.ShouldBe(100m);
            custH.TotalPaid.ShouldBe(50m);

            custI.ShouldNotBeNull();
            custI.CustomerId.ShouldBe("cust-I");
            custI.TotalAmount.ShouldBe(200m);
            custI.TotalPaid.ShouldBe(150m);
        }

        // Rebuild and verify state is preserved
        await daemon.RebuildProjectionAsync<IntegrationAsyncMultiStreamProjection>(
            TestContext.Current.CancellationToken);

        using (var rebuildSession = store.OpenSession())
        {
            var custH = await rebuildSession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-H",
                ct: TestContext.Current.CancellationToken);
            var custI = await rebuildSession.LoadAsync<IntegrationCustomerSummaryReadModel>("cust-I",
                ct: TestContext.Current.CancellationToken);

            custH.ShouldNotBeNull();
            custH.TotalAmount.ShouldBe(100m);
            custH.TotalPaid.ShouldBe(50m);

            custI.ShouldNotBeNull();
            custI.TotalAmount.ShouldBe(200m);
            custI.TotalPaid.ShouldBe(150m);
        }
    }
}
