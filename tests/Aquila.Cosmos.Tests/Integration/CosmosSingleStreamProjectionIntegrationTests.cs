using System.Text.Json;
using Newtonsoft.Json.Linq;
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

// ─── Single Stream Events ───────────────────────────────────────────────────

public sealed record IntegrationAccountOpened(string AccountId, string OwnerName, decimal InitialBalance);
public sealed record IntegrationAccountDeposited(string AccountId, decimal Amount);
public sealed record IntegrationAccountWithdrawn(string AccountId, decimal Amount);
public sealed record IntegrationAccountClosed(string AccountId, string Reason);

// ─── Single Stream Read Model ──────────────────────────────────────────────

public class IntegrationAccountReadModel
{
    public string AccountId { get; set; } = string.Empty;
    public string OwnerName { get; set; } = string.Empty;
    public decimal CurrentBalance { get; set; }
    public int TransactionCount { get; set; }
    public bool IsOpen { get; set; } = true;
}

// ─── Inline Single Stream Projection ──────────────────────────────────────

public class IntegrationSingleStreamProjection : SingleStreamProjection<IntegrationAccountReadModel>
{
    public IntegrationSingleStreamProjection()
    {
        CreateEvent<IntegrationAccountOpened>(e => new IntegrationAccountReadModel
        {
            AccountId = e.AccountId,
            OwnerName = e.OwnerName,
            CurrentBalance = e.InitialBalance,
            TransactionCount = 1,
            IsOpen = true
        });

        ProjectEvent<IntegrationAccountDeposited>((e, doc) =>
        {
            doc.AccountId = e.AccountId;
            doc.CurrentBalance += e.Amount;
            doc.TransactionCount++;
        });

        ProjectEvent<IntegrationAccountWithdrawn>((e, doc) =>
        {
            doc.AccountId = e.AccountId;
            doc.CurrentBalance -= e.Amount;
            doc.TransactionCount++;
        });

        ProjectEvent<IntegrationAccountClosed>((e, doc) =>
        {
            doc.AccountId = e.AccountId;
            doc.IsOpen = false;
        });
    }
}

// ─── Async Single Stream Projection ───────────────────────────────────────

public class IntegrationAsyncSingleStreamProjection : SingleStreamProjection<IntegrationAccountReadModel>
{
    public IntegrationAsyncSingleStreamProjection()
    {
        Lifecycle = ProjectionLifecycle.Async;

        CreateEvent<IntegrationAccountOpened>(e => new IntegrationAccountReadModel
        {
            AccountId = e.AccountId,
            OwnerName = e.OwnerName,
            CurrentBalance = e.InitialBalance,
            TransactionCount = 1,
            IsOpen = true
        });

        ProjectEvent<IntegrationAccountDeposited>((e, doc) =>
        {
            doc.AccountId = e.AccountId;
            doc.CurrentBalance += e.Amount;
            doc.TransactionCount++;
        });

        ProjectEvent<IntegrationAccountWithdrawn>((e, doc) =>
        {
            doc.AccountId = e.AccountId;
            doc.CurrentBalance -= e.Amount;
            doc.TransactionCount++;
        });

        ProjectEvent<IntegrationAccountClosed>((e, doc) =>
        {
            doc.AccountId = e.AccountId;
            doc.IsOpen = false;
        });
    }
}

// ─── Integration Tests ─────────────────────────────────────────────────────

[Collection("CosmosIntegration")]
public sealed class CosmosSingleStreamProjectionIntegrationTests
{
    private readonly CosmosContainerFixture _fixture;

    public CosmosSingleStreamProjectionIntegrationTests(CosmosContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Inline_SingleStreamProjection_Transforms_Single_Stream_Events_Into_ReadModel()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "SingleStreamContainer");
            options.Projections.Add<IntegrationSingleStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);
        var streamId = $"accounts/acc-{Guid.NewGuid():N}";

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<IntegrationAccountReadModel>(streamId,
                new IntegrationAccountOpened(streamId, "Alice", 1000.00m));
            session.Events.Append(streamId, new IntegrationAccountDeposited(streamId, 500.00m));
            session.Events.Append(streamId, new IntegrationAccountWithdrawn(streamId, 200.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var session = store.OpenSession())
        {
            var readModel = await session.LoadAsync<IntegrationAccountReadModel>(streamId,
                ct: TestContext.Current.CancellationToken);

            readModel.ShouldNotBeNull();
            readModel.AccountId.ShouldBe(streamId);
            readModel.OwnerName.ShouldBe("Alice");
            readModel.CurrentBalance.ShouldBe(1300.00m); // 1000 + 500 - 200
            readModel.TransactionCount.ShouldBe(3);
            readModel.IsOpen.ShouldBeTrue();
        }
    }

    [Fact]
    public async Task Inline_SingleStreamProjection_Projects_Multiple_Streams_Independently()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "SingleStreamContainer");
            options.Projections.Add<IntegrationSingleStreamProjection>(ProjectionLifecycle.Inline);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamA = $"accounts/acc-A-{Guid.NewGuid():N}";
        var streamB = $"accounts/acc-B-{Guid.NewGuid():N}";

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<IntegrationAccountReadModel>(streamA,
                new IntegrationAccountOpened(streamA, "Alice", 200m));
            session.Events.StartStream<IntegrationAccountReadModel>(streamB,
                new IntegrationAccountOpened(streamB, "Bob", 500m));

            session.Events.Append(streamA, new IntegrationAccountDeposited(streamA, 100m));
            session.Events.Append(streamB, new IntegrationAccountWithdrawn(streamB, 50m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var session = store.OpenSession())
        {
            var accA = await session.LoadAsync<IntegrationAccountReadModel>(streamA,
                ct: TestContext.Current.CancellationToken);
            var accB = await session.LoadAsync<IntegrationAccountReadModel>(streamB,
                ct: TestContext.Current.CancellationToken);

            accA.ShouldNotBeNull();
            accA.OwnerName.ShouldBe("Alice");
            accA.CurrentBalance.ShouldBe(300m);
            accA.TransactionCount.ShouldBe(2);

            accB.ShouldNotBeNull();
            accB.OwnerName.ShouldBe("Bob");
            accB.CurrentBalance.ShouldBe(450m);
            accB.TransactionCount.ShouldBe(2);
        }
    }

    [Fact]
    public async Task Async_SingleStreamProjection_CatchUp_Processes_All_Events()
    {
        var containerName = $"SingleStreamContainer-{Guid.NewGuid():N}";
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", containerName);
            options.Projections.Add<IntegrationAsyncSingleStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = $"accounts/acc-catchup-{Guid.NewGuid():N}";

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<IntegrationAccountReadModel>(streamId,
                new IntegrationAccountOpened(streamId, "Charlie", 100.00m));
            session.Events.Append(streamId, new IntegrationAccountDeposited(streamId, 50.00m));
            session.Events.Append(streamId, new IntegrationAccountDeposited(streamId, 25.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(3);

        using (var readSession = store.OpenSession())
        {
            var readModel = await readSession.LoadAsync<IntegrationAccountReadModel>(streamId,
                ct: TestContext.Current.CancellationToken);

            readModel.ShouldNotBeNull();
            readModel.OwnerName.ShouldBe("Charlie");
            readModel.CurrentBalance.ShouldBe(175.00m);
            readModel.TransactionCount.ShouldBe(3);
        }
    }

    [Fact]
    public async Task Async_SingleStreamProjection_ProcessChangeFeedBatch_Handles_JObject_And_JsonElement()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "SingleStreamContainer");
            options.Projections.Add<IntegrationAsyncSingleStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);
        var streamId = $"accounts/acc-cf-{Guid.NewGuid():N}";

        // 1. JObject payload
        var jObjectItem = JObject.FromObject(new
        {
            id = $"$event_{streamId}_v1",
            PartitionKey = streamId,
            DocType = "$event",
            Data = new EventEnvelope<IntegrationAccountOpened>
            {
                Id = Guid.NewGuid(),
                StreamId = streamId,
                GlobalSequence = 201,
                Version = 1,
                EventType = typeof(IntegrationAccountOpened).FullName!,
                Data = new IntegrationAccountOpened(streamId, "Dave", 400.00m)
            }
        });

        // 2. JsonElement payload
        var jsonText = JsonSerializer.Serialize(new
        {
            id = $"$event_{streamId}_v2",
            PartitionKey = streamId,
            DocType = "$event",
            Data = new EventEnvelope<IntegrationAccountDeposited>
            {
                Id = Guid.NewGuid(),
                StreamId = streamId,
                GlobalSequence = 202,
                Version = 2,
                EventType = typeof(IntegrationAccountDeposited).FullName!,
                Data = new IntegrationAccountDeposited(streamId, 150.00m)
            }
        });
        using var jsonDoc = JsonDocument.Parse(jsonText);
        var jsonElementItem = jsonDoc.RootElement;

        var batch = new object[] { jObjectItem, jsonElementItem };

        await daemon.ProcessChangeFeedBatchAsync(batch, TestContext.Current.CancellationToken);

        var checkpoint = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);
        checkpoint.ShouldBe(202);

        using var session = store.OpenSession();
        var readModel = await session.LoadAsync<IntegrationAccountReadModel>(streamId,
            ct: TestContext.Current.CancellationToken);

        readModel.ShouldNotBeNull();
        readModel.OwnerName.ShouldBe("Dave");
        readModel.CurrentBalance.ShouldBe(550.00m);
        readModel.TransactionCount.ShouldBe(2);
    }

    [Fact]
    public async Task Async_SingleStreamProjection_Rebuild_Clears_And_Reprocesses()
    {
        var containerName = $"SingleStreamContainer-{Guid.NewGuid():N}";
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", containerName);
            options.Projections.Add<IntegrationAsyncSingleStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var streamId = $"accounts/acc-rebuild-{Guid.NewGuid():N}";

        using (var session = store.OpenSession())
        {
            session.Events.StartStream<IntegrationAccountReadModel>(streamId,
                new IntegrationAccountOpened(streamId, "Eve", 1000.00m));
            session.Events.Append(streamId, new IntegrationAccountDeposited(streamId, 200.00m));

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);

        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // Rebuild single stream projection
        await daemon.RebuildProjectionAsync<IntegrationAsyncSingleStreamProjection>(
            TestContext.Current.CancellationToken);

        var checkpointAfter = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);
        checkpointAfter.ShouldBe(2);

        using (var readSession = store.OpenSession())
        {
            var readModel = await readSession.LoadAsync<IntegrationAccountReadModel>(streamId,
                ct: TestContext.Current.CancellationToken);

            readModel.ShouldNotBeNull();
            readModel.OwnerName.ShouldBe("Eve");
            readModel.CurrentBalance.ShouldBe(1200.00m);
            readModel.TransactionCount.ShouldBe(2);
        }
    }

    [Fact]
    public async Task Async_SingleStreamProjection_Stop_And_Start_Controls_Dispatch()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "integration-tenant";
            options.UseCosmos(_fixture.Client, "IntegrationDb", "SingleStreamContainer");
            options.Projections.Add<IntegrationAsyncSingleStreamProjection>(ProjectionLifecycle.Async);
        });

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new InMemoryProjectionCheckpointStore();
        var daemon = new CosmosProjectionDaemon(store, checkpointStore);
        var streamId = $"accounts/acc-stop-{Guid.NewGuid():N}";

        await daemon.StopProjectionAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);

        var eventDoc = new CosmosDocumentEnvelope<object>
        {
            Id = $"$event_{streamId}_v1",
            PartitionKey = streamId,
            DocType = "$event",
            Data = new EventEnvelope<IntegrationAccountOpened>
            {
                Id = Guid.NewGuid(),
                StreamId = streamId,
                GlobalSequence = 301,
                Version = 1,
                EventType = typeof(IntegrationAccountOpened).FullName!,
                Data = new IntegrationAccountOpened(streamId, "Frank", 300.00m)
            }
        };

        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc }, TestContext.Current.CancellationToken);

        // Checkpoint should not advance while stopped
        var checkpointStopped = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);
        checkpointStopped.ShouldBe(0);

        // Start projection
        await daemon.StartProjectionAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);
        await daemon.ProcessChangeFeedBatchAsync(new[] { eventDoc }, TestContext.Current.CancellationToken);

        var checkpointStarted = await checkpointStore.GetCheckpointAsync(
            nameof(IntegrationAsyncSingleStreamProjection), TestContext.Current.CancellationToken);
        checkpointStarted.ShouldBe(301);
    }
}
