using Aquila.Core.Events;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Storage;

public enum TestStatusEnum
{
    Pending = 1,
    Active = 2
}

public class NonListCustomCollection
{
    private readonly List<string> _items = new();
    public void Add(string item) => _items.Add(item);
    public void Remove(string item) => _items.Remove(item);
    public IReadOnlyList<string> Items => _items;
}

public class PatchableDoc
{
    public string Id { get; set; } = string.Empty;
    public string? Description { get; set; }
    public int Counter { get; set; }
    public long LongCounter { get; set; }
    public TestStatusEnum Status { get; set; }
    public List<string>? TagList { get; set; }
    public NonListCustomCollection CustomCol { get; set; } = new();
    public PatchableNested? SubData { get; set; }
    public string ReadOnlyProperty => "ReadOnly";
}

public class PatchableNested
{
    public string Info { get; set; } = string.Empty;
}

public class InMemoryStorageProviderBranchTests
{
    [Fact]
    public async Task QueryDocumentsAsync_Options_PartitionKey_Empty_Or_MaxItemCount_NonPositive()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = new InMemoryStorageProvider();

        await provider.UpsertDocumentAsync(new DocumentEnvelope<SampleDocument>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(SampleDocument),
            Data = new SampleDocument("doc-1", "A", 10m)
        }, ct);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<SampleDocument>
        {
            Id = "doc-2",
            PartitionKey = "pk-2",
            DocType = nameof(SampleDocument),
            Data = new SampleDocument("doc-2", "B", 20m)
        }, ct);

        // options != null but PartitionKey is empty string
        var optionsEmptyPk = new QueryOptions { PartitionKey = "" };
        var res1 = await provider.QueryDocumentsAsync<SampleDocument>(null, optionsEmptyPk, ct);
        res1.Count.ShouldBe(2);

        // options != null but MaxItemCount <= 0
        var optionsZeroMax = new QueryOptions { MaxItemCount = 0 };
        var res2 = await provider.QueryDocumentsAsync<SampleDocument>(null, optionsZeroMax, ct);
        res2.Count.ShouldBe(2);

        var optionsNegativeMax = new QueryOptions { MaxItemCount = -5 };
        var res3 = await provider.QueryDocumentsAsync<SampleDocument>(null, optionsNegativeMax, ct);
        res3.Count.ShouldBe(2);
    }

    [Fact]
    public async Task AppendEventsAsync_EmptyEvents_And_NullTenant_And_NegativeExpectedVersion_And_PreexistingGlobalSequence()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-branch";

        // 1. Empty events list returns Task.CompletedTask
        await provider.Events.AppendEventsAsync(streamId, Array.Empty<IEvent>(), 0, ct);
        var headerNull = await provider.Events.GetStreamHeaderAsync(streamId, ct: ct);
        headerNull.ShouldBeNull();

        // 2. Append event with null TenantId (defaults to "default") and pre-existing GlobalSequence
        var evt1 = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            TenantId = null!,
            GlobalSequence = 999,
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m)
        };

        await provider.Events.AppendEventsAsync(streamId, new IEvent[] { evt1 }, expectedVersion: -1, ct: ct);

        var header = await provider.Events.GetStreamHeaderAsync(streamId, ct: ct);
        header.ShouldNotBeNull();
        header.TenantId.ShouldBe("default");
        header.Version.ShouldBe(1);

        var fetched = await provider.Events.FetchEventsAsync(streamId, ct: ct);
        fetched.Count.ShouldBe(1);
        fetched[0].GlobalSequence.ShouldBe(999);

        // 3. Append event with custom TenantId
        var evt2 = new EventEnvelope<MoneyDepositedEvent>
        {
            StreamId = "stream-custom-tenant",
            Version = 1,
            TenantId = "tenant-custom",
            Data = new MoneyDepositedEvent(Guid.NewGuid(), 50m)
        };

        await provider.Events.AppendEventsAsync("stream-custom-tenant", new IEvent[] { evt2 }, expectedVersion: -1, ct: ct);

        var headerCustom = await provider.Events.GetStreamHeaderAsync("stream-custom-tenant", ct: ct);
        headerCustom.ShouldNotBeNull();
        headerCustom.TenantId.ShouldBe("tenant-custom");
    }

    [Fact]
    public async Task FetchEventsAsync_And_FetchGlobalEventsAsync_WithTenantAndVersionFilters()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-filter";

        var evt1 = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            TenantId = "tenant-a",
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m)
        };
        var evt2 = new EventEnvelope<MoneyDepositedEvent>
        {
            StreamId = streamId,
            Version = 2,
            TenantId = "tenant-a",
            Data = new MoneyDepositedEvent(Guid.NewGuid(), 50m)
        };

        await provider.Events.AppendEventsAsync(streamId, new IEvent[] { evt1, evt2 }, expectedVersion: 0, ct: ct);

        // FetchEventsAsync fromVersion filter
        var eventsFromV2 = await provider.Events.FetchEventsAsync(streamId, tenantId: "tenant-a", fromVersion: 2, ct: ct);
        eventsFromV2.Count.ShouldBe(1);
        eventsFromV2[0].Version.ShouldBe(2);

        // FetchEventsAsync tenant mismatch
        var eventsMismatch = await provider.Events.FetchEventsAsync(streamId, tenantId: "tenant-b", fromVersion: 1, ct: ct);
        eventsMismatch.ShouldBeEmpty();

        // FetchEventsAsync non-existent stream
        var eventsMissing = await provider.Events.FetchEventsAsync("non-existent-stream", ct: ct);
        eventsMissing.ShouldBeEmpty();

        // FetchGlobalEventsAsync tenant filter
        var globalTenantA = await provider.Events.FetchGlobalEventsAsync(0, batchSize: 100, tenantId: "tenant-a", ct: ct);
        globalTenantA.Count.ShouldBe(2);

        var globalTenantB = await provider.Events.FetchGlobalEventsAsync(0, batchSize: 100, tenantId: "tenant-b", ct: ct);
        globalTenantB.ShouldBeEmpty();
    }

    [Fact]
    public async Task SaveSnapshotAsync_And_GetSnapshotAsync_TenantMatching()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-snap";
        var sampleDoc = new SampleDocument("doc-snap", "Snap Title", 123.45m);

        await provider.SaveSnapshotAsync(streamId, version: 5, snapshot: sampleDoc, tenantId: "tenant-alpha", ct: ct);

        // Matching tenant
        var (snap, ver) = await provider.GetSnapshotAsync<SampleDocument>(streamId, tenantId: "tenant-alpha", ct: ct);
        snap.ShouldNotBeNull();
        snap.Title.ShouldBe("Snap Title");
        ver.ShouldBe(5);

        // Non-matching tenant
        var (snapMismatch, verMismatch) = await provider.GetSnapshotAsync<SampleDocument>(streamId, tenantId: "tenant-beta", ct: ct);
        snapMismatch.ShouldBeNull();
        verMismatch.ShouldBe(0);

        // Non-existent stream
        var (snapMissing, verMissing) = await provider.GetSnapshotAsync<SampleDocument>("missing-stream", tenantId: "tenant-alpha", ct: ct);
        snapMissing.ShouldBeNull();
        verMissing.ShouldBe(0);
    }

    [Fact]
    public async Task ExecuteBatchAsync_Patch_Operations_Comprehensive_BranchCoverage()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = new InMemoryStorageProvider();

        var initialDoc = new DocumentEnvelope<PatchableDoc>
        {
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            TenantId = "default",
            Data = new PatchableDoc
            {
                Id = "patch-1",
                Description = "Initial",
                Counter = 10,
                Status = TestStatusEnum.Pending
            }
        };

        await provider.UpsertDocumentAsync(initialDoc, ct);

        // 1. Patch operation on non-existent document (ignored)
        var patchNonExistent = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "missing-doc",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "/Data/Description", Action = PatchAction.Set, Value = "New" }
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchNonExistent }, ct);

        // 2. Patch operation with null / empty operations list
        var patchEmptyOps = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>()
        };
        await provider.ExecuteBatchAsync(new[] { patchEmptyOps }, ct);

        // 3. Patch operation with invalid path / property info null / read-only property
        var patchInvalidPath = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "", Action = PatchAction.Set, Value = "Ignore" },
                new() { Path = "/Data/NonExistentProperty", Action = PatchAction.Set, Value = "Ignore" },
                new() { Path = "/Data/ReadOnlyProperty", Action = PatchAction.Set, Value = "Ignore" }
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchInvalidPath }, ct);

        // 4. Set null value, Enum string, Enum object, Convert type
        var patchSetOps = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "/Data/Description", Action = PatchAction.Set, Value = null },
                new() { Path = "/Data/Status", Action = PatchAction.Set, Value = "Active" },
                new() { Path = "/Data/Counter", Action = PatchAction.Set, Value = "50" }, // Convertible from string to int
                new() { Path = "/Data/LongCounter", Action = PatchAction.Set, Value = 100L }
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchSetOps }, ct);

        var doc1 = (await provider.ReadDocumentAsync<PatchableDoc>("patch-1", "pk-1", ct))!;
        doc1.Data.Description.ShouldBeNull();
        doc1.Data.Status.ShouldBe(TestStatusEnum.Active);
        doc1.Data.Counter.ShouldBe(50);
        doc1.Data.LongCounter.ShouldBe(100L);

        // 5. Increment with null initial value and default null inc value
        var patchIncOps = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "/Data/Counter", Action = PatchAction.Increment, Value = 5 },
                new() { Path = "/Data/LongCounter", Action = PatchAction.Increment, Value = null } // defaults to +1
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchIncOps }, ct);

        var doc2 = (await provider.ReadDocumentAsync<PatchableDoc>("patch-1", "pk-1", ct))!;
        doc2.Data.Counter.ShouldBe(55);
        doc2.Data.LongCounter.ShouldBe(101L);

        // 6. Append to null List<T> property (auto-instantiates List<T>) and Non-List collection
        var patchAppendOps = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "/Data/TagList", Action = PatchAction.Append, Value = "tag1" },
                new() { Path = "/Data/CustomCol", Action = PatchAction.Append, Value = "item1" }
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchAppendOps }, ct);

        var doc3 = (await provider.ReadDocumentAsync<PatchableDoc>("patch-1", "pk-1", ct))!;
        doc3.Data.TagList.ShouldNotBeNull();
        doc3.Data.TagList.ShouldContain("tag1");
        doc3.Data.CustomCol.Items.ShouldContain("item1");

        // 7. Remove from List<T> and Non-List collection
        var patchRemoveOps = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "/Data/TagList", Action = PatchAction.Remove, Value = "tag1" },
                new() { Path = "/Data/CustomCol", Action = PatchAction.Remove, Value = "item1" }
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchRemoveOps }, ct);

        var doc4 = (await provider.ReadDocumentAsync<PatchableDoc>("patch-1", "pk-1", ct))!;
        doc4.Data.TagList!.ShouldNotContain("tag1");
        doc4.Data.CustomCol.Items.ShouldNotContain("item1");

        // 8. Nested property auto-creation during patch
        var patchNested = new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = "patch-1",
            PartitionKey = "pk-1",
            DocType = nameof(PatchableDoc),
            PatchOperations = new List<PatchOperationData>
            {
                new() { Path = "/Data/SubData/Info", Action = PatchAction.Set, Value = "NestedValue" }
            }
        };
        await provider.ExecuteBatchAsync(new[] { patchNested }, ct);

        var doc5 = (await provider.ReadDocumentAsync<PatchableDoc>("patch-1", "pk-1", ct))!;
        doc5.Data.SubData.ShouldNotBeNull();
        doc5.Data.SubData.Info.ShouldBe("NestedValue");
    }

    [Fact]
    public async Task Dispose_And_DisposeAsync_Execution()
    {
        var provider = new InMemoryStorageProvider();
        provider.Dispose();
        await provider.DisposeAsync();
    }
}
