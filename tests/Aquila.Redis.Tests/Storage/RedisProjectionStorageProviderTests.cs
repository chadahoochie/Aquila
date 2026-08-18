using Aquila.Core.Storage;
using Aquila.Redis.Configuration;
using Aquila.Redis.Storage;
using Aquila.Redis.Tests.Fixtures;
using Shouldly;

namespace Aquila.Redis.Tests.Storage;

public class RedisProjectionStorageProviderTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fixture;

    public RedisProjectionStorageProviderTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    public class CustomerView
    {
        public string Id { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public decimal TotalSpend { get; set; }
    }

    public class OtherView
    {
        public string Id { get; set; } = string.Empty;
        public string Title { get; set; } = string.Empty;
    }

    [Fact]
    public async Task PurgeProjectionAsync_UnlinksOnlyTargetReadModelKeys()
    {
        var prefix = $"test:{Guid.NewGuid():N}:";
        var options = new RedisStorageOptions { KeyPrefix = prefix, BatchChunkSize = 10 };
        var provider = new RedisProjectionStorageProvider(_fixture.Multiplexer, options);

        // Store 25 CustomerView read models
        for (int i = 1; i <= 25; i++)
        {
            await provider.UpsertDocumentAsync(new DocumentEnvelope<CustomerView>
            {
                Id = $"cust-{i}",
                PartitionKey = $"pk-{i}",
                DocType = nameof(CustomerView),
                Data = new CustomerView { Id = $"cust-{i}", CustomerName = $"Customer {i}", TotalSpend = i * 100m }
            }, TestContext.Current.CancellationToken);
        }

        // Store 5 OtherView read models (should NOT be purged)
        for (int i = 1; i <= 5; i++)
        {
            await provider.UpsertDocumentAsync(new DocumentEnvelope<OtherView>
            {
                Id = $"other-{i}",
                PartitionKey = $"pk-{i}",
                DocType = nameof(OtherView),
                Data = new OtherView { Id = $"other-{i}", Title = $"Other {i}" }
            }, TestContext.Current.CancellationToken);
        }

        // Verify loaded before purge
        var sampleBefore = await provider.ReadDocumentAsync<CustomerView>("cust-1", "pk-1", TestContext.Current.CancellationToken);
        sampleBefore.ShouldNotBeNull();

        var otherBefore = await provider.ReadDocumentAsync<OtherView>("other-1", "pk-1", TestContext.Current.CancellationToken);
        otherBefore.ShouldNotBeNull();

        // Purge CustomerView projection
        await provider.PurgeProjectionAsync("CustomerViewProjection", typeof(CustomerView), TestContext.Current.CancellationToken);

        // Verify all CustomerViews are wiped
        for (int i = 1; i <= 25; i++)
        {
            var loaded = await provider.ReadDocumentAsync<CustomerView>($"cust-{i}", $"pk-{i}", TestContext.Current.CancellationToken);
            loaded.ShouldBeNull();
        }

        // Verify OtherViews remain untouched
        for (int i = 1; i <= 5; i++)
        {
            var loaded = await provider.ReadDocumentAsync<OtherView>($"other-{i}", $"pk-{i}", TestContext.Current.CancellationToken);
            loaded.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task Validation_ThrowsOnNullAndInvalidArguments()
    {
        Should.Throw<ArgumentNullException>(() => new RedisProjectionStorageProvider(null!));

        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisProjectionStorageProvider(_fixture.Multiplexer, options);

        await Should.ThrowAsync<ArgumentException>(() => provider.PurgeProjectionAsync("", typeof(CustomerView), TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => provider.PurgeProjectionAsync("   ", typeof(CustomerView), TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.PurgeProjectionAsync("proj", null!, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Metadata_And_Lifecycle_WorkCorrectly()
    {
        var options = new RedisStorageOptions();
        var provider = new RedisProjectionStorageProvider(_fixture.Multiplexer, options);

        provider.ProviderName.ShouldBe("Redis");
        provider.LastRequestCharge.ShouldBe(0.0);
        provider.CumulativeRequestCharge.ShouldBe(0.0);

        await provider.InitializeAsync(TestContext.Current.CancellationToken);
        provider.Dispose();
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task Delegated_CRUD_And_Query_Operations_Work()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisProjectionStorageProvider(_fixture.Multiplexer, options);

        var doc = new DocumentEnvelope<CustomerView>
        {
            Id = "cv-1",
            PartitionKey = "pk-1",
            DocType = nameof(CustomerView),
            Data = new CustomerView { Id = "cv-1", CustomerName = "Charlie", TotalSpend = 500m }
        };

        // Upsert
        await provider.UpsertDocumentAsync(doc, TestContext.Current.CancellationToken);

        // Read
        var loaded = await provider.ReadDocumentAsync<CustomerView>("cv-1", "pk-1", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Data.CustomerName.ShouldBe("Charlie");

        // Query
        var queryResults = await provider.QueryDocumentsAsync<CustomerView>(c => c.Data.TotalSpend >= 500m, ct: TestContext.Current.CancellationToken);
        queryResults.Count.ShouldBe(1);

        // Paged query
        var pagedResults = await provider.QueryPagedDocumentsAsync<CustomerView>(c => c.Data.TotalSpend >= 500m, new Aquila.Core.Storage.QueryOptions { MaxItemCount = 10 }, TestContext.Current.CancellationToken);
        pagedResults.Documents.Count.ShouldBe(1);

        // Batch
        var batchOp = new StorageOperation
        {
            OperationType = StorageOperationType.Upsert,
            Id = "cv-batch",
            PartitionKey = "pk-b",
            DocType = nameof(CustomerView),
            Document = new DocumentEnvelope<CustomerView>
            {
                Id = "cv-batch",
                PartitionKey = "pk-b",
                DocType = nameof(CustomerView),
                Data = new CustomerView { Id = "cv-batch", CustomerName = "BatchCustomer", TotalSpend = 100m }
            }
        };
        await provider.ExecuteBatchAsync(new[] { batchOp }, TestContext.Current.CancellationToken);

        var loadedBatch = await provider.ReadDocumentAsync<CustomerView>("cv-batch", "pk-b", TestContext.Current.CancellationToken);
        loadedBatch.ShouldNotBeNull();

        // Delete
        await provider.DeleteDocumentAsync<CustomerView>("cv-1", "pk-1", TestContext.Current.CancellationToken);
        var deleted = await provider.ReadDocumentAsync<CustomerView>("cv-1", "pk-1", TestContext.Current.CancellationToken);
        deleted.ShouldBeNull();
    }
}
