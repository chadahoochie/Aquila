using System.Linq.Expressions;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Aquila.Redis.Configuration;
using Aquila.Redis.Storage;
using Aquila.Redis.Tests.Fixtures;
using Shouldly;

namespace Aquila.Redis.Tests.Storage;

public class RedisDocumentStorageProviderTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fixture;

    public RedisDocumentStorageProviderTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    public class UserProfile
    {
        public string Id { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    [Fact]
    public async Task ReadDocumentAsync_ReturnsNull_WhenNotFound()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        var doc = await provider.ReadDocumentAsync<UserProfile>("non-existent", "non-existent", TestContext.Current.CancellationToken);
        doc.ShouldBeNull();
    }

    [Fact]
    public async Task UpsertDocumentAsync_And_ReadDocumentAsync_RoundtripsSuccessfully()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        var envelope = new DocumentEnvelope<UserProfile>
        {
            Id = "user-1",
            PartitionKey = "pk-1",
            DocType = nameof(UserProfile),
            TenantId = "default",
            Data = new UserProfile { Id = "user-1", Username = "alice", Age = 30 }
        };

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        var loaded = await provider.ReadDocumentAsync<UserProfile>("user-1", "pk-1", TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe("user-1");
        loaded.Data.Username.ShouldBe("alice");
        loaded.Data.Age.ShouldBe(30);
    }

    [Fact]
    public async Task DeleteDocumentAsync_RemovesKeyFromRedis()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        var envelope = new DocumentEnvelope<UserProfile>
        {
            Id = "user-to-del",
            PartitionKey = "pk-1",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "user-to-del", Username = "bob", Age = 25 }
        };

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);
        var loadedBefore = await provider.ReadDocumentAsync<UserProfile>("user-to-del", "pk-1", TestContext.Current.CancellationToken);
        loadedBefore.ShouldNotBeNull();

        await provider.DeleteDocumentAsync<UserProfile>("user-to-del", "pk-1", TestContext.Current.CancellationToken);
        var loadedAfter = await provider.ReadDocumentAsync<UserProfile>("user-to-del", "pk-1", TestContext.Current.CancellationToken);
        loadedAfter.ShouldBeNull();
    }

    [Fact]
    public async Task ExecuteBatchAsync_PipelinesOperationsInSingleBatch()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        var operations = new List<StorageOperation>();
        for (int i = 1; i <= 50; i++)
        {
            operations.Add(new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = $"batch-user-{i}",
                PartitionKey = "batch-pk",
                DocType = nameof(UserProfile),
                Document = new DocumentEnvelope<UserProfile>
                {
                    Id = $"batch-user-{i}",
                    PartitionKey = "batch-pk",
                    DocType = nameof(UserProfile),
                    Data = new UserProfile { Id = $"batch-user-{i}", Username = $"user_{i}", Age = 20 + i }
                }
            });
        }

        await provider.ExecuteBatchAsync(operations, TestContext.Current.CancellationToken);

        var first = await provider.ReadDocumentAsync<UserProfile>("batch-user-1", "batch-pk", TestContext.Current.CancellationToken);
        first.ShouldNotBeNull();
        first.Data.Username.ShouldBe("user_1");

        var last = await provider.ReadDocumentAsync<UserProfile>("batch-user-50", "batch-pk", TestContext.Current.CancellationToken);
        last.ShouldNotBeNull();
        last.Data.Username.ShouldBe("user_50");
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_FiltersAndPaginates()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        for (int i = 1; i <= 10; i++)
        {
            await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
            {
                Id = $"paged-{i:D2}",
                PartitionKey = "pk",
                DocType = nameof(UserProfile),
                Data = new UserProfile { Id = $"paged-{i:D2}", Username = $"user_{i}", Age = i * 5 }
            }, TestContext.Current.CancellationToken);
        }

        // Query with predicate: Age > 20 (i.e. i >= 5, so 6 items: 5,6,7,8,9,10)
        var queryOptions = new QueryOptions { MaxItemCount = 3 };
        var page1 = await provider.QueryPagedDocumentsAsync<UserProfile>(e => e.Data.Age > 20, queryOptions, TestContext.Current.CancellationToken);

        page1.Documents.Count.ShouldBe(3);
        page1.ContinuationToken.ShouldNotBeNull();

        var queryOptions2 = new QueryOptions { MaxItemCount = 3, ContinuationToken = page1.ContinuationToken };
        var page2 = await provider.QueryPagedDocumentsAsync<UserProfile>(e => e.Data.Age > 20, queryOptions2, TestContext.Current.CancellationToken);
        page2.Documents.Count.ShouldBe(3);
    }

    [Fact]
    public async Task Validation_ThrowsOnNullAndInvalidArguments()
    {
        Should.Throw<ArgumentNullException>(() => new RedisDocumentStorageProvider(null!));

        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        await Should.ThrowAsync<ArgumentException>(() => provider.ReadDocumentAsync<UserProfile>("", "pk", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => provider.ReadDocumentAsync<UserProfile>("   ", "pk", TestContext.Current.CancellationToken));

        await Should.ThrowAsync<ArgumentNullException>(() => provider.UpsertDocumentAsync<UserProfile>(null!, TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile> { Id = "" }, TestContext.Current.CancellationToken));

        await Should.ThrowAsync<ArgumentException>(() => provider.DeleteDocumentAsync<UserProfile>("", "pk", TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => provider.DeleteDocumentAsync<UserProfile>("   ", "pk", TestContext.Current.CancellationToken));

        await Should.ThrowAsync<ArgumentNullException>(() => provider.ExecuteBatchAsync(null!, TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => provider.ExecuteBatchAsync(new[] { new StorageOperation { Id = "" } }, TestContext.Current.CancellationToken));

        // Empty operations does not throw
        await provider.ExecuteBatchAsync(Enumerable.Empty<StorageOperation>(), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Metadata_And_Lifecycle_WorkCorrectly()
    {
        var options = new RedisStorageOptions();
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        provider.ProviderName.ShouldBe("Redis");
        provider.LastRequestCharge.ShouldBe(0.0);
        provider.CumulativeRequestCharge.ShouldBe(0.0);

        await provider.InitializeAsync(TestContext.Current.CancellationToken);
        provider.Dispose();
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task ExecuteBatchAsync_SupportsDelete_And_PatchOperations()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        // First create a user to delete and one to patch
        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "user-del-batch",
            PartitionKey = "pk-b",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "user-del-batch", Username = "delme", Age = 20 }
        }, TestContext.Current.CancellationToken);

        var batchOps = new List<StorageOperation>
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Delete,
                Id = "user-del-batch",
                PartitionKey = "pk-b",
                DocType = nameof(UserProfile)
            },
            new StorageOperation
            {
                OperationType = StorageOperationType.Patch,
                Id = "user-patched",
                PartitionKey = "pk-b",
                DocType = nameof(UserProfile),
                Document = new DocumentEnvelope<UserProfile>
                {
                    Id = "user-patched",
                    PartitionKey = "pk-b",
                    DocType = nameof(UserProfile),
                    Data = new UserProfile { Id = "user-patched", Username = "patched", Age = 99 }
                }
            }
        };

        await provider.ExecuteBatchAsync(batchOps, TestContext.Current.CancellationToken);

        var deleted = await provider.ReadDocumentAsync<UserProfile>("user-del-batch", "pk-b", TestContext.Current.CancellationToken);
        deleted.ShouldBeNull();

        var patched = await provider.ReadDocumentAsync<UserProfile>("user-patched", "pk-b", TestContext.Current.CancellationToken);
        patched.ShouldNotBeNull();
        patched.Data.Username.ShouldBe("patched");
        patched.Data.Age.ShouldBe(99);
    }

    [Fact]
    public async Task QueryDocumentsAsync_And_PartitionKeyFilter_Work()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "u-pk1",
            PartitionKey = "region-east",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "u-pk1", Username = "east_user", Age = 30 }
        }, TestContext.Current.CancellationToken);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "u-pk2",
            PartitionKey = "region-west",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "u-pk2", Username = "west_user", Age = 30 }
        }, TestContext.Current.CancellationToken);

        var allEast = await provider.QueryDocumentsAsync<UserProfile>(
            predicate: u => u.Data.Age == 30,
            options: new QueryOptions { PartitionKey = "region-east" },
            ct: TestContext.Current.CancellationToken);

        allEast.Count.ShouldBe(1);
        allEast[0].Id.ShouldBe("u-pk1");
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_With_Skip_OffsetPagination()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        for (int i = 1; i <= 5; i++)
        {
            await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
            {
                Id = $"skip-{i:D2}",
                PartitionKey = "pk",
                DocType = nameof(UserProfile),
                Data = new UserProfile { Id = $"skip-{i:D2}", Username = $"user_{i}", Age = i }
            }, TestContext.Current.CancellationToken);
        }

        var paged = await provider.QueryPagedDocumentsAsync<UserProfile>(
            options: new QueryOptions { Skip = 2, MaxItemCount = 2 },
            ct: TestContext.Current.CancellationToken);

        paged.Documents.Count.ShouldBe(2);
        paged.TotalCount.ShouldBe(5);
        paged.Documents[0].Id.ShouldBe("skip-03");
        paged.Documents[1].Id.ShouldBe("skip-04");
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_With_Complex_Orderings()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "ord-1",
            PartitionKey = "pk",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "ord-1", Username = "Charlie", Age = 30 }
        }, TestContext.Current.CancellationToken);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "ord-2",
            PartitionKey = "pk",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "ord-2", Username = "Alice", Age = 40 }
        }, TestContext.Current.CancellationToken);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "ord-3",
            PartitionKey = "pk",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "ord-3", Username = "Bob", Age = 30 }
        }, TestContext.Current.CancellationToken);

        // Order by Age Ascending, then Username Ascending (testing both DocumentEnvelope<T> and T lambda forms)
        Expression<Func<DocumentEnvelope<UserProfile>, object?>> ageExpr = e => e.Data.Age;
        Expression<Func<UserProfile, object?>> usernameExpr = u => u.Username;

        var queryOptions = new QueryOptions
        {
            Orderings = new List<SortDescriptor>
            {
                new SortDescriptor(ageExpr, SortOrder.Ascending),
                new SortDescriptor(usernameExpr, SortOrder.Ascending)
            }
        };

        var results = await provider.QueryPagedDocumentsAsync<UserProfile>(options: queryOptions, ct: TestContext.Current.CancellationToken);
        results.Documents.Count.ShouldBe(3);
        // Age 30: Bob before Charlie; then Age 40: Alice
        results.Documents[0].Data.Username.ShouldBe("Bob");
        results.Documents[1].Data.Username.ShouldBe("Charlie");
        results.Documents[2].Data.Username.ShouldBe("Alice");

        // Order by Age Descending
        var descOptions = new QueryOptions
        {
            Orderings = new List<SortDescriptor>
            {
                new SortDescriptor(ageExpr, SortOrder.Descending),
                new SortDescriptor(usernameExpr, SortOrder.Descending)
            }
        };

        var descResults = await provider.QueryPagedDocumentsAsync<UserProfile>(options: descOptions, ct: TestContext.Current.CancellationToken);
        descResults.Documents[0].Data.Username.ShouldBe("Alice"); // Age 40
        descResults.Documents[1].Data.Username.ShouldBe("Charlie"); // Age 30
        descResults.Documents[2].Data.Username.ShouldBe("Bob"); // Age 30
    }

    [Fact]
    public void NullSafeComparer_ComparesDifferentTypesAndNulls()
    {
        var comparer = RedisDocumentStorageProvider.NullSafeComparer.Instance;

        string? nullStr1 = null;
        string? nullStr2 = null;
        comparer.Compare(nullStr1, nullStr2).ShouldBe(0);
        comparer.Compare(nullStr1, "a").ShouldBe(-1);
        comparer.Compare("a", nullStr1).ShouldBe(1);

        var obj = new object();
        comparer.Compare(obj, obj).ShouldBe(0);

        comparer.Compare(10, 20).ShouldBeLessThan(0);
        comparer.Compare(20, 10).ShouldBeGreaterThan(0);
        comparer.Compare(10, 10).ShouldBe(0);

        // Convertible type: int vs string representation
        comparer.Compare(10, "10").ShouldBe(0);

        // Inconvertible IComparable instances fallback to string comparison
        var date = new DateTime(2025, 1, 1);
        comparer.Compare(date, 12345).ShouldNotBe(0);
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_With_EmptyOrderings_And_InvalidContinuationToken()
    {
        var options = new RedisStorageOptions { KeyPrefix = $"test:{Guid.NewGuid():N}:" };
        var provider = new RedisDocumentStorageProvider(_fixture.Multiplexer, options);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<UserProfile>
        {
            Id = "item-1",
            PartitionKey = "pk",
            DocType = nameof(UserProfile),
            Data = new UserProfile { Id = "item-1", Username = "user1", Age = 25 }
        }, TestContext.Current.CancellationToken);

        // Empty Orderings list
        var emptyOrderingsResult = await provider.QueryPagedDocumentsAsync<UserProfile>(
            options: new QueryOptions { Orderings = new List<SortDescriptor>() },
            ct: TestContext.Current.CancellationToken);
        emptyOrderingsResult.Documents.Count.ShouldBe(1);

        // Invalid continuation token (non-base64)
        var invalidTokenResult = await provider.QueryPagedDocumentsAsync<UserProfile>(
            options: new QueryOptions { ContinuationToken = "not-valid-base64!!!" },
            ct: TestContext.Current.CancellationToken);
        invalidTokenResult.Documents.Count.ShouldBe(1);
    }
}
