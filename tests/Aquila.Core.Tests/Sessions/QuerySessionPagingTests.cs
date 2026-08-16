using System.Linq.Expressions;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Queries;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Sessions;

public class QuerySessionPagingTests
{
    public sealed record Book(string Id, string Title, string Genre, decimal Price);

    public sealed class CheapBooksPagedQuery : ICompiledPagedQuery<Book>
    {
        public int PageSize { get; init; } = 2;
        public string? ContinuationToken { get; init; }
        public string? PartitionKey { get; init; }
        public decimal MaxPrice { get; init; } = 20m;

        public Expression<Func<DocumentEnvelope<Book>, bool>>? Predicate() =>
            env => env.Data.Price <= MaxPrice;
    }

    private static (IDocumentStore Store, InMemoryStorageProvider Storage) CreateTestStore(string tenantId = "default")
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions
        {
            DocumentStorage = storage,
            EventStorage = storage,
            DefaultTenantId = tenantId
        };
        options.Schema.For<Book>().Identity(b => b.Id).PartitionKey(b => b.Genre);

        var store = new DocumentStore(options);
        return (store, storage);
    }

    [Fact]
    public async Task QueryPagedAsync_PagesThroughResults_UsingContinuationTokens()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;

        for (int i = 1; i <= 5; i++)
        {
            await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
            {
                Id = $"b-{i}",
                PartitionKey = "SciFi",
                DocType = nameof(Book),
                TenantId = "default",
                Data = new Book($"b-{i}", $"Book {i}", "SciFi", i * 10m)
            }, ct);
        }

        using var session = store.QuerySession();

        // Page 1
        var page1 = await session.QueryPagedAsync<Book>(pageSize: 2, ct: ct);
        page1.Items.Count.ShouldBe(2);
        page1.HasMore.ShouldBeTrue();
        page1.ContinuationToken.ShouldNotBeNullOrWhiteSpace();

        // Page 2
        var page2 = await session.QueryPagedAsync<Book>(pageSize: 2, continuationToken: page1.ContinuationToken, ct: ct);
        page2.Items.Count.ShouldBe(2);
        page2.HasMore.ShouldBeTrue();

        // Page 3 (final item)
        var page3 = await session.QueryPagedAsync<Book>(pageSize: 2, continuationToken: page2.ContinuationToken, ct: ct);
        page3.Items.Count.ShouldBe(1);
        page3.HasMore.ShouldBeFalse();
        page3.ContinuationToken.ShouldBeNull();
    }

    [Fact]
    public async Task QueryPagedAsync_EnforcesTenantIsolation()
    {
        var (store, storage) = CreateTestStore(tenantId: "tenant-A");
        var ct = TestContext.Current.CancellationToken;

        await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
        {
            Id = "b-1",
            PartitionKey = "SciFi",
            DocType = nameof(Book),
            TenantId = "tenant-A",
            Data = new Book("b-1", "Book A", "SciFi", 15m)
        }, ct);

        await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
        {
            Id = "b-2",
            PartitionKey = "SciFi",
            DocType = nameof(Book),
            TenantId = "tenant-B",
            Data = new Book("b-2", "Book B", "SciFi", 25m)
        }, ct);

        using var session = store.QuerySession("tenant-A");
        var paged = await session.QueryPagedAsync<Book>(pageSize: 10, ct: ct);

        paged.Items.Count.ShouldBe(1);
        paged.Items[0].Id.ShouldBe("b-1");
    }

    [Fact]
    public async Task QueryPagedAsync_TracksLoadedEntitiesInIdentityMap()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;

        await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
        {
            Id = "b-track",
            PartitionKey = "Fantasy",
            DocType = nameof(Book),
            TenantId = "default",
            Data = new Book("b-track", "Magic Quest", "Fantasy", 29.99m)
        }, ct);

        using var session = store.OpenSession(TrackingMode.DirtyTracking);
        var paged = await session.QueryPagedAsync<Book>(pageSize: 10, ct: ct);

        paged.Items.Count.ShouldBe(1);
        session.IdentityMap.TryGet<Book>("b-track", out var cached).ShouldBeTrue();
        cached.ShouldNotBeNull();
        cached.Title.ShouldBe("Magic Quest");
    }

    [Fact]
    public async Task QueryPagedByOffsetAsync_ReturnsCorrectPageAndTotalCount()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;

        for (int i = 1; i <= 10; i++)
        {
            await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
            {
                Id = $"b-{i:D2}",
                PartitionKey = "History",
                DocType = nameof(Book),
                TenantId = "default",
                Data = new Book($"b-{i:D2}", $"History Vol {i}", "History", i * 5m)
            }, ct);
        }

        using var session = store.QuerySession();

        // Page 2 with pageSize 3 (items 4, 5, 6)
        var paged = await session.QueryPagedByOffsetAsync<Book>(pageNumber: 2, pageSize: 3, ct: ct);

        paged.Items.Count.ShouldBe(3);
        paged.PageNumber.ShouldBe(2);
        paged.PageSize.ShouldBe(3);
        paged.TotalCount.ShouldBe(10);
        paged.Items[0].Id.ShouldBe("b-04");
        paged.Items[1].Id.ShouldBe("b-05");
        paged.Items[2].Id.ShouldBe("b-06");
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(-1, 10)]
    [InlineData(1, 0)]
    [InlineData(1, -5)]
    public async Task QueryPagedByOffsetAsync_ThrowsOnInvalidPageNumberOrSize(int pageNumber, int pageSize)
    {
        var (store, _) = CreateTestStore();
        using var session = store.QuerySession();

        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await session.QueryPagedByOffsetAsync<Book>(pageNumber, pageSize, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task StreamPagesAsync_YieldsAllPagesSequentially()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;

        for (int i = 1; i <= 7; i++)
        {
            await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
            {
                Id = $"b-{i}",
                PartitionKey = "Tech",
                DocType = nameof(Book),
                TenantId = "default",
                Data = new Book($"b-{i}", $"Tech {i}", "Tech", 50m)
            }, ct);
        }

        using var session = store.QuerySession();

        var pages = new List<PagedResult<Book>>();
        await foreach (var page in session.StreamPagesAsync<Book>(pageSize: 3, ct: ct))
        {
            pages.Add(page);
        }

        pages.Count.ShouldBe(3);
        pages[0].Items.Count.ShouldBe(3);
        pages[1].Items.Count.ShouldBe(3);
        pages[2].Items.Count.ShouldBe(1);
    }

    [Fact]
    public async Task StreamAsync_StreamsAllItemsAcrossPages()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;

        for (int i = 1; i <= 6; i++)
        {
            await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
            {
                Id = $"b-{i}",
                PartitionKey = "Art",
                DocType = nameof(Book),
                TenantId = "default",
                Data = new Book($"b-{i}", $"Art {i}", "Art", 30m)
            }, ct);
        }

        using var session = store.QuerySession();

        var items = new List<Book>();
        await foreach (var item in session.StreamAsync<Book>(batchSize: 2, ct: ct))
        {
            items.Add(item);
        }

        items.Count.ShouldBe(6);
        items.Select(b => b.Id).ShouldBe(new[] { "b-1", "b-2", "b-3", "b-4", "b-5", "b-6" });
    }

    [Fact]
    public async Task QueryPagedAsync_WithCompiledPagedQuery_ExecutesSuccessfully()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;

        await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
        {
            Id = "b-1",
            PartitionKey = "Fiction",
            DocType = nameof(Book),
            TenantId = "default",
            Data = new Book("b-1", "Cheap Novel", "Fiction", 9.99m)
        }, ct);

        await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
        {
            Id = "b-2",
            PartitionKey = "Fiction",
            DocType = nameof(Book),
            TenantId = "default",
            Data = new Book("b-2", "Mid Novel", "Fiction", 18.50m)
        }, ct);

        await storage.UpsertDocumentAsync(new DocumentEnvelope<Book>
        {
            Id = "b-3",
            PartitionKey = "Fiction",
            DocType = nameof(Book),
            TenantId = "default",
            Data = new Book("b-3", "Expensive Novel", "Fiction", 55.00m)
        }, ct);

        using var session = store.QuerySession();

        var query = new CheapBooksPagedQuery { MaxPrice = 20m, PageSize = 10 };
        var result = await session.QueryPagedAsync(query, ct);

        result.Items.Count.ShouldBe(2);
        result.Items.ShouldAllBe(b => b.Price <= 20m);
    }
}
