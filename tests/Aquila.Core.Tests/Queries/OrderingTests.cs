using System.Linq.Expressions;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Queries;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Queries;

public sealed record ProductItem(string Id, string Name, string Category, decimal Price, int Stock, DateTime CreatedAt);

public sealed class SortedProductsPagedQuery : ICompiledPagedQuery<ProductItem>
{
    public int PageSize { get; init; } = 3;
    public string? ContinuationToken { get; init; }
    public string? PartitionKey { get; init; }

    public Expression<Func<DocumentEnvelope<ProductItem>, bool>>? Predicate() =>
        env => env.Data.Price > 10m;

    public Expression<Func<DocumentEnvelope<ProductItem>, object?>>? OrderBy() =>
        env => env.Data.Price;

    public SortOrder SortOrder => SortOrder.Descending;
}

public sealed class MultiSortedProductsPagedQuery : ICompiledPagedQuery<ProductItem>
{
    public int PageSize { get; init; } = 10;
    public string? ContinuationToken { get; init; }
    public string? PartitionKey { get; init; }

    public Expression<Func<DocumentEnvelope<ProductItem>, bool>>? Predicate() => null;

    public IEnumerable<SortOrderDefinition<ProductItem>>? Orderings() =>
        new[]
        {
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Category),
            SortOrderDefinition<ProductItem>.Descending(env => env.Data.Price)
        };
}

public class OrderingTests
{
    private static (IDocumentStore Store, InMemoryStorageProvider Storage) CreateTestStore(string tenantId = "default")
    {
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions
        {
            DocumentStorage = storage,
            EventStorage = storage,
            DefaultTenantId = tenantId
        };
        options.Schema.For<ProductItem>().Identity(p => p.Id).PartitionKey(p => p.Category);

        var store = new DocumentStore(options);
        return (store, storage);
    }

    private static async Task SeedProductsAsync(InMemoryStorageProvider storage, CancellationToken ct)
    {
        var products = new List<ProductItem>
        {
            new("p-1", "Laptop Pro", "Electronics", 1200m, 10, new DateTime(2025, 1, 15)),
            new("p-2", "Wireless Mouse", "Electronics", 25m, 150, new DateTime(2025, 2, 1)),
            new("p-3", "4K Monitor", "Electronics", 450m, 30, new DateTime(2025, 1, 20)),
            new("p-4", "Mechanical Keyboard", "Electronics", 120m, 80, new DateTime(2025, 3, 10)),
            new("p-5", "Sci-Fi Novel", "Books", 15m, 200, new DateTime(2025, 1, 5)),
            new("p-6", "Cookbook Master", "Books", 35m, 50, new DateTime(2025, 2, 25)),
            new("p-7", "Desk Lamp", "Furniture", 45m, 40, new DateTime(2025, 4, 1)),
            new("p-8", "Ergonomic Chair", "Furniture", 350m, 15, new DateTime(2025, 3, 15)),
        };

        foreach (var p in products)
        {
            await storage.UpsertDocumentAsync(new DocumentEnvelope<ProductItem>
            {
                Id = p.Id,
                PartitionKey = p.Category,
                DocType = nameof(ProductItem),
                TenantId = "default",
                Data = p
            }, ct);
        }
    }

    [Fact]
    public async Task QueryAsync_WithSingleOrderByAscending_ReturnsSortedResults()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var results = await session.QueryAsync<ProductItem>(
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Ascending,
            ct: ct);

        results.Count.ShouldBe(8);
        results.Select(r => r.Price).ShouldBeInOrder(SortDirection.Ascending);
        results[0].Price.ShouldBe(15m);
        results[^1].Price.ShouldBe(1200m);
    }

    [Fact]
    public async Task QueryAsync_WithSingleOrderByDescending_ReturnsSortedResults()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var results = await session.QueryAsync<ProductItem>(
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Descending,
            ct: ct);

        results.Count.ShouldBe(8);
        results.Select(r => r.Price).ShouldBeInOrder(SortDirection.Descending);
        results[0].Price.ShouldBe(1200m);
        results[^1].Price.ShouldBe(15m);
    }

    [Fact]
    public async Task QueryAsync_WithPredicateAndOrderBy_FiltersAndSorts()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var results = await session.QueryAsync<ProductItem>(
            predicate: env => env.Data.Category == "Electronics",
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Ascending,
            ct: ct);

        results.Count.ShouldBe(4);
        results.ShouldAllBe(r => r.Category == "Electronics");
        results.Select(r => r.Price).ShouldBe(new[] { 25m, 120m, 450m, 1200m });
    }

    [Fact]
    public async Task QueryAsync_WithMultipleOrderings_AppliesCompositeSort()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var orderings = new[]
        {
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Category),
            SortOrderDefinition<ProductItem>.Descending(env => env.Data.Price)
        };

        var results = await session.QueryAsync<ProductItem>(
            predicate: null,
            orderings: orderings,
            ct: ct);

        results.Count.ShouldBe(8);

        // Books: Cookbook Master (35), Sci-Fi Novel (15)
        // Electronics: Laptop Pro (1200), 4K Monitor (450), Mechanical Keyboard (120), Wireless Mouse (25)
        // Furniture: Ergonomic Chair (350), Desk Lamp (45)
        var categories = results.Select(r => r.Category).Distinct().ToList();
        categories.ShouldBe(new[] { "Books", "Electronics", "Furniture" });

        var bookPrices = results.Where(r => r.Category == "Books").Select(r => r.Price).ToList();
        bookPrices.ShouldBe(new[] { 35m, 15m });

        var electronicsPrices = results.Where(r => r.Category == "Electronics").Select(r => r.Price).ToList();
        electronicsPrices.ShouldBe(new[] { 1200m, 450m, 120m, 25m });

        var furniturePrices = results.Where(r => r.Category == "Furniture").Select(r => r.Price).ToList();
        furniturePrices.ShouldBe(new[] { 350m, 45m });
    }

    [Fact]
    public async Task QueryAsync_WithQueryOptions_FluentBuilder_ExecutesSuccessfully()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var options = new QueryOptions()
            .OrderBy<ProductItem>(env => env.Data.Stock, SortOrder.Ascending);

        var results = await session.QueryAsync<ProductItem>(
            predicate: null,
            options: options,
            ct: ct);

        results.Count.ShouldBe(8);
        results.Select(r => r.Stock).ShouldBeInOrder(SortDirection.Ascending);
        results[0].Stock.ShouldBe(10);
        results[^1].Stock.ShouldBe(200);
    }

    [Fact]
    public async Task QueryPagedAsync_WithOrderBy_SortsAcrossContinuationPages()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();

        // Page 1 (PageSize: 3, Ordered by Price Ascending)
        var page1 = await session.QueryPagedAsync<ProductItem>(
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Ascending,
            pageSize: 3,
            ct: ct);

        page1.Items.Count.ShouldBe(3);
        page1.Items.Select(r => r.Price).ShouldBe(new[] { 15m, 25m, 35m });
        page1.HasMore.ShouldBeTrue();

        // Page 2
        var page2 = await session.QueryPagedAsync<ProductItem>(
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Ascending,
            pageSize: 3,
            continuationToken: page1.ContinuationToken,
            ct: ct);

        page2.Items.Count.ShouldBe(3);
        page2.Items.Select(r => r.Price).ShouldBe(new[] { 45m, 120m, 350m });
        page2.HasMore.ShouldBeTrue();

        // Page 3 (final 2 items)
        var page3 = await session.QueryPagedAsync<ProductItem>(
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Ascending,
            pageSize: 3,
            continuationToken: page2.ContinuationToken,
            ct: ct);

        page3.Items.Count.ShouldBe(2);
        page3.Items.Select(r => r.Price).ShouldBe(new[] { 450m, 1200m });
        page3.HasMore.ShouldBeFalse();
    }

    [Fact]
    public async Task QueryPagedAsync_WithMultipleOrderings_PagesCorrectly()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var orderings = new[]
        {
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Category),
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Price)
        };

        var page1 = await session.QueryPagedAsync<ProductItem>(
            predicate: null,
            orderings: orderings,
            pageSize: 4,
            ct: ct);

        page1.Items.Count.ShouldBe(4);
        page1.Items.Select(r => r.Id).ShouldBe(new[] { "p-5", "p-6", "p-2", "p-4" });
        page1.HasMore.ShouldBeTrue();

        var page2 = await session.QueryPagedAsync<ProductItem>(
            predicate: null,
            orderings: orderings,
            pageSize: 4,
            continuationToken: page1.ContinuationToken,
            ct: ct);

        page2.Items.Count.ShouldBe(4);
        page2.Items.Select(r => r.Id).ShouldBe(new[] { "p-3", "p-1", "p-7", "p-8" });
        page2.HasMore.ShouldBeFalse();
    }

    [Fact]
    public async Task QueryPagedByOffsetAsync_WithOrderBy_ReturnsCorrectPage()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();

        // Page 2, PageSize 3, Ordered by Price Descending (Items 4, 5, 6 out of 8)
        // All prices descending: 1200, 450, 350, 120, 45, 35, 25, 15
        // Page 1: 1200, 450, 350
        // Page 2: 120, 45, 35
        // Page 3: 25, 15
        var paged = await session.QueryPagedByOffsetAsync<ProductItem>(
            pageNumber: 2,
            pageSize: 3,
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Descending,
            ct: ct);

        paged.Items.Count.ShouldBe(3);
        paged.PageNumber.ShouldBe(2);
        paged.PageSize.ShouldBe(3);
        paged.TotalCount.ShouldBe(8);
        paged.Items.Select(r => r.Price).ShouldBe(new[] { 120m, 45m, 35m });
    }

    [Fact]
    public async Task QueryPagedByOffsetAsync_WithMultipleOrderings_ReturnsCorrectPage()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var orderings = new[]
        {
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Category),
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Price)
        };

        // All items sorted: Books (15, 35), Electronics (25, 120, 450, 1200), Furniture (45, 350)
        // Page 2 with pageSize 3 -> items 4, 5, 6 -> Electronics (120), Electronics (450), Electronics (1200)
        var paged = await session.QueryPagedByOffsetAsync<ProductItem>(
            pageNumber: 2,
            pageSize: 3,
            predicate: null,
            orderings: orderings,
            ct: ct);

        paged.Items.Count.ShouldBe(3);
        paged.Items.Select(r => r.Price).ShouldBe(new[] { 120m, 450m, 1200m });
    }

    [Fact]
    public async Task StreamPagesAsync_WithOrderBy_StreamsSortedPages()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var pages = new List<PagedResult<ProductItem>>();

        await foreach (var page in session.StreamPagesAsync<ProductItem>(
            predicate: null,
            orderBy: env => env.Data.Price,
            sortOrder: SortOrder.Ascending,
            pageSize: 3,
            ct: ct))
        {
            pages.Add(page);
        }

        pages.Count.ShouldBe(3);
        var allStreamedPrices = pages.SelectMany(p => p.Items).Select(r => r.Price).ToList();
        allStreamedPrices.ShouldBeInOrder(SortDirection.Ascending);
        allStreamedPrices.Count.ShouldBe(8);
    }

    [Fact]
    public async Task StreamAsync_WithMultipleOrderings_StreamsAllSortedItems()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var orderings = new[]
        {
            SortOrderDefinition<ProductItem>.Ascending(env => env.Data.Category),
            SortOrderDefinition<ProductItem>.Descending(env => env.Data.Price)
        };

        var items = new List<ProductItem>();
        await foreach (var item in session.StreamAsync<ProductItem>(
            predicate: null,
            orderings: orderings,
            batchSize: 2,
            ct: ct))
        {
            items.Add(item);
        }

        items.Count.ShouldBe(8);
        items.Select(r => r.Id).ShouldBe(new[] { "p-6", "p-5", "p-1", "p-3", "p-4", "p-2", "p-8", "p-7" });
    }

    [Fact]
    public async Task QueryPagedAsync_WithCompiledPagedQuery_OrderBy_ExecutesCorrectly()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var query = new SortedProductsPagedQuery { PageSize = 4 };

        var result = await session.QueryPagedAsync(query, ct);

        // Price > 10m, Ordered by Price Descending (1200, 450, 350, 120, ...)
        result.Items.Count.ShouldBe(4);
        result.Items.Select(r => r.Price).ShouldBe(new[] { 1200m, 450m, 350m, 120m });
    }

    [Fact]
    public async Task QueryPagedAsync_WithCompiledPagedQuery_MultiOrderings_ExecutesCorrectly()
    {
        var (store, storage) = CreateTestStore();
        var ct = TestContext.Current.CancellationToken;
        await SeedProductsAsync(storage, ct);

        using var session = store.QuerySession();
        var query = new MultiSortedProductsPagedQuery { PageSize = 10 };

        var result = await session.QueryPagedAsync(query, ct);

        result.Items.Count.ShouldBe(8);
        result.Items.Select(r => r.Id).ShouldBe(new[] { "p-6", "p-5", "p-1", "p-3", "p-4", "p-2", "p-8", "p-7" });
    }

    [Fact]
    public async Task QueryAsync_ThrowsArgumentNullException_OnNullOrderByOrOrderings()
    {
        var (store, _) = CreateTestStore();
        using var session = store.QuerySession();

        await Should.ThrowAsync<ArgumentNullException>(() =>
            session.QueryAsync<ProductItem>(null, (Expression<Func<DocumentEnvelope<ProductItem>, object?>>)null!, SortOrder.Ascending));

        await Should.ThrowAsync<ArgumentNullException>(() =>
            session.QueryAsync<ProductItem>(null, (IEnumerable<SortOrderDefinition<ProductItem>>)null!));
    }
}
