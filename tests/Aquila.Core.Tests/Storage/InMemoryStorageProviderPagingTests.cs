using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Storage;

public class InMemoryStorageProviderPagingTests
{
    private record Product(string Id, string Name, string Category, decimal Price);

    private static DocumentEnvelope<Product> CreateEnvelope(string id, string name, string category, decimal price) =>
        new()
        {
            Id = id,
            PartitionKey = category,
            DocType = nameof(Product),
            Data = new Product(id, name, category, price)
        };

    [Fact]
    public async Task QueryPagedDocumentsAsync_EmptyStorage_ReturnsEmptyResult()
    {
        var provider = new InMemoryStorageProvider();
        var ct = TestContext.Current.CancellationToken;

        var result = await provider.QueryPagedDocumentsAsync<Product>(ct: ct);

        result.Documents.ShouldBeEmpty();
        result.ContinuationToken.ShouldBeNull();
        result.TotalCount.ShouldBe(0);
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_WithMaxItemCount_ReturnsFirstPageAndContinuationToken()
    {
        var provider = new InMemoryStorageProvider();
        var ct = TestContext.Current.CancellationToken;

        for (int i = 1; i <= 5; i++)
        {
            await provider.UpsertDocumentAsync(CreateEnvelope($"p-{i}", $"Product {i}", "Electronics", i * 10m), ct);
        }

        var options = new QueryOptions { MaxItemCount = 2 };
        var page1 = await provider.QueryPagedDocumentsAsync<Product>(options: options, ct: ct);

        page1.Documents.Count.ShouldBe(2);
        page1.ContinuationToken.ShouldNotBeNullOrWhiteSpace();
        page1.TotalCount.ShouldBe(5);

        // Fetch page 2 using continuation token
        var options2 = new QueryOptions { MaxItemCount = 2, ContinuationToken = page1.ContinuationToken };
        var page2 = await provider.QueryPagedDocumentsAsync<Product>(options: options2, ct: ct);

        page2.Documents.Count.ShouldBe(2);
        page2.ContinuationToken.ShouldNotBeNullOrWhiteSpace();

        // Fetch page 3 (last page with 1 item)
        var options3 = new QueryOptions { MaxItemCount = 2, ContinuationToken = page2.ContinuationToken };
        var page3 = await provider.QueryPagedDocumentsAsync<Product>(options: options3, ct: ct);

        page3.Documents.Count.ShouldBe(1);
        page3.ContinuationToken.ShouldBeNull();
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_OffsetPaging_ReturnsCorrectSliceAndTotalCount()
    {
        var provider = new InMemoryStorageProvider();
        var ct = TestContext.Current.CancellationToken;

        for (int i = 1; i <= 10; i++)
        {
            await provider.UpsertDocumentAsync(CreateEnvelope($"p-{i}", $"Product {i}", "Books", i * 5m), ct);
        }

        // Page 2: Skip 4, Take 3
        var options = new QueryOptions { Skip = 4, MaxItemCount = 3 };
        var result = await provider.QueryPagedDocumentsAsync<Product>(options: options, ct: ct);

        result.Documents.Count.ShouldBe(3);
        result.TotalCount.ShouldBe(10);
        result.ContinuationToken.ShouldBeNull();
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_WithPredicateAndPartitionKey_FiltersCorrectly()
    {
        var provider = new InMemoryStorageProvider();
        var ct = TestContext.Current.CancellationToken;

        await provider.UpsertDocumentAsync(CreateEnvelope("p-1", "Laptop", "Electronics", 1200m), ct);
        await provider.UpsertDocumentAsync(CreateEnvelope("p-2", "Phone", "Electronics", 800m), ct);
        await provider.UpsertDocumentAsync(CreateEnvelope("p-3", "Novel", "Books", 15m), ct);
        await provider.UpsertDocumentAsync(CreateEnvelope("p-4", "Headphones", "Electronics", 150m), ct);

        var options = new QueryOptions { PartitionKey = "Electronics", MaxItemCount = 10 };
        var result = await provider.QueryPagedDocumentsAsync<Product>(
            predicate: env => env.Data.Price > 500m,
            options: options,
            ct: ct);

        result.Documents.Count.ShouldBe(2);
        result.Documents.ShouldAllBe(d => d.Data.Category == "Electronics" && d.Data.Price > 500m);
    }

    [Fact]
    public async Task QueryPagedDocumentsAsync_InvalidContinuationToken_FallsBackToBeginning()
    {
        var provider = new InMemoryStorageProvider();
        var ct = TestContext.Current.CancellationToken;

        await provider.UpsertDocumentAsync(CreateEnvelope("p-1", "A", "Cat", 10m), ct);
        await provider.UpsertDocumentAsync(CreateEnvelope("p-2", "B", "Cat", 20m), ct);

        var options = new QueryOptions { ContinuationToken = "invalid-token-format", MaxItemCount = 1 };
        var result = await provider.QueryPagedDocumentsAsync<Product>(options: options, ct: ct);

        result.Documents.Count.ShouldBe(1);
        result.ContinuationToken.ShouldNotBeNull();
    }
}
