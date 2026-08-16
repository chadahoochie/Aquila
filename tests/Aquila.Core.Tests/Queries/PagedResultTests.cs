using Aquila.Core.Queries;
using Shouldly;

namespace Aquila.Core.Tests.Queries;

public class PagedResultTests
{
    private record SampleItem(string Id, string Name);

    [Fact]
    public void DefaultConstructor_InitializesWithSensibleDefaults()
    {
        var result = new PagedResult<SampleItem>();

        result.Items.ShouldNotBeNull();
        result.Items.ShouldBeEmpty();
        result.ContinuationToken.ShouldBeNull();
        result.HasMore.ShouldBeFalse();
        result.TotalCount.ShouldBeNull();
        result.PageNumber.ShouldBeNull();
        result.PageSize.ShouldBeNull();
    }

    [Fact]
    public void ContinuationTokenConstructor_WithValidToken_SetsPropertiesCorrectly()
    {
        var items = new List<SampleItem>
        {
            new("1", "First"),
            new("2", "Second")
        };

        var result = new PagedResult<SampleItem>(items, "next-page-token-123", pageSize: 2);

        result.Items.Count.ShouldBe(2);
        result.Items[0].Id.ShouldBe("1");
        result.ContinuationToken.ShouldBe("next-page-token-123");
        result.HasMore.ShouldBeTrue();
        result.PageSize.ShouldBe(2);
        result.TotalCount.ShouldBeNull();
    }

    [Fact]
    public void ContinuationTokenConstructor_WithNullOrWhitespaceToken_NormalizesToNull()
    {
        var items = new List<SampleItem> { new("1", "Solo") };

        var resultNull = new PagedResult<SampleItem>(items, null);
        resultNull.ContinuationToken.ShouldBeNull();
        resultNull.HasMore.ShouldBeFalse();
        resultNull.PageSize.ShouldBe(1);

        var resultEmpty = new PagedResult<SampleItem>(items, "");
        resultEmpty.ContinuationToken.ShouldBeNull();
        resultEmpty.HasMore.ShouldBeFalse();

        var resultWhitespace = new PagedResult<SampleItem>(items, "   ");
        resultWhitespace.ContinuationToken.ShouldBeNull();
        resultWhitespace.HasMore.ShouldBeFalse();
    }

    [Fact]
    public void OffsetConstructor_WithValidParameters_SetsPropertiesCorrectly()
    {
        var items = new List<SampleItem>
        {
            new("1", "Alpha"),
            new("2", "Beta")
        };

        var result = new PagedResult<SampleItem>(items, pageNumber: 2, pageSize: 10, totalCount: 42);

        result.Items.Count.ShouldBe(2);
        result.PageNumber.ShouldBe(2);
        result.PageSize.ShouldBe(10);
        result.TotalCount.ShouldBe(42);
        result.ContinuationToken.ShouldBeNull();
        result.HasMore.ShouldBeFalse();
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(-1, 10)]
    [InlineData(1, 0)]
    [InlineData(1, -5)]
    public void OffsetConstructor_ThrowsOnNonPositivePageNumberOrPageSize(int pageNumber, int pageSize)
    {
        var items = new List<SampleItem>();

        Should.Throw<ArgumentOutOfRangeException>(() =>
            new PagedResult<SampleItem>(items, pageNumber: pageNumber, pageSize: pageSize));
    }

    [Fact]
    public void Empty_CreatesEmptyPagedResult()
    {
        var empty = PagedResult<SampleItem>.Empty(pageSize: 25);

        empty.Items.ShouldBeEmpty();
        empty.ContinuationToken.ShouldBeNull();
        empty.HasMore.ShouldBeFalse();
        empty.PageSize.ShouldBe(25);
        empty.TotalCount.ShouldBe(0);
    }
}
