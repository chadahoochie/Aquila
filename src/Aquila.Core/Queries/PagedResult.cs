namespace Aquila.Core.Queries;

/// <summary>
/// Represents a paginated result set containing data items, pagination tokens, and metadata.
/// </summary>
/// <typeparam name="T">The type of the item in the result set.</typeparam>
public sealed class PagedResult<T>
{
    /// <summary>
    /// The items contained in the current page.
    /// </summary>
    public IReadOnlyList<T> Items { get; init; } = Array.Empty<T>();

    /// <summary>
    /// The opaque continuation token to retrieve the subsequent page of results, or <c>null</c> if no more results exist.
    /// </summary>
    public string? ContinuationToken { get; init; }

    /// <summary>
    /// Indicates whether additional pages are available.
    /// </summary>
    public bool HasMore => !string.IsNullOrWhiteSpace(ContinuationToken);

    /// <summary>
    /// The total count of matching items across all pages, if computed (typically populated for offset-based queries).
    /// </summary>
    public int? TotalCount { get; init; }

    /// <summary>
    /// The current 1-based page number, if applicable.
    /// </summary>
    public int? PageNumber { get; init; }

    /// <summary>
    /// The requested or configured page size (maximum items per page).
    /// </summary>
    public int? PageSize { get; init; }

    /// <summary>
    /// Default parameterless constructor.
    /// </summary>
    public PagedResult()
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="PagedResult{T}"/> using continuation-token pagination.
    /// </summary>
    /// <param name="items">The items on the current page.</param>
    /// <param name="continuationToken">The continuation token for the next page.</param>
    /// <param name="pageSize">The page size used for the query.</param>
    public PagedResult(IReadOnlyList<T> items, string? continuationToken, int? pageSize = null)
    {
        Items = items ?? Array.Empty<T>();
        ContinuationToken = string.IsNullOrWhiteSpace(continuationToken) ? null : continuationToken;
        PageSize = pageSize ?? items?.Count;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="PagedResult{T}"/> using offset-based pagination.
    /// </summary>
    /// <param name="items">The items on the current page.</param>
    /// <param name="pageNumber">The 1-based page number.</param>
    /// <param name="pageSize">The number of items per page.</param>
    /// <param name="totalCount">The total count of items across all pages, if available.</param>
    public PagedResult(IReadOnlyList<T> items, int pageNumber, int pageSize, int? totalCount = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageNumber);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        Items = items ?? Array.Empty<T>();
        PageNumber = pageNumber;
        PageSize = pageSize;
        TotalCount = totalCount;
    }

    /// <summary>
    /// Creates an empty <see cref="PagedResult{T}"/>.
    /// </summary>
    /// <param name="pageSize">The page size configured for the query.</param>
    /// <returns>An empty paged result.</returns>
    public static PagedResult<T> Empty(int pageSize = 0) => new()
    {
        Items = Array.Empty<T>(),
        ContinuationToken = null,
        PageSize = pageSize,
        TotalCount = 0
    };
}
