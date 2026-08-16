using System.Linq.Expressions;
using Aquila.Core.Storage;

namespace Aquila.Core.Queries;

/// <summary>
/// Defines a strongly-typed compiled paged query for document retrieval.
/// </summary>
/// <typeparam name="TDoc">The document type being queried.</typeparam>
public interface ICompiledPagedQuery<TDoc> where TDoc : class
{
    /// <summary>
    /// The page size (maximum items per page) for this query.
    /// </summary>
    int PageSize { get; }

    /// <summary>
    /// The optional continuation token to resume paging from.
    /// </summary>
    string? ContinuationToken { get; }

    /// <summary>
    /// The optional partition key to scope the query.
    /// </summary>
    string? PartitionKey { get; }

    /// <summary>
    /// The filter expression targeting <see cref="DocumentEnvelope{TDoc}"/>.
    /// </summary>
    Expression<Func<DocumentEnvelope<TDoc>, bool>>? Predicate();

    /// <summary>
    /// The optional single order-by expression targeting <see cref="DocumentEnvelope{TDoc}"/>.
    /// </summary>
    Expression<Func<DocumentEnvelope<TDoc>, object?>>? OrderBy() => null;

    /// <summary>
    /// The sort direction for <see cref="OrderBy"/> (defaults to Ascending).
    /// </summary>
    SortOrder SortOrder => SortOrder.Ascending;

    /// <summary>
    /// The optional collection of sort definitions for multi-column ordering targeting <see cref="DocumentEnvelope{TDoc}"/>.
    /// </summary>
    IEnumerable<SortOrderDefinition<TDoc>>? Orderings() => null;
}
