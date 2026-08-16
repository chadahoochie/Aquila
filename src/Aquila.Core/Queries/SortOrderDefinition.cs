using System.Linq.Expressions;
using Aquila.Core.Storage;

namespace Aquila.Core.Queries;

/// <summary>
/// Defines a strongly-typed sort specification for <see cref="DocumentEnvelope{T}"/>.
/// </summary>
/// <typeparam name="T">The entity type.</typeparam>
public sealed class SortOrderDefinition<T>
{
    /// <summary>
    /// The expression selecting the property to sort on <see cref="DocumentEnvelope{T}"/>.
    /// </summary>
    public Expression<Func<DocumentEnvelope<T>, object?>> KeySelector { get; }

    /// <summary>
    /// The sort direction (Ascending or Descending).
    /// </summary>
    public SortOrder Direction { get; }

    public SortOrderDefinition(Expression<Func<DocumentEnvelope<T>, object?>> keySelector, SortOrder direction = SortOrder.Ascending)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        KeySelector = keySelector;
        Direction = direction;
    }

    /// <summary>
    /// Creates an ascending sort definition.
    /// </summary>
    public static SortOrderDefinition<T> Ascending(Expression<Func<DocumentEnvelope<T>, object?>> keySelector) =>
        new(keySelector, SortOrder.Ascending);

    /// <summary>
    /// Creates a descending sort definition.
    /// </summary>
    public static SortOrderDefinition<T> Descending(Expression<Func<DocumentEnvelope<T>, object?>> keySelector) =>
        new(keySelector, SortOrder.Descending);

    /// <summary>
    /// Converts this strongly-typed sort definition to a <see cref="SortDescriptor"/>.
    /// </summary>
    public SortDescriptor ToDescriptor() => new(KeySelector, Direction);
}
