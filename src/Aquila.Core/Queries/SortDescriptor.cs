using System.Linq.Expressions;

namespace Aquila.Core.Queries;

/// <summary>
/// Represents a non-generic sort instruction with an expression and direction.
/// </summary>
public sealed class SortDescriptor
{
    /// <summary>
    /// The expression selecting the member or property to sort by.
    /// </summary>
    public LambdaExpression KeySelector { get; init; }

    /// <summary>
    /// The direction to sort.
    /// </summary>
    public SortOrder Direction { get; init; }

    public SortDescriptor(LambdaExpression keySelector, SortOrder direction = SortOrder.Ascending)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        KeySelector = keySelector;
        Direction = direction;
    }
}
