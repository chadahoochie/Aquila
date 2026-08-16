using System.Linq.Expressions;
using Aquila.Core.Queries;

namespace Aquila.Core.Storage;

/// <summary>
/// Service provider interface for translating LINQ expressions on DocumentEnvelope into provider SQL clauses with parameterized inputs.
/// </summary>
public interface ISqlExpressionTranslator
{
    TranslationResult Translate<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate);
    string TranslateOrderBy<T>(Expression<Func<DocumentEnvelope<T>, object?>> orderBy, SortOrder direction = SortOrder.Ascending);
    string TranslateOrderBy(IEnumerable<SortDescriptor> orderings);
}

/// <summary>
/// Contains the generated SQL clause string and associated parameter values.
/// </summary>
public sealed class TranslationResult
{
    public string SqlClause { get; set; } = string.Empty;
    public IReadOnlyDictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>();
}
