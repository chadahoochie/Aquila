using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Aquila.Core.Storage;

/// <summary>
/// Service provider interface for translating LINQ expressions on DocumentEnvelope into provider SQL clauses with parameterized inputs.
/// </summary>
public interface ISqlExpressionTranslator
{
    TranslationResult Translate<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate);
}

/// <summary>
/// Contains the generated SQL clause string and associated parameter values.
/// </summary>
public sealed class TranslationResult
{
    public string SqlClause { get; set; } = string.Empty;
    public IReadOnlyDictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>();
}
