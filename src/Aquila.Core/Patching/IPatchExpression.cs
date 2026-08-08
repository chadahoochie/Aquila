using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Aquila.Core.Patching;

/// <summary>
/// Fluent interface for building document patch operations.
/// </summary>
public interface IPatchExpression<T>
{
    IPatchExpression<T> Set<TValue>(Expression<Func<T, TValue>> property, TValue value);
    IPatchExpression<T> Increment(Expression<Func<T, int>> property, int value = 1);
    IPatchExpression<T> Append<TElement>(Expression<Func<T, IEnumerable<TElement>>> property, TElement element);
    IPatchExpression<T> Remove<TElement>(Expression<Func<T, IEnumerable<TElement>>> property, TElement element);
}
