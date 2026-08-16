using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Aquila.Core.Storage;

namespace Aquila.Core.Queries;

public static class CompiledQueryCache
{
    private static readonly ConcurrentDictionary<object, object> _cache = new();

    public static TResult Execute<TDoc, TResult>(IQueryable<TDoc> queryable, ICompiledQuery<TDoc, TResult> query) where TDoc : class
    {
        ArgumentNullException.ThrowIfNull(queryable);
        ArgumentNullException.ThrowIfNull(query);

        var queryType = query.GetType();

        var compiledDelegate = (Func<IQueryable<TDoc>, object, TResult>)_cache.GetOrAdd(queryType, t =>
        {
            var expression = query.QueryIs();
            if (expression == null)
            {
                throw new InvalidOperationException($"QueryIs() returned null for compiled query type '{((Type)t).FullName}'.");
            }

            var queryableParam = expression.Parameters[0];
            var queryParam = Expression.Parameter(typeof(object), "query");
            var typedQueryParam = Expression.Convert(queryParam, (Type)t);

            var rewriter = new QueryParameterBindingVisitor(query, (Type)t, typedQueryParam);
            var newBody = rewriter.Visit(expression.Body);

            var lambda = Expression.Lambda<Func<IQueryable<TDoc>, object, TResult>>(newBody, queryableParam, queryParam);
            return lambda.Compile();
        });

        return compiledDelegate(queryable, query);
    }

    public static Expression<Func<DocumentEnvelope<TDoc>, bool>>? ExtractPredicate<TDoc>(ICompiledPagedQuery<TDoc> query) where TDoc : class
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.Predicate();
    }

    public static void Clear()
    {
        _cache.Clear();
    }

    private class QueryParameterBindingVisitor : ExpressionVisitor
    {
        private readonly object _originalQueryInstance;
        private readonly Type _queryType;
        private readonly Expression _replacementParameter;

        public QueryParameterBindingVisitor(object originalQueryInstance, Type queryType, Expression replacementParameter)
        {
            _originalQueryInstance = originalQueryInstance;
            _queryType = queryType;
            _replacementParameter = replacementParameter;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value != null)
            {
                if (ReferenceEquals(node.Value, _originalQueryInstance) ||
                    node.Value.GetType() == _queryType ||
                    _queryType.IsAssignableFrom(node.Value.GetType()))
                {
                    return _replacementParameter;
                }
            }
            return base.VisitConstant(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression is ConstantExpression constExp && constExp.Value != null)
            {
                if (ReferenceEquals(constExp.Value, _originalQueryInstance) ||
                    constExp.Value.GetType() == _queryType ||
                    _queryType.IsAssignableFrom(constExp.Value.GetType()))
                {
                    return Expression.MakeMemberAccess(_replacementParameter, node.Member);
                }

                // Check if member access is on a closure object pointing to the original query instance
                if (node.Member is FieldInfo fieldInfo)
                {
                    var val = fieldInfo.GetValue(constExp.Value);
                    if (ReferenceEquals(val, _originalQueryInstance) || (val != null && (val.GetType() == _queryType || _queryType.IsAssignableFrom(val.GetType()))))
                    {
                        return _replacementParameter;
                    }
                }
                else if (node.Member is PropertyInfo propInfo)
                {
                    var val = propInfo.GetValue(constExp.Value);
                    if (ReferenceEquals(val, _originalQueryInstance) || (val != null && (val.GetType() == _queryType || _queryType.IsAssignableFrom(val.GetType()))))
                    {
                        return _replacementParameter;
                    }
                }
            }

            return base.VisitMember(node);
        }
    }
}
