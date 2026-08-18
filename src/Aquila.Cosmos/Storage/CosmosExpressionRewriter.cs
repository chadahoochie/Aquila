using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

/// <summary>
/// Expression visitor that rewrites predicates targeting <see cref="DocumentEnvelope{T}"/>
/// into equivalent expressions targeting <see cref="CosmosDocumentEnvelope{T}"/>.
/// </summary>
public sealed class CosmosExpressionRewriter : ExpressionVisitor
{
    // Performance Optimization: Cache property lookups and rewritten expression trees to avoid
    // reflection GetProperty() overhead and AST allocations on high-frequency Cosmos query translations.
    private static readonly ConcurrentDictionary<(Type Type, string Name), PropertyInfo?> _propertyCache = new();
    private static readonly ConcurrentDictionary<LambdaExpression, LambdaExpression> _rewriteCache = new();

    private readonly ParameterExpression _oldParam;
    private readonly ParameterExpression _newParam;

    private CosmosExpressionRewriter(ParameterExpression oldParam, ParameterExpression newParam)
    {
        _oldParam = oldParam;
        _newParam = newParam;
    }

    public static Expression<Func<CosmosDocumentEnvelope<T>, bool>>? Rewrite<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate) where T : class
    {
        if (predicate == null) return null;

        return (Expression<Func<CosmosDocumentEnvelope<T>, bool>>)_rewriteCache.GetOrAdd(predicate, static p =>
        {
            if (p.Parameters.Count == 0)
            {
                throw new ArgumentException("Predicate must have at least one parameter.", nameof(p));
            }

            var oldParam = p.Parameters[0];
            var newParam = Expression.Parameter(typeof(CosmosDocumentEnvelope<T>), oldParam.Name);

            var rewriter = new CosmosExpressionRewriter(oldParam, newParam);
            var newBody = rewriter.Visit(p.Body);

            return Expression.Lambda<Func<CosmosDocumentEnvelope<T>, bool>>(newBody, newParam);
        });
    }

    public static LambdaExpression? Rewrite<T>(LambdaExpression? lambda) where T : class
    {
        if (lambda == null) return null;

        return _rewriteCache.GetOrAdd(lambda, static l =>
        {
            if (l.Parameters.Count == 0)
            {
                throw new ArgumentException("Expression must have at least one parameter.", nameof(l));
            }

            var oldParam = l.Parameters[0];
            var newParam = Expression.Parameter(typeof(CosmosDocumentEnvelope<T>), oldParam.Name);

            var rewriter = new CosmosExpressionRewriter(oldParam, newParam);
            var newBody = rewriter.Visit(l.Body);

            return Expression.Lambda(newBody, newParam);
        });
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        if (node == _oldParam)
        {
            return _newParam;
        }
        return base.VisitParameter(node);
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        if (node.Expression != null && (node.Expression == _oldParam || node.Expression.Type == _oldParam.Type))
        {
            var visitedExpr = Visit(node.Expression);
            var memberName = node.Member.Name;
            var targetProp = _propertyCache.GetOrAdd((visitedExpr.Type, memberName), static key => key.Type.GetProperty(key.Name));

            if (targetProp != null)
            {
                return Expression.Property(visitedExpr, targetProp);
            }
        }

        return base.VisitMember(node);
    }
}
