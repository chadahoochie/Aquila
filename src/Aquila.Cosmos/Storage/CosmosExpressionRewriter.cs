using System.Linq.Expressions;
using Aquila.Core.Storage;

namespace Aquila.Cosmos.Storage;

/// <summary>
/// Expression visitor that rewrites predicates targeting <see cref="DocumentEnvelope{T}"/>
/// into equivalent expressions targeting <see cref="CosmosDocumentEnvelope{T}"/>.
/// </summary>
public class CosmosExpressionRewriter : ExpressionVisitor
{
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

        if (predicate.Parameters.Count == 0)
        {
            throw new ArgumentException("Predicate must have at least one parameter.", nameof(predicate));
        }

        var oldParam = predicate.Parameters[0];
        var newParam = Expression.Parameter(typeof(CosmosDocumentEnvelope<T>), oldParam.Name);

        var rewriter = new CosmosExpressionRewriter(oldParam, newParam);
        var newBody = rewriter.Visit(predicate.Body);

        return Expression.Lambda<Func<CosmosDocumentEnvelope<T>, bool>>(newBody, newParam);
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
            var targetProp = visitedExpr.Type.GetProperty(memberName);

            if (targetProp != null)
            {
                return Expression.Property(visitedExpr, targetProp);
            }
        }

        return base.VisitMember(node);
    }
}
