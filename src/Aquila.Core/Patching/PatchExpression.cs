using System.Linq.Expressions;
using Aquila.Core.Storage;

namespace Aquila.Core.Patching;

/// <summary>
/// Default implementation of IPatchExpression to parse property lambda expressions into JSON pointer paths
/// and collect PatchOperationData.
/// </summary>
public sealed class PatchExpression<T> : IPatchExpression<T>
{
    public List<PatchOperationData> Operations { get; } = new();

    public IPatchExpression<T> Set<TValue>(Expression<Func<T, TValue>> property, TValue value)
    {
        ArgumentNullException.ThrowIfNull(property);
        var path = BuildJsonPointerPath(property);
        Operations.Add(new PatchOperationData
        {
            Path = path,
            Action = PatchAction.Set,
            Value = value
        });
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, int>> property, int value = 1)
    {
        ArgumentNullException.ThrowIfNull(property);
        var path = BuildJsonPointerPath(property);
        Operations.Add(new PatchOperationData
        {
            Path = path,
            Action = PatchAction.Increment,
            Value = value
        });
        return this;
    }

    public IPatchExpression<T> Append<TElement>(Expression<Func<T, IEnumerable<TElement>>> property, TElement element)
    {
        ArgumentNullException.ThrowIfNull(property);
        var path = BuildJsonPointerPath(property);
        Operations.Add(new PatchOperationData
        {
            Path = path,
            Action = PatchAction.Append,
            Value = element
        });
        return this;
    }

    public IPatchExpression<T> Remove<TElement>(Expression<Func<T, IEnumerable<TElement>>> property, TElement element)
    {
        ArgumentNullException.ThrowIfNull(property);
        var path = BuildJsonPointerPath(property);
        Operations.Add(new PatchOperationData
        {
            Path = path,
            Action = PatchAction.Remove,
            Value = element
        });
        return this;
    }

    // Performance Optimization: Cache resolved JSON pointer paths for direct properties (keyed by MemberInfo)
    // and complex nested lambda expressions to eliminate per-operation AST traversal, Stack<string>, and string concatenation allocations.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<System.Reflection.MemberInfo, string> _simpleMemberCache = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> _expressionPathCache = new();

    private static string BuildJsonPointerPath(LambdaExpression lambda)
    {
        var body = lambda.Body;
        while (body is UnaryExpression unary && unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
        {
            body = unary.Operand;
        }

        // Fast-path: single direct property access (e.g. o => o.Status)
        if (body is MemberExpression directMember && directMember.Expression is ParameterExpression)
        {
            return _simpleMemberCache.GetOrAdd(directMember.Member, static m => $"/Data/{m.Name}");
        }

        // Fallback with caching for nested property paths (e.g. o => o.ShippingAddress.City)
        return _expressionPathCache.GetOrAdd(lambda.ToString(), _ =>
        {
            var parts = new Stack<string>();
            var current = body;

            while (current is MemberExpression member)
            {
                parts.Push(member.Member.Name);
                current = member.Expression;
                while (current is UnaryExpression u && u.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
                {
                    current = u.Operand;
                }
            }

            if (parts.Count == 0)
            {
                throw new ArgumentException("Expression must specify a property access (e.g. x => x.Property).", nameof(lambda));
            }

            return "/Data/" + string.Join('/', parts);
        });
    }
}
