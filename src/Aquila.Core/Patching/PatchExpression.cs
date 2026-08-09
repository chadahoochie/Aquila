using System.Linq.Expressions;
using Aquila.Core.Storage;

namespace Aquila.Core.Patching;

/// <summary>
/// Default implementation of IPatchExpression to parse property lambda expressions into JSON pointer paths
/// and collect PatchOperationData.
/// </summary>
public class PatchExpression<T> : IPatchExpression<T>
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

    private static string BuildJsonPointerPath(LambdaExpression lambda)
    {
        var body = lambda.Body;
        while (body is UnaryExpression unary && unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
        {
            body = unary.Operand;
        }

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
    }
}
