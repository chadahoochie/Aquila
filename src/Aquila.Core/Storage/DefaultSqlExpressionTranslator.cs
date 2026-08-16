using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Aquila.Core.Queries;

namespace Aquila.Core.Storage;

/// <summary>
/// Default implementation of ISqlExpressionTranslator using ExpressionVisitor to produce parameterized SQL query clauses.
/// </summary>
public class DefaultSqlExpressionTranslator : ExpressionVisitor, ISqlExpressionTranslator
{
    [ThreadStatic]
    private static StringBuilder? t_builder;

    private readonly Dictionary<string, object> _parameters = new();
    private int _parameterIndex;
    private ParameterExpression? _parameter;
    private StringBuilder _builder = null!;

    public TranslationResult Translate<T>(Expression<Func<DocumentEnvelope<T>, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        _builder = t_builder ??= new StringBuilder(256);
        _builder.Clear();
        _parameters.Clear();
        _parameterIndex = 0;
        _parameter = predicate.Parameters[0];

        Visit(predicate.Body);

        var sqlClause = _builder.ToString();
        _builder.Clear();

        return new TranslationResult
        {
            SqlClause = sqlClause,
            Parameters = new Dictionary<string, object>(_parameters)
        };
    }

    public string TranslateOrderBy<T>(Expression<Func<DocumentEnvelope<T>, object?>> orderBy, SortOrder direction = SortOrder.Ascending)
    {
        ArgumentNullException.ThrowIfNull(orderBy);
        return TranslateOrderBy(new[] { new SortDescriptor(orderBy, direction) });
    }

    public string TranslateOrderBy(IEnumerable<SortDescriptor> orderings)
    {
        ArgumentNullException.ThrowIfNull(orderings);
        var orderList = orderings.Where(o => o != null && o.KeySelector != null).ToList();
        if (orderList.Count == 0) return string.Empty;

        var clauses = new List<string>(orderList.Count);
        foreach (var ord in orderList)
        {
            var param = ord.KeySelector.Parameters[0];
            var body = ord.KeySelector.Body;
            while (body is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            {
                body = u.Operand;
            }

            if (body is MemberExpression memberExpr && IsParameterRooted(memberExpr, param))
            {
                var path = GetMemberPath(memberExpr, param);
                var dirSql = ord.Direction == SortOrder.Descending ? "DESC" : "ASC";
                clauses.Add($"{path} {dirSql}");
            }
            else
            {
                throw new NotSupportedException($"Order expression '{ord.KeySelector}' is not a supported property access.");
            }
        }

        return clauses.Count > 0 ? "ORDER BY " + string.Join(", ", clauses) : string.Empty;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        var isLogical = node.NodeType is ExpressionType.AndAlso or ExpressionType.And or ExpressionType.OrElse or ExpressionType.Or;

        if (isLogical)
        {
            _builder.Append('(');
        }

        // Check for null comparison: e.g. x.Name == null
        if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
        {
            if (IsNullConstant(node.Right))
            {
                Visit(node.Left);
                _builder.Append(node.NodeType == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL");
                if (isLogical) _builder.Append(')');
                return node;
            }
            if (IsNullConstant(node.Left))
            {
                Visit(node.Right);
                _builder.Append(node.NodeType == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL");
                if (isLogical) _builder.Append(')');
                return node;
            }
        }

        Visit(node.Left);

        var opSql = node.NodeType switch
        {
            ExpressionType.Equal => " = ",
            ExpressionType.NotEqual => " != ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.LessThan => " < ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.LessThanOrEqual => " <= ",
            ExpressionType.AndAlso or ExpressionType.And => " AND ",
            ExpressionType.OrElse or ExpressionType.Or => " OR ",
            _ => throw new NotSupportedException($"Binary operator '{node.NodeType}' is not supported in SQL translation.")
        };

        _builder.Append(opSql);
        Visit(node.Right);

        if (isLogical)
        {
            _builder.Append(')');
        }

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        if (_parameter != null && IsParameterRooted(node, _parameter))
        {
            var path = GetMemberPath(node, _parameter);
            _builder.Append(path);
            return node;
        }

        var value = EvaluateExpression(node);
        AddParameter(value);
        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value != null && IsCompilerGenerated(node.Value.GetType()))
        {
            return base.VisitConstant(node);
        }

        AddParameter(node.Value);
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
        {
            Visit(node.Operand);
            return node;
        }

        if (node.NodeType == ExpressionType.Not)
        {
            _builder.Append("NOT (");
            Visit(node.Operand);
            _builder.Append(')');
            return node;
        }

        return base.VisitUnary(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var method = node.Method;
        var methodName = method.Name;

        if (method.DeclaringType == typeof(string))
        {
            if (methodName == "StartsWith" && node.Object != null && node.Arguments.Count >= 1)
            {
                _builder.Append("STARTSWITH(");
                Visit(node.Object);
                _builder.Append(", ");
                Visit(node.Arguments[0]);
                _builder.Append(')');
                return node;
            }
            if (methodName == "EndsWith" && node.Object != null && node.Arguments.Count >= 1)
            {
                _builder.Append("ENDSWITH(");
                Visit(node.Object);
                _builder.Append(", ");
                Visit(node.Arguments[0]);
                _builder.Append(')');
                return node;
            }
            if (methodName == "Contains" && node.Object != null && node.Arguments.Count >= 1)
            {
                _builder.Append("CONTAINS(");
                Visit(node.Object);
                _builder.Append(", ");
                Visit(node.Arguments[0]);
                _builder.Append(')');
                return node;
            }
        }

        if (methodName == "Contains")
        {
            Expression? collectionExpr = null;
            Expression? itemExpr = null;

            if (node.Arguments.Count == 2)
            {
                // Static extension method: e.g. Enumerable.Contains, MemoryExtensions.Contains
                collectionExpr = node.Arguments[0];
                itemExpr = node.Arguments[1];
            }
            else if (node.Object != null && node.Arguments.Count == 1)
            {
                // Instance method: e.g. List<T>.Contains, ICollection<T>.Contains
                collectionExpr = node.Object;
                itemExpr = node.Arguments[0];
            }

            if (collectionExpr != null && itemExpr != null)
            {
                collectionExpr = UnwrapCollectionExpression(collectionExpr);

                if (_parameter != null && IsParameterRooted(collectionExpr, _parameter))
                {
                    _builder.Append("ARRAY_CONTAINS(");
                    Visit(collectionExpr);
                    _builder.Append(", ");
                    Visit(itemExpr);
                    _builder.Append(')');
                    return node;
                }

                var rawCollection = EvaluateExpression(collectionExpr);
                if (rawCollection is IEnumerable enumerable && rawCollection is not string)
                {
                    var elements = new List<object?>();
                    foreach (var elem in enumerable)
                    {
                        elements.Add(elem);
                    }

                    if (elements.Count == 0)
                    {
                        _builder.Append("1=0");
                        return node;
                    }

                    Visit(itemExpr);
                    _builder.Append(" IN (");
                    for (int i = 0; i < elements.Count; i++)
                    {
                        if (i > 0) _builder.Append(", ");
                        AddParameter(elements[i]);
                    }
                    _builder.Append(')');
                    return node;
                }
            }
        }

        throw new NotSupportedException($"Method call '{method.DeclaringType?.Name}.{methodName}' is not supported in SQL translation.");
    }

    private void AddParameter(object? value)
    {
        var paramName = $"@p{_parameterIndex++}";
        _parameters[paramName] = value!;
        _builder.Append(paramName);
    }

    private static bool IsNullConstant(Expression expr)
    {
        return expr is ConstantExpression c && c.Value == null;
    }

    private static bool IsCompilerGenerated(Type type)
    {
        return type.Attributes.HasFlag(TypeAttributes.NestedPrivate) && type.Name.Contains("<>c");
    }

    private static bool IsParameterRooted(Expression? node, ParameterExpression param)
    {
        while (node != null)
        {
            if (node == param) return true;
            if (node is MemberExpression member)
            {
                node = member.Expression;
            }
            else if (node is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                node = unary.Operand;
            }
            else
            {
                return false;
            }
        }
        return false;
    }

    private static string GetMemberPath(MemberExpression node, ParameterExpression param)
    {
        var names = new List<string>(4);
        Expression? current = node;

        while (current is MemberExpression member)
        {
            names.Add(member.Member.Name);
            current = member.Expression;
            if (current is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                current = unary.Operand;
            }
        }

        names.Reverse();
        return "c." + string.Join('.', names);
    }

    private static Expression UnwrapCollectionExpression(Expression expr)
    {
        while (true)
        {
            if (expr is UnaryExpression u && u.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                expr = u.Operand;
                continue;
            }
            if (expr is MethodCallExpression mc && (mc.Method.DeclaringType?.Name == "MemoryExtensions" || expr.Type.IsByRefLike))
            {
                if (mc.Arguments.Count > 0)
                {
                    expr = mc.Arguments[0];
                    continue;
                }
            }
            break;
        }
        return expr;
    }

    private static object? EvaluateExpression(Expression expression)
    {
        if (expression is ConstantExpression c)
        {
            return c.Value;
        }

        if (expression is MemberExpression m)
        {
            var target = m.Expression != null ? EvaluateExpression(m.Expression) : null;
            if (m.Member is FieldInfo f)
            {
                return f.GetValue(target);
            }
            if (m.Member is PropertyInfo p)
            {
                return p.GetValue(target);
            }
        }

        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
    }
}
