using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Aquila.Core.Queries;
using Shouldly;
using Xunit;

namespace Aquila.Core.Tests.Queries;

public class CompiledQueryCacheBranchTests
{
    public CompiledQueryCacheBranchTests()
    {
        CompiledQueryCache.Clear();
    }

    [Fact]
    public void Execute_VisitConstant_WithIsAssignableFrom_And_TypeMatch_Disjuncts()
    {
        CompiledQueryCache.Clear();
        var data = new List<UserDoc>
        {
            new("1", "alice@example.com", 30, true),
            new("2", "bob@example.com", 25, false)
        }.AsQueryable();

        var query = new DerivedQueryDocQuery { Email = "alice@example.com" };
        var result = CompiledQueryCache.Execute(data, query);

        result.ShouldNotBeNull();
        result.Id.ShouldBe("1");
    }

    [Fact]
    public void Execute_VisitConstant_WithNull_And_UnrelatedConstant()
    {
        CompiledQueryCache.Clear();
        var data = new List<UserDoc>
        {
            new("1", "alice@example.com", 30, true),
            new("2", "bob@example.com", 25, false)
        }.AsQueryable();

        var query = new QueryWithNullAndUnrelatedConstant();
        var result = CompiledQueryCache.Execute(data, query);

        result.ShouldNotBeNull();
        result.Id.ShouldBe("1");
    }

    [Fact]
    public void Execute_VisitMember_TypeMatch_OnSameTypeInstance()
    {
        CompiledQueryCache.Clear();
        var data = new List<UserDoc>
        {
            new("1", "alice@example.com", 30, true),
            new("2", "bob@example.com", 25, false)
        }.AsQueryable();

        var query = new QueryWithMemberOnSameTypeInstance { Email = "alice@example.com" };
        var result = CompiledQueryCache.Execute(data, query);

        result.ShouldNotBeNull();
        result.Id.ShouldBe("1");
    }

    [Fact]
    public void Execute_VisitMember_FieldInfo_Disjuncts()
    {
        var data = new List<UserDoc>
        {
            new("1", "alice@example.com", 30, true),
            new("2", "bob@example.com", 25, false)
        }.AsQueryable();

        var holder = new FieldClosureHolder();

        // 1. Field value is original query instance
        CompiledQueryCache.Clear();
        var query1 = new QueryWithFieldClosureDisjuncts(holder) { Email = "alice@example.com" };
        holder.QueryField = query1;
        var res1 = CompiledQueryCache.Execute(data, query1);
        res1.ShouldNotBeNull();
        res1.Id.ShouldBe("1");

        // 2. Field value is distinct instance of same query type (Type match)
        CompiledQueryCache.Clear();
        var query2 = new QueryWithFieldClosureDisjuncts(holder) { Email = "bob@example.com" };
        var otherQuery2 = new QueryWithFieldClosureDisjuncts(holder) { Email = "bob@example.com" };
        holder.QueryField = otherQuery2;
        var res2 = CompiledQueryCache.Execute(data, query2);
        res2.ShouldNotBeNull();
        res2.Id.ShouldBe("2");

        // 3. Field value is derived instance (IsAssignableFrom match)
        CompiledQueryCache.Clear();
        var derivedQuery = new SubQueryWithFieldClosureDisjuncts(holder) { Email = "alice@example.com" };
        holder.QueryField = derivedQuery;
        var res3 = CompiledQueryCache.Execute(data, query1);
        res3.ShouldNotBeNull();
        res3.Id.ShouldBe("1");

        // 4. Field value is null
        CompiledQueryCache.Clear();
        holder.QueryField = null;
        Should.Throw<NullReferenceException>(() => CompiledQueryCache.Execute(data, query1));
    }

    [Fact]
    public void Execute_VisitMember_PropertyInfo_Disjuncts()
    {
        var data = new List<UserDoc>
        {
            new("1", "alice@example.com", 30, true),
            new("2", "bob@example.com", 25, false)
        }.AsQueryable();

        var holder = new PropClosureHolder();

        // 1. Property value is original query instance
        CompiledQueryCache.Clear();
        var query1 = new QueryWithPropClosureDisjuncts(holder) { Email = "alice@example.com" };
        holder.QueryProp = query1;
        var res1 = CompiledQueryCache.Execute(data, query1);
        res1.ShouldNotBeNull();
        res1.Id.ShouldBe("1");

        // 2. Property value is distinct instance of same query type
        CompiledQueryCache.Clear();
        var query2 = new QueryWithPropClosureDisjuncts(holder) { Email = "bob@example.com" };
        var otherQuery2 = new QueryWithPropClosureDisjuncts(holder) { Email = "bob@example.com" };
        holder.QueryProp = otherQuery2;
        var res2 = CompiledQueryCache.Execute(data, query2);
        res2.ShouldNotBeNull();
        res2.Id.ShouldBe("2");

        // 3. Property value is derived instance
        CompiledQueryCache.Clear();
        var derivedQuery = new SubQueryWithPropClosureDisjuncts(holder) { Email = "alice@example.com" };
        holder.QueryProp = derivedQuery;
        var res3 = CompiledQueryCache.Execute(data, query1);
        res3.ShouldNotBeNull();
        res3.Id.ShouldBe("1");

        // 4. Property value is null
        CompiledQueryCache.Clear();
        holder.QueryProp = null;
        Should.Throw<NullReferenceException>(() => CompiledQueryCache.Execute(data, query1));
    }

    [Fact]
    public void Execute_VisitMember_UnrelatedFieldAndProperty_Fallthrough()
    {
        CompiledQueryCache.Clear();
        var data = new List<UserDoc>
        {
            new("1", "alice@example.com", 30, true),
            new("2", "bob@example.com", 25, false)
        }.AsQueryable();

        var query = new QueryWithUnrelatedMemberAccess();
        var result = CompiledQueryCache.Execute(data, query);

        result.ShouldNotBeNull();
        result.Id.ShouldBe("1");
    }
}

public abstract class BaseQueryDocQuery : ICompiledQuery<UserDoc, UserDoc?>
{
    public string Email { get; set; } = string.Empty;
    public abstract Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs();
}

public class DerivedQueryDocQuery : BaseQueryDocQuery
{
    public override Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        var usersParam = Expression.Parameter(typeof(IQueryable<UserDoc>), "users");
        
        var subInstance = new SubDerivedQueryDocQuery { Email = "alice@example.com" };
        var subConst = Expression.Constant(subInstance, typeof(SubDerivedQueryDocQuery));
        var emailProp = Expression.Property(subConst, nameof(Email));

        var uParam = Expression.Parameter(typeof(UserDoc), "u");
        var uEmailProp = Expression.Property(uParam, nameof(UserDoc.Email));
        var equal = Expression.Equal(uEmailProp, emailProp);
        var predicate = Expression.Lambda<Func<UserDoc, bool>>(equal, uParam);

        var firstOrDefaultCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(UserDoc) },
            usersParam,
            predicate);

        return Expression.Lambda<Func<IQueryable<UserDoc>, UserDoc?>>(firstOrDefaultCall, usersParam);
    }
}

public class SubDerivedQueryDocQuery : DerivedQueryDocQuery
{
}

public class QueryWithNullAndUnrelatedConstant : ICompiledQuery<UserDoc, UserDoc?>
{
    public Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        var usersParam = Expression.Parameter(typeof(IQueryable<UserDoc>), "users");
        
        var nullConst = Expression.Constant(null, typeof(string));
        var unrelatedConst = Expression.Constant("unrelated_value", typeof(string));

        var uParam = Expression.Parameter(typeof(UserDoc), "u");
        var uEmailProp = Expression.Property(uParam, nameof(UserDoc.Email));
        
        var notNull = Expression.NotEqual(uEmailProp, nullConst);
        var notUnrelated = Expression.NotEqual(uEmailProp, unrelatedConst);
        var combined = Expression.AndAlso(notNull, notUnrelated);

        var predicate = Expression.Lambda<Func<UserDoc, bool>>(combined, uParam);

        var firstOrDefaultCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(UserDoc) },
            usersParam,
            predicate);

        return Expression.Lambda<Func<IQueryable<UserDoc>, UserDoc?>>(firstOrDefaultCall, usersParam);
    }
}

public class QueryWithMemberOnSameTypeInstance : ICompiledQuery<UserDoc, UserDoc?>
{
    public string Email { get; set; } = string.Empty;

    public Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        var usersParam = Expression.Parameter(typeof(IQueryable<UserDoc>), "users");
        
        var secondInstance = new QueryWithMemberOnSameTypeInstance { Email = "bob@example.com" };
        var secondConst = Expression.Constant(secondInstance, typeof(QueryWithMemberOnSameTypeInstance));
        var emailProp = Expression.Property(secondConst, nameof(Email));

        var uParam = Expression.Parameter(typeof(UserDoc), "u");
        var uEmailProp = Expression.Property(uParam, nameof(UserDoc.Email));
        var equal = Expression.Equal(uEmailProp, emailProp);
        var predicate = Expression.Lambda<Func<UserDoc, bool>>(equal, uParam);

        var firstOrDefaultCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(UserDoc) },
            usersParam,
            predicate);

        return Expression.Lambda<Func<IQueryable<UserDoc>, UserDoc?>>(firstOrDefaultCall, usersParam);
    }
}

public class FieldClosureHolder
{
    public object? QueryField;
}

public class QueryWithFieldClosureDisjuncts : ICompiledQuery<UserDoc, UserDoc?>
{
    public string Email { get; set; } = string.Empty;
    private readonly FieldClosureHolder _holder;

    public QueryWithFieldClosureDisjuncts(FieldClosureHolder holder)
    {
        _holder = holder;
    }

    public Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        var usersParam = Expression.Parameter(typeof(IQueryable<UserDoc>), "users");
        
        var holderConst = Expression.Constant(_holder, typeof(FieldClosureHolder));
        var fieldExpr = Expression.Field(holderConst, nameof(FieldClosureHolder.QueryField));
        var typedField = Expression.Convert(fieldExpr, typeof(QueryWithFieldClosureDisjuncts));
        var emailProp = Expression.Property(typedField, nameof(Email));

        var uParam = Expression.Parameter(typeof(UserDoc), "u");
        var uEmailProp = Expression.Property(uParam, nameof(UserDoc.Email));
        var equal = Expression.Equal(uEmailProp, emailProp);
        var predicate = Expression.Lambda<Func<UserDoc, bool>>(equal, uParam);

        var firstOrDefaultCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(UserDoc) },
            usersParam,
            predicate);

        return Expression.Lambda<Func<IQueryable<UserDoc>, UserDoc?>>(firstOrDefaultCall, usersParam);
    }
}

public class SubQueryWithFieldClosureDisjuncts : QueryWithFieldClosureDisjuncts
{
    public SubQueryWithFieldClosureDisjuncts(FieldClosureHolder holder) : base(holder) { }
}

public class PropClosureHolder
{
    public object? QueryProp { get; set; }
}

public class QueryWithPropClosureDisjuncts : ICompiledQuery<UserDoc, UserDoc?>
{
    public string Email { get; set; } = string.Empty;
    private readonly PropClosureHolder _holder;

    public QueryWithPropClosureDisjuncts(PropClosureHolder holder)
    {
        _holder = holder;
    }

    public Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        var usersParam = Expression.Parameter(typeof(IQueryable<UserDoc>), "users");
        
        var holderConst = Expression.Constant(_holder, typeof(PropClosureHolder));
        var propExpr = Expression.Property(holderConst, nameof(PropClosureHolder.QueryProp));
        var typedProp = Expression.Convert(propExpr, typeof(QueryWithPropClosureDisjuncts));
        var emailProp = Expression.Property(typedProp, nameof(Email));

        var uParam = Expression.Parameter(typeof(UserDoc), "u");
        var uEmailProp = Expression.Property(uParam, nameof(UserDoc.Email));
        var equal = Expression.Equal(uEmailProp, emailProp);
        var predicate = Expression.Lambda<Func<UserDoc, bool>>(equal, uParam);

        var firstOrDefaultCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(UserDoc) },
            usersParam,
            predicate);

        return Expression.Lambda<Func<IQueryable<UserDoc>, UserDoc?>>(firstOrDefaultCall, usersParam);
    }
}

public class SubQueryWithPropClosureDisjuncts : QueryWithPropClosureDisjuncts
{
    public SubQueryWithPropClosureDisjuncts(PropClosureHolder holder) : base(holder) { }
}

public class UnrelatedHolder
{
    public string UnrelatedField = "hello";
    public string UnrelatedProp { get; set; } = "world";
}

public class QueryWithUnrelatedMemberAccess : ICompiledQuery<UserDoc, UserDoc?>
{
    public Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        var usersParam = Expression.Parameter(typeof(IQueryable<UserDoc>), "users");
        
        var holder = new UnrelatedHolder();
        var holderConst = Expression.Constant(holder, typeof(UnrelatedHolder));
        var fieldExpr = Expression.Field(holderConst, nameof(UnrelatedHolder.UnrelatedField));
        var propExpr = Expression.Property(holderConst, nameof(UnrelatedHolder.UnrelatedProp));

        var uParam = Expression.Parameter(typeof(UserDoc), "u");
        var uEmailProp = Expression.Property(uParam, nameof(UserDoc.Email));
        
        var equal1 = Expression.NotEqual(uEmailProp, fieldExpr);
        var equal2 = Expression.NotEqual(uEmailProp, propExpr);
        var combined = Expression.AndAlso(equal1, equal2);

        var predicate = Expression.Lambda<Func<UserDoc, bool>>(combined, uParam);

        var firstOrDefaultCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(UserDoc) },
            usersParam,
            predicate);

        return Expression.Lambda<Func<IQueryable<UserDoc>, UserDoc?>>(firstOrDefaultCall, usersParam);
    }
}
