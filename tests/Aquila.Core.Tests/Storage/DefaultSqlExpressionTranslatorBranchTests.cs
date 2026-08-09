using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Aquila.Core.Storage;
using Shouldly;
using Xunit;

namespace Aquila.Core.Tests.Storage;

public static class TestUserStatic
{
    public static readonly int DefaultAge = 25;
    public static string DefaultName => "StaticName";
}

public class HolderFactory
{
    public TestUser CreateUser() => new TestUser { Age = 30 };
}

public class TestUserNullable
{
    public int? NullableAge { get; set; }
}

public static class MemoryExtensions
{
    public static IEnumerable<T> AsEnumerable<T>(T[] array) => array;
}

public sealed class DefaultSqlExpressionTranslatorBranchTests
{
    private readonly DefaultSqlExpressionTranslator _translator = new();

    [Fact]
    public void Translate_Binary_LogicalAnd_LogicalOr()
    {
        // ExpressionType.And (&)
        var param = Expression.Parameter(typeof(DocumentEnvelope<TestUser>), "u");
        var activeProp = Expression.Property(Expression.Property(param, nameof(DocumentEnvelope<TestUser>.Data)), nameof(TestUser.IsActive));
        var ageProp = Expression.Property(Expression.Property(param, nameof(DocumentEnvelope<TestUser>.Data)), nameof(TestUser.Age));
        var ageGt = Expression.GreaterThan(ageProp, Expression.Constant(18));
        var bitwiseAnd = Expression.And(activeProp, ageGt);
        var predAnd = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(bitwiseAnd, param);

        var resAnd = _translator.Translate(predAnd);
        resAnd.SqlClause.ShouldBe("(c.Data.IsActive AND c.Data.Age > @p0)");

        // ExpressionType.Or (|)
        var bitwiseOr = Expression.Or(activeProp, ageGt);
        var predOr = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(bitwiseOr, param);

        var resOr = _translator.Translate(predOr);
        resOr.SqlClause.ShouldBe("(c.Data.IsActive OR c.Data.Age > @p0)");
    }

    [Fact]
    public void Translate_Binary_Null_Comparison_On_Left_NotEqual()
    {
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => null != u.Data.Name;

        var result = _translator.Translate(predicate);

        result.SqlClause.ShouldBe("c.Data.Name IS NOT NULL");
        result.Parameters.ShouldBeEmpty();
    }

    [Fact]
    public void Translate_VisitConstant_Null_Value()
    {
        var param = Expression.Parameter(typeof(DocumentEnvelope<TestUserNullable>), "u");
        var ageProp = Expression.Property(Expression.Property(param, nameof(DocumentEnvelope<TestUserNullable>.Data)), nameof(TestUserNullable.NullableAge));
        var nullConst = Expression.Constant(null, typeof(int?));
        var equal = Expression.Equal(ageProp, nullConst);
        var pred = Expression.Lambda<Func<DocumentEnvelope<TestUserNullable>, bool>>(equal, param);

        var res = _translator.Translate(pred);
        res.SqlClause.ShouldBe("c.Data.NullableAge IS NULL");
    }

    [Fact]
    public void Translate_Unary_ConvertChecked_And_Negate_Fallthrough()
    {
        // ExpressionType.ConvertChecked
        var param = Expression.Parameter(typeof(DocumentEnvelope<TestUser>), "u");
        var ageProp = Expression.Property(Expression.Property(param, nameof(DocumentEnvelope<TestUser>.Data)), nameof(TestUser.Age));
        var convertChecked = Expression.ConvertChecked(ageProp, typeof(long));
        var equal = Expression.Equal(convertChecked, Expression.Constant(21L));
        var predConvert = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(equal, param);

        var resConvert = _translator.Translate(predConvert);
        resConvert.SqlClause.ShouldBe("c.Data.Age = @p0");
    }

    [Fact]
    public void Translate_Instance_Contains_On_Captured_List_And_HashSet()
    {
        var list = new List<int> { 10, 20 };
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predList = u => list.Contains(u.Data.Age);

        var resList = _translator.Translate(predList);
        resList.SqlClause.ShouldBe("c.Data.Age IN (@p0, @p1)");
        resList.Parameters["@p0"].ShouldBe(10);
        resList.Parameters["@p1"].ShouldBe(20);

        var hashSet = new HashSet<string> { "alice", "bob" };
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predSet = u => hashSet.Contains(u.Data.Name);

        var resSet = _translator.Translate(predSet);
        resSet.SqlClause.ShouldBe("c.Data.Name IN (@p0, @p1)");
    }

    [Fact]
    public void Translate_Instance_Contains_Null_Collection_ThrowsNotSupportedException()
    {
        List<int>? nullList = null;
        Expression<Func<DocumentEnvelope<TestUser>, bool>> pred = u => nullList!.Contains(u.Data.Age);

        Should.Throw<NotSupportedException>(() => _translator.Translate(pred));
    }

    [Fact]
    public void Translate_UnwrapCollectionExpression_ConvertChecked_And_MemoryExtensions()
    {
        // ConvertChecked wrapping collection
        var allowedArray = new[] { 1, 2 };
        var param = Expression.Parameter(typeof(DocumentEnvelope<TestUser>), "u");
        var ageProp = Expression.Property(Expression.Property(param, nameof(DocumentEnvelope<TestUser>.Data)), nameof(TestUser.Age));
        
        var arrayConst = Expression.Constant(allowedArray);
        var convertChecked = Expression.ConvertChecked(arrayConst, typeof(IEnumerable<int>));
        var containsMethod = typeof(Enumerable).GetMethods()
            .First(m => m.Name == "Contains" && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(int));
        var call = Expression.Call(containsMethod, convertChecked, ageProp);
        var predConvertCollection = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(call, param);

        var resConvertCollection = _translator.Translate(predConvertCollection);
        resConvertCollection.SqlClause.ShouldBe("c.Data.Age IN (@p0, @p1)");

        // MemoryExtensions wrapping collection
        var asMemoryMethod = typeof(MemoryExtensions).GetMethods()
            .First(m => m.Name == "AsEnumerable")
            .MakeGenericMethod(typeof(int));
        var asMemoryCall = Expression.Call(asMemoryMethod, arrayConst);
        var callMemory = Expression.Call(containsMethod, asMemoryCall, ageProp);
        var predMemory = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(callMemory, param);

        var resMemory = _translator.Translate(predMemory);
        resMemory.SqlClause.ShouldBe("c.Data.Age IN (@p0, @p1)");
    }

    [Fact]
    public void Translate_IsParameterRooted_WithConvert_And_NonMember_Branch()
    {
        // Convert inside parameter expression chain: (object)u.Data.Age == (object)21
        var param = Expression.Parameter(typeof(DocumentEnvelope<TestUser>), "u");
        var ageProp = Expression.Property(Expression.Property(param, nameof(DocumentEnvelope<TestUser>.Data)), nameof(TestUser.Age));
        var convertProp = Expression.Convert(ageProp, typeof(object));
        var equal = Expression.Equal(convertProp, Expression.Constant(21, typeof(object)));
        var pred = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(equal, param);

        var res = _translator.Translate(pred);
        res.SqlClause.ShouldBe("c.Data.Age = @p0");

        // Binary logical expression (u.Data.IsActive == true)
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predActive = u => u.Data.IsActive == true;
        var resActive = _translator.Translate(predActive);
        resActive.SqlClause.ShouldBe("c.Data.IsActive = @p0");
    }

    [Fact]
    public void Translate_GetMemberPath_WithConvert()
    {
        // Member expression chain with Convert: ((TestAddress)u.Data.Address).City
        var param = Expression.Parameter(typeof(DocumentEnvelope<TestUser>), "u");
        var dataProp = Expression.Property(param, nameof(DocumentEnvelope<TestUser>.Data));
        var addressProp = Expression.Property(dataProp, nameof(TestUser.Address));
        var convertAddress = Expression.Convert(addressProp, typeof(TestAddress));
        var cityProp = Expression.Property(convertAddress, nameof(TestAddress.City));
        var equal = Expression.Equal(cityProp, Expression.Constant("Seattle"));
        var pred = Expression.Lambda<Func<DocumentEnvelope<TestUser>, bool>>(equal, param);

        var res = _translator.Translate(pred);
        res.SqlClause.ShouldBe("c.Data.Address.City = @p0");
    }

    [Fact]
    public void Translate_EvaluateExpression_StaticFields_Properties_And_DynamicInvoke()
    {
        // Static field access
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predField = u => u.Data.Age == TestUserStatic.DefaultAge;
        var resField = _translator.Translate(predField);
        resField.SqlClause.ShouldBe("c.Data.Age = @p0");
        resField.Parameters["@p0"].ShouldBe(25);

        // Static property access
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predProp = u => u.Data.Name == TestUserStatic.DefaultName;
        var resProp = _translator.Translate(predProp);
        resProp.SqlClause.ShouldBe("c.Data.Name = @p0");
        resProp.Parameters["@p0"].ShouldBe("StaticName");

        // Dynamic invoke path in EvaluateExpression via member on method call expression
        var factory = new HolderFactory();
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predFactory = u => u.Data.Age == factory.CreateUser().Age;
        var resFactory = _translator.Translate(predFactory);
        resFactory.SqlClause.ShouldBe("c.Data.Age = @p0");
        resFactory.Parameters["@p0"].ShouldBe(30);
    }
}
