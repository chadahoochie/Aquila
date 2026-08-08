using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Aquila.Core.Storage;
using Shouldly;
using Xunit;

namespace Aquila.Core.Tests;

public sealed class TestAddress
{
    public string City { get; set; } = string.Empty;
}

public sealed class TestUser
{
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
    public decimal Score { get; set; }
    public bool IsActive { get; set; }
    public List<string> Tags { get; set; } = new();
    public TestAddress Address { get; set; } = new();
}

public sealed class SqlExpressionTranslatorTests
{
    private readonly DefaultSqlExpressionTranslator _translator = new();

    [Fact]
    public void Translate_Null_Predicate_ThrowsArgumentNullException()
    {
        Should.Throw<ArgumentNullException>(() => _translator.Translate<TestUser>(null!));
    }

    [Fact]
    public void Translate_Binary_Equal_And_GreaterThanOrEqual_Returns_Parameterized_Sql()
    {
        // Act
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.Data.Name == "Alice" && u.Data.Age >= 21;
        var result = _translator.Translate(predicate);

        // Assert
        result.ShouldNotBeNull();
        result.SqlClause.ShouldBe("(c.Data.Name = @p0 AND c.Data.Age >= @p1)");
        result.Parameters.Count.ShouldBe(2);
        result.Parameters["@p0"].ShouldBe("Alice");
        result.Parameters["@p1"].ShouldBe(21);
    }

    [Fact]
    public void Translate_Binary_Comparisons_Or_And_NotEqual()
    {
        // Act
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.Data.Age > 18 || u.Data.Age <= 65;
        var result = _translator.Translate(predicate);

        // Assert
        result.SqlClause.ShouldBe("(c.Data.Age > @p0 OR c.Data.Age <= @p1)");
        result.Parameters["@p0"].ShouldBe(18);
        result.Parameters["@p1"].ShouldBe(65);

        // Act 2
        Expression<Func<DocumentEnvelope<TestUser>, bool>> pred2 = u => u.Data.Name != "Bob" && u.Data.Score < 100m;
        var res2 = _translator.Translate(pred2);

        // Assert 2
        res2.SqlClause.ShouldBe("(c.Data.Name != @p0 AND c.Data.Score < @p1)");
        res2.Parameters["@p0"].ShouldBe("Bob");
        res2.Parameters["@p1"].ShouldBe(100m);
    }

    [Fact]
    public void Translate_Envelope_Root_Properties()
    {
        // Act
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.TenantId == "tenant-1" && u.IsDeleted == false;
        var result = _translator.Translate(predicate);

        // Assert
        result.SqlClause.ShouldBe("(c.TenantId = @p0 AND c.IsDeleted = @p1)");
        result.Parameters["@p0"].ShouldBe("tenant-1");
        result.Parameters["@p1"].ShouldBe(false);
    }

    [Fact]
    public void Translate_String_StartsWith_EndsWith_Contains()
    {
        // StartsWith
        Expression<Func<DocumentEnvelope<TestUser>, bool>> pStartsWith = u => u.Data.Name.StartsWith("Alice");
        var r1 = _translator.Translate(pStartsWith);
        r1.SqlClause.ShouldBe("STARTSWITH(c.Data.Name, @p0)");
        r1.Parameters["@p0"].ShouldBe("Alice");

        // EndsWith
        Expression<Func<DocumentEnvelope<TestUser>, bool>> pEndsWith = u => u.Data.Name.EndsWith("Smith");
        var r2 = _translator.Translate(pEndsWith);
        r2.SqlClause.ShouldBe("ENDSWITH(c.Data.Name, @p0)");
        r2.Parameters["@p0"].ShouldBe("Smith");

        // Contains
        Expression<Func<DocumentEnvelope<TestUser>, bool>> pContains = u => u.Data.Name.Contains("lic");
        var r3 = _translator.Translate(pContains);
        r3.SqlClause.ShouldBe("CONTAINS(c.Data.Name, @p0)");
        r3.Parameters["@p0"].ShouldBe("lic");
    }

    [Fact]
    public void Translate_Enumerable_Contains_Captured_Collection()
    {
        var allowedAges = new[] { 20, 30, 40 };
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => allowedAges.Contains(u.Data.Age);

        var result = _translator.Translate(predicate);

        result.SqlClause.ShouldBe("c.Data.Age IN (@p0, @p1, @p2)");
        result.Parameters.Count.ShouldBe(3);
        result.Parameters["@p0"].ShouldBe(20);
        result.Parameters["@p1"].ShouldBe(30);
        result.Parameters["@p2"].ShouldBe(40);
    }

    [Fact]
    public void Translate_Enumerable_Contains_Document_Collection_Property()
    {
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.Data.Tags.Contains("admin");

        var result = _translator.Translate(predicate);

        result.SqlClause.ShouldBe("ARRAY_CONTAINS(c.Data.Tags, @p0)");
        result.Parameters.Count.ShouldBe(1);
        result.Parameters["@p0"].ShouldBe("admin");
    }

    [Fact]
    public void Translate_Captured_Closure_Variables()
    {
        string targetName = "Charlie";
        int minimumAge = 25;
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.Data.Name == targetName && u.Data.Age >= minimumAge;

        var result = _translator.Translate(predicate);

        result.SqlClause.ShouldBe("(c.Data.Name = @p0 AND c.Data.Age >= @p1)");
        result.Parameters["@p0"].ShouldBe("Charlie");
        result.Parameters["@p1"].ShouldBe(25);
    }

    [Fact]
    public void Translate_Sql_Injection_Attempt_Safely_Parameterized()
    {
        string maliciousInput = "Alice' OR '1'='1";
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.Data.Name == maliciousInput;

        var result = _translator.Translate(predicate);

        result.SqlClause.ShouldBe("c.Data.Name = @p0");
        result.Parameters["@p0"].ShouldBe("Alice' OR '1'='1");
    }

    [Fact]
    public void Translate_Nested_Property_Access()
    {
        Expression<Func<DocumentEnvelope<TestUser>, bool>> predicate = u => u.Data.Address.City == "Seattle";

        var result = _translator.Translate(predicate);

        result.SqlClause.ShouldBe("c.Data.Address.City = @p0");
        result.Parameters["@p0"].ShouldBe("Seattle");
    }
}
