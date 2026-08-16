using System.Linq.Expressions;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Storage;

public sealed record CustomerRecord(string Id, string Name, int Age, decimal Balance, DateTime JoinedAt);

public class SqlExpressionTranslatorOrderingTests
{
    private readonly DefaultSqlExpressionTranslator _translator = new();

    [Fact]
    public void TranslateOrderBy_SingleProperty_Ascending_ProducesCorrectSql()
    {
        Expression<Func<DocumentEnvelope<CustomerRecord>, object?>> orderBy = env => env.Data.Age;
        var sql = _translator.TranslateOrderBy(orderBy, SortOrder.Ascending);

        sql.ShouldBe("ORDER BY c.Data.Age ASC");
    }

    [Fact]
    public void TranslateOrderBy_SingleProperty_Descending_ProducesCorrectSql()
    {
        Expression<Func<DocumentEnvelope<CustomerRecord>, object?>> orderBy = env => env.Data.Balance;
        var sql = _translator.TranslateOrderBy(orderBy, SortOrder.Descending);

        sql.ShouldBe("ORDER BY c.Data.Balance DESC");
    }

    [Fact]
    public void TranslateOrderBy_RootIdProperty_ProducesCorrectSql()
    {
        Expression<Func<DocumentEnvelope<CustomerRecord>, object?>> orderBy = env => env.Id;
        var sql = _translator.TranslateOrderBy(orderBy, SortOrder.Ascending);

        sql.ShouldBe("ORDER BY c.Id ASC");
    }

    [Fact]
    public void TranslateOrderBy_MultipleProperties_ProducesCorrectSql()
    {
        var orderings = new[]
        {
            new SortDescriptor((Expression<Func<DocumentEnvelope<CustomerRecord>, object?>>)(env => env.Data.Name), SortOrder.Ascending),
            new SortDescriptor((Expression<Func<DocumentEnvelope<CustomerRecord>, object?>>)(env => env.Data.Age), SortOrder.Descending)
        };

        var sql = _translator.TranslateOrderBy(orderings);

        sql.ShouldBe("ORDER BY c.Data.Name ASC, c.Data.Age DESC");
    }

    [Fact]
    public void TranslateOrderBy_EmptyList_ReturnsEmptyString()
    {
        var sql = _translator.TranslateOrderBy(Array.Empty<SortDescriptor>());
        sql.ShouldBe(string.Empty);
    }

    [Fact]
    public void TranslateOrderBy_NullExpressions_ThrowsArgumentNullException()
    {
        Should.Throw<ArgumentNullException>(() =>
            _translator.TranslateOrderBy<CustomerRecord>(null!, SortOrder.Ascending));

        Should.Throw<ArgumentNullException>(() =>
            _translator.TranslateOrderBy(null!));
    }

    [Fact]
    public void TranslateOrderBy_InvalidExpression_ThrowsNotSupportedException()
    {
        // Method call inside orderBy expression (e.g. env => env.Data.Name.ToUpper()) is not a simple property access
        Expression<Func<DocumentEnvelope<CustomerRecord>, object?>> invalidOrderBy = env => env.Data.Name.ToUpper();

        Should.Throw<NotSupportedException>(() =>
            _translator.TranslateOrderBy(invalidOrderBy, SortOrder.Ascending));
    }
}
