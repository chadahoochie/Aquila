using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Aquila.Core.Configuration;
using Aquila.Core.Queries;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Shouldly;
using Xunit;

namespace Aquila.Core.Tests;

public sealed record UserDoc(string Id, string Email, int Age, bool IsActive);

public class UserByEmailQuery : ICompiledQuery<UserDoc, UserDoc?>
{
    public int QueryIsCallCount { get; private set; }
    public string Email { get; set; } = string.Empty;

    public Expression<Func<IQueryable<UserDoc>, UserDoc?>> QueryIs()
    {
        QueryIsCallCount++;
        return users => users.FirstOrDefault(u => u.Email == Email);
    }
}

public class ActiveUsersByMinAgeQuery : ICompiledQuery<UserDoc, IEnumerable<UserDoc>>
{
    public int QueryIsCallCount { get; private set; }
    public int MinAge { get; set; }

    public Expression<Func<IQueryable<UserDoc>, IEnumerable<UserDoc>>> QueryIs()
    {
        QueryIsCallCount++;
        return users => users.Where(u => u.IsActive && u.Age >= MinAge);
    }
}

public class ActiveUsersCountQuery : ICompiledQuery<UserDoc, int>
{
    public int QueryIsCallCount { get; private set; }

    public Expression<Func<IQueryable<UserDoc>, int>> QueryIs()
    {
        QueryIsCallCount++;
        return users => users.Count(u => u.IsActive);
    }
}

public class CompiledQueryTests
{
    private readonly InMemoryStorageProvider _storage;
    private readonly StoreOptions _options;

    public CompiledQueryTests()
    {
        _storage = new InMemoryStorageProvider();
        _options = new StoreOptions { StorageProvider = _storage };
        CompiledQueryCache.Clear();
    }

    [Fact]
    public async Task QueryAsync_WithCompiledQuery_ExecutesAndReturnsResult()
    {
        // Arrange
        using var session = new DocumentSession(_storage, _options);
        session.Store(new UserDoc("1", "alice@example.com", 30, true));
        session.Store(new UserDoc("2", "bob@example.com", 25, false));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var query = new UserByEmailQuery { Email = "alice@example.com" };

        // Act
        var result = await session.QueryAsync(query, TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe("1");
        result.Email.ShouldBe("alice@example.com");
    }

    [Fact]
    public async Task QueryAsync_MultipleInvocations_CachesExpressionPlanAndBindsParameters()
    {
        // Arrange
        using var session = new DocumentSession(_storage, _options);
        session.Store(new UserDoc("1", "alice@example.com", 30, true));
        session.Store(new UserDoc("2", "bob@example.com", 25, true));
        session.Store(new UserDoc("3", "charlie@example.com", 35, false));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var query1 = new UserByEmailQuery { Email = "alice@example.com" };
        var query2 = new UserByEmailQuery { Email = "bob@example.com" };
        var query3 = new UserByEmailQuery { Email = "charlie@example.com" };

        // Act
        var result1 = await session.QueryAsync(query1, TestContext.Current.CancellationToken);
        var result2 = await session.QueryAsync(query2, TestContext.Current.CancellationToken);
        var result3 = await session.QueryAsync(query3, TestContext.Current.CancellationToken);

        // Assert
        result1.ShouldNotBeNull();
        result1.Id.ShouldBe("1");

        result2.ShouldNotBeNull();
        result2.Id.ShouldBe("2");

        result3.ShouldNotBeNull();
        result3.Id.ShouldBe("3");

        // QueryIs should only be called once when compiling the query plan for UserByEmailQuery
        query1.QueryIsCallCount.ShouldBe(1);
        query2.QueryIsCallCount.ShouldBe(0);
        query3.QueryIsCallCount.ShouldBe(0);
    }

    [Fact]
    public async Task QueryAsync_CollectionResult_ExecutesFilteringCorrectly()
    {
        // Arrange
        using var session = new DocumentSession(_storage, _options);
        session.Store(new UserDoc("1", "alice@example.com", 30, true));
        session.Store(new UserDoc("2", "bob@example.com", 20, true));
        session.Store(new UserDoc("3", "charlie@example.com", 35, false));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var query = new ActiveUsersByMinAgeQuery { MinAge = 25 };

        // Act
        var results = (await session.QueryAsync(query, TestContext.Current.CancellationToken)).ToList();

        // Assert
        results.Count.ShouldBe(1);
        results[0].Email.ShouldBe("alice@example.com");
    }

    [Fact]
    public async Task QueryAsync_ScalarResult_ReturnsCorrectValue()
    {
        // Arrange
        using var session = new DocumentSession(_storage, _options);
        session.Store(new UserDoc("1", "alice@example.com", 30, true));
        session.Store(new UserDoc("2", "bob@example.com", 20, true));
        session.Store(new UserDoc("3", "charlie@example.com", 35, false));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var query = new ActiveUsersCountQuery();

        // Act
        var count = await session.QueryAsync(query, TestContext.Current.CancellationToken);

        // Assert
        count.ShouldBe(2);
    }

    [Fact]
    public async Task CompiledQueryCache_Clear_ResetsCache()
    {
        // Arrange
        using var session = new DocumentSession(_storage, _options);
        session.Store(new UserDoc("1", "alice@example.com", 30, true));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var query1 = new UserByEmailQuery { Email = "alice@example.com" };
        await session.QueryAsync(query1, TestContext.Current.CancellationToken);
        query1.QueryIsCallCount.ShouldBe(1);

        // Act
        CompiledQueryCache.Clear();

        var query2 = new UserByEmailQuery { Email = "alice@example.com" };
        await session.QueryAsync(query2, TestContext.Current.CancellationToken);

        // Assert
        query2.QueryIsCallCount.ShouldBe(1);
    }

    [Fact]
    public async Task QueryAsync_NullQuery_ThrowsArgumentNullException()
    {
        using var session = new DocumentSession(_storage, _options);
        await Should.ThrowAsync<ArgumentNullException>(() => session.QueryAsync<UserDoc, UserDoc?>(null!));
    }
}
