using System.Linq.Expressions;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record SortItem(string Id, string Name, int Score, decimal Price);

public class CosmosOrderingPushdownTests
{
    [Fact]
    public void CosmosExpressionRewriter_Rewrites_LambdaExpression_SortKey()
    {
        Expression<Func<DocumentEnvelope<SortItem>, object?>> keySelector = env => env.Data.Score;
        var rewritten = CosmosExpressionRewriter.Rewrite<SortItem>(keySelector);

        rewritten.ShouldNotBeNull();
        rewritten.Parameters.Count.ShouldBe(1);
        rewritten.Parameters[0].Type.ShouldBe(typeof(CosmosDocumentEnvelope<SortItem>));
    }

    [Fact]
    public void CosmosDocumentStorageProvider_ApplyOrdering_OrdersQueryableAscending()
    {
        var items = new List<CosmosDocumentEnvelope<SortItem>>
        {
            new() { Id = "3", Data = new SortItem("3", "Charlie", 30, 300m) },
            new() { Id = "1", Data = new SortItem("1", "Alice", 10, 100m) },
            new() { Id = "2", Data = new SortItem("2", "Bob", 20, 200m) },
        };

        var queryable = items.AsQueryable();
        var orderings = new List<SortDescriptor>
        {
            new((Expression<Func<DocumentEnvelope<SortItem>, object?>>)(env => env.Data.Score), SortOrder.Ascending)
        };

        var ordered = CosmosDocumentStorageProvider.ApplyOrdering(queryable, orderings);
        var resultList = ordered.ToList();

        resultList.Count.ShouldBe(3);
        resultList[0].Data.Score.ShouldBe(10);
        resultList[1].Data.Score.ShouldBe(20);
        resultList[2].Data.Score.ShouldBe(30);
    }

    [Fact]
    public void CosmosDocumentStorageProvider_ApplyOrdering_OrdersQueryableDescending()
    {
        var items = new List<CosmosDocumentEnvelope<SortItem>>
        {
            new() { Id = "1", Data = new SortItem("1", "Alice", 10, 100m) },
            new() { Id = "3", Data = new SortItem("3", "Charlie", 30, 300m) },
            new() { Id = "2", Data = new SortItem("2", "Bob", 20, 200m) },
        };

        var queryable = items.AsQueryable();
        var orderings = new List<SortDescriptor>
        {
            new((Expression<Func<DocumentEnvelope<SortItem>, object?>>)(env => env.Data.Score), SortOrder.Descending)
        };

        var ordered = CosmosDocumentStorageProvider.ApplyOrdering(queryable, orderings);
        var resultList = ordered.ToList();

        resultList.Count.ShouldBe(3);
        resultList[0].Data.Score.ShouldBe(30);
        resultList[1].Data.Score.ShouldBe(20);
        resultList[2].Data.Score.ShouldBe(10);
    }

    [Fact]
    public void CosmosDocumentStorageProvider_ApplyOrdering_MultipleOrderings_CompositeSort()
    {
        var items = new List<CosmosDocumentEnvelope<SortItem>>
        {
            new() { Id = "1", Data = new SortItem("1", "Alice", 10, 50m) },
            new() { Id = "2", Data = new SortItem("2", "Alice", 10, 100m) },
            new() { Id = "3", Data = new SortItem("3", "Bob", 20, 20m) },
            new() { Id = "4", Data = new SortItem("4", "Bob", 20, 80m) },
        };

        var queryable = items.AsQueryable();
        var orderings = new List<SortDescriptor>
        {
            new((Expression<Func<DocumentEnvelope<SortItem>, object?>>)(env => env.Data.Name), SortOrder.Ascending),
            new((Expression<Func<DocumentEnvelope<SortItem>, object?>>)(env => env.Data.Price), SortOrder.Descending)
        };

        var ordered = CosmosDocumentStorageProvider.ApplyOrdering(queryable, orderings);
        var resultList = ordered.ToList();

        resultList.Count.ShouldBe(4);
        resultList[0].Id.ShouldBe("2"); // Alice, 100
        resultList[1].Id.ShouldBe("1"); // Alice, 50
        resultList[2].Id.ShouldBe("4"); // Bob, 80
        resultList[3].Id.ShouldBe("3"); // Bob, 20
    }

    [Fact]
    public async Task CosmosStorageProvider_QueryDocumentsAsync_Passes_Orderings_In_QueryOptions()
    {
        var mockContainer = Substitute.For<Container>();
        var mockClient = Substitute.For<CosmosClient>();
        mockClient.GetContainer(Arg.Any<string>(), Arg.Any<string>()).Returns(mockContainer);

        var provider = new CosmosStorageProvider(mockClient, "TestDb", "TestContainer");

        var fakeList = new List<CosmosDocumentEnvelope<SortItem>>().AsQueryable() as IOrderedQueryable<CosmosDocumentEnvelope<SortItem>>;
        mockContainer.GetItemLinqQueryable<CosmosDocumentEnvelope<SortItem>>(
            Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<QueryRequestOptions>(), Arg.Any<CosmosLinqSerializerOptions>())
            .Returns(fakeList);

        var options = new QueryOptions()
            .OrderBy<SortItem>(env => env.Data.Price, SortOrder.Descending);

        await provider.QueryDocumentsAsync<SortItem>(options: options, ct: TestContext.Current.CancellationToken);

        mockContainer.Received(1).GetItemLinqQueryable<CosmosDocumentEnvelope<SortItem>>(
            false,
            null,
            Arg.Any<QueryRequestOptions>(),
            Arg.Any<CosmosLinqSerializerOptions>());
    }
}
