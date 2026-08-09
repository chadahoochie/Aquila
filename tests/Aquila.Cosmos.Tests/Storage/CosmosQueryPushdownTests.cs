using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using Shouldly;
using Xunit;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record PushdownDoc(string Name, int Value);

public sealed class CosmosQueryPushdownTests
{
    [Fact]
    public void CosmosExpressionRewriter_Rewrites_DocumentEnvelope_To_CosmosDocumentEnvelope()
    {
        Expression<Func<DocumentEnvelope<PushdownDoc>, bool>> predicate = x =>
            x.Id == "doc-1" &&
            x.PartitionKey == "pk-1" &&
            x.DocType == nameof(PushdownDoc) &&
            x.TenantId == "tenant-1" &&
            !x.IsDeleted &&
            x.Version == "v1" &&
            x.ETag == "etag-1" &&
            x.Data.Value > 10;

        var rewritten = CosmosExpressionRewriter.Rewrite(predicate);

        rewritten.ShouldNotBeNull();
        rewritten.Parameters.Count.ShouldBe(1);
        rewritten.Parameters[0].Type.ShouldBe(typeof(CosmosDocumentEnvelope<PushdownDoc>));

        var compiled = rewritten.Compile();

        var matchingEnvelope = new CosmosDocumentEnvelope<PushdownDoc>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(PushdownDoc),
            TenantId = "tenant-1",
            IsDeleted = false,
            Version = "v1",
            ETag = "etag-1",
            Data = new PushdownDoc("Test", 42)
        };

        var nonMatchingEnvelope = new CosmosDocumentEnvelope<PushdownDoc>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(PushdownDoc),
            TenantId = "tenant-1",
            IsDeleted = false,
            Version = "v1",
            ETag = "etag-1",
            Data = new PushdownDoc("Test", 5)
        };

        compiled(matchingEnvelope).ShouldBeTrue();
        compiled(nonMatchingEnvelope).ShouldBeFalse();
    }

    [Fact]
    public void CosmosExpressionRewriter_Handles_Null_Predicate()
    {
        var rewritten = CosmosExpressionRewriter.Rewrite<PushdownDoc>(null);
        rewritten.ShouldBeNull();
    }

    [Fact]
    public async Task CosmosStorageProvider_QueryDocumentsAsync_Uses_GetItemLinqQueryable_With_QueryOptions()
    {
        var mockContainer = Substitute.For<Container>();
        var mockClient = Substitute.For<CosmosClient>();
        mockClient.GetContainer(Arg.Any<string>(), Arg.Any<string>()).Returns(mockContainer);

        var provider = new CosmosStorageProvider(mockClient, "TestDb", "TestContainer");

        var fakeList = new List<CosmosDocumentEnvelope<PushdownDoc>>().AsQueryable() as IOrderedQueryable<CosmosDocumentEnvelope<PushdownDoc>>;
        mockContainer.GetItemLinqQueryable<CosmosDocumentEnvelope<PushdownDoc>>(
            Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<QueryRequestOptions>(), Arg.Any<CosmosLinqSerializerOptions>())
            .Returns(fakeList);

        Expression<Func<DocumentEnvelope<PushdownDoc>, bool>> predicate = x => x.Data.Value > 20;
        var options = new QueryOptions
        {
            PartitionKey = "pk-test",
            MaxItemCount = 50,
            ContinuationToken = "token-123"
        };

        await provider.Documents.QueryDocumentsAsync(predicate, options, TestContext.Current.CancellationToken);

        mockContainer.Received(1).GetItemLinqQueryable<CosmosDocumentEnvelope<PushdownDoc>>(
            false,
            "token-123",
            Arg.Is<QueryRequestOptions>(r =>
                r.PartitionKey == new PartitionKey("pk-test") &&
                r.MaxItemCount == 50),
            Arg.Any<CosmosLinqSerializerOptions>());
    }
    [Fact]
    public void CosmosExpressionRewriter_Throws_ArgumentException_When_Predicate_Has_No_Parameters()
    {
        var validLambda = Expression.Lambda<Func<DocumentEnvelope<PushdownDoc>, bool>>(
            Expression.Constant(true),
            Expression.Parameter(typeof(DocumentEnvelope<PushdownDoc>), "x"));

        var type = validLambda.GetType();
        while (type != null && type != typeof(object))
        {
            var fields = type.GetFields(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
            foreach (var f in fields)
            {
                if (f.FieldType == typeof(System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>))
                {
                    f.SetValue(validLambda, new System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>(new List<ParameterExpression>()));
                }
                else if (f.FieldType == typeof(object) && f.Name.Contains("par", StringComparison.OrdinalIgnoreCase))
                {
                    f.SetValue(validLambda, new System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>(new List<ParameterExpression>()));
                }
            }
            type = type.BaseType;
        }

        Should.Throw<ArgumentException>(() => CosmosExpressionRewriter.Rewrite(validLambda));
    }

    [Fact]
    public void CosmosExpressionRewriter_Handles_Different_Parameter_Nodes()
    {
        var oldParam = Expression.Parameter(typeof(DocumentEnvelope<PushdownDoc>), "x");
        var otherParam = Expression.Parameter(typeof(string), "other");
        var body = Expression.Equal(otherParam, Expression.Constant("val"));
        var lambda = Expression.Lambda<Func<DocumentEnvelope<PushdownDoc>, bool>>(body, oldParam);

        var rewritten = CosmosExpressionRewriter.Rewrite(lambda);
        rewritten.ShouldNotBeNull();
    }

    [Fact]
    public void CosmosExpressionRewriter_Handles_Static_Member_Access()
    {
        Expression<Func<DocumentEnvelope<PushdownDoc>, bool>> predicate = x => DateTime.Now.Year > 2020;
        var rewritten = CosmosExpressionRewriter.Rewrite(predicate);
        rewritten.ShouldNotBeNull();
    }

    [Fact]
    public void CosmosExpressionRewriter_Handles_Member_Access_On_Nested_Objects()
    {
        Expression<Func<DocumentEnvelope<PushdownDoc>, bool>> predicate = x => x.Data.Name == "nested";
        var rewritten = CosmosExpressionRewriter.Rewrite(predicate);
        rewritten.ShouldNotBeNull();

        var compiled = rewritten.Compile();
        var matching = new CosmosDocumentEnvelope<PushdownDoc> { Data = new PushdownDoc("nested", 1) };
        var nonMatching = new CosmosDocumentEnvelope<PushdownDoc> { Data = new PushdownDoc("other", 1) };

        compiled(matching).ShouldBeTrue();
        compiled(nonMatching).ShouldBeFalse();
    }
}
