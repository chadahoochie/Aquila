using System.Linq.Expressions;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Benchmarks.Benchmarks.Cosmos;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class CosmosExpressionRewriterBenchmarks
{
    private Expression<Func<DocumentEnvelope<OrderDocument>, bool>> _simplePredicate = null!;
    private Expression<Func<DocumentEnvelope<OrderDocument>, bool>> _twoTermPredicate = null!;
    private Expression<Func<DocumentEnvelope<OrderDocument>, bool>> _complexPredicate = null!;
    private Expression<Func<DocumentEnvelope<OrderDocument>, bool>> _nestedPredicate = null!;

    [GlobalSetup]
    public void Setup()
    {
        _simplePredicate = e => e.Data.Id == "ORD-000001";

        _twoTermPredicate = e => e.Data.Region == "US-East" && e.Data.Status == "Pending";

        _complexPredicate = e => e.TenantId == "tenant-1" &&
                                 !e.IsDeleted &&
                                 (e.Data.Region == "US-East" || e.Data.Region == "US-West") &&
                                 e.Data.TotalAmount >= 100.00m &&
                                 e.Data.Status == "Pending";

        _nestedPredicate = e => e.Id == "ORD-000001" &&
                                e.DocType == "OrderDocument" &&
                                e.Data.ShippingAddress.City == "Seattle";
    }

    [Benchmark(Description = "Rewrite Simple Single Property Predicate")]
    public Expression<Func<CosmosDocumentEnvelope<OrderDocument>, bool>>? Rewrite_Simple()
    {
        return CosmosExpressionRewriter.Rewrite(_simplePredicate);
    }

    [Benchmark(Description = "Rewrite Two-Term And Predicate")]
    public Expression<Func<CosmosDocumentEnvelope<OrderDocument>, bool>>? Rewrite_TwoTerm()
    {
        return CosmosExpressionRewriter.Rewrite(_twoTermPredicate);
    }

    [Benchmark(Description = "Rewrite Complex Composite Predicate (5 Clauses)")]
    public Expression<Func<CosmosDocumentEnvelope<OrderDocument>, bool>>? Rewrite_Complex()
    {
        return CosmosExpressionRewriter.Rewrite(_complexPredicate);
    }

    [Benchmark(Description = "Rewrite Nested Property + Envelope Predicate")]
    public Expression<Func<CosmosDocumentEnvelope<OrderDocument>, bool>>? Rewrite_Nested()
    {
        return CosmosExpressionRewriter.Rewrite(_nestedPredicate);
    }
}
