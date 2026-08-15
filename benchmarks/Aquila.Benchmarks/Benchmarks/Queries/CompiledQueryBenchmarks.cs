using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Queries;
using Aquila.Core.Sessions;

namespace Aquila.Benchmarks.Benchmarks.Queries;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class CompiledQueryBenchmarks
{
    private IDocumentStore _store = null!;
    private IQueryable<OrderDocument> _inMemoryQueryable = null!;
    private ActiveOrdersByCustomerQuery _compiledQuery = null!;
    private const string TargetCustomerId = "CUST-000005";
    private const string TargetStatus = "Pending";

    [GlobalSetup]
    public async Task Setup()
    {
        _store = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Schema.For<OrderDocument>()
                .Identity(o => o.Id)
                .PartitionKey(o => o.Region);
        });

        var orders = BenchmarkDataGenerator.CreateOrders(500);

        using (var session = _store.OpenSession(TrackingMode.Lightweight))
        {
            foreach (var order in orders)
            {
                session.Store(order);
            }
            await session.SaveChangesAsync();
        }

        _inMemoryQueryable = orders.AsQueryable();
        _compiledQuery = new ActiveOrdersByCustomerQuery(TargetCustomerId, TargetStatus);

        // Warm up the compiled query cache for steady-state benchmarks
        CompiledQueryCache.Execute(_inMemoryQueryable, _compiledQuery);
    }

    [Benchmark(Description = "Ad-Hoc LINQ Lambda Where Execution")]
    public List<OrderDocument> AdHoc_Linq_Where()
    {
        return _inMemoryQueryable
            .Where(o => o.CustomerId == TargetCustomerId && o.Status == TargetStatus)
            .ToList();
    }

    [Benchmark(Description = "CompiledQueryCache.Execute (Cached Delegate Steady-State)")]
    public List<OrderDocument> CompiledQuery_Cache_Hit()
    {
        return CompiledQueryCache.Execute(_inMemoryQueryable, _compiledQuery).ToList();
    }

    [Benchmark(Description = "CompiledQueryCache Compilation (Cache Miss Cold Path)")]
    public List<OrderDocument> CompiledQuery_Cache_Miss_Cold()
    {
        CompiledQueryCache.Clear();
        return CompiledQueryCache.Execute(_inMemoryQueryable, _compiledQuery).ToList();
    }

    [Benchmark(Description = "Session.QueryAsync (Ad-Hoc Predicate)")]
    public async Task<IReadOnlyList<OrderDocument>> Session_QueryAsync_AdHoc()
    {
        using var session = _store.QuerySession();
        return await session.QueryAsync<OrderDocument>(e => e.Data.CustomerId == TargetCustomerId && e.Data.Status == TargetStatus);
    }

    [Benchmark(Description = "Session.QueryAsync (Compiled Query Cached)")]
    public async Task<List<OrderDocument>> Session_QueryAsync_CompiledQuery()
    {
        using var session = _store.QuerySession();
        var queryable = await session.QueryAsync(_compiledQuery);
        return queryable.ToList();
    }
}
