using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Core.Abstractions;
using Aquila.Core.Patching;
using Aquila.Core.Sessions;

namespace Aquila.Benchmarks.Benchmarks.Patching;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class PatchExpressionBenchmarks
{
    private IDocumentStore _store = null!;
    private OrderDocument _seededOrder = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _store = DocumentStore.For(options =>
        {
            options.UseInMemoryStorage();
            options.Schema.For<OrderDocument>()
                .Identity(o => o.Id)
                .PartitionKey(o => o.Region);
            options.Schema.For<CustomerProfileDocument>()
                .Identity(c => c.Id)
                .PartitionKey(c => c.Region);
        });

        _seededOrder = BenchmarkDataGenerator.CreateOrder(1);

        using var session = _store.OpenSession(TrackingMode.Lightweight);
        session.Store(_seededOrder);
        await session.SaveChangesAsync();
    }

    [Benchmark(Description = "Build Single Set Operation")]
    public PatchExpression<OrderDocument> Build_Single_Set()
    {
        var patch = new PatchExpression<OrderDocument>();
        patch.Set(o => o.Status, "Shipped");
        return patch;
    }

    [Benchmark(Description = "Build Single Increment Operation")]
    public PatchExpression<CustomerProfileDocument> Build_Single_Increment()
    {
        var patch = new PatchExpression<CustomerProfileDocument>();
        patch.Increment(c => c.LoginCount, 1);
        return patch;
    }

    [Benchmark(Description = "Build Single Append Operation")]
    public PatchExpression<OrderDocument> Build_Single_Append()
    {
        var patch = new PatchExpression<OrderDocument>();
        patch.Append(o => o.Tags, "Expedited");
        return patch;
    }

    [Benchmark(Description = "Build Single Remove Operation")]
    public PatchExpression<OrderDocument> Build_Single_Remove()
    {
        var patch = new PatchExpression<OrderDocument>();
        patch.Remove(o => o.Tags, "Pending");
        return patch;
    }

    [Benchmark(Description = "Build Nested Property Pointer (Address.City)")]
    public PatchExpression<OrderDocument> Build_Nested_Property()
    {
        var patch = new PatchExpression<OrderDocument>();
        patch.Set(o => o.ShippingAddress.City, "San Francisco");
        return patch;
    }

    [Benchmark(Description = "Build Multi-Operation Compound Patch (4 Ops)")]
    public PatchExpression<CustomerProfileDocument> Build_MultiOperation_Patch()
    {
        var patch = new PatchExpression<CustomerProfileDocument>();
        patch.Set(c => c.Name, "Updated John Doe");
        patch.Increment(c => c.LoginCount, 1);
        patch.Append(c => c.Tags, "VIP");
        patch.Remove(c => c.Tags, "trial");
        return patch;
    }

    [Benchmark(Description = "Session.Patch Fluent Registration")]
    public void Session_Patch_Registration()
    {
        using var session = _store.OpenSession(TrackingMode.Lightweight);
        session.Patch<OrderDocument>(_seededOrder.Id, _seededOrder.Region)
            .Set(o => o.Status, "Completed")
            .Append(o => o.Tags, "benchmarked");
    }
}
