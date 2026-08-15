using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Cosmos.Storage;
using Microsoft.Azure.Cosmos;

namespace Aquila.Benchmarks.Benchmarks.Cosmos;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class CosmosPartitionKeyBenchmarks
{
    private const string EmptyKey = "";
    private const string SinglePartKey = "US-East";
    private const string TwoPartKey = "tenant-primary|US-East";
    private const string ThreePartKey = "tenant-primary|US-East|engineering";
    private const string FourPartKey = "tenant-primary|US-East|engineering|team-aquila";

    [Benchmark(Description = "Empty / Null PartitionKey")]
    public PartitionKey Empty_PartitionKey()
    {
        return CosmosPartitionKeyHelper.CreatePartitionKey(EmptyKey);
    }

    [Benchmark(Description = "Single-Part PartitionKey")]
    public PartitionKey SinglePart_PartitionKey()
    {
        return CosmosPartitionKeyHelper.CreatePartitionKey(SinglePartKey);
    }

    [Benchmark(Description = "Hierarchical 2-Part PartitionKey")]
    public PartitionKey TwoPart_Hierarchical_PartitionKey()
    {
        return CosmosPartitionKeyHelper.CreatePartitionKey(TwoPartKey);
    }

    [Benchmark(Description = "Hierarchical 3-Part PartitionKey")]
    public PartitionKey ThreePart_Hierarchical_PartitionKey()
    {
        return CosmosPartitionKeyHelper.CreatePartitionKey(ThreePartKey);
    }

    [Benchmark(Description = "Hierarchical 4-Part PartitionKey")]
    public PartitionKey FourPart_Hierarchical_PartitionKey()
    {
        return CosmosPartitionKeyHelper.CreatePartitionKey(FourPartKey);
    }
}
