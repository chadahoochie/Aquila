using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Aquila.Benchmarks.Models;
using Aquila.Cosmos.Storage;

namespace Aquila.Benchmarks.Benchmarks.Cosmos;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class CosmosSerializationBenchmarks
{
    private AquilaCosmosJsonSerializer _serializer = null!;

    private CosmosDocumentEnvelope<CustomerProfileDocument> _smallEnvelope = null!;
    private CosmosDocumentEnvelope<OrderDocument> _largeEnvelope = null!;

    private byte[] _smallSerializedBytes = null!;
    private byte[] _largeSerializedBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _serializer = new AquilaCosmosJsonSerializer();

        var customer = BenchmarkDataGenerator.CreateCustomer(1);
        _smallEnvelope = new CosmosDocumentEnvelope<CustomerProfileDocument>
        {
            Id = customer.Id,
            PartitionKey = customer.Region,
            DocType = nameof(CustomerProfileDocument),
            TenantId = "tenant-001",
            IsDeleted = false,
            Data = customer
        };

        var order = BenchmarkDataGenerator.CreateOrder(1, itemCount: 5);
        _largeEnvelope = new CosmosDocumentEnvelope<OrderDocument>
        {
            Id = order.Id,
            PartitionKey = order.Region,
            DocType = nameof(OrderDocument),
            TenantId = "tenant-001",
            IsDeleted = false,
            Data = order
        };

        using var smallStream = _serializer.ToStream(_smallEnvelope);
        using var smallMs = new MemoryStream();
        smallStream.CopyTo(smallMs);
        _smallSerializedBytes = smallMs.ToArray();

        using var largeStream = _serializer.ToStream(_largeEnvelope);
        using var largeMs = new MemoryStream();
        largeStream.CopyTo(largeMs);
        _largeSerializedBytes = largeMs.ToArray();
    }

    [Benchmark(Description = "ToStream (Small Document Envelope)")]
    public Stream ToStream_SmallDocument()
    {
        return _serializer.ToStream(_smallEnvelope);
    }

    [Benchmark(Description = "FromStream (Small Document Envelope)")]
    public CosmosDocumentEnvelope<CustomerProfileDocument> FromStream_SmallDocument()
    {
        var ms = new MemoryStream(_smallSerializedBytes, writable: false);
        return _serializer.FromStream<CosmosDocumentEnvelope<CustomerProfileDocument>>(ms);
    }

    [Benchmark(Description = "Roundtrip SerDe (Small Document Envelope)")]
    public CosmosDocumentEnvelope<CustomerProfileDocument> Roundtrip_SmallDocument()
    {
        using var stream = _serializer.ToStream(_smallEnvelope);
        return _serializer.FromStream<CosmosDocumentEnvelope<CustomerProfileDocument>>(stream);
    }

    [Benchmark(Description = "ToStream (Large Document Envelope)")]
    public Stream ToStream_LargeDocument()
    {
        return _serializer.ToStream(_largeEnvelope);
    }

    [Benchmark(Description = "FromStream (Large Document Envelope)")]
    public CosmosDocumentEnvelope<OrderDocument> FromStream_LargeDocument()
    {
        var ms = new MemoryStream(_largeSerializedBytes, writable: false);
        return _serializer.FromStream<CosmosDocumentEnvelope<OrderDocument>>(ms);
    }

    [Benchmark(Description = "Roundtrip SerDe (Large Document Envelope)")]
    public CosmosDocumentEnvelope<OrderDocument> Roundtrip_LargeDocument()
    {
        using var stream = _serializer.ToStream(_largeEnvelope);
        return _serializer.FromStream<CosmosDocumentEnvelope<OrderDocument>>(stream);
    }
}
