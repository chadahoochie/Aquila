using System;
using System.Text.Json;
using Shouldly;
using Xunit;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record SampleData(string Key, string Value);

public sealed class CosmosDocumentEnvelopeTests
{
    [Fact]
    public void CosmosDocumentEnvelope_Initializes_With_Defaults()
    {
        var envelope = new CosmosDocumentEnvelope<SampleData>();

        envelope.Id.ShouldBe(string.Empty);
        envelope.PartitionKey.ShouldBe(string.Empty);
        envelope.DocType.ShouldBe(nameof(SampleData));
        envelope.TenantId.ShouldBe("default");
        envelope.IsDeleted.ShouldBeFalse();
        envelope.Version.ShouldBe("1");

        var nonGeneric = new CosmosDocumentEnvelope();
        nonGeneric.Id.ShouldBe(string.Empty);
        nonGeneric.PartitionKey.ShouldBe(string.Empty);
        nonGeneric.DocType.ShouldBe(string.Empty);
        nonGeneric.TenantId.ShouldBe("default");
        nonGeneric.IsDeleted.ShouldBeFalse();
        nonGeneric.Version.ShouldBe("1");
    }

    [Fact]
    public void CosmosDocumentEnvelope_Serializes_And_Deserializes_Correctly()
    {
        var data = new SampleData("K1", "V1");
        var envelope = new CosmosDocumentEnvelope<SampleData>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(SampleData),
            TenantId = "tenant-a",
            IsDeleted = false,
            Version = "v1",
            Data = data
        };

        var json = JsonSerializer.Serialize(envelope);
        json.ShouldContain("\"id\":\"doc-1\"");
        json.ShouldContain("\"pk\":\"pk-1\"");
        json.ShouldContain("\"_docType\":\"SampleData\"");
        json.ShouldContain("\"_tenantId\":\"tenant-a\"");

        var deserialized = JsonSerializer.Deserialize<CosmosDocumentEnvelope<SampleData>>(json);
        deserialized.ShouldNotBeNull();
        deserialized.Id.ShouldBe("doc-1");
        deserialized.PartitionKey.ShouldBe("pk-1");
        deserialized.TenantId.ShouldBe("tenant-a");
        deserialized.Data.Key.ShouldBe("K1");
        deserialized.Data.Value.ShouldBe("V1");
    }

    [Fact]
    public void CosmosStorageProvider_Constructor_Validates_Parameters()
    {
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider("", "db", "container"));
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider("conn", "", "container"));
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider("conn", "db", ""));
        Should.Throw<ArgumentNullException>(() => new CosmosStorageProvider((Microsoft.Azure.Cosmos.CosmosClient)null!));
    }
}
