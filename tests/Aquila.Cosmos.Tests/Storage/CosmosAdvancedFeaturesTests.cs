using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Shouldly;
using Aquila.Core.Events;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed record TestPayload(string Name, int Count);

public sealed class CosmosAdvancedFeaturesTests
{
    [Fact]
    public void AquilaCosmosJsonSerializer_Serializes_And_Deserializes_Correctly()
    {
        var serializer = new AquilaCosmosJsonSerializer();
        var payload = new TestPayload("AquilaTest", 42);

        using var stream = serializer.ToStream(payload);
        stream.ShouldNotBeNull();
        stream.Length.ShouldBeGreaterThan(0);

        var deserialized = serializer.FromStream<TestPayload>(stream);
        deserialized.ShouldNotBeNull();
        deserialized.Name.ShouldBe("AquilaTest");
        deserialized.Count.ShouldBe(42);
    }

    [Fact]
    public void AquilaCosmosJsonSerializer_Handles_Null_And_Unreadable_Streams()
    {
        var serializer = new AquilaCosmosJsonSerializer();

        var nullRes = serializer.FromStream<TestPayload>(null!);
        nullRes.ShouldBeNull();

        var closedStream = new MemoryStream();
        closedStream.Dispose();
        var closedRes = serializer.FromStream<TestPayload>(closedStream);
        closedRes.ShouldBeNull();
    }

    [Fact]
    public void CosmosPartitionKeyHelper_Handles_Single_And_Hierarchical_PartitionKeys()
    {
        var singlePk = CosmosPartitionKeyHelper.CreatePartitionKey("pk-single");
        singlePk.ShouldNotBe(PartitionKey.Null);

        var hierarchicalPk = CosmosPartitionKeyHelper.CreatePartitionKey("tenant-A|stream-123");
        hierarchicalPk.ShouldNotBe(PartitionKey.Null);

        var emptyPk = CosmosPartitionKeyHelper.CreatePartitionKey("");
        emptyPk.ShouldBe(PartitionKey.Null);

        var nullPk = CosmosPartitionKeyHelper.CreatePartitionKey(null!);
        nullPk.ShouldBe(PartitionKey.Null);
    }

    [Fact]
    public void CreateDefaultContainerProperties_Configures_Composite_Indexes()
    {
        var docProps = CosmosStorageProvider.CreateDefaultContainerProperties("TestDocContainer", "/pk");
        docProps.Id.ShouldBe("TestDocContainer");
        docProps.PartitionKeyPath.ShouldBe("/pk");
        docProps.IndexingPolicy.CompositeIndexes.Count.ShouldBeGreaterThanOrEqualTo(1);
        docProps.IndexingPolicy.CompositeIndexes[0].ShouldContain(x => x.Path == "/_docType");
        docProps.IndexingPolicy.ExcludedPaths.ShouldNotContain(x => x.Path == "/data/*");

        var eventProps = CosmosStorageProvider.CreateDefaultEventsContainerProperties("TestEventContainer", "/pk");
        eventProps.Id.ShouldBe("TestEventContainer");
        eventProps.PartitionKeyPath.ShouldBe("/pk");
        eventProps.IndexingPolicy.CompositeIndexes.Count.ShouldBeGreaterThanOrEqualTo(2);
        eventProps.IndexingPolicy.CompositeIndexes[0].ShouldContain(x => x.Path == "/_docType");
        eventProps.IndexingPolicy.CompositeIndexes[1].ShouldContain(x => x.Path == "/data/GlobalSequence");
        eventProps.IndexingPolicy.ExcludedPaths.ShouldContain(x => x.Path == "/data/*");
    }

    [Fact]
    public void CosmosEventTypeResolver_Resolves_Types_Correctly()
    {
        var resolver = CosmosEventTypeResolver.Default;

        resolver.ResolveEventType("").ShouldBeNull();
        resolver.ResolveEventType("   ").ShouldBeNull();
        resolver.ResolveEventType("NonExistent.Assembly.TypeName").ShouldBeNull();

        var resolved = resolver.ResolveEventType(typeof(TestPayload).AssemblyQualifiedName!);
        resolved.ShouldBe(typeof(TestPayload));
    }

    [Fact]
    public void CosmosEventTypeResolver_EnsureTypedPayload_Handles_JToken_And_JsonElement()
    {
        var resolver = new CosmosEventTypeResolver();

        // 1. Null data
        var nullEvt = new EventEnvelope<object> { EventType = typeof(TestPayload).FullName!, Data = null! };
        resolver.EnsureTypedPayload(nullEvt);
        nullEvt.Data.ShouldBeNull();

        // 2. JToken data
        var jToken = JToken.FromObject(new TestPayload("JTokenItem", 99));
        var jTokenEvt = new EventEnvelope<object> { EventType = typeof(TestPayload).FullName!, Data = jToken };
        resolver.EnsureTypedPayload(jTokenEvt);
        jTokenEvt.Data.ShouldBeOfType<TestPayload>();
        ((TestPayload)jTokenEvt.Data).Name.ShouldBe("JTokenItem");

        // 3. JsonElement data
        var jsonBytes = Encoding.UTF8.GetBytes("{\"Name\":\"JsonElemItem\",\"Count\":88}");
        using var jsonDoc = JsonDocument.Parse(jsonBytes);
        var jsonElemEvt = new EventEnvelope<object> { EventType = typeof(TestPayload).FullName!, Data = jsonDoc.RootElement };
        resolver.EnsureTypedPayload(jsonElemEvt);
        jsonElemEvt.Data.ShouldBeOfType<TestPayload>();
        ((TestPayload)jsonElemEvt.Data).Name.ShouldBe("JsonElemItem");
    }

    [Fact]
    public void Direct_SubProvider_Instantiation_Validation()
    {
        Should.Throw<ArgumentNullException>(() => new CosmosDocumentStorageProvider((Func<Container>)null!));
        Should.Throw<ArgumentNullException>(() => new CosmosDocumentStorageProvider((Container)null!));
        Should.Throw<ArgumentNullException>(() => new CosmosEventStorageProvider((Func<Container>)null!));
        Should.Throw<ArgumentNullException>(() => new CosmosEventStorageProvider((Container)null!));
    }

    [Fact]
    public void StorageQueryResult_And_PagedResult_Hold_RequestCharge()
    {
        var result = new StorageQueryResult<TestPayload>(
            new[] { new DocumentEnvelope<TestPayload> { Id = "1", Data = new TestPayload("A", 1) } },
            continuationToken: "tok-1",
            totalCount: 1,
            requestCharge: 4.52);

        result.RequestCharge.ShouldBe(4.52);
        result.ContinuationToken.ShouldBe("tok-1");
        result.TotalCount.ShouldBe(1);

        var paged = new PagedResult<TestPayload>(new[] { new TestPayload("A", 1) }, pageNumber: 1, pageSize: 10, totalCount: 1)
        {
            RequestCharge = 4.52
        };
        paged.RequestCharge.ShouldBe(4.52);
    }
}
