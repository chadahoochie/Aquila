using Aquila.Core.Sessions;
using Shouldly;

namespace Aquila.Core.Tests;

public class StoreMetadataTests
{
    private class MetadataTestOrder
    {
        public Guid CustomOrderId { get; set; }
        public string Region { get; set; } = string.Empty;
    }

    private class MetadataTestCustomer
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    private class UnregisteredDoc
    {
        public string Id { get; set; } = string.Empty;
    }

    [Fact]
    public void RegisteredDocumentTypes_Returns_Configured_Types()
    {
        var store = DocumentStore.For(options =>
        {
            options.Schema.For<MetadataTestOrder>()
                .Identity(x => x.CustomOrderId)
                .PartitionKey(x => x.Region);

            options.Schema.For<MetadataTestCustomer>()
                .SoftDeleted();
        });

        var metadata = store.Metadata;
        metadata.RegisteredDocumentTypes.Count.ShouldBe(2);
        metadata.RegisteredDocumentTypes.ShouldContain(typeof(MetadataTestOrder));
        metadata.RegisteredDocumentTypes.ShouldContain(typeof(MetadataTestCustomer));
    }

    [Fact]
    public void MappingFor_Returns_Configured_DocumentMapping()
    {
        var store = DocumentStore.For(options =>
        {
            options.Schema.For<MetadataTestOrder>()
                .Identity(x => x.CustomOrderId)
                .PartitionKey(x => x.Region);
        });

        var mapping = store.Metadata.MappingFor<MetadataTestOrder>();
        mapping.DocumentType.ShouldBe(typeof(MetadataTestOrder));
        mapping.DocTypeName.ShouldBe(nameof(MetadataTestOrder));
        mapping.IdentityPropertyName.ShouldBe("CustomOrderId");
        mapping.PartitionKeyPropertyName.ShouldBe("Region");
        mapping.SoftDeletesEnabled.ShouldBeFalse();

        var untypedMapping = store.Metadata.MappingFor(typeof(MetadataTestOrder));
        untypedMapping.IdentityPropertyName.ShouldBe("CustomOrderId");
        untypedMapping.PartitionKeyPropertyName.ShouldBe("Region");
    }

    [Fact]
    public void IsSoftDeleted_Returns_True_When_SoftDelete_Configured()
    {
        var store = DocumentStore.For(options =>
        {
            options.Schema.For<MetadataTestCustomer>()
                .SoftDeleted();
        });

        store.Metadata.IsSoftDeleted(typeof(MetadataTestCustomer)).ShouldBeTrue();
        store.Metadata.IsSoftDeleted(typeof(MetadataTestOrder)).ShouldBeFalse();
        store.Metadata.MappingFor<MetadataTestCustomer>().SoftDeletesEnabled.ShouldBeTrue();
    }

    [Fact]
    public void MappingFor_Unregistered_Type_Returns_Default_Mapping()
    {
        var store = DocumentStore.For(options => { });

        var mapping = store.Metadata.MappingFor<UnregisteredDoc>();
        mapping.DocumentType.ShouldBe(typeof(UnregisteredDoc));
        mapping.DocTypeName.ShouldBe(nameof(UnregisteredDoc));
        mapping.IdentityPropertyName.ShouldBe("Id");
        mapping.PartitionKeyPropertyName.ShouldBe(string.Empty);
        mapping.SoftDeletesEnabled.ShouldBeFalse();

        store.Metadata.IsSoftDeleted(typeof(UnregisteredDoc)).ShouldBeFalse();
    }
}
