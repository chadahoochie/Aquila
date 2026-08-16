using Shouldly;
using Aquila.Core.Configuration;

namespace Aquila.Core.Tests.Configuration;

public class DocumentMappingTests
{
    private class DocWithUppercaseId
    {
        public string Id { get; set; } = string.Empty;
    }

    private class DocWithLowercaseId
    {
        public string id { get; set; } = string.Empty;
    }

    private class DocWithoutId
    {
        public string CustomCode { get; set; } = string.Empty;
        public int IntKey { get; set; }
        public string? Region { get; set; }
    }

    [Fact]
    public void DefaultIdDiscovery_UppercaseId_DiscoversPropertyAndSelectsValue()
    {
        var mapping = new DocumentMapping<DocWithUppercaseId>();
        mapping.IdentityPropertyName.ShouldBe("Id");
        mapping.DocumentType.ShouldBe(typeof(DocWithUppercaseId));
        mapping.DocTypeName.ShouldBe(nameof(DocWithUppercaseId));

        var doc = new DocWithUppercaseId { Id = "abc-123" };
        mapping.IdSelector(doc).ShouldBe("abc-123");
    }

    [Fact]
    public void DefaultIdDiscovery_LowercaseId_DiscoversPropertyAndSelectsValue()
    {
        var mapping = new DocumentMapping<DocWithLowercaseId>();
        mapping.IdentityPropertyName.ShouldBe("id");

        var doc = new DocWithLowercaseId { id = "xyz-789" };
        mapping.IdSelector(doc).ShouldBe("xyz-789");
    }

    [Fact]
    public void DefaultIdDiscovery_NoIdProperty_FallsBackToDefaultIdentityPropertyNameAndGuidGenerator()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.IdentityPropertyName.ShouldBe("Id");

        var doc = new DocWithoutId { CustomCode = "Code1" };
        var generatedId = mapping.IdSelector(doc);

        generatedId.ShouldNotBeNullOrWhiteSpace();
        Guid.TryParse(generatedId, out _).ShouldBeTrue();
    }

    [Fact]
    public void DefaultIdSelector_NullDoc_ThrowsArgumentNullException()
    {
        var mapping = new DocumentMapping<DocWithUppercaseId>();
        Should.Throw<ArgumentNullException>(() => mapping.IdSelector(null!));

        var mappingNoId = new DocumentMapping<DocWithoutId>();
        Should.Throw<ArgumentNullException>(() => mappingNoId.IdSelector(null!));
    }

    [Fact]
    public void Identity_MemberExpression_ConfiguresIdentityPropertyAndSelector()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.Identity(x => x.CustomCode);

        mapping.IdentityPropertyName.ShouldBe("CustomCode");

        var doc = new DocWithoutId { CustomCode = "CODE-100" };
        mapping.IdSelector(doc).ShouldBe("CODE-100");
    }

    [Fact]
    public void Identity_ValueTypeBoxing_UnaryExpression_ConfiguresIdentityProperty()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.Identity(x => x.IntKey);

        mapping.IdentityPropertyName.ShouldBe("IntKey");

        var doc = new DocWithoutId { IntKey = 42 };
        mapping.IdSelector(doc).ShouldBe("42");
    }

    [Fact]
    public void Identity_NonMemberExpression_FallsBackToDefaultPropertyName()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.Identity(x => x.CustomCode.Substring(0));

        mapping.IdentityPropertyName.ShouldBe("Id");
    }

    [Fact]
    public void Identity_NullPropertyResult_FallsBackToGuid()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.Identity(x => x.CustomCode);

        var doc = new DocWithoutId { CustomCode = null! };
        var generatedId = mapping.IdSelector(doc);

        generatedId.ShouldNotBeNullOrWhiteSpace();
        Guid.TryParse(generatedId, out _).ShouldBeTrue();
    }

    [Fact]
    public void Identity_NullArgument_ThrowsArgumentNullException()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        Should.Throw<ArgumentNullException>(() => mapping.Identity(null!));
    }

    [Fact]
    public void Identity_NullDocPassedToIdSelector_ThrowsArgumentNullException()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.Identity(x => x.CustomCode);

        Should.Throw<ArgumentNullException>(() => mapping.IdSelector(null!));
    }

    [Fact]
    public void DefaultPartitionKeySelector_ReturnsTypeName()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.PartitionKeyPropertyName.ShouldBe(string.Empty);

        var doc = new DocWithoutId();
        mapping.PartitionKeySelector(doc).ShouldBe(nameof(DocWithoutId));

        Should.Throw<ArgumentNullException>(() => mapping.PartitionKeySelector(null!));
    }

    [Fact]
    public void PartitionKey_MemberExpression_ConfiguresPartitionKeyPropertyAndSelector()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.PartitionKey(x => x.Region!);

        mapping.PartitionKeyPropertyName.ShouldBe("Region");

        var doc = new DocWithoutId { Region = "US-East" };
        mapping.PartitionKeySelector(doc).ShouldBe("US-East");
    }

    [Fact]
    public void PartitionKey_NullPropertyValue_FallsBackToTypeName()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.PartitionKey(x => x.Region!);

        var doc = new DocWithoutId { Region = null };
        mapping.PartitionKeySelector(doc).ShouldBe(nameof(DocWithoutId));
    }

    [Fact]
    public void PartitionKey_NonMemberExpression_FallsBackToEmptyPropertyName()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.PartitionKey(x => x.CustomCode.ToUpper());

        mapping.PartitionKeyPropertyName.ShouldBe(string.Empty);
    }

    [Fact]
    public void PartitionKey_NullArgument_ThrowsArgumentNullException()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        Should.Throw<ArgumentNullException>(() => mapping.PartitionKey(null!));
    }

    [Fact]
    public void PartitionKey_NullDocPassedToPartitionKeySelector_ThrowsArgumentNullException()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.PartitionKey(x => x.Region!);

        Should.Throw<ArgumentNullException>(() => mapping.PartitionKeySelector(null!));
    }

    [Fact]
    public void SoftDeleted_And_UseOptimisticConcurrency_TogglesFlags()
    {
        var mapping = new DocumentMapping<DocWithoutId>();
        mapping.SoftDeletesEnabled.ShouldBeFalse();
        mapping.UseSoftDeletes.ShouldBeFalse();
        mapping.OptimisticConcurrencyEnabled.ShouldBeFalse();

        mapping.SoftDeleted();
        mapping.SoftDeletesEnabled.ShouldBeTrue();
        mapping.UseSoftDeletes.ShouldBeTrue();

        mapping.UseOptimisticConcurrency(true);
        mapping.OptimisticConcurrencyEnabled.ShouldBeTrue();

        mapping.UseOptimisticConcurrency(false);
        mapping.OptimisticConcurrencyEnabled.ShouldBeFalse();
    }

    [Fact]
    public void UseIdentityAsPartitionKey_RoutesPartitionKeyToDocumentId()
    {
        var mapping = new DocumentMapping<DocWithUppercaseId>();
        mapping.UseIdentityAsPartitionKey();

        mapping.PartitionKeyPropertyName.ShouldBe("Id");
        var doc = new DocWithUppercaseId { Id = "doc-999" };
        mapping.PartitionKeySelector(doc).ShouldBe("doc-999");
    }

    [Fact]
    public void SchemaPolicy_UseIdentityAsDefaultPartitionKey_AppliesToNewMappings()
    {
        var schema = new SchemaPolicy { UseIdentityAsDefaultPartitionKey = true };
        var mapping = schema.For<DocWithUppercaseId>();

        mapping.PartitionKeyPropertyName.ShouldBe("Id");
        var doc = new DocWithUppercaseId { Id = "doc-123" };
        mapping.PartitionKeySelector(doc).ShouldBe("doc-123");
    }
}
