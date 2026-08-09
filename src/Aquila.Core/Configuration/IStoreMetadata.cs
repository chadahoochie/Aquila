namespace Aquila.Core.Configuration;

public interface IStoreMetadata
{
    IReadOnlyCollection<Type> RegisteredDocumentTypes { get; }
    DocumentMapping MappingFor(Type documentType);
    DocumentMapping MappingFor<T>();
    bool IsSoftDeleted(Type documentType);
}

public sealed class DocumentMapping
{
    public Type DocumentType { get; set; } = default!;
    public string DocTypeName { get; set; } = string.Empty;
    public string IdentityPropertyName { get; set; } = "Id";
    public string PartitionKeyPropertyName { get; set; } = string.Empty;
    public bool SoftDeletesEnabled { get; set; }
}

internal interface IDocumentMappingInfo
{
    Type DocumentType { get; }
    string DocTypeName { get; }
    string IdentityPropertyName { get; }
    string PartitionKeyPropertyName { get; }
    bool SoftDeletesEnabled { get; }
}
