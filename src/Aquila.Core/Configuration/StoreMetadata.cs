using System.Collections.Concurrent;

namespace Aquila.Core.Configuration;

public sealed class StoreMetadata : IStoreMetadata
{
    private readonly ConcurrentDictionary<Type, DocumentMapping> _cache = new();
    private readonly StoreOptions _options;

    public StoreMetadata(StoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    public IReadOnlyCollection<Type> RegisteredDocumentTypes => _options.Schema.Mappings.Keys.ToList();

    public DocumentMapping MappingFor<T>()
    {
        return MappingFor(typeof(T));
    }

    public DocumentMapping MappingFor(Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentType);
        return _cache.GetOrAdd(documentType, BuildMappingForType);
    }

    public bool IsSoftDeleted(Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentType);
        return MappingFor(documentType).SoftDeletesEnabled;
    }

    private DocumentMapping BuildMappingForType(Type documentType)
    {
        if (_options.Schema.Mappings.TryGetValue(documentType, out var rawMapping) && rawMapping is IDocumentMappingInfo info)
        {
            return new DocumentMapping
            {
                DocumentType = info.DocumentType,
                DocTypeName = info.DocTypeName,
                IdentityPropertyName = info.IdentityPropertyName,
                PartitionKeyPropertyName = info.PartitionKeyPropertyName,
                SoftDeletesEnabled = info.SoftDeletesEnabled
            };
        }

        var defaultIdProp = documentType.GetProperty("Id") ?? documentType.GetProperty("id");
        var identityName = defaultIdProp?.Name ?? "Id";

        return new DocumentMapping
        {
            DocumentType = documentType,
            DocTypeName = documentType.Name,
            IdentityPropertyName = identityName,
            PartitionKeyPropertyName = string.Empty,
            SoftDeletesEnabled = false
        };
    }
}
