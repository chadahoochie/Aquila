using System;
using System.Text.Json.Serialization;

namespace Aquila.Cosmos.Storage;

/// <summary>
/// Cosmos DB document envelope for Aquila document storage, events, and stream metadata.
/// </summary>
public sealed class CosmosDocumentEnvelope<T>
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("pk")]
    public string PartitionKey { get; set; } = string.Empty;

    [JsonPropertyName("_docType")]
    public string DocType { get; set; } = typeof(T).Name;

    [JsonPropertyName("_tenantId")]
    public string TenantId { get; set; } = "default";

    [JsonPropertyName("_isDeleted")]
    public bool IsDeleted { get; set; }

    [JsonPropertyName("_version")]
    public string Version { get; set; } = "1";

    [JsonPropertyName("_etag")]
    public string? ETag { get; set; }

    [JsonPropertyName("data")]
    public T Data { get; set; } = default!;
}

/// <summary>
/// Non-generic Cosmos DB document envelope for polymorphic querying.
/// </summary>
public sealed class CosmosDocumentEnvelope
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("pk")]
    public string PartitionKey { get; set; } = string.Empty;

    [JsonPropertyName("_docType")]
    public string DocType { get; set; } = string.Empty;

    [JsonPropertyName("_tenantId")]
    public string TenantId { get; set; } = "default";

    [JsonPropertyName("_isDeleted")]
    public bool IsDeleted { get; set; }

    [JsonPropertyName("_version")]
    public string Version { get; set; } = "1";

    [JsonPropertyName("_etag")]
    public string? ETag { get; set; }

    [JsonPropertyName("data")]
    public object Data { get; set; } = default!;
}
