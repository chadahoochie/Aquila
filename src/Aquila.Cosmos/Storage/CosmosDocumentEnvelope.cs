using System.Text.Json.Serialization;
using Newtonsoft.Json;

namespace Aquila.Cosmos.Storage;

/// <summary>
/// Cosmos DB document envelope for Aquila document storage, events, and stream metadata.
/// </summary>
public sealed class CosmosDocumentEnvelope<T>
{
    [JsonProperty("id")]
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonProperty("pk")]
    [JsonPropertyName("pk")]
    public string PartitionKey { get; set; } = string.Empty;

    [JsonProperty("_docType")]
    [JsonPropertyName("_docType")]
    public string DocType { get; set; } = typeof(T).Name;

    [JsonProperty("_tenantId")]
    [JsonPropertyName("_tenantId")]
    public string TenantId { get; set; } = "default";

    [JsonProperty("_isDeleted")]
    [JsonPropertyName("_isDeleted")]
    public bool IsDeleted { get; set; }

    [JsonProperty("_version")]
    [JsonPropertyName("_version")]
    public string Version { get; set; } = "1";

    [JsonProperty("_etag")]
    [JsonPropertyName("_etag")]
    public string? ETag { get; set; }

    [JsonProperty("data")]
    [JsonPropertyName("data")]
    public T Data { get; set; } = default!;
}

/// <summary>
/// Non-generic Cosmos DB document envelope for polymorphic querying.
/// </summary>
public sealed class CosmosDocumentEnvelope
{
    [JsonProperty("id")]
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonProperty("pk")]
    [JsonPropertyName("pk")]
    public string PartitionKey { get; set; } = string.Empty;

    [JsonProperty("_docType")]
    [JsonPropertyName("_docType")]
    public string DocType { get; set; } = string.Empty;

    [JsonProperty("_tenantId")]
    [JsonPropertyName("_tenantId")]
    public string TenantId { get; set; } = "default";

    [JsonProperty("_isDeleted")]
    [JsonPropertyName("_isDeleted")]
    public bool IsDeleted { get; set; }

    [JsonProperty("_version")]
    [JsonPropertyName("_version")]
    public string Version { get; set; } = "1";

    [JsonProperty("_etag")]
    [JsonPropertyName("_etag")]
    public string? ETag { get; set; }
}
