using System.Text.Json;

namespace Aquila.Redis.Configuration;

/// <summary>
/// Configuration options for Redis document storage, projections, and checkpoint persistence.
/// </summary>
public sealed class RedisStorageOptions
{
    /// <summary>
    /// Key prefix applied to all Redis keys. Defaults to "aquila:".
    /// </summary>
    public string KeyPrefix { get; set; } = "aquila:";

    /// <summary>
    /// Target Redis database index. Defaults to 0.
    /// </summary>
    public int Database { get; set; } = 0;

    /// <summary>
    /// Maximum number of keys unlinked per batch during projection purges. Defaults to 500.
    /// </summary>
    public int BatchChunkSize { get; set; } = 500;

    /// <summary>
    /// JSON serialization options used for UTF-8 document serialization.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Builds a cluster-shard-aware key with hash tag: "{KeyPrefix}{{{tenantId}:{partitionKey}}}:{docType}:{id}".
    /// </summary>
    public string BuildKey(string tenantId, string docType, string partitionKey, string id)
    {
        var safeTenant = string.IsNullOrWhiteSpace(tenantId) ? "default" : tenantId;
        var safePk = string.IsNullOrWhiteSpace(partitionKey) ? id : partitionKey;
        return $"{KeyPrefix}{{{safeTenant}:{safePk}}}:{docType}:{id}";
    }

    /// <summary>
    /// Builds a search pattern for a document type across all partitions: "{KeyPrefix}*:{docType}:*".
    /// </summary>
    public string BuildTypePattern(string docType) => $"{KeyPrefix}*:{docType}:*";
}
