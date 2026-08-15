using Microsoft.Azure.Cosmos;

namespace Aquila.Cosmos.Configuration;

/// <summary>
/// Throughput provisioning configuration (manual RU or autoscale max-RU).
/// </summary>
public sealed class ThroughputSettings
{
    public int? ManualThroughput { get; set; }
    public int? AutoscaleMaxThroughput { get; set; }
    public bool IsAutoscale => AutoscaleMaxThroughput.HasValue;
    public bool IsManual => ManualThroughput.HasValue;

    public static ThroughputSettings Manual(int ru) => new() { ManualThroughput = ru };
    public static ThroughputSettings Autoscale(int maxRu) => new() { AutoscaleMaxThroughput = maxRu };

    public ThroughputProperties? ToThroughputProperties()
    {
        if (AutoscaleMaxThroughput.HasValue)
        {
            return ThroughputProperties.CreateAutoscaleThroughput(AutoscaleMaxThroughput.Value);
        }
        if (ManualThroughput.HasValue)
        {
            return ThroughputProperties.CreateManualThroughput(ManualThroughput.Value);
        }
        return null;
    }
}

/// <summary>
/// Defines database and container coordinates for a segregated Cosmos DB storage target.
/// </summary>
public sealed class StorageLocationOptions
{
    /// <summary>
    /// The target database name, or null to inherit the default database.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// The target container name.
    /// </summary>
    public string Container { get; set; }

    /// <summary>
    /// Optional throughput provisioning settings for this container.
    /// </summary>
    public ThroughputSettings? Throughput { get; set; }

    public StorageLocationOptions(string container, string? database = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        Container = container;
        Database = database;
    }

    public StorageLocationOptions SetContainer(string container)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        Container = container;
        return this;
    }

    public StorageLocationOptions SetDatabase(string? database)
    {
        Database = database;
        return this;
    }

    public StorageLocationOptions WithManualThroughput(int ru)
    {
        Throughput = ThroughputSettings.Manual(ru);
        return this;
    }

    public StorageLocationOptions WithAutoscaleThroughput(int maxRu)
    {
        Throughput = ThroughputSettings.Autoscale(maxRu);
        return this;
    }

    public (string DatabaseName, string ContainerName) Resolve(string fallbackDatabase)
    {
        var db = string.IsNullOrWhiteSpace(Database) ? fallbackDatabase : Database;
        return (db, Container);
    }
}
