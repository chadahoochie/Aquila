namespace Aquila.Cosmos.Configuration;

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

    public (string DatabaseName, string ContainerName) Resolve(string fallbackDatabase)
    {
        var db = string.IsNullOrWhiteSpace(Database) ? fallbackDatabase : Database;
        return (db, Container);
    }
}
