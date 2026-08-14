namespace Aquila.Cosmos.Configuration;

/// <summary>
/// Configuration options for Cosmos DB storage segregation across events, snapshots, documents, and projections.
/// </summary>
public sealed class CosmosStorageOptions
{
    /// <summary>
    /// Default database name used when a specific store does not override its database name. Defaults to "AquilaDB".
    /// </summary>
    public string DefaultDatabase { get; set; } = "AquilaDB";

    /// <summary>
    /// Database and container coordinates for the Event Store. Defaults to "Events" container in DefaultDatabase.
    /// </summary>
    public StorageLocationOptions Events { get; private set; } = new("Events");

    /// <summary>
    /// Database and container coordinates for Aggregate Snapshots. Defaults to "Snapshots" container in DefaultDatabase.
    /// </summary>
    public StorageLocationOptions Snapshots { get; private set; } = new("Snapshots");

    /// <summary>
    /// Database and container coordinates for Documents. Defaults to "Documents" container in DefaultDatabase.
    /// </summary>
    public StorageLocationOptions Documents { get; private set; } = new("Documents");

    /// <summary>
    /// Configuration for Read Model Projections. Defaults to inheriting the Documents container.
    /// </summary>
    public ProjectionStorageOptions Projections { get; } = new();

    public CosmosStorageOptions()
    {
    }

    /// <summary>
    /// Configures the Event Store container and optional database.
    /// </summary>
    public CosmosStorageOptions ConfigureEvents(string container, string? database = null)
    {
        Events = new StorageLocationOptions(container, database);
        return this;
    }

    /// <summary>
    /// Configures the Snapshot Store container and optional database.
    /// </summary>
    public CosmosStorageOptions ConfigureSnapshots(string container, string? database = null)
    {
        Snapshots = new StorageLocationOptions(container, database);
        return this;
    }

    /// <summary>
    /// Configures the Documents container and optional database.
    /// </summary>
    public CosmosStorageOptions ConfigureDocuments(string container, string? database = null)
    {
        Documents = new StorageLocationOptions(container, database);
        return this;
    }

    /// <summary>
    /// Alias for ConfigureEvents.
    /// </summary>
    public CosmosStorageOptions EventsLocation(string container, string? database = null) => ConfigureEvents(container, database);

    /// <summary>
    /// Alias for ConfigureSnapshots.
    /// </summary>
    public CosmosStorageOptions SnapshotsLocation(string container, string? database = null) => ConfigureSnapshots(container, database);

    /// <summary>
    /// Alias for ConfigureDocuments.
    /// </summary>
    public CosmosStorageOptions DocumentsLocation(string container, string? database = null) => ConfigureDocuments(container, database);
}
