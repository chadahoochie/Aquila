namespace Aquila.Cosmos.Configuration;

/// <summary>
/// Storage segregation modes for read model projections.
/// </summary>
public enum ProjectionStorageMode
{
    /// <summary>
    /// Projections save to the documents database and container definition (default).
    /// </summary>
    InheritDocuments,

    /// <summary>
    /// Projections save to a dedicated shared database and container.
    /// </summary>
    DedicatedContainer,

    /// <summary>
    /// Each projection is saved to its own dedicated container within a target database.
    /// </summary>
    AutoContainerPerProjection
}

/// <summary>
/// Configuration options for projection storage segregation and container resolution.
/// </summary>
public sealed class ProjectionStorageOptions
{
    public ProjectionStorageMode Mode { get; set; } = ProjectionStorageMode.InheritDocuments;
    public string? Database { get; set; }
    public string? Container { get; set; }
    public Func<Type, string> ContainerNameFormatter { get; set; } = type => type.Name;
    public ThroughputSettings? Throughput { get; set; }

    private readonly Dictionary<Type, StorageLocationOptions> _projectionOverrides = new();
    public IReadOnlyDictionary<Type, StorageLocationOptions> Overrides => _projectionOverrides;

    public ProjectionStorageOptions WithManualThroughput(int ru)
    {
        Throughput = ThroughputSettings.Manual(ru);
        return this;
    }

    public ProjectionStorageOptions WithAutoscaleThroughput(int maxRu)
    {
        Throughput = ThroughputSettings.Autoscale(maxRu);
        return this;
    }

    public ProjectionStorageOptions ToContainer(string container, string? database = null, ThroughputSettings? throughput = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        Mode = ProjectionStorageMode.DedicatedContainer;
        Container = container;
        Database = database;
        if (throughput != null)
        {
            Throughput = throughput;
        }
        return this;
    }

    public ProjectionStorageOptions AutoContainerPerProjection(string? database = null, Func<Type, string>? nameFormatter = null, ThroughputSettings? throughput = null)
    {
        Mode = ProjectionStorageMode.AutoContainerPerProjection;
        Database = database;
        if (nameFormatter != null)
        {
            ContainerNameFormatter = nameFormatter;
        }
        if (throughput != null)
        {
            Throughput = throughput;
        }
        return this;
    }

    public ProjectionStorageOptions For<TProjection>(string container, string? database = null, ThroughputSettings? throughput = null) where TProjection : Core.Projections.IProjection
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        var location = new StorageLocationOptions(container, database) { Throughput = throughput };
        _projectionOverrides[typeof(TProjection)] = location;
        return this;
    }

    public ProjectionStorageOptions For(Type projectionType, string container, string? database = null, ThroughputSettings? throughput = null)
    {
        ArgumentNullException.ThrowIfNull(projectionType);
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        var location = new StorageLocationOptions(container, database) { Throughput = throughput };
        _projectionOverrides[projectionType] = location;
        return this;
    }
}
