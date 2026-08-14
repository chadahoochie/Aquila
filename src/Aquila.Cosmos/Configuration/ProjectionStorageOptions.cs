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

    private readonly Dictionary<Type, StorageLocationOptions> _projectionOverrides = new();
    public IReadOnlyDictionary<Type, StorageLocationOptions> Overrides => _projectionOverrides;

    public ProjectionStorageOptions ToContainer(string container, string? database = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        Mode = ProjectionStorageMode.DedicatedContainer;
        Container = container;
        Database = database;
        return this;
    }

    public ProjectionStorageOptions AutoContainerPerProjection(string? database = null, Func<Type, string>? nameFormatter = null)
    {
        Mode = ProjectionStorageMode.AutoContainerPerProjection;
        Database = database;
        if (nameFormatter != null)
        {
            ContainerNameFormatter = nameFormatter;
        }
        return this;
    }

    public ProjectionStorageOptions For<TProjection>(string container, string? database = null) where TProjection : Core.Projections.IProjection
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        _projectionOverrides[typeof(TProjection)] = new StorageLocationOptions(container, database);
        return this;
    }

    public ProjectionStorageOptions For(Type projectionType, string container, string? database = null)
    {
        ArgumentNullException.ThrowIfNull(projectionType);
        ArgumentException.ThrowIfNullOrWhiteSpace(container);
        _projectionOverrides[projectionType] = new StorageLocationOptions(container, database);
        return this;
    }
}
