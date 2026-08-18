using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;

namespace Aquila.Core.Sessions;

public sealed class DocumentStore : IDocumentStore
{
    public StoreOptions Options { get; }
    public IStoreMetadata Metadata { get; }

    public DocumentStore(StoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = options;
        Metadata = new StoreMetadata(options);
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        Options.Freeze();
        if (Options.DocumentStorage != null)
        {
            await Options.DocumentStorage.InitializeAsync(ct);
        }
        if (Options.EventStorage != null && !ReferenceEquals(Options.EventStorage, Options.DocumentStorage))
        {
            await Options.EventStorage.InitializeAsync(ct);
        }
        if (Options.ProjectionStorage != null &&
            !ReferenceEquals(Options.ProjectionStorage, Options.DocumentStorage) &&
            !ReferenceEquals(Options.ProjectionStorage, Options.EventStorage))
        {
            await Options.ProjectionStorage.InitializeAsync(ct);
        }
    }

    public static IDocumentStore For(Action<StoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new StoreOptions();
        configure(options);
        return new DocumentStore(options);
    }

    public IQuerySession QuerySession(string? tenantId = null)
    {
        if (tenantId != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        }
        return new QuerySession(Options.DocumentStorage, Options.EventStorage, Options, tenantId);
    }

    public IDocumentSession OpenSession(TrackingMode trackingMode = TrackingMode.DirtyTracking, string? tenantId = null)
    {
        if (tenantId != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        }
        return new DocumentSession(Options.DocumentStorage, Options.EventStorage, Options, trackingMode, tenantId);
    }

    public IDocumentSession OpenSession(string? tenantId)
    {
        return OpenSession(TrackingMode.DirtyTracking, tenantId);
    }

    public IDocumentSession LightweightSession(string? tenantId = null)
    {
        return OpenSession(TrackingMode.Lightweight, tenantId);
    }

    public void Dispose()
    {
        Options.DocumentStorage?.Dispose();
        if (Options.EventStorage != null && !ReferenceEquals(Options.EventStorage, Options.DocumentStorage))
        {
            Options.EventStorage.Dispose();
        }
        if (Options.ProjectionStorage != null &&
            !ReferenceEquals(Options.ProjectionStorage, Options.DocumentStorage) &&
            !ReferenceEquals(Options.ProjectionStorage, Options.EventStorage))
        {
            Options.ProjectionStorage.Dispose();
        }
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        if (Options.DocumentStorage != null)
        {
            await Options.DocumentStorage.DisposeAsync();
        }
        if (Options.EventStorage != null && !ReferenceEquals(Options.EventStorage, Options.DocumentStorage))
        {
            await Options.EventStorage.DisposeAsync();
        }
        if (Options.ProjectionStorage != null &&
            !ReferenceEquals(Options.ProjectionStorage, Options.DocumentStorage) &&
            !ReferenceEquals(Options.ProjectionStorage, Options.EventStorage))
        {
            await Options.ProjectionStorage.DisposeAsync();
        }
        GC.SuppressFinalize(this);
    }
}
