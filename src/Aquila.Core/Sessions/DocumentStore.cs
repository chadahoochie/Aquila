using System;
using System.Threading.Tasks;
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

    public async Task InitializeAsync(System.Threading.CancellationToken ct = default)
    {
        if (Options.StorageProvider != null)
        {
            await Options.StorageProvider.InitializeAsync(ct);
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
        return new QuerySession(Options.StorageProvider, Options, tenantId);
    }

    public IDocumentSession OpenSession(TrackingMode trackingMode = TrackingMode.DirtyTracking, string? tenantId = null)
    {
        if (tenantId != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        }
        return new DocumentSession(Options.StorageProvider, Options, trackingMode, tenantId);
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
        Options.StorageProvider?.Dispose();
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        if (Options.StorageProvider != null)
        {
            return Options.StorageProvider.DisposeAsync();
        }
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
