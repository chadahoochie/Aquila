using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Extension methods for configuring Aquila projection daemon and checkpoint stores.
/// </summary>
public static class DaemonExtensions
{
    /// <summary>
    /// Adds Aquila Async Projection Daemon infrastructure to the service collection.
    /// </summary>
    public static IServiceCollection AddAquilaDaemon(
        this IServiceCollection services,
        Func<IServiceProvider, IProjectionCheckpointStore>? checkpointStoreFactory = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (checkpointStoreFactory != null)
        {
            services.AddSingleton(checkpointStoreFactory);
        }
        else
        {
            services.TryAddSingleton<IProjectionCheckpointStore>(sp =>
            {
                var options = sp.GetService<StoreOptions>();
                if (options != null)
                {
                    return new DocumentStorageProjectionCheckpointStore(options.StorageProvider.Documents);
                }
                var provider = sp.GetRequiredService<IAquilaStorageProvider>();
                return new DocumentStorageProjectionCheckpointStore(provider.Documents);
            });
        }

        services.TryAddSingleton<ProjectionDaemon>(sp =>
        {
            var checkpointStore = sp.GetRequiredService<IProjectionCheckpointStore>();
            var logger = sp.GetService<ILogger<ProjectionDaemon>>();
            var docStore = sp.GetService<IDocumentStore>();
            if (docStore != null)
            {
                return new ProjectionDaemon(docStore, checkpointStore, logger);
            }
            var options = sp.GetRequiredService<StoreOptions>();
            return new ProjectionDaemon(options, checkpointStore, logger);
        });
        services.TryAddSingleton<IProjectionDaemon>(sp => sp.GetRequiredService<ProjectionDaemon>());
        services.AddHostedService(sp => sp.GetRequiredService<ProjectionDaemon>());

        return services;
    }

    /// <summary>
    /// Enables Async Projection Daemon on StoreOptions.
    /// </summary>
    public static StoreOptions AddAsyncDaemon(this StoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options;
    }
}
