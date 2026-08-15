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
    public static IServiceCollection AddAquilaDaemon(
        this IServiceCollection services,
        Action<ProjectionDaemonOptions> configureOptions)
    {
        return services.AddAquilaDaemon(null, configureOptions);
    }

    /// <summary>
    /// Adds Aquila Async Projection Daemon infrastructure to the service collection.
    /// </summary>
    public static IServiceCollection AddAquilaDaemon(
        this IServiceCollection services,
        Func<IServiceProvider, IProjectionCheckpointStore>? checkpointStoreFactory = null,
        Action<ProjectionDaemonOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configureOptions != null)
        {
            var daemonOptions = new ProjectionDaemonOptions();
            configureOptions(daemonOptions);
            services.TryAddSingleton(daemonOptions);
        }

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
                    return new DocumentStorageProjectionCheckpointStore(options.DocumentStorage);
                }
                var provider = sp.GetRequiredService<IDocumentStorageProvider>();
                return new DocumentStorageProjectionCheckpointStore(provider);
            });
        }

        services.TryAddSingleton(sp =>
        {
            var checkpointStore = sp.GetRequiredService<IProjectionCheckpointStore>();
            var logger = sp.GetService<ILogger<ProjectionDaemon>>();
            var daemonOptions = sp.GetService<ProjectionDaemonOptions>();
            var docStore = sp.GetService<IDocumentStore>();
            if (docStore != null)
            {
                return new ProjectionDaemon(docStore, checkpointStore, logger, daemonOptions);
            }
            var options = sp.GetRequiredService<StoreOptions>();
            return new ProjectionDaemon(options, checkpointStore, logger, daemonOptions);
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
