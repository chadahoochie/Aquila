using Microsoft.Extensions.DependencyInjection;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Projections.Daemon;
using Aquila.Cosmos.Projections;

namespace Aquila.Cosmos.Extensions
{
    public static class CosmosDaemonExtensions
    {
        public static IServiceCollection AddCosmosDaemon(this IServiceCollection services, Action<ProjectionDaemonOptions>? configureOptions = null)
        {
            ArgumentNullException.ThrowIfNull(services);

            if (configureOptions != null)
            {
                var daemonOptions = new ProjectionDaemonOptions();
                configureOptions(daemonOptions);
                services.AddSingleton(daemonOptions);
            }

            services.AddSingleton<IProjectionCheckpointStore>(sp =>
            {
                var store = sp.GetRequiredService<IDocumentStore>();
                return new DocumentStorageProjectionCheckpointStore(store.Options.DocumentStorage);
            });

            services.AddSingleton(sp =>
                new CosmosProjectionDaemon(
                    sp.GetRequiredService<IDocumentStore>(),
                    sp.GetRequiredService<IProjectionCheckpointStore>(),
                    sp.GetService<Microsoft.Extensions.Logging.ILogger<CosmosProjectionDaemon>>(),
                    sp.GetService<ProjectionDaemonOptions>()));
            services.AddSingleton<IProjectionDaemon>(sp => sp.GetRequiredService<CosmosProjectionDaemon>());
            services.AddHostedService(sp => sp.GetRequiredService<CosmosProjectionDaemon>());

            return services;
        }

        public static StoreOptions AddCosmosDaemon(this StoreOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            return options;
        }
    }
}

namespace Aquila.Cosmos
{
    public static class AquilaCosmosDaemonGlobalExtensions
    {
        public static IServiceCollection AddCosmosDaemon(this IServiceCollection services, Action<ProjectionDaemonOptions>? configureOptions = null)
            => Aquila.Cosmos.Extensions.CosmosDaemonExtensions.AddCosmosDaemon(services, configureOptions);

        public static StoreOptions AddCosmosDaemon(this StoreOptions options)
            => Aquila.Cosmos.Extensions.CosmosDaemonExtensions.AddCosmosDaemon(options);
    }
}
