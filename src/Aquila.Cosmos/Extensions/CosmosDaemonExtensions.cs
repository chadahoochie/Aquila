using Microsoft.Extensions.DependencyInjection;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Projections.Daemon;
using Aquila.Cosmos.Projections;

namespace Aquila.Cosmos.Extensions
{
    public static class CosmosDaemonExtensions
    {
        public static IServiceCollection AddCosmosDaemon(this IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);

            services.AddSingleton<IProjectionCheckpointStore>(sp =>
            {
                var store = sp.GetRequiredService<IDocumentStore>();
                return new DocumentStorageProjectionCheckpointStore(store.Options.DocumentStorage);
            });

            services.AddSingleton(sp =>
                new CosmosProjectionDaemon(
                    sp.GetRequiredService<IDocumentStore>(),
                    sp.GetRequiredService<IProjectionCheckpointStore>(),
                    sp.GetService<Microsoft.Extensions.Logging.ILogger<CosmosProjectionDaemon>>()));
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
        public static IServiceCollection AddCosmosDaemon(this IServiceCollection services)
            => Aquila.Cosmos.Extensions.CosmosDaemonExtensions.AddCosmosDaemon(services);

        public static StoreOptions AddCosmosDaemon(this StoreOptions options)
            => Aquila.Cosmos.Extensions.CosmosDaemonExtensions.AddCosmosDaemon(options);
    }
}
