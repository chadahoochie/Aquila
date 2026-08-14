using Microsoft.Extensions.DependencyInjection;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Sessions;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Extensions;

public static class ServiceCollectionExtensions
{
    public static StoreOptions UseCosmos(this StoreOptions options, string connectionString, string databaseName = "AquilaDB", string containerName = "Documents")
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        var provider = new CosmosStorageProvider(connectionString, databaseName, containerName);
        options.UseStorageProvider(provider);
        return options;
    }

    public static StoreOptions UseCosmos(this StoreOptions options, Microsoft.Azure.Cosmos.CosmosClient client, string databaseName = "AquilaDB", string containerName = "Documents")
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

        var provider = new CosmosStorageProvider(client, databaseName, containerName);
        options.UseStorageProvider(provider);
        return options;
    }

    public static StoreOptions UseCosmos(this StoreOptions options, string connectionString, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(configure);

        var cosmosOptions = new Aquila.Cosmos.Configuration.CosmosStorageOptions();
        configure(cosmosOptions);

        var provider = new CosmosStorageProvider(connectionString, cosmosOptions, options);
        options.UseStorageProvider(provider);
        return options;
    }

    public static StoreOptions UseCosmos(this StoreOptions options, Microsoft.Azure.Cosmos.CosmosClient client, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(configure);

        var cosmosOptions = new Aquila.Cosmos.Configuration.CosmosStorageOptions();
        configure(cosmosOptions);

        var provider = new CosmosStorageProvider(client, cosmosOptions, options);
        options.UseStorageProvider(provider);
        return options;
    }

    public static StoreOptions UseCosmos(this StoreOptions options, string connectionString, Aquila.Cosmos.Configuration.CosmosStorageOptions cosmosOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(cosmosOptions);

        var provider = new CosmosStorageProvider(connectionString, cosmosOptions, options);
        options.UseStorageProvider(provider);
        return options;
    }

    public static StoreOptions UseCosmos(this StoreOptions options, Microsoft.Azure.Cosmos.CosmosClient client, Aquila.Cosmos.Configuration.CosmosStorageOptions cosmosOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(cosmosOptions);

        var provider = new CosmosStorageProvider(client, cosmosOptions, options);
        options.UseStorageProvider(provider);
        return options;
    }

    public static IServiceCollection AddAquila(this IServiceCollection services, Action<StoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new StoreOptions();
        configure(options);

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(sp => new DocumentStore(options));

        services.AddScoped(sp =>
        {
            var store = sp.GetRequiredService<IDocumentStore>();
            return store.QuerySession();
        });

        services.AddScoped(sp =>
        {
            var store = sp.GetRequiredService<IDocumentStore>();
            return store.OpenSession();
        });

        return services;
    }
}
