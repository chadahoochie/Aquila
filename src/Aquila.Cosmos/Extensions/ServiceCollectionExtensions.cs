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

    public static StoreOptions UseCosmosDocuments(this StoreOptions options, Microsoft.Azure.Cosmos.CosmosClient client, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);

        var cosmosOptions = new Aquila.Cosmos.Configuration.CosmosStorageOptions();
        configure?.Invoke(cosmosOptions);

        var resolver = new CosmosContainerResolver(client, cosmosOptions, options);
        var provider = new CosmosDocumentStorageProvider(type => resolver.GetContainerForDocumentType(type));
        options.DocumentStorage = provider;
        return options;
    }

    public static StoreOptions UseCosmosDocuments(this StoreOptions options, string connectionString, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var client = new Microsoft.Azure.Cosmos.CosmosClient(connectionString, CosmosStorageProvider.CreateDefaultClientOptions());
        return options.UseCosmosDocuments(client, configure);
    }

    public static StoreOptions UseCosmosEvents(this StoreOptions options, Microsoft.Azure.Cosmos.CosmosClient client, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);

        var cosmosOptions = new Aquila.Cosmos.Configuration.CosmosStorageOptions();
        configure?.Invoke(cosmosOptions);

        var resolver = new CosmosContainerResolver(client, cosmosOptions, options);
        var provider = new CosmosEventStorageProvider(() => resolver.GetEventsContainer(), () => resolver.GetSnapshotsContainer());
        options.EventStorage = provider;
        return options;
    }

    public static StoreOptions UseCosmosEvents(this StoreOptions options, string connectionString, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var client = new Microsoft.Azure.Cosmos.CosmosClient(connectionString, CosmosStorageProvider.CreateDefaultClientOptions());
        return options.UseCosmosEvents(client, configure);
    }

    public static StoreOptions UseCosmosProjections(this StoreOptions options, Microsoft.Azure.Cosmos.CosmosClient client, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);

        var cosmosOptions = new Aquila.Cosmos.Configuration.CosmosStorageOptions();
        configure?.Invoke(cosmosOptions);

        var resolver = new CosmosContainerResolver(client, cosmosOptions, options);
        var provider = new CosmosProjectionStorageProvider(type => resolver.GetContainerForDocumentType(type));
        options.ProjectionStorage = provider;
        return options;
    }

    public static StoreOptions UseCosmosProjections(this StoreOptions options, string connectionString, Action<Aquila.Cosmos.Configuration.CosmosStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var client = new Microsoft.Azure.Cosmos.CosmosClient(connectionString, CosmosStorageProvider.CreateDefaultClientOptions());
        return options.UseCosmosProjections(client, configure);
    }

    public static IServiceCollection AddAquila(this IServiceCollection services, Action<StoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new StoreOptions();
        configure(options);

        // Freeze here rather than leaving it to the DocumentStore factory below, which does not run
        // until something first resolves IDocumentStore. Freezing at registration means the polyglot
        // inline-projection guard reports a misconfiguration during startup, where it is actionable,
        // instead of on the first request that happens to open a session.
        options.Freeze();

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(sp => new DocumentStore(options));

        // Nothing in the DI path called InitializeAsync, so containers went unprovisioned and the
        // event store's global sequence restarted at 0 on every process start. See AquilaStoreInitializer.
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService, AquilaStoreInitializer>();

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
