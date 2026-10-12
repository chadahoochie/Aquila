using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Aquila.Core.Configuration;
using Aquila.Core.Projections.Daemon;
using Aquila.Redis.Configuration;
using Aquila.Redis.Storage;

namespace Aquila.Redis.Extensions;

/// <summary>
/// Service collection extensions for configuring Redis as a projection storage provider, document store, and checkpoint repository.
/// </summary>
public static class RedisServiceCollectionExtensions
{
    public static StoreOptions UseRedisProjections(this StoreOptions options, IConnectionMultiplexer multiplexer, Action<RedisStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(multiplexer);

        var redisOptions = new RedisStorageOptions();
        configure?.Invoke(redisOptions);
        options.ProjectionStorage = new RedisProjectionStorageProvider(multiplexer, redisOptions);
        return options;
    }

    public static StoreOptions UseRedisProjections(this StoreOptions options, string connectionString, Action<RedisStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var multiplexer = ConnectionMultiplexer.Connect(connectionString);
        return options.UseRedisProjections(multiplexer, configure);
    }

    public static StoreOptions UseRedisDocuments(this StoreOptions options, IConnectionMultiplexer multiplexer, Action<RedisStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(multiplexer);

        var redisOptions = new RedisStorageOptions();
        configure?.Invoke(redisOptions);
        options.DocumentStorage = new RedisDocumentStorageProvider(multiplexer, redisOptions);
        return options;
    }

    public static StoreOptions UseRedisDocuments(this StoreOptions options, string connectionString, Action<RedisStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var multiplexer = ConnectionMultiplexer.Connect(connectionString);
        return options.UseRedisDocuments(multiplexer, configure);
    }

    public static IServiceCollection AddRedisCheckpointStore(this IServiceCollection services, IConnectionMultiplexer multiplexer, string keyPrefix = "aquila:checkpoints:", int database = 0)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(multiplexer);

        services.AddSingleton<IProjectionCheckpointStore>(new RedisProjectionCheckpointStore(multiplexer, keyPrefix, database));
        return services;
    }

    public static IServiceCollection AddAquilaRedis(this IServiceCollection services, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(connectionString));
        return services;
    }

    /// <summary>
    /// Configures Redis projection storage using Microsoft Entra ID authentication against Azure Cache for Redis.
    /// </summary>
    /// <param name="endpoint">Cache host, optionally with port (for example <c>mycache.redis.cache.windows.net</c>). TLS is always enabled.</param>
    /// <param name="credential">Entra credential; defaults to <see cref="DefaultAzureCredential"/>.</param>
    /// <param name="configureConnection">Optional tweak of the StackExchange.Redis connection options before connecting.</param>
    public static StoreOptions UseRedisProjectionsWithEntra(this StoreOptions options, string endpoint, TokenCredential? credential, Action<RedisStorageOptions>? configure = null, Action<ConfigurationOptions>? configureConnection = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);

        var multiplexer = RedisEntraConnector.Connect(endpoint, credential, configureConnection);
        return options.UseRedisProjections(multiplexer, configure);
    }

    /// <summary>
    /// Configures Redis document storage using Microsoft Entra ID authentication against Azure Cache for Redis.
    /// </summary>
    /// <inheritdoc cref="UseRedisProjectionsWithEntra(StoreOptions, string, TokenCredential?, Action{RedisStorageOptions}?, Action{ConfigurationOptions}?)"/>
    public static StoreOptions UseRedisDocumentsWithEntra(this StoreOptions options, string endpoint, TokenCredential? credential, Action<RedisStorageOptions>? configure = null, Action<ConfigurationOptions>? configureConnection = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);

        var multiplexer = RedisEntraConnector.Connect(endpoint, credential, configureConnection);
        return options.UseRedisDocuments(multiplexer, configure);
    }

    /// <summary>
    /// Registers an <see cref="IConnectionMultiplexer"/> authenticated with Microsoft Entra ID. The connection is
    /// established on first resolution; token refresh is handled by the multiplexer thereafter.
    /// </summary>
    /// <inheritdoc cref="UseRedisProjectionsWithEntra(StoreOptions, string, TokenCredential?, Action{RedisStorageOptions}?, Action{ConfigurationOptions}?)"/>
    public static IServiceCollection AddAquilaRedisWithEntra(this IServiceCollection services, string endpoint, TokenCredential? credential, Action<ConfigurationOptions>? configureConnection = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        services.AddSingleton<IConnectionMultiplexer>(_ => RedisEntraConnector.Connect(endpoint, credential, configureConnection));
        return services;
    }
}
