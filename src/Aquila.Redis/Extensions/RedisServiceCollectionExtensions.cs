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
}
