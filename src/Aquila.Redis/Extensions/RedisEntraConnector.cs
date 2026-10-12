using Azure.Core;
using Azure.Identity;
using Microsoft.Azure.StackExchangeRedis;
using StackExchange.Redis;

namespace Aquila.Redis.Extensions;

internal static class RedisEntraConnector
{
    internal static ConfigurationOptions BuildOptions(string endpoint, Action<ConfigurationOptions>? configureConnection)
    {
        var options = new ConfigurationOptions
        {
            Ssl = true,
            AbortOnConnectFail = false
        };
        options.EndPoints.Add(endpoint);
        configureConnection?.Invoke(options);
        return options;
    }

    internal static IConnectionMultiplexer Connect(string endpoint, TokenCredential? credential, Action<ConfigurationOptions>? configureConnection)
    {
        var options = BuildOptions(endpoint, configureConnection);
        // Sync-over-async is deliberate: callers are synchronous configuration/DI factories, and this runs once at startup.
        return ConnectAsync(options, credential ?? new DefaultAzureCredential()).GetAwaiter().GetResult();
    }

    private static async Task<IConnectionMultiplexer> ConnectAsync(ConfigurationOptions options, TokenCredential credential)
    {
        await options.ConfigureForAzureWithTokenCredentialAsync(credential).ConfigureAwait(false);
        return await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
    }
}
