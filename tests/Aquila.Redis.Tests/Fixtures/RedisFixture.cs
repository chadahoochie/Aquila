using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;

namespace Aquila.Redis.Tests.Fixtures;

public sealed class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("redis:7.4-alpine")
        .Build();

    public IConnectionMultiplexer Multiplexer { get; private set; } = null!;
    public string ConnectionString { get; private set; } = string.Empty;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
        Multiplexer = await ConnectionMultiplexer.ConnectAsync(ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (Multiplexer != null)
        {
            await Multiplexer.DisposeAsync();
        }
        await _container.DisposeAsync();
    }
}
