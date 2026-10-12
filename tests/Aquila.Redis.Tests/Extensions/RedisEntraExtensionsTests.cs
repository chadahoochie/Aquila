using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Redis.Extensions;

namespace Aquila.Redis.Tests.Extensions;

public sealed class RedisEntraExtensionsTests
{
    private const string Endpoint = "mycache.redis.cache.windows.net";

    [Fact]
    public void BuildOptions_EnablesTlsAndAddsEndpoint()
    {
        var options = RedisEntraConnector.BuildOptions(Endpoint, null);

        options.Ssl.ShouldBeTrue();
        options.AbortOnConnectFail.ShouldBeFalse();
        options.EndPoints.Count.ShouldBe(1);
        options.EndPoints[0].ToString()!.ShouldContain(Endpoint);
    }

    [Fact]
    public void BuildOptions_InvokesConnectionCallback()
    {
        var options = RedisEntraConnector.BuildOptions(Endpoint, o => o.ConnectTimeout = 1234);

        options.ConnectTimeout.ShouldBe(1234);
    }

    [Fact]
    public void UseRedisProjectionsWithEntra_NullOptions_Throws()
    {
        StoreOptions nullOptions = null!;

        Should.Throw<ArgumentNullException>(() => nullOptions.UseRedisProjectionsWithEntra(Endpoint, Substitute.For<TokenCredential>()));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void UseRedisProjectionsWithEntra_BlankEndpoint_Throws(string? endpoint)
    {
        Should.Throw<ArgumentException>(() => new StoreOptions().UseRedisProjectionsWithEntra(endpoint!, Substitute.For<TokenCredential>()));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void UseRedisDocumentsWithEntra_BlankEndpoint_Throws(string? endpoint)
    {
        Should.Throw<ArgumentException>(() => new StoreOptions().UseRedisDocumentsWithEntra(endpoint!, Substitute.For<TokenCredential>()));
    }

    [Fact]
    public void AddAquilaRedisWithEntra_NullServices_Throws()
    {
        IServiceCollection nullServices = null!;

        Should.Throw<ArgumentNullException>(() => nullServices.AddAquilaRedisWithEntra(Endpoint, Substitute.For<TokenCredential>()));
    }

    [Fact]
    public void AddAquilaRedisWithEntra_BlankEndpoint_Throws()
    {
        Should.Throw<ArgumentException>(() => new ServiceCollection().AddAquilaRedisWithEntra(" ", Substitute.For<TokenCredential>()));
    }

    [Fact]
    public void AddAquilaRedisWithEntra_RegistersMultiplexerLazily()
    {
        var services = new ServiceCollection();

        services.AddAquilaRedisWithEntra(Endpoint, Substitute.For<TokenCredential>());

        services.ShouldContain(d => d.ServiceType == typeof(StackExchange.Redis.IConnectionMultiplexer) && d.Lifetime == ServiceLifetime.Singleton);
    }
}
