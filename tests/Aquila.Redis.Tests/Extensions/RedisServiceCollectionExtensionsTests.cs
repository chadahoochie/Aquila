using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Shouldly;
using StackExchange.Redis;
using Aquila.Core.Configuration;
using Aquila.Core.Projections.Daemon;
using Aquila.Redis.Configuration;
using Aquila.Redis.Extensions;
using Aquila.Redis.Storage;
using Aquila.Redis.Tests.Fixtures;

namespace Aquila.Redis.Tests.Extensions;

public sealed class RedisServiceCollectionExtensionsTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fixture;

    public RedisServiceCollectionExtensionsTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    #region UseRedisProjections Tests

    [Fact]
    public void UseRedisProjections_WithMultiplexer_NullOptions_ThrowsArgumentNullException()
    {
        StoreOptions nullOptions = null!;
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        Should.Throw<ArgumentNullException>(() => nullOptions.UseRedisProjections(multiplexer));
    }

    [Fact]
    public void UseRedisProjections_WithMultiplexer_NullMultiplexer_ThrowsArgumentNullException()
    {
        var options = new StoreOptions();
        IConnectionMultiplexer nullMultiplexer = null!;

        Should.Throw<ArgumentNullException>(() => options.UseRedisProjections(nullMultiplexer));
    }

    [Fact]
    public void UseRedisProjections_WithMultiplexer_ConfiguresProjectionStorage_DefaultOptions()
    {
        var options = new StoreOptions();
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        var result = options.UseRedisProjections(multiplexer);

        result.ShouldBeSameAs(options);
        options.ProjectionStorage.ShouldNotBeNull();
        options.ProjectionStorage.ShouldBeOfType<RedisProjectionStorageProvider>();
        options.ProjectionStorage.ProviderName.ShouldBe("Redis");
    }

    [Fact]
    public void UseRedisProjections_WithMultiplexer_ConfiguresProjectionStorage_CustomOptions()
    {
        var options = new StoreOptions();
        var multiplexer = Substitute.For<IConnectionMultiplexer>();
        var configured = false;

        var result = options.UseRedisProjections(multiplexer, opt =>
        {
            opt.KeyPrefix = "custom:proj:";
            opt.Database = 3;
            opt.BatchChunkSize = 250;
            configured = true;
        });

        result.ShouldBeSameAs(options);
        configured.ShouldBeTrue();
        options.ProjectionStorage.ShouldNotBeNull();
        options.ProjectionStorage.ShouldBeOfType<RedisProjectionStorageProvider>();
    }

    [Fact]
    public void UseRedisProjections_WithConnectionString_NullOptions_ThrowsArgumentNullException()
    {
        StoreOptions nullOptions = null!;

        Should.Throw<ArgumentNullException>(() => nullOptions.UseRedisProjections(_fixture.ConnectionString));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void UseRedisProjections_WithConnectionString_NullOrWhiteSpace_ThrowsArgumentException(string? invalidConnectionString)
    {
        var options = new StoreOptions();

        Should.Throw<ArgumentException>(() => options.UseRedisProjections(invalidConnectionString!));
    }

    [Fact]
    public void UseRedisProjections_WithConnectionString_ConnectsAndConfiguresProjectionStorage()
    {
        var options = new StoreOptions();

        var result = options.UseRedisProjections(_fixture.ConnectionString);

        result.ShouldBeSameAs(options);
        options.ProjectionStorage.ShouldNotBeNull();
        options.ProjectionStorage.ShouldBeOfType<RedisProjectionStorageProvider>();
        options.ProjectionStorage.ProviderName.ShouldBe("Redis");
    }

    [Fact]
    public void UseRedisProjections_WithConnectionString_AndConfigureCallback_AppliesOptions()
    {
        var options = new StoreOptions();
        var configured = false;

        var result = options.UseRedisProjections(_fixture.ConnectionString, opt =>
        {
            opt.KeyPrefix = "test:conn:proj:";
            opt.Database = 1;
            configured = true;
        });

        result.ShouldBeSameAs(options);
        configured.ShouldBeTrue();
        options.ProjectionStorage.ShouldNotBeNull();
        options.ProjectionStorage.ShouldBeOfType<RedisProjectionStorageProvider>();
    }

    #endregion

    #region UseRedisDocuments Tests

    [Fact]
    public void UseRedisDocuments_WithMultiplexer_NullOptions_ThrowsArgumentNullException()
    {
        StoreOptions nullOptions = null!;
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        Should.Throw<ArgumentNullException>(() => nullOptions.UseRedisDocuments(multiplexer));
    }

    [Fact]
    public void UseRedisDocuments_WithMultiplexer_NullMultiplexer_ThrowsArgumentNullException()
    {
        var options = new StoreOptions();
        IConnectionMultiplexer nullMultiplexer = null!;

        Should.Throw<ArgumentNullException>(() => options.UseRedisDocuments(nullMultiplexer));
    }

    [Fact]
    public void UseRedisDocuments_WithMultiplexer_ConfiguresDocumentStorage_DefaultOptions()
    {
        var options = new StoreOptions();
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        var result = options.UseRedisDocuments(multiplexer);

        result.ShouldBeSameAs(options);
        options.DocumentStorage.ShouldNotBeNull();
        options.DocumentStorage.ShouldBeOfType<RedisDocumentStorageProvider>();
        options.DocumentStorage.ProviderName.ShouldBe("Redis");
    }

    [Fact]
    public void UseRedisDocuments_WithMultiplexer_ConfiguresDocumentStorage_CustomOptions()
    {
        var options = new StoreOptions();
        var multiplexer = Substitute.For<IConnectionMultiplexer>();
        var configured = false;

        var result = options.UseRedisDocuments(multiplexer, opt =>
        {
            opt.KeyPrefix = "custom:docs:";
            opt.Database = 2;
            configured = true;
        });

        result.ShouldBeSameAs(options);
        configured.ShouldBeTrue();
        options.DocumentStorage.ShouldNotBeNull();
        options.DocumentStorage.ShouldBeOfType<RedisDocumentStorageProvider>();
    }

    [Fact]
    public void UseRedisDocuments_WithConnectionString_NullOptions_ThrowsArgumentNullException()
    {
        StoreOptions nullOptions = null!;

        Should.Throw<ArgumentNullException>(() => nullOptions.UseRedisDocuments(_fixture.ConnectionString));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void UseRedisDocuments_WithConnectionString_NullOrWhiteSpace_ThrowsArgumentException(string? invalidConnectionString)
    {
        var options = new StoreOptions();

        Should.Throw<ArgumentException>(() => options.UseRedisDocuments(invalidConnectionString!));
    }

    [Fact]
    public void UseRedisDocuments_WithConnectionString_ConnectsAndConfiguresDocumentStorage()
    {
        var options = new StoreOptions();

        var result = options.UseRedisDocuments(_fixture.ConnectionString);

        result.ShouldBeSameAs(options);
        options.DocumentStorage.ShouldNotBeNull();
        options.DocumentStorage.ShouldBeOfType<RedisDocumentStorageProvider>();
        options.DocumentStorage.ProviderName.ShouldBe("Redis");
    }

    [Fact]
    public void UseRedisDocuments_WithConnectionString_AndConfigureCallback_AppliesOptions()
    {
        var options = new StoreOptions();
        var configured = false;

        var result = options.UseRedisDocuments(_fixture.ConnectionString, opt =>
        {
            opt.KeyPrefix = "test:conn:docs:";
            opt.Database = 4;
            configured = true;
        });

        result.ShouldBeSameAs(options);
        configured.ShouldBeTrue();
        options.DocumentStorage.ShouldNotBeNull();
        options.DocumentStorage.ShouldBeOfType<RedisDocumentStorageProvider>();
    }

    #endregion

    #region AddRedisCheckpointStore Tests

    [Fact]
    public void AddRedisCheckpointStore_NullServices_ThrowsArgumentNullException()
    {
        IServiceCollection nullServices = null!;
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        Should.Throw<ArgumentNullException>(() => nullServices.AddRedisCheckpointStore(multiplexer));
    }

    [Fact]
    public void AddRedisCheckpointStore_NullMultiplexer_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        IConnectionMultiplexer nullMultiplexer = null!;

        Should.Throw<ArgumentNullException>(() => services.AddRedisCheckpointStore(nullMultiplexer));
    }

    [Fact]
    public void AddRedisCheckpointStore_RegistersSingleton_WithDefaultParameters()
    {
        var services = new ServiceCollection();
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        var result = services.AddRedisCheckpointStore(multiplexer);

        result.ShouldBeSameAs(services);
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IProjectionCheckpointStore));
        descriptor.ShouldNotBeNull();
        descriptor.Lifetime.ShouldBe(ServiceLifetime.Singleton);
        descriptor.ImplementationInstance.ShouldNotBeNull();
        descriptor.ImplementationInstance.ShouldBeOfType<RedisProjectionCheckpointStore>();

        var provider = services.BuildServiceProvider();
        var store = provider.GetService<IProjectionCheckpointStore>();
        store.ShouldNotBeNull();
        store.ShouldBeOfType<RedisProjectionCheckpointStore>();
    }

    [Fact]
    public void AddRedisCheckpointStore_RegistersSingleton_WithCustomParameters()
    {
        var services = new ServiceCollection();
        var multiplexer = Substitute.For<IConnectionMultiplexer>();

        var result = services.AddRedisCheckpointStore(multiplexer, keyPrefix: "custom:chk:", database: 2);

        result.ShouldBeSameAs(services);
        var provider = services.BuildServiceProvider();
        var store = provider.GetService<IProjectionCheckpointStore>();
        store.ShouldNotBeNull();
        store.ShouldBeOfType<RedisProjectionCheckpointStore>();
    }

    #endregion

    #region AddAquilaRedis Tests

    [Fact]
    public void AddAquilaRedis_NullServices_ThrowsArgumentNullException()
    {
        IServiceCollection nullServices = null!;

        Should.Throw<ArgumentNullException>(() => nullServices.AddAquilaRedis(_fixture.ConnectionString));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddAquilaRedis_NullOrWhiteSpaceConnectionString_ThrowsArgumentException(string? invalidConnectionString)
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddAquilaRedis(invalidConnectionString!));
    }

    [Fact]
    public void AddAquilaRedis_RegistersSingletonFactory_ResolvesMultiplexer()
    {
        var services = new ServiceCollection();

        var result = services.AddAquilaRedis(_fixture.ConnectionString);

        result.ShouldBeSameAs(services);
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IConnectionMultiplexer));
        descriptor.ShouldNotBeNull();
        descriptor.Lifetime.ShouldBe(ServiceLifetime.Singleton);
        descriptor.ImplementationFactory.ShouldNotBeNull();

        var provider = services.BuildServiceProvider();
        var multiplexer = provider.GetService<IConnectionMultiplexer>();
        multiplexer.ShouldNotBeNull();
        multiplexer.IsConnected.ShouldBeTrue();
    }

    #endregion
}
