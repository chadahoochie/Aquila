using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Sessions;
using Aquila.Cosmos.Configuration;
using Aquila.Cosmos.Extensions;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Tests;

public sealed class ServiceCollectionExtensionsTests
{
    private const string DummyConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    [Fact]
    public void AddAquila_Registers_DocumentStore_And_Scoped_Sessions_In_DI()
    {
        var services = new ServiceCollection();

        services.AddAquila(options =>
        {
            options.DefaultTenantId = "di-tenant";
            options.UseInMemoryStorage();
        });

        var provider = services.BuildServiceProvider();

        var store = provider.GetService<IDocumentStore>();
        store.ShouldNotBeNull();
        store.Options.DefaultTenantId.ShouldBe("di-tenant");

        using var scope = provider.CreateScope();
        var querySession = scope.ServiceProvider.GetService<IQuerySession>();
        querySession.ShouldNotBeNull();
        querySession.TenantId.ShouldBe("di-tenant");

        var docSession = scope.ServiceProvider.GetService<IDocumentSession>();
        docSession.ShouldNotBeNull();
        docSession.TenantId.ShouldBe("di-tenant");
    }

    [Fact]
    public void AddAquila_Throws_On_Null_Arguments()
    {
        IServiceCollection services = null!;
        Should.Throw<ArgumentNullException>(() => services.AddAquila(options => { }));

        var validServices = new ServiceCollection();
        Should.Throw<ArgumentNullException>(() => validServices.AddAquila(null!));
    }

    [Fact]
    public void UseCosmos_With_ConnectionString_And_Strings()
    {
        var options = new StoreOptions();
        options.UseCosmos(DummyConnectionString, "CustomDb", "CustomContainer");

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        options.EventStorage.ShouldBeOfType<CosmosStorageProvider>();

        var provider = (CosmosStorageProvider)options.DocumentStorage;
        provider.Options.DefaultDatabase.ShouldBe("CustomDb");
        provider.Options.Documents.Container.ShouldBe("CustomContainer");
    }

    [Fact]
    public void UseCosmos_With_Client_And_Strings()
    {
        var client = Substitute.For<CosmosClient>();
        var options = new StoreOptions();
        options.UseCosmos(client, "ClientDb", "ClientContainer");

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        var provider = (CosmosStorageProvider)options.DocumentStorage;
        provider.Options.DefaultDatabase.ShouldBe("ClientDb");
        provider.Options.Documents.Container.ShouldBe("ClientContainer");
    }

    [Fact]
    public void UseCosmos_With_ConnectionString_And_Action()
    {
        var options = new StoreOptions();
        options.UseCosmos(DummyConnectionString, cosmos =>
        {
            cosmos.DefaultDatabase = "ActionDb";
            cosmos.ConfigureEvents("ActionEvents");
        });

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        var provider = (CosmosStorageProvider)options.DocumentStorage;
        provider.Options.DefaultDatabase.ShouldBe("ActionDb");
        provider.Options.Events.Container.ShouldBe("ActionEvents");
    }

    [Fact]
    public void UseCosmos_With_Client_And_Action()
    {
        var client = Substitute.For<CosmosClient>();
        var options = new StoreOptions();
        options.UseCosmos(client, cosmos =>
        {
            cosmos.DefaultDatabase = "ClientActionDb";
            cosmos.ConfigureSnapshots("ClientSnapshots");
        });

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        var provider = (CosmosStorageProvider)options.DocumentStorage;
        provider.Options.DefaultDatabase.ShouldBe("ClientActionDb");
        provider.Options.Snapshots.Container.ShouldBe("ClientSnapshots");
    }

    [Fact]
    public void UseCosmos_With_ConnectionString_And_CosmosStorageOptions_Instance()
    {
        var cosmosOptions = new CosmosStorageOptions
        {
            DefaultDatabase = "InstanceDb"
        };
        var options = new StoreOptions();
        options.UseCosmos(DummyConnectionString, cosmosOptions);

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        var provider = (CosmosStorageProvider)options.DocumentStorage;
        provider.Options.DefaultDatabase.ShouldBe("InstanceDb");
    }

    [Fact]
    public void UseCosmos_With_Client_And_CosmosStorageOptions_Instance()
    {
        var client = Substitute.For<CosmosClient>();
        var cosmosOptions = new CosmosStorageOptions
        {
            DefaultDatabase = "ClientInstanceDb"
        };
        var options = new StoreOptions();
        options.UseCosmos(client, cosmosOptions);

        options.DocumentStorage.ShouldBeOfType<CosmosStorageProvider>();
        var provider = (CosmosStorageProvider)options.DocumentStorage;
        provider.Options.DefaultDatabase.ShouldBe("ClientInstanceDb");
    }

    [Fact]
    public void UseCosmos_Validates_Null_And_Empty_Arguments()
    {
        var client = Substitute.For<CosmosClient>();
        var cosmosOptions = new CosmosStorageOptions();
        StoreOptions nullOptions = null!;
        var validOptions = new StoreOptions();

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmos(DummyConnectionString));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos(""));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos(DummyConnectionString, ""));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos(DummyConnectionString, "db", ""));

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmos(client));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos((CosmosClient)null!));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos(client, ""));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos(client, "db", ""));

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmos(DummyConnectionString, cosmos => { }));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos("", cosmos => { }));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos(DummyConnectionString, (Action<CosmosStorageOptions>)null!));

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmos(client, cosmos => { }));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos((CosmosClient)null!, cosmos => { }));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos(client, (Action<CosmosStorageOptions>)null!));

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmos(DummyConnectionString, cosmosOptions));
        Should.Throw<ArgumentException>(() => validOptions.UseCosmos("", cosmosOptions));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos(DummyConnectionString, (CosmosStorageOptions)null!));

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmos(client, cosmosOptions));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos((CosmosClient)null!, cosmosOptions));
        Should.Throw<ArgumentNullException>(() => validOptions.UseCosmos(client, (CosmosStorageOptions)null!));
    }
}
