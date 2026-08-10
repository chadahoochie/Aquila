using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Aquila.Core.Abstractions;
using Aquila.Cosmos.Extensions;

namespace Aquila.Cosmos.Tests;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAquila_Registers_DocumentStore_And_Options_In_DI()
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
    }

    [Fact]
    public void UseCosmos_Sets_CosmosStorageProvider()
    {
        var services = new ServiceCollection();

        services.AddAquila(options =>
        {
            options.UseCosmos("AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==", "TestDb", "TestContainer");
        });

        var provider = services.BuildServiceProvider();

        var store = provider.GetService<IDocumentStore>();
        store.ShouldNotBeNull();
        store.Options.DocumentStorage.ProviderName.ShouldBe("AzureCosmosDB");
    }
}
