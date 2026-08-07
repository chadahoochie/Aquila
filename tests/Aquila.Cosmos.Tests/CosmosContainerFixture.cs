using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Testcontainers.CosmosDb;
using Xunit;

namespace Aquila.Cosmos.Tests;

[CollectionDefinition("CosmosIntegration")]
public sealed class CosmosCollection : ICollectionFixture<CosmosContainerFixture>
{
}

public sealed class CosmosContainerFixture : IAsyncLifetime
{
    public CosmosDbContainer Container { get; } = new CosmosDbBuilder("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview-testcontainers")
        .Build();

    public CosmosClient Client { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await Container.StartAsync();

        var clientOptions = new CosmosClientOptions
        {
            HttpClientFactory = () =>
            {
                var handler = new System.Net.Http.HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback = System.Net.Http.HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };
                return new System.Net.Http.HttpClient(handler);
            },
            ConnectionMode = ConnectionMode.Gateway,
            LimitToEndpoint = true
        };

        Client = new CosmosClient(Container.GetConnectionString(), clientOptions);
    }

    public async ValueTask DisposeAsync()
    {
        Client?.Dispose();
        await Container.DisposeAsync();
    }
}
