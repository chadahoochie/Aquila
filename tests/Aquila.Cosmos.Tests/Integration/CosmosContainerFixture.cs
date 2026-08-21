using Microsoft.Azure.Cosmos;
using Testcontainers.CosmosDb;

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
                var handler = new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback = System.Net.Http.HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };
                return new HttpClient(handler);
            },
            ConnectionMode = ConnectionMode.Gateway,
            LimitToEndpoint = true
        };

        Client = new CosmosClient(Container.GetConnectionString(), clientOptions);

        await WaitUntilReadyAsync();
    }

    /// <summary>
    /// Blocks until the emulator answers a real data-plane request.
    /// </summary>
    /// <remarks>
    /// The container reports started well before the emulator can serve requests, and until then it
    /// returns 500s such as <c>schema "cosmos_api" does not exist</c> or a refused backend
    /// connection. Without this gate the first tests to run fail for reasons unrelated to the code
    /// under test, and which tests those are depends on scheduling order.
    /// </remarks>
    private async Task WaitUntilReadyAsync()
    {
        const int maxAttempts = 40;
        Exception? last = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                var db = await Client.CreateDatabaseIfNotExistsAsync("ReadinessProbe");
                await db.Database.DeleteAsync();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        }

        throw new InvalidOperationException(
            $"Cosmos emulator did not become ready after {maxAttempts} attempts.", last);
    }

    public async ValueTask DisposeAsync()
    {
        Client?.Dispose();
        await Container.DisposeAsync();
    }
}
