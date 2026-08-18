using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Core.Storage;
using Aquila.Cosmos.Configuration;
using Aquila.Cosmos.Extensions;
using Aquila.Cosmos.Storage;
using Shouldly;

namespace Aquila.Cosmos.Tests.Projections;

public class CosmosProjectionStorageTests
{
    public class SampleReadModel
    {
        public string Id { get; set; } = string.Empty;
        public string Title { get; set; } = string.Empty;
    }

    [Fact]
    public void CosmosProjectionStorageProvider_DelegatesCorrectly_AndExposesRequestCharges()
    {
        var options = new StoreOptions();
        options.UseCosmosDocuments("AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==", cosmos =>
        {
            cosmos.DefaultDatabase = "TestDB";
        });
        options.UseCosmosProjections("AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==", cosmos =>
        {
            cosmos.DefaultDatabase = "ReadModelsDB";
        });

        options.DocumentStorage.ProviderName.ShouldBe("AzureCosmosDB");
        options.ProjectionStorage.ProviderName.ShouldBe("AzureCosmosDB");
        options.ProjectionStorage.LastRequestCharge.ShouldBe(0.0);
        options.ProjectionStorage.CumulativeRequestCharge.ShouldBe(0.0);
    }
}
