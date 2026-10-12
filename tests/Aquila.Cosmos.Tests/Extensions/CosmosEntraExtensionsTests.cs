using Azure.Core;
using NSubstitute;
using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Cosmos.Extensions;

namespace Aquila.Cosmos.Tests;

public sealed class CosmosEntraExtensionsTests
{
    private const string Endpoint = "https://myaccount.documents.azure.com:443/";

    [Fact]
    public void CreateEntraClient_WithCredential_UsesEndpoint()
    {
        using var client = ServiceCollectionExtensions.CreateEntraClient(Endpoint, Substitute.For<TokenCredential>());

        client.Endpoint.ShouldBe(new Uri(Endpoint));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void CreateEntraClient_BlankEndpoint_Throws(string? endpoint)
    {
        Should.Throw<ArgumentException>(() => ServiceCollectionExtensions.CreateEntraClient(endpoint!, Substitute.For<TokenCredential>()));
    }

    [Fact]
    public void UseCosmosDocumentsWithEntra_ConfiguresDocumentStorage()
    {
        var options = new StoreOptions();

        var result = options.UseCosmosDocumentsWithEntra(Endpoint, Substitute.For<TokenCredential>());

        result.ShouldBeSameAs(options);
        options.DocumentStorage.ShouldNotBeNull();
    }

    [Fact]
    public void UseCosmosEventsWithEntra_ConfiguresEventStorage()
    {
        var options = new StoreOptions();

        options.UseCosmosEventsWithEntra(Endpoint, Substitute.For<TokenCredential>());

        options.EventStorage.ShouldNotBeNull();
    }

    [Fact]
    public void UseCosmosProjectionsWithEntra_ConfiguresProjectionStorage()
    {
        var options = new StoreOptions();

        options.UseCosmosProjectionsWithEntra(Endpoint, Substitute.For<TokenCredential>());

        options.ProjectionStorage.ShouldNotBeNull();
    }

    [Fact]
    public void UseCosmosDocumentsWithEntra_NullOptions_Throws()
    {
        StoreOptions nullOptions = null!;

        Should.Throw<ArgumentNullException>(() => nullOptions.UseCosmosDocumentsWithEntra(Endpoint, Substitute.For<TokenCredential>()));
    }
}
