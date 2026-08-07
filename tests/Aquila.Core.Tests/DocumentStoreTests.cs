using System;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record DocWithoutId(string Title);

public sealed class DocumentStoreTests
{
    [Fact]
    public async Task DocumentStore_Initializes_Creates_Store_And_Opens_Sessions()
    {
        var store = DocumentStore.For(options =>
        {
            options.DefaultTenantId = "custom-tenant";
            options.UseInMemoryStorage();
            options.Schema.For<SampleDocument>()
                .Identity(x => x.Id)
                .PartitionKey(x => x.Title);
        });

        store.Options.ShouldNotBeNull();
        store.Options.StorageProvider.ProviderName.ShouldBe("InMemory");
        store.Options.DefaultTenantId.ShouldBe("custom-tenant");

        await store.InitializeAsync(TestContext.Current.CancellationToken);

        using (var querySession = store.QuerySession("tenant-99"))
        {
            querySession.ShouldNotBeNull();
        }

        using (var docSession = store.OpenSession("tenant-99"))
        {
            docSession.ShouldNotBeNull();
        }

        using (var lightweightSession = store.LightweightSession("tenant-99"))
        {
            lightweightSession.ShouldNotBeNull();
        }

        store.Dispose();
        await store.DisposeAsync();
    }

    [Fact]
    public void DocumentMapping_And_StoreOptions_Validate_Config()
    {
        var options = new StoreOptions();
        options.Projections.Add<UserProjection>(ProjectionLifecycle.Inline);
        options.Projections.Projections.Count.ShouldBe(1);

        var mapping = options.Schema.For<SampleDocument>();
        mapping.UseSoftDeletes.ShouldBeFalse();
        mapping.OptimisticConcurrencyEnabled.ShouldBeFalse();

        mapping.SoftDeleted();
        mapping.UseSoftDeletes.ShouldBeTrue();

        mapping.UseOptimisticConcurrency();
        mapping.OptimisticConcurrencyEnabled.ShouldBeTrue();

        var docWithId = new SampleDocument("custom-id", "Title", 10m);
        mapping.IdSelector(docWithId).ShouldBe("custom-id");
        mapping.PartitionKeySelector(docWithId).ShouldBe("SampleDocument");

        var noIdMapping = options.Schema.For<DocWithoutId>();
        var docNoId = new DocWithoutId("No ID");
        noIdMapping.IdSelector(docNoId).ShouldNotBeNullOrWhiteSpace();
        noIdMapping.PartitionKeySelector(docNoId).ShouldBe("DocWithoutId");
    }
}
