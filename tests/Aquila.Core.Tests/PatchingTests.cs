using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Aquila.Core.Configuration;
using Aquila.Core.Patching;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Shouldly;
using Xunit;

namespace Aquila.Core.Tests;

public class PatchTestAddress
{
    public string City { get; set; } = string.Empty;
    public string ZipCode { get; set; } = string.Empty;
}

public class PatchTestDocument
{
    public string Id { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public int Count { get; set; }
    public PatchTestAddress Address { get; set; } = new();
    public List<string> Tags { get; set; } = new();
}

public class PatchingTests
{
    [Fact]
    public void ExpressionParsing_Parses_Property_Lambdas_To_JsonPointer_Paths()
    {
        var expr = new PatchExpression<PatchTestDocument>();

        expr.Set(x => x.Status, "Active");
        expr.Set(x => x.Address.City, "New York");
        expr.Increment(x => x.Count, 5);
        expr.Append(x => x.Tags, "tag1");
        expr.Remove(x => x.Tags, "tag1");

        expr.Operations.Count.ShouldBe(5);

        expr.Operations[0].Path.ShouldBe("/Data/Status");
        expr.Operations[0].Action.ShouldBe(PatchAction.Set);
        expr.Operations[0].Value.ShouldBe("Active");

        expr.Operations[1].Path.ShouldBe("/Data/Address/City");
        expr.Operations[1].Action.ShouldBe(PatchAction.Set);
        expr.Operations[1].Value.ShouldBe("New York");

        expr.Operations[2].Path.ShouldBe("/Data/Count");
        expr.Operations[2].Action.ShouldBe(PatchAction.Increment);
        expr.Operations[2].Value.ShouldBe(5);

        expr.Operations[3].Path.ShouldBe("/Data/Tags");
        expr.Operations[3].Action.ShouldBe(PatchAction.Append);
        expr.Operations[3].Value.ShouldBe("tag1");

        expr.Operations[4].Path.ShouldBe("/Data/Tags");
        expr.Operations[4].Action.ShouldBe(PatchAction.Remove);
        expr.Operations[4].Value.ShouldBe("tag1");
    }

    [Fact]
    public void ExpressionParsing_Throws_On_Null_Or_NonProperty_Expression()
    {
        var expr = new PatchExpression<PatchTestDocument>();

        Should.Throw<ArgumentNullException>(() => expr.Set<string>(null!, "test"));
        Should.Throw<ArgumentNullException>(() => expr.Increment(null!, 1));
        Should.Throw<ArgumentNullException>(() => expr.Append<string>(null!, "test"));
        Should.Throw<ArgumentNullException>(() => expr.Remove<string>(null!, "test"));

        Should.Throw<ArgumentException>(() => expr.Set(x => x, new PatchTestDocument()));
    }

    [Fact]
    public void DocumentSession_Patch_Throws_On_Invalid_Arguments()
    {
        using var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        Should.Throw<ArgumentException>(() => session.Patch<PatchTestDocument>(""));
        Should.Throw<ArgumentException>(() => session.Patch<PatchTestDocument>("   "));
        Should.Throw<ArgumentException>(() => session.Patch<PatchTestDocument>("doc-1", "   "));
    }

    [Fact]
    public async Task DocumentSession_Patch_Registers_StorageOperation_And_Executes_InMemory()
    {
        using var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var doc = new PatchTestDocument
        {
            Id = "doc-1",
            Status = "Pending",
            Count = 10,
            Address = new PatchTestAddress { City = "Chicago", ZipCode = "60601" },
            Tags = new List<string> { "initial", "remove-me" }
        };

        session.Store(doc);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Act: Apply Patch operations
        session.Patch<PatchTestDocument>("doc-1")
            .Set(x => x.Status, "Completed")
            .Set(x => x.Address.City, "Seattle")
            .Increment(x => x.Count, 3)
            .Append(x => x.Tags, "new-tag")
            .Remove(x => x.Tags, "remove-me");

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Verify: Load document in a new session to confirm persistence in InMemoryStorageProvider
        using var session2 = new DocumentSession(storage, options);
        var loaded = await session2.LoadAsync<PatchTestDocument>("doc-1", ct: TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Status.ShouldBe("Completed");
        loaded.Address.City.ShouldBe("Seattle");
        loaded.Count.ShouldBe(13);
        loaded.Tags.ShouldContain("initial");
        loaded.Tags.ShouldContain("new-tag");
        loaded.Tags.ShouldNotContain("remove-me");
    }

    [Fact]
    public async Task DocumentSession_Patch_Supports_Custom_PartitionKey()
    {
        using var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var doc = new PatchTestDocument
        {
            Id = "doc-custom-pk",
            Status = "OldStatus",
            Count = 1
        };

        session.Store(doc, partitionKey: "custom-pk");
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        session.Patch<PatchTestDocument>("doc-custom-pk", partitionKey: "custom-pk")
            .Set(x => x.Status, "NewStatus")
            .Increment(x => x.Count, 9);

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        using var session2 = new DocumentSession(storage, options);
        var loaded = await session2.LoadAsync<PatchTestDocument>("doc-custom-pk", partitionKey: "custom-pk", ct: TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Status.ShouldBe("NewStatus");
        loaded.Count.ShouldBe(10);
    }
}
