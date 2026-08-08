using System;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed class IdentityMapTests
{
    public record TestDoc(string Id, string Name);

    [Fact]
    public void IdentityMap_TryGet_Returns_Tracked_Entity()
    {
        IIdentityMap map = new IdentityMap();
        var doc = new TestDoc("1", "Item 1");
        var envelope = new DocumentEnvelope<TestDoc> { Id = "1", Data = doc };

        map.Track("1", doc, envelope);

        var found = map.TryGet<TestDoc>("1", out var result);

        found.ShouldBeTrue();
        result.ShouldBeSameAs(doc);
    }

    [Fact]
    public void IdentityMap_TryGet_Returns_False_When_Not_Tracked()
    {
        IIdentityMap map = new IdentityMap();

        var found = map.TryGet<TestDoc>("999", out var result);

        found.ShouldBeFalse();
        result.ShouldBeNull();
    }

    [Fact]
    public void IdentityMap_GetEnvelope_Returns_Tracked_Envelope()
    {
        IIdentityMap map = new IdentityMap();
        var doc = new TestDoc("1", "Item 1");
        var envelope = new DocumentEnvelope<TestDoc> { Id = "1", Data = doc };

        map.Track("1", doc, envelope);

        var retrievedEnvelope = map.GetEnvelope<TestDoc>("1");

        retrievedEnvelope.ShouldNotBeNull();
        retrievedEnvelope.ShouldBeSameAs(envelope);
    }

    [Fact]
    public void IdentityMap_Clear_Removes_All_Tracked_Items()
    {
        IIdentityMap map = new IdentityMap();
        var doc = new TestDoc("1", "Item 1");
        var envelope = new DocumentEnvelope<TestDoc> { Id = "1", Data = doc };

        map.Track("1", doc, envelope);
        map.Clear();

        var found = map.TryGet<TestDoc>("1", out var result);

        found.ShouldBeFalse();
        result.ShouldBeNull();
        map.GetEnvelope<TestDoc>("1").ShouldBeNull();
    }

    [Theory, AutoNSubstituteData]
    public async Task LoadAsync_Returns_Cached_Instance_On_Subsequent_Calls(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage)
    {
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        var doc = new TestDoc("doc-1", "Original");
        var envelope = new DocumentEnvelope<TestDoc>
        {
            Id = "doc-1",
            PartitionKey = nameof(TestDoc),
            DocType = nameof(TestDoc),
            TenantId = "default",
            Data = doc
        };

        docStorage.ReadDocumentAsync<TestDoc>("doc-1", nameof(TestDoc), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DocumentEnvelope<TestDoc>?>(envelope));

        using var session = new DocumentSession(storage, options);

        var doc1 = await session.LoadAsync<TestDoc>("doc-1", ct: TestContext.Current.CancellationToken);
        var doc2 = await session.LoadAsync<TestDoc>("doc-1", ct: TestContext.Current.CancellationToken);

        doc1.ShouldNotBeNull();
        doc2.ShouldNotBeNull();
        doc1.ShouldBeSameAs(doc2);

        await docStorage.Received(1).ReadDocumentAsync<TestDoc>("doc-1", nameof(TestDoc), Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task Store_Tracks_Entity_In_IdentityMap(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage)
    {
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);
        var doc = new TestDoc("doc-100", "Store Test");

        session.Store(doc);

        var loaded = await session.LoadAsync<TestDoc>("doc-100", ct: TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.ShouldBeSameAs(doc);
        await docStorage.DidNotReceive().ReadDocumentAsync<TestDoc>("doc-100", Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task Dispose_Clears_IdentityMap(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage)
    {
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        var session = new DocumentSession(storage, options);
        var doc = new TestDoc("doc-1", "Original");
        var envelope = new DocumentEnvelope<TestDoc>
        {
            Id = "doc-1",
            PartitionKey = nameof(TestDoc),
            DocType = nameof(TestDoc),
            TenantId = "default",
            Data = doc
        };

        docStorage.ReadDocumentAsync<TestDoc>("doc-1", nameof(TestDoc), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DocumentEnvelope<TestDoc>?>(envelope));

        var doc1 = await session.LoadAsync<TestDoc>("doc-1", ct: TestContext.Current.CancellationToken);
        doc1.ShouldNotBeNull();

        session.Dispose();

        session.IdentityMap.TryGet<TestDoc>("doc-1", out _).ShouldBeFalse();
    }
}
