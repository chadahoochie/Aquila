using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record SampleDocument(string Id, string Title, decimal Price);

public sealed class MutableSampleDocument
{
    public string Id { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
}

public sealed class DocumentSessionTests
{
    [Theory, AutoNSubstituteData]
    public async Task LoadAsync_Delegates_To_Storage_Provider(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        SampleDocument document)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            TenantId = "default",
            Data = document
        };

        docStorage.ReadDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DocumentEnvelope<SampleDocument>?>(envelope));

        using var session = new DocumentSession(storage, options);

        // Act
        var result = await session.LoadAsync<SampleDocument>(document.Id, ct: TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe(document.Id);
        result.Title.ShouldBe(document.Title);
        result.Price.ShouldBe(document.Price);
    }

    [Theory, AutoNSubstituteData]
    public async Task LoadAsync_Guid_Delegates_To_String_Overload(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        Guid guidId)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        var document = new SampleDocument(guidId.ToString(), "Guid Doc", 49.99m);
        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = guidId.ToString(),
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            TenantId = "default",
            Data = document
        };

        docStorage.ReadDocumentAsync<SampleDocument>(guidId.ToString(), nameof(SampleDocument), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DocumentEnvelope<SampleDocument>?>(envelope));

        using var session = new DocumentSession(storage, options);

        // Act
        var result = await session.LoadAsync<SampleDocument>(guidId, ct: TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe(guidId.ToString());
    }

    [Theory, AutoNSubstituteData]
    public async Task LoadManyAsync_Loads_Multiple_Documents(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        SampleDocument doc1,
        SampleDocument doc2)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        var envelopes = new List<DocumentEnvelope<SampleDocument>>
        {
            new DocumentEnvelope<SampleDocument> { Id = doc1.Id, PartitionKey = nameof(SampleDocument), DocType = nameof(SampleDocument), TenantId = "default", Data = doc1 },
            new DocumentEnvelope<SampleDocument> { Id = doc2.Id, PartitionKey = nameof(SampleDocument), DocType = nameof(SampleDocument), TenantId = "default", Data = doc2 }
        };

        docStorage.QueryDocumentsAsync<SampleDocument>(Arg.Any<System.Linq.Expressions.Expression<Func<DocumentEnvelope<SampleDocument>, bool>>>(), Arg.Any<QueryOptions>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<DocumentEnvelope<SampleDocument>>>(envelopes));

        using var session = new DocumentSession(storage, options);

        // Act
        var results = await session.LoadManyAsync<SampleDocument>(new[] { doc1.Id, doc2.Id }, ct: TestContext.Current.CancellationToken);

        // Assert
        results.Count.ShouldBe(2);
    }

    [Theory, AutoNSubstituteData]
    public async Task SaveChangesAsync_Executes_Pending_Store_Operations(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        SampleDocument document)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        // Act
        session.Store(document);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).ExecuteBatchAsync(
            Arg.Any<IEnumerable<StorageOperation>>(),
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task Store_Enumerable_Stores_All_Documents(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);
        var documents = new List<SampleDocument>
        {
            new SampleDocument("doc-1", "Title 1", 10.00m),
            new SampleDocument("doc-2", "Title 2", 20.00m)
        };

        // Act
        session.Store(documents);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).ExecuteBatchAsync(
            Arg.Any<IEnumerable<StorageOperation>>(),
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task Store_Snapshots_Document_State_Isolating_Post_Store_Mutations(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        var doc = new MutableSampleDocument { Id = "doc-100", Title = "Original Title" };

        // Act
        session.Store(doc);
        doc.Title = "Mutated Title";
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).ExecuteBatchAsync(
            Arg.Is<IEnumerable<StorageOperation>>(ops =>
                System.Linq.Enumerable.Any(ops, op =>
                    op.Id == "doc-100" &&
                    ((DocumentEnvelope<MutableSampleDocument>)op.Document).Data.Title == "Original Title")),
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task Delete_By_Document_Queues_Delete_Operation(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        SampleDocument document)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        // Act
        session.Delete(document);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).ExecuteBatchAsync(
            Arg.Is<IEnumerable<StorageOperation>>(ops => System.Linq.Enumerable.Any(ops, op => op.Id == document.Id && op.OperationType == StorageOperationType.Delete)),
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task SoftDeleteAsync_By_Document_Queues_And_Persists_SoftDelete(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        SampleDocument document)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        // Act
        await session.SoftDeleteAsync(document, TestContext.Current.CancellationToken);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).ExecuteBatchAsync(
            Arg.Is<IEnumerable<StorageOperation>>(ops => System.Linq.Enumerable.Any(ops, op => op.Id == document.Id && ((DocumentEnvelope<SampleDocument>)op.Document).IsDeleted)),
            Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public void DocumentSession_InputValidation_ThrowsExceptions_OnInvalidParameters(
        IAquilaStorageProvider storage)
    {
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

        Should.Throw<ArgumentNullException>(() => new DocumentSession(null!, options));
        Should.Throw<ArgumentNullException>(() => new DocumentSession(storage, null!));

        Should.Throw<ArgumentNullException>(() => session.Store<SampleDocument>((SampleDocument)null!));
        Should.Throw<ArgumentNullException>(() => session.Store<SampleDocument>((IEnumerable<SampleDocument>)null!));
        Should.Throw<ArgumentException>(() => session.Store(new SampleDocument("id", "title", 10m), partitionKey: "   "));

        Should.Throw<ArgumentNullException>(() => session.Delete<SampleDocument>((SampleDocument)null!));
        Should.Throw<ArgumentException>(() => session.Delete<SampleDocument>(""));
        Should.Throw<ArgumentException>(() => session.Delete<SampleDocument>("id", "   "));

        Should.Throw<ArgumentNullException>(() => session.SoftDelete<SampleDocument>((SampleDocument)null!));
        Should.Throw<ArgumentException>(() => session.SoftDelete<SampleDocument>("   "));
        Should.Throw<ArgumentException>(() => session.SoftDelete<SampleDocument>("id", "   "));

        Should.ThrowAsync<ArgumentException>(() => session.LoadAsync<SampleDocument>(""));
        Should.ThrowAsync<ArgumentException>(() => session.LoadAsync<SampleDocument>("id", "   "));
        Should.ThrowAsync<ArgumentNullException>(() => session.LoadManyAsync<SampleDocument>(null!));
        Should.ThrowAsync<ArgumentException>(() => session.LoadManyAsync<SampleDocument>(new[] { "id1", "  " }));
    }

    [Theory, AutoNSubstituteData]
    public async Task SoftDelete_Sets_IsDeleted_Flag_And_Persists(
        IAquilaStorageProvider storage,
        IDocumentStorageProvider docStorage,
        SampleDocument document)
    {
        // Arrange
        storage.Documents.Returns(docStorage);
        var options = new StoreOptions { StorageProvider = storage };
        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            IsDeleted = false,
            Data = document
        };

        docStorage.ReadDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DocumentEnvelope<SampleDocument>?>(envelope));

        using var session = new DocumentSession(storage, options);

        // Act
        session.SoftDelete<SampleDocument>(document.Id);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Assert
        await docStorage.Received(1).ExecuteBatchAsync(
            Arg.Is<IEnumerable<StorageOperation>>(ops => System.Linq.Enumerable.Any(ops, op => op.Id == document.Id && ((DocumentEnvelope<SampleDocument>)op.Document).IsDeleted)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public void AquilaConcurrencyException_InheritsFrom_AquilaException()
    {
        var ex = new Aquila.Core.Exceptions.AquilaConcurrencyException("doc-1", "1", "2");
        ex.ShouldBeAssignableTo<Aquila.Core.Exceptions.AquilaException>();
        ex.DocumentId.ShouldBe("doc-1");
        ex.ExpectedVersion.ShouldBe("1");
        ex.ActualVersion.ShouldBe("2");
    }

    [Theory, AutoNSubstituteData]
    public void Query_Method_Throws_NotSupportedException(
        IAquilaStorageProvider storage)
    {
        var options = new StoreOptions { StorageProvider = storage };
        using var session = new DocumentSession(storage, options);

#pragma warning disable CS0618
        Should.Throw<NotSupportedException>(() => session.Query<SampleDocument>());
#pragma warning restore CS0618
    }
}

