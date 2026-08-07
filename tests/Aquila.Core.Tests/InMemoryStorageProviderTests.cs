using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed class InMemoryStorageProviderTests
{
    [Fact]
    public async Task InMemoryStorageProvider_Performs_Document_Crud()
    {
        var provider = new InMemoryStorageProvider();
        provider.ProviderName.ShouldBe("InMemory");

        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = nameof(SampleDocument),
            TenantId = "default",
            Data = new SampleDocument("doc-1", "Test Title", 99.99m)
        };

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        var read = await provider.ReadDocumentAsync<SampleDocument>("doc-1", "pk-1", TestContext.Current.CancellationToken);
        read.ShouldNotBeNull();
        read.Data.Title.ShouldBe("Test Title");

        await provider.DeleteDocumentAsync<SampleDocument>("doc-1", "pk-1", TestContext.Current.CancellationToken);

        var readDeleted = await provider.ReadDocumentAsync<SampleDocument>("doc-1", "pk-1", TestContext.Current.CancellationToken);
        readDeleted.ShouldBeNull();
    }

    [Fact]
    public async Task InMemoryStorageProvider_Appends_And_Fetches_Events_With_Concurrency_Check()
    {
        var provider = new InMemoryStorageProvider();
        var streamId = "stream-100";

        var event1 = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m)
        };

        await provider.Events.AppendEventsAsync(streamId, new[] { event1 }, 0, TestContext.Current.CancellationToken);

        var events = await provider.Events.FetchEventsAsync(streamId, ct: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);

        var header = await provider.Events.GetStreamHeaderAsync(streamId, ct: TestContext.Current.CancellationToken);
        header.ShouldNotBeNull();
        header.Version.ShouldBe(1);

        var event2 = new EventEnvelope<MoneyDepositedEvent>
        {
            StreamId = streamId,
            Version = 2,
            Data = new MoneyDepositedEvent(Guid.NewGuid(), 50m)
        };

        // Assert optimistic concurrency violation when expectedVersion is wrong
        await Should.ThrowAsync<AquilaConcurrencyException>(() =>
            provider.Events.AppendEventsAsync(streamId, new[] { event2 }, 99));
    }

    [Fact]
    public async Task InMemoryStorageProvider_Returns_Null_For_Missing_Document_And_Header()
    {
        var provider = new InMemoryStorageProvider();

        var doc = await provider.ReadDocumentAsync<SampleDocument>("missing-id", "missing-pk", TestContext.Current.CancellationToken);
        doc.ShouldBeNull();

        var header = await provider.Events.GetStreamHeaderAsync("non-existent-stream", ct: TestContext.Current.CancellationToken);
        header.ShouldBeNull();
    }

    [Fact]
    public async Task InMemoryStorageProvider_Queries_Documents_With_Predicate()
    {
        var provider = new InMemoryStorageProvider();

        await provider.UpsertDocumentAsync(new DocumentEnvelope<SampleDocument>
        {
            Id = "doc-1",
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            TenantId = "tenant-1",
            Data = new SampleDocument("doc-1", "Alpha", 10m)
        }, TestContext.Current.CancellationToken);

        await provider.UpsertDocumentAsync(new DocumentEnvelope<SampleDocument>
        {
            Id = "doc-2",
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            TenantId = "tenant-1",
            Data = new SampleDocument("doc-2", "Beta", 50m)
        }, TestContext.Current.CancellationToken);

        var queried = await provider.QueryDocumentsAsync<SampleDocument>(x => x.Data.Price > 20m, TestContext.Current.CancellationToken);
        queried.Count.ShouldBe(1);
        queried[0].Data.Title.ShouldBe("Beta");
    }

    [Fact]
    public async Task InMemoryStorageProvider_Throws_On_Invalid_Batch_Operation()
    {
        var provider = new InMemoryStorageProvider();

        var invalidOp = new StorageOperation
        {
            OperationType = StorageOperationType.Upsert,
            Id = "",
            PartitionKey = "pk",
            DocType = nameof(SampleDocument),
            Document = new SampleDocument("", "Title", 10m)
        };

        await Should.ThrowAsync<ArgumentException>(() =>
            provider.ExecuteBatchAsync(new[] { invalidOp }, TestContext.Current.CancellationToken));
    }
}
