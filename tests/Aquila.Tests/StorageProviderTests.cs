using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using Xunit;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Tests;

public sealed class StorageProviderTests
{
    [Theory, AutoNSubstituteData]
    public async Task InMemory_ReadDocumentAsync_ReturnsEnvelope_WhenDocumentExists(
        SampleDocument document)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            Data = document
        };

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        // Act
        var result = await provider.ReadDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe(document.Id);
        result.Data.ShouldBe(document);
    }

    [Fact]
    public async Task InMemory_ReadDocumentAsync_ReturnsNull_WhenDocumentDoesNotExist()
    {
        // Arrange
        var provider = new InMemoryStorageProvider();

        // Act
        var result = await provider.ReadDocumentAsync<SampleDocument>("non-existent-id", "pk", TestContext.Current.CancellationToken);

        // Assert
        result.ShouldBeNull();
    }

    [Theory, AutoNSubstituteData]
    public async Task InMemory_ReadDocumentAsync_ReturnsNull_WhenDocumentIsSoftDeleted(
        SampleDocument document)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            IsDeleted = true,
            Data = document
        };

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        // Act
        var result = await provider.ReadDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);

        // Assert
        result.ShouldBeNull();
    }

    [Theory, AutoNSubstituteData]
    public async Task InMemory_DeleteDocumentAsync_RemovesDocument(
        SampleDocument document)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            Data = document
        };

        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        // Act
        await provider.DeleteDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);
        var result = await provider.ReadDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);

        // Assert
        result.ShouldBeNull();
    }

    [Theory, AutoNSubstituteData]
    public async Task InMemory_ExecuteBatchAsync_ProcessesUpsertAndDeleteOperations(
        SampleDocument document1, SampleDocument document2)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var envelope1 = new DocumentEnvelope<SampleDocument>
        {
            Id = document1.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            Data = document1
        };

        await provider.UpsertDocumentAsync(envelope1, TestContext.Current.CancellationToken);

        var ops = new List<StorageOperation>
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = document2.Id,
                PartitionKey = nameof(SampleDocument),
                DocType = nameof(SampleDocument),
                Document = new DocumentEnvelope<SampleDocument>
                {
                    Id = document2.Id,
                    PartitionKey = nameof(SampleDocument),
                    DocType = nameof(SampleDocument),
                    Data = document2
                }
            },
            new StorageOperation
            {
                OperationType = StorageOperationType.Delete,
                Id = document1.Id,
                PartitionKey = nameof(SampleDocument),
                DocType = nameof(SampleDocument)
            }
        };

        // Act
        await provider.ExecuteBatchAsync(ops, TestContext.Current.CancellationToken);

        // Assert
        var res1 = await provider.ReadDocumentAsync<SampleDocument>(document1.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);
        var res2 = await provider.ReadDocumentAsync<SampleDocument>(document2.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);

        res1.ShouldBeNull();
        res2.ShouldNotBeNull();
        res2.Id.ShouldBe(document2.Id);
    }

    [Theory, AutoNSubstituteData]
    public async Task InMemory_AppendEventsAsync_AppendsEventsAndIncrementsVersion(
        string streamId, Guid accountId, string ownerName)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var events = new List<IEvent>
        {
            new EventEnvelope<AccountCreatedEvent>
            {
                StreamId = streamId,
                Version = 1,
                Data = new AccountCreatedEvent(accountId, ownerName, 100m)
            }
        };

        // Act
        await provider.AppendEventsAsync(streamId, events, expectedVersion: 0, TestContext.Current.CancellationToken);
        var fetched = await provider.FetchEventsAsync(streamId, fromVersion: 0, ct: TestContext.Current.CancellationToken);
        var header = await provider.GetStreamHeaderAsync(streamId, ct: TestContext.Current.CancellationToken);

        // Assert
        fetched.Count.ShouldBe(1);
        header.ShouldNotBeNull();
        header.Version.ShouldBe(1);
        header.StreamId.ShouldBe(streamId);
    }

    [Theory, AutoNSubstituteData]
    public async Task InMemory_AppendEventsAsync_ThrowsAquilaConcurrencyException_OnVersionMismatch(
        string streamId, Guid accountId, string ownerName)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var events = new List<IEvent>
        {
            new EventEnvelope<AccountCreatedEvent>
            {
                StreamId = streamId,
                Version = 1,
                Data = new AccountCreatedEvent(accountId, ownerName, 100m)
            }
        };

        await provider.AppendEventsAsync(streamId, events, expectedVersion: 0, TestContext.Current.CancellationToken);

        // Act & Assert
        await Should.ThrowAsync<AquilaConcurrencyException>(async () =>
            await provider.AppendEventsAsync(streamId, events, expectedVersion: 5, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InMemory_InputValidation_ThrowsExceptions_OnInvalidInputs()
    {
        var provider = new InMemoryStorageProvider();

        await Should.ThrowAsync<ArgumentException>(() => provider.ReadDocumentAsync<SampleDocument>("", "pk"));
        await Should.ThrowAsync<ArgumentException>(() => provider.ReadDocumentAsync<SampleDocument>("id", "   "));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.UpsertDocumentAsync<SampleDocument>(null!));
        await Should.ThrowAsync<ArgumentException>(() => provider.DeleteDocumentAsync<SampleDocument>("   ", "pk"));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.ExecuteBatchAsync(null!));
        await Should.ThrowAsync<ArgumentException>(() => provider.AppendEventsAsync("   ", Array.Empty<IEvent>(), -1));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.AppendEventsAsync("stream1", null!, -1));
        await Should.ThrowAsync<ArgumentException>(() => provider.FetchEventsAsync(""));
        await Should.ThrowAsync<ArgumentException>(() => provider.GetStreamHeaderAsync("   "));
    }

    [Fact]
    public void CosmosDocumentEnvelope_MapsPropertiesCorrectly()
    {
        var envelope = new CosmosDocumentEnvelope<SampleDocument>
        {
            Id = "doc-1",
            PartitionKey = "pk-1",
            DocType = "SampleDocument",
            TenantId = "tenant-1",
            IsDeleted = false,
            Version = "v1",
            ETag = "etag-123",
            Data = new SampleDocument("doc-1", "Test", 99.99m)
        };

        envelope.Id.ShouldBe("doc-1");
        envelope.PartitionKey.ShouldBe("pk-1");
        envelope.DocType.ShouldBe("SampleDocument");
        envelope.TenantId.ShouldBe("tenant-1");
        envelope.IsDeleted.ShouldBeFalse();
        envelope.Version.ShouldBe("v1");
        envelope.ETag.ShouldBe("etag-123");
        envelope.Data.Title.ShouldBe("Test");
    }

    [Theory, AutoNSubstituteData]
    public async Task CosmosStorageProvider_UpsertDocumentAsync_MapsEnvelopeAndCallsContainer(
        Container container,
        SampleDocument document)
    {
        // Arrange
        var client = Substitute.For<CosmosClient>();
        var provider = new CosmosStorageProvider(client);

        var containerField = typeof(CosmosStorageProvider).GetField("_container", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        containerField?.SetValue(provider, container);

        var envelope = new DocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            TenantId = "tenant1",
            Data = document
        };

        // Act
        await provider.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        // Assert
        await container.Received(1).UpsertItemAsync(
            Arg.Is<CosmosDocumentEnvelope<SampleDocument>>(env => env.Id == document.Id && env.PartitionKey == nameof(SampleDocument) && env.TenantId == "tenant1"),
            Arg.Is<PartitionKey>(pk => pk.Equals(new PartitionKey(nameof(SampleDocument)))),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Theory, AutoNSubstituteData]
    public async Task CosmosStorageProvider_ReadDocumentAsync_ReturnsMappedEnvelope_WhenFound(
        Container container,
        SampleDocument document)
    {
        // Arrange
        var client = Substitute.For<CosmosClient>();
        var provider = new CosmosStorageProvider(client);

        var containerField = typeof(CosmosStorageProvider).GetField("_container", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        containerField?.SetValue(provider, container);

        var cosmosEnvelope = new CosmosDocumentEnvelope<SampleDocument>
        {
            Id = document.Id,
            PartitionKey = nameof(SampleDocument),
            DocType = nameof(SampleDocument),
            TenantId = "tenant1",
            IsDeleted = false,
            Version = "v1",
            ETag = "etag1",
            Data = document
        };

        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<SampleDocument>>>();
        response.Resource.Returns(cosmosEnvelope);

        container.ReadItemAsync<CosmosDocumentEnvelope<SampleDocument>>(
            document.Id,
            Arg.Is<PartitionKey>(pk => pk.Equals(new PartitionKey(nameof(SampleDocument)))),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        // Act
        var result = await provider.ReadDocumentAsync<SampleDocument>(document.Id, nameof(SampleDocument), TestContext.Current.CancellationToken);

        // Assert
        result.ShouldNotBeNull();
        result.Id.ShouldBe(document.Id);
        result.PartitionKey.ShouldBe(nameof(SampleDocument));
        result.TenantId.ShouldBe("tenant1");
        result.Data.ShouldBe(document);
    }

    [Fact]
    public async Task CosmosStorageProvider_InputValidation_ThrowsExceptions_OnInvalidInputs()
    {
        Should.Throw<ArgumentNullException>(() => new CosmosStorageProvider((CosmosClient)null!));
        Should.Throw<ArgumentException>(() => new CosmosStorageProvider(""));

        var client = Substitute.For<CosmosClient>();
        var container = Substitute.For<Container>();
        var provider = new CosmosStorageProvider(client);

        var containerField = typeof(CosmosStorageProvider).GetField("_container", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        containerField?.SetValue(provider, container);

        await Should.ThrowAsync<ArgumentException>(() => provider.ReadDocumentAsync<SampleDocument>("", "pk"));
        await Should.ThrowAsync<ArgumentException>(() => provider.ReadDocumentAsync<SampleDocument>("id", "   "));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.UpsertDocumentAsync<SampleDocument>(null!));
        await Should.ThrowAsync<ArgumentException>(() => provider.DeleteDocumentAsync<SampleDocument>("   ", "pk"));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.ExecuteBatchAsync(null!));
        await Should.ThrowAsync<ArgumentException>(() => provider.AppendEventsAsync("   ", Array.Empty<IEvent>(), -1));
        await Should.ThrowAsync<ArgumentNullException>(() => provider.AppendEventsAsync("stream1", null!, -1));
        await Should.ThrowAsync<ArgumentException>(() => provider.FetchEventsAsync(""));
        await Should.ThrowAsync<ArgumentException>(() => provider.GetStreamHeaderAsync("   "));
    }

    [Theory, AutoNSubstituteData]
    public async Task CosmosStorageProvider_AppendEventsAsync_ThrowsAquilaConcurrencyException_OnVersionMismatch(
        Container container,
        string streamId,
        Guid accountId)
    {
        // Arrange
        var client = Substitute.For<CosmosClient>();
        var provider = new CosmosStorageProvider(client);

        var containerField = typeof(CosmosStorageProvider).GetField("_container", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        containerField?.SetValue(provider, container);

        // Header exists with version 2
        var headerEnv = new CosmosDocumentEnvelope<EventStreamHeader>
        {
            Id = $"$stream_{streamId}",
            PartitionKey = streamId,
            Data = new EventStreamHeader { StreamId = streamId, Version = 2 }
        };
        var response = Substitute.For<ItemResponse<CosmosDocumentEnvelope<EventStreamHeader>>>();
        response.Resource.Returns(headerEnv);

        container.ReadItemAsync<CosmosDocumentEnvelope<EventStreamHeader>>(
            $"$stream_{streamId}",
            Arg.Is<PartitionKey>(pk => pk.Equals(new PartitionKey(streamId))),
            cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(response));

        var events = new List<IEvent>
        {
            new EventEnvelope<AccountCreatedEvent> { StreamId = streamId, Version = 1, Data = new AccountCreatedEvent(accountId, "Owner", 50m) }
        };

        // Act & Assert (Expected version is 0, but current version is 2)
        await Should.ThrowAsync<AquilaConcurrencyException>(async () =>
            await provider.AppendEventsAsync(streamId, events, expectedVersion: 0, TestContext.Current.CancellationToken));
    }
}
