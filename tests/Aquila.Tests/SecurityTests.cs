using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Tests;

public sealed record SecurityTestDocument(string Id, string Content);

public sealed class SecurityAccountAggregate
{
    public Guid Id { get; private set; }
    public string OwnerName { get; private set; } = string.Empty;
    public decimal Balance { get; private set; }

    public void Apply(AccountCreatedEvent evt)
    {
        Id = evt.AccountId;
        OwnerName = evt.OwnerName;
        Balance = evt.InitialBalance;
    }

    public void Apply(MoneyDepositedEvent evt)
    {
        Balance += evt.Amount;
    }
}

public sealed class SecurityTests
{
    [Fact]
    public async Task LoadAsync_EnforcesTenantIsolation_TenantCannotReadOtherTenantDocument()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };

        var envelope = new DocumentEnvelope<SecurityTestDocument>
        {
            Id = "doc-1",
            PartitionKey = nameof(SecurityTestDocument),
            DocType = nameof(SecurityTestDocument),
            TenantId = "tenant-a",
            Data = new SecurityTestDocument("doc-1", "Tenant A Data")
        };
        await storage.Documents.UpsertDocumentAsync(envelope, TestContext.Current.CancellationToken);

        using var sessionTenantA = new DocumentSession(storage, options, tenantId: "tenant-a");
        using var sessionTenantB = new DocumentSession(storage, options, tenantId: "tenant-b");

        // Act
        var docForTenantA = await sessionTenantA.LoadAsync<SecurityTestDocument>("doc-1", ct: TestContext.Current.CancellationToken);
        var docForTenantB = await sessionTenantB.LoadAsync<SecurityTestDocument>("doc-1", ct: TestContext.Current.CancellationToken);

        // Assert
        docForTenantA.ShouldNotBeNull();
        docForTenantA.Content.ShouldBe("Tenant A Data");
        docForTenantB.ShouldBeNull();
    }

    [Fact]
    public async Task LoadManyAsync_EnforcesTenantIsolation_ReturnsOnlyCurrentTenantDocuments()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };

        var docA = new DocumentEnvelope<SecurityTestDocument>
        {
            Id = "doc-a",
            PartitionKey = nameof(SecurityTestDocument),
            DocType = nameof(SecurityTestDocument),
            TenantId = "tenant-a",
            Data = new SecurityTestDocument("doc-a", "Tenant A Data")
        };
        var docB = new DocumentEnvelope<SecurityTestDocument>
        {
            Id = "doc-b",
            PartitionKey = nameof(SecurityTestDocument),
            DocType = nameof(SecurityTestDocument),
            TenantId = "tenant-b",
            Data = new SecurityTestDocument("doc-b", "Tenant B Data")
        };

        await storage.Documents.UpsertDocumentAsync(docA, TestContext.Current.CancellationToken);
        await storage.Documents.UpsertDocumentAsync(docB, TestContext.Current.CancellationToken);

        using var sessionTenantA = new DocumentSession(storage, options, tenantId: "tenant-a");

        // Act
        var docs = await sessionTenantA.LoadManyAsync<SecurityTestDocument>(new[] { "doc-a", "doc-b" }, ct: TestContext.Current.CancellationToken);

        // Assert
        docs.Count.ShouldBe(1);
        docs[0].Id.ShouldBe("doc-a");
    }

    [Fact]
    public async Task QueryAsync_EnforcesTenantIsolation_FiltersOtherTenantDocuments()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };

        await storage.Documents.UpsertDocumentAsync(new DocumentEnvelope<SecurityTestDocument>
        {
            Id = "doc-1",
            PartitionKey = nameof(SecurityTestDocument),
            DocType = nameof(SecurityTestDocument),
            TenantId = "tenant-a",
            Data = new SecurityTestDocument("doc-1", "A-1")
        }, TestContext.Current.CancellationToken);

        await storage.Documents.UpsertDocumentAsync(new DocumentEnvelope<SecurityTestDocument>
        {
            Id = "doc-2",
            PartitionKey = nameof(SecurityTestDocument),
            DocType = nameof(SecurityTestDocument),
            TenantId = "tenant-b",
            Data = new SecurityTestDocument("doc-2", "B-1")
        }, TestContext.Current.CancellationToken);

        using var sessionTenantA = new DocumentSession(storage, options, tenantId: "tenant-a");

        // Act
        var results = await sessionTenantA.QueryAsync<SecurityTestDocument>(ct: TestContext.Current.CancellationToken);

        // Assert
        results.Count.ShouldBe(1);
        results[0].Id.ShouldBe("doc-1");
    }

    [Fact]
    public async Task FetchEventsAsync_EnforcesTenantIsolation_ReturnsOnlyMatchingTenantEvents()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var streamId = "stream-100";

        var evtA = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            TenantId = "tenant-a",
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Alice", 100m)
        };
        var evtB = new EventEnvelope<MoneyDepositedEvent>
        {
            StreamId = streamId,
            Version = 2,
            TenantId = "tenant-b",
            Data = new MoneyDepositedEvent(Guid.NewGuid(), 50m)
        };

        await storage.Events.AppendEventsAsync(streamId, new IEvent[] { evtA, evtB }, expectedVersion: -1, TestContext.Current.CancellationToken);

        // Act
        var eventsA = await storage.Events.FetchEventsAsync(streamId, tenantId: "tenant-a", fromVersion: 0, ct: TestContext.Current.CancellationToken);
        var eventsB = await storage.Events.FetchEventsAsync(streamId, tenantId: "tenant-b", fromVersion: 0, ct: TestContext.Current.CancellationToken);

        // Assert
        eventsA.Count.ShouldBe(1);
        eventsA[0].TenantId.ShouldBe("tenant-a");

        eventsB.Count.ShouldBe(1);
        eventsB[0].TenantId.ShouldBe("tenant-b");
    }

    [Fact]
    public async Task GetStreamHeaderAsync_EnforcesTenantIsolation_ReturnsNullForDifferentTenant()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var streamId = "stream-200";

        var evtA = new EventEnvelope<AccountCreatedEvent>
        {
            StreamId = streamId,
            Version = 1,
            TenantId = "tenant-a",
            Data = new AccountCreatedEvent(Guid.NewGuid(), "Bob", 200m)
        };

        await storage.Events.AppendEventsAsync(streamId, new[] { evtA }, expectedVersion: -1, TestContext.Current.CancellationToken);

        // Act
        var headerA = await storage.Events.GetStreamHeaderAsync(streamId, tenantId: "tenant-a", ct: TestContext.Current.CancellationToken);
        var headerB = await storage.Events.GetStreamHeaderAsync(streamId, tenantId: "tenant-b", ct: TestContext.Current.CancellationToken);

        // Assert
        headerA.ShouldNotBeNull();
        headerA.TenantId.ShouldBe("tenant-a");
        headerB.ShouldBeNull();
    }

    [Fact]
    public async Task AggregateStreamAsync_EnforcesTenantIsolation_TenantCannotRehydrateOtherTenantAggregate()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();
        var options = new StoreOptions { StorageProvider = storage };
        var accountId = Guid.NewGuid();

        using var sessionTenantA = new DocumentSession(storage, options, tenantId: "tenant-a");
        sessionTenantA.Events.StartStream<SecurityAccountAggregate>(accountId, new AccountCreatedEvent(accountId, "Charlie", 500m));
        await sessionTenantA.SaveChangesAsync(TestContext.Current.CancellationToken);

        using var sessionTenantB = new DocumentSession(storage, options, tenantId: "tenant-b");

        // Act
        var aggTenantA = await sessionTenantA.Events.AggregateStreamAsync<SecurityAccountAggregate>(accountId, ct: TestContext.Current.CancellationToken);
        var aggTenantB = await sessionTenantB.Events.AggregateStreamAsync<SecurityAccountAggregate>(accountId, ct: TestContext.Current.CancellationToken);

        // Assert
        aggTenantA.ShouldNotBeNull();
        aggTenantA.OwnerName.ShouldBe("Charlie");
        aggTenantB.ShouldBeNull();
    }

    [Fact]
    public async Task ExecuteBatchAsync_ThrowsArgumentException_OnNullOrWhiteSpaceIdOrPartitionKey()
    {
        // Arrange
        var storage = new InMemoryStorageProvider();

        var opInvalidId = new[]
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = "  ",
                PartitionKey = "pk1",
                DocType = "doc",
                Document = new object()
            }
        };

        var opInvalidPk = new[]
        {
            new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = "id1",
                PartitionKey = "",
                DocType = "doc",
                Document = new object()
            }
        };

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(() => storage.ExecuteBatchAsync(opInvalidId, TestContext.Current.CancellationToken));
        await Should.ThrowAsync<ArgumentException>(() => storage.ExecuteBatchAsync(opInvalidPk, TestContext.Current.CancellationToken));
    }
}
