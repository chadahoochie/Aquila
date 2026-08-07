using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests;

public sealed record SecureDocument(string Id, string Content);
public sealed record SecureEvent(string StreamId, string SecretPayload);

public sealed class SecurityTests
{
    [Theory, AutoNSubstituteData]
    public async Task LoadAsync_Enforces_Tenant_Isolation(
        string docId)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var optionsTenantA = new StoreOptions { DefaultTenantId = "tenant-a", StorageProvider = provider };
        var optionsTenantB = new StoreOptions { DefaultTenantId = "tenant-b", StorageProvider = provider };

        using (var sessionA = new DocumentSession(provider, optionsTenantA, "tenant-a"))
        {
            sessionA.Store(new SecureDocument(docId, "Secret Tenant A Data"));
            await sessionA.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act - Attempt cross-tenant read from Tenant B context
        using var sessionB = new DocumentSession(provider, optionsTenantB, "tenant-b");
        var result = await sessionB.LoadAsync<SecureDocument>(docId, ct: TestContext.Current.CancellationToken);

        // Assert - Cross tenant document read MUST return null
        result.ShouldBeNull();
    }

    [Theory, AutoNSubstituteData]
    public async Task LoadManyAsync_Enforces_Tenant_Isolation(
        string docId1, string docId2)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var optionsTenantA = new StoreOptions { DefaultTenantId = "tenant-a", StorageProvider = provider };
        var optionsTenantB = new StoreOptions { DefaultTenantId = "tenant-b", StorageProvider = provider };

        using (var sessionA = new DocumentSession(provider, optionsTenantA, "tenant-a"))
        {
            sessionA.Store(new SecureDocument(docId1, "Secret A1"));
            sessionA.Store(new SecureDocument(docId2, "Secret A2"));
            await sessionA.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act - Tenant B attempts to load Tenant A's documents
        using var sessionB = new DocumentSession(provider, optionsTenantB, "tenant-b");
        var results = await sessionB.LoadManyAsync<SecureDocument>(new[] { docId1, docId2 }, ct: TestContext.Current.CancellationToken);

        // Assert
        results.ShouldBeEmpty();
    }

    [Theory, AutoNSubstituteData]
    public async Task EventStream_FetchEventsAsync_Enforces_Tenant_Isolation(
        string streamId)
    {
        // Arrange
        var provider = new InMemoryStorageProvider();
        var optionsTenantA = new StoreOptions { DefaultTenantId = "tenant-a", StorageProvider = provider };
        var optionsTenantB = new StoreOptions { DefaultTenantId = "tenant-b", StorageProvider = provider };

        using (var sessionA = new DocumentSession(provider, optionsTenantA, "tenant-a"))
        {
            sessionA.Events.StartStream<BankAccountAggregate>(streamId, new SecureEvent(streamId, "Confidential A"));
            await sessionA.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Act - Tenant B attempts to fetch Tenant A's stream
        using var sessionB = new DocumentSession(provider, optionsTenantB, "tenant-b");
        var eventsB = await sessionB.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);

        // Assert
        eventsB.ShouldBeEmpty();
    }

    [Theory, AutoNSubstituteData]
    public async Task BatchOperations_Sanitize_Input_Validation(
        string validId, string validPk)
    {
        var provider = new InMemoryStorageProvider();

        var invalidOp1 = new StorageOperation { OperationType = StorageOperationType.Upsert, Id = "", PartitionKey = validPk, DocType = "Test" };
        var invalidOp2 = new StorageOperation { OperationType = StorageOperationType.Upsert, Id = validId, PartitionKey = "   ", DocType = "Test" };

        await Should.ThrowAsync<ArgumentException>(() => provider.ExecuteBatchAsync(new[] { invalidOp1 }));
        await Should.ThrowAsync<ArgumentException>(() => provider.ExecuteBatchAsync(new[] { invalidOp2 }));
    }
}
