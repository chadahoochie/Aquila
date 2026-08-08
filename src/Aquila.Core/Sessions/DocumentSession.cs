using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Core.Storage;

namespace Aquila.Core.Sessions;

public sealed class DocumentSession : QuerySessionBase, IDocumentSession
{
    private readonly List<StorageOperation> _pendingOperations = new();
    private readonly List<Func<CancellationToken, Task>> _pendingDeferredOperations = new();

    public DocumentSession(IAquilaStorageProvider storage, StoreOptions options, string? tenantId = null)
        : base(storage, options, tenantId)
    {
    }

    public void Store<T>(T document, string? partitionKey = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }

        var mapping = Options.Schema.For<T>();
        var id = mapping.IdSelector(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        var pk = partitionKey ?? mapping.PartitionKeySelector(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(pk);

        var docType = typeof(T).Name;

        // Snapshot document state upon Store<T>() to isolate from post-store object mutations
        var snapshot = SnapshotDocument(document);

        var envelope = new DocumentEnvelope<T>
        {
            Id = id,
            PartitionKey = pk,
            DocType = docType,
            TenantId = TenantId,
            IsDeleted = false,
            Version = Guid.NewGuid().ToString(),
            Data = snapshot
        };

        _pendingOperations.Add(new StorageOperation
        {
            OperationType = StorageOperationType.Upsert,
            Id = id,
            PartitionKey = pk,
            DocType = docType,
            Document = envelope
        });

        InnerIdentityMap.Track(id, document, envelope);
    }

    public void Store<T>(IEnumerable<T> documents) where T : class
    {
        ArgumentNullException.ThrowIfNull(documents);
        foreach (var doc in documents) Store(doc);
    }

    public void Delete<T>(T document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);

        var mapping = Options.Schema.For<T>();
        var id = mapping.IdSelector(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        var pk = mapping.PartitionKeySelector(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(pk);

        Delete<T>(id, pk);
    }

    public void Delete<T>(Guid id, string? partitionKey = null) where T : class
    {
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }
        Delete<T>(id.ToString(), partitionKey);
    }

    public void Delete<T>(string id, string? partitionKey = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }

        var pk = partitionKey ?? typeof(T).Name;
        _pendingOperations.Add(new StorageOperation
        {
            OperationType = StorageOperationType.Delete,
            Id = id,
            PartitionKey = pk,
            DocType = typeof(T).Name
        });
    }

    public void SoftDelete<T>(T document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);

        var mapping = Options.Schema.For<T>();
        var id = mapping.IdSelector(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        var pk = mapping.PartitionKeySelector(document);
        ArgumentException.ThrowIfNullOrWhiteSpace(pk);

        var docType = typeof(T).Name;
        var snapshot = SnapshotDocument(document);

        var envelope = new DocumentEnvelope<T>
        {
            Id = id,
            PartitionKey = pk,
            DocType = docType,
            TenantId = TenantId,
            IsDeleted = true,
            Version = Guid.NewGuid().ToString(),
            Data = snapshot
        };

        _pendingOperations.Add(new StorageOperation
        {
            OperationType = StorageOperationType.Upsert,
            Id = id,
            PartitionKey = pk,
            DocType = docType,
            Document = envelope
        });
    }

    public void SoftDelete<T>(string id, string? partitionKey = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }

        _pendingDeferredOperations.Add(ct => SoftDeleteAsync<T>(id, partitionKey, ct));
    }

    public Task SoftDeleteAsync<T>(T document, CancellationToken ct = default) where T : class
    {
        SoftDelete(document);
        return Task.CompletedTask;
    }

    public async Task SoftDeleteAsync<T>(string id, string? partitionKey = null, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }

        var pk = partitionKey ?? typeof(T).Name;
        var existing = await LoadAsync<T>(id, pk, ct);
        if (existing != null)
        {
            var envelope = new DocumentEnvelope<T>
            {
                Id = id,
                PartitionKey = pk,
                DocType = typeof(T).Name,
                TenantId = TenantId,
                IsDeleted = true,
                Data = existing
            };

            _pendingOperations.Add(new StorageOperation
            {
                OperationType = StorageOperationType.Upsert,
                Id = id,
                PartitionKey = pk,
                DocType = typeof(T).Name,
                Document = envelope
            });
        }
    }

    public async Task SaveChangesAsync(CancellationToken ct = default)
    {
        // 0. Execute deferred operations (e.g. non-blocking soft deletes)
        foreach (var deferredOp in _pendingDeferredOperations)
        {
            await deferredOp(ct);
        }
        _pendingDeferredOperations.Clear();

        // 1. Flush uncommitted events to storage provider
        var uncommittedEvents = EventStore.UncommittedEvents.ToList();
        if (uncommittedEvents.Count > 0)
        {
            var groupedByStream = uncommittedEvents.GroupBy(e => e.StreamId);
            foreach (var group in groupedByStream)
            {
                var expectedVersion = EventStore.StreamExpectedVersions.TryGetValue(group.Key, out var exp) ? exp : -1;
                await Storage.Events.AppendEventsAsync(group.Key, group, expectedVersion, ct);
            }
        }

        // 2. Flush pending storage operations
        if (_pendingOperations.Count > 0)
        {
            await Storage.Documents.ExecuteBatchAsync(_pendingOperations.ToList(), ct);
        }

        // 3. Process inline projections
        var inlineProjections = Options.Projections.Projections
            .Where(p => p.Lifecycle == ProjectionLifecycle.Inline)
            .ToList();

        foreach (var proj in inlineProjections)
        {
            foreach (var @evt in uncommittedEvents)
            {
                var aggregateId = @evt.StreamId;
                var existingAggregate = await LoadAsync<object>(aggregateId, aggregateId, ct) ?? Activator.CreateInstance(proj.AggregateType)!;
                proj.ApplyEvent(@evt, existingAggregate);

                var envelope = new DocumentEnvelope<object>
                {
                    Id = aggregateId,
                    PartitionKey = aggregateId,
                    DocType = proj.AggregateType.Name,
                    TenantId = TenantId,
                    IsDeleted = false,
                    Data = existingAggregate
                };

                await Storage.Documents.UpsertDocumentAsync(envelope, ct);
            }
        }

        _pendingOperations.Clear();
        EventStore.ClearUncommittedEvents();
    }

    private static T SnapshotDocument<T>(T document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);
        var type = document.GetType();
        var json = System.Text.Json.JsonSerializer.Serialize(document, type);
        return (T)System.Text.Json.JsonSerializer.Deserialize(json, type)!;
    }
}
