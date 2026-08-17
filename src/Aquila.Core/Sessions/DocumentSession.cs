using System.Reflection;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Patching;
using Aquila.Core.Projections;
using Aquila.Core.Storage;

namespace Aquila.Core.Sessions;

public sealed class DocumentSession : QuerySessionBase, IDocumentSession
{
    private readonly List<StorageOperation> _pendingOperations = new();
    private readonly List<Func<CancellationToken, Task>> _pendingDeferredOperations = new();

    public DocumentSession(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, StoreOptions options, TrackingMode trackingMode = TrackingMode.DirtyTracking, string? tenantId = null)
        : base(documentStorage, eventStorage, options, trackingMode, tenantId)
    {
    }

    public DocumentSession(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, StoreOptions options, string? tenantId)
        : base(documentStorage, eventStorage, options, TrackingMode.DirtyTracking, tenantId)
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

        // Performance Optimization: Clone document to isolate from external mutations and reuse the generated
        // UTF-8 byte snapshot for DirtyTracking, eliminating a duplicate serialization pass on Track().
        var (snapshot, snapshotBytes) = CloneAndSnapshotDocument(document);

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

        if (TrackingMode != TrackingMode.Lightweight)
        {
            byte[]? recordSnapshot = TrackingMode == TrackingMode.DirtyTracking ? snapshotBytes : null;
            InnerIdentityMap.Track(id, document, envelope, recordSnapshot);
        }
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

    public IPatchExpression<T> Patch<T>(string id, string? partitionKey = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }

        var pk = partitionKey ?? typeof(T).Name;
        var expr = new PatchExpression<T>();
        _pendingOperations.Add(new StorageOperation
        {
            OperationType = StorageOperationType.Patch,
            Id = id,
            PartitionKey = pk,
            DocType = typeof(T).Name,
            PatchOperations = expr.Operations
        });

        return expr;
    }

    public async Task SaveChangesAsync(CancellationToken ct = default)
    {
        // 0. Execute deferred operations (e.g. non-blocking soft deletes)
        foreach (var deferredOp in _pendingDeferredOperations)
        {
            await deferredOp(ct);
        }
        _pendingDeferredOperations.Clear();

        // 0.5 Automatic Dirty Checking for TrackingMode.DirtyTracking
        if (TrackingMode == TrackingMode.DirtyTracking)
        {
            DetectAndQueueDirtyEntities();
        }

        // 1. Flush uncommitted events to storage provider
        var uncommittedEvents = EventStore.UncommittedEvents.ToList();
        if (uncommittedEvents.Count > 0)
        {
            var groupedByStream = uncommittedEvents.GroupBy(e => e.StreamId);
            foreach (var group in groupedByStream)
            {
                var expectedVersion = EventStore.StreamExpectedVersions.TryGetValue(group.Key, out var exp) ? exp : -1;
                await EventStorage.AppendEventsAsync(group.Key, group, expectedVersion, ct);

                if (EventStore.StreamAggregateTypes.TryGetValue(group.Key, out var aggType))
                {
                    await CheckAndPersistAutoSnapshotAsync(group.Key, aggType, ct).ConfigureAwait(false);
                }
            }
        }

        // 2. Flush pending storage operations
        if (_pendingOperations.Count > 0)
        {
            await DocumentStorage.ExecuteBatchAsync(_pendingOperations.ToList(), ct);
        }

        // 3. Process inline projections
        var inlineProjections = Options.Projections.Projections
            .Where(p => p.Lifecycle == ProjectionLifecycle.Inline)
            .ToList();

        foreach (var proj in inlineProjections)
        {
            if (proj is IMultiStreamProjection multiProj)
            {
                foreach (var @evt in uncommittedEvents)
                {
                    await multiProj.ProcessEventAsync(this, @evt, ct);
                }
            }
            else
            {
                foreach (var @evt in uncommittedEvents)
                {
                    await ProcessSingleStreamInlineEventAsync(proj, @evt, ct);
                }
            }
        }

        _pendingOperations.Clear();
        EventStore.ClearUncommittedEvents();
    }

    private async Task ProcessSingleStreamInlineEventAsync(IProjection proj, IEvent @evt, CancellationToken ct)
    {
        var aggregateId = @evt.StreamId;
        var loadMethod = typeof(IQuerySession)
            .GetMethods()
            .First(m => m.Name == nameof(LoadAsync) && m.IsGenericMethod && m.GetParameters().Length == 3 && m.GetParameters()[0].ParameterType == typeof(string))
            .MakeGenericMethod(proj.AggregateType);

        var loadTask = (Task)loadMethod.Invoke(this, new object?[] { aggregateId, aggregateId, ct })!;
        await loadTask.ConfigureAwait(false);

        var resultProp = loadTask.GetType().GetProperty("Result")!;
        var existingAggregate = resultProp.GetValue(loadTask) ?? Activator.CreateInstance(proj.AggregateType)!;

        proj.ApplyEvent(@evt, existingAggregate);

        var envelopeType = typeof(DocumentEnvelope<>).MakeGenericType(proj.AggregateType);
        var envelope = Activator.CreateInstance(envelopeType)!;
        envelopeType.GetProperty("Id")!.SetValue(envelope, aggregateId);
        envelopeType.GetProperty("PartitionKey")!.SetValue(envelope, aggregateId);
        envelopeType.GetProperty("DocType")!.SetValue(envelope, proj.AggregateType.Name);
        envelopeType.GetProperty("TenantId")!.SetValue(envelope, TenantId);
        envelopeType.GetProperty("IsDeleted")!.SetValue(envelope, false);
        envelopeType.GetProperty("Data")!.SetValue(envelope, existingAggregate);

        var upsertMethod = typeof(IDocumentStorageProvider)
            .GetMethod(nameof(IDocumentStorageProvider.UpsertDocumentAsync))!
            .MakeGenericMethod(proj.AggregateType);

        var upsertTask = (Task)upsertMethod.Invoke(DocumentStorage, new object[] { envelope, ct })!;
        await upsertTask.ConfigureAwait(false);

        if (TrackingMode != TrackingMode.Lightweight)
        {
            var trackMethod = typeof(IIdentityMap)
                .GetMethods()
                .First(m => m.Name == nameof(IIdentityMap.Track) && m.GetParameters().Length == 3)
                .MakeGenericMethod(proj.AggregateType);
            trackMethod.Invoke(InnerIdentityMap, new[] { aggregateId, existingAggregate, envelope });
        }
    }

    private async Task CheckAndPersistAutoSnapshotAsync(string streamId, Type aggregateType, CancellationToken ct)
    {
        var strategy = Options.Events.GetSnapshotStrategy(aggregateType);
        if (strategy == null) return;

        var header = await EventStorage.GetStreamHeaderAsync(streamId, TenantId, ct).ConfigureAwait(false);
        if (header == null || header.Version <= 0) return;
        long currentVersion = header.Version;

        var getSnapshotMethod = typeof(IEventStorageProvider)
            .GetMethod(nameof(IEventStorageProvider.GetSnapshotAsync))!
            .MakeGenericMethod(aggregateType);

        var task = (Task)getSnapshotMethod.Invoke(EventStorage, new object[] { streamId, TenantId, ct })!;
        await task.ConfigureAwait(false);

        var resultProperty = task.GetType().GetProperty("Result")!;
        var tuple = resultProperty.GetValue(task)!;
        var snapshotVersionField = tuple.GetType().GetField("SnapshotVersion") ?? tuple.GetType().GetField("Item2");
        long lastSnapshotVersion = (long)(snapshotVersionField?.GetValue(tuple) ?? 0L);

        int eventsSinceLastSnapshot = (int)(currentVersion - lastSnapshotVersion);

        var shouldSnapshotMethod = strategy.GetType().GetMethod(nameof(ISnapshotStrategy<object>.ShouldSnapshot))!;
        bool shouldSnapshot = (bool)shouldSnapshotMethod.Invoke(strategy, new object[] { currentVersion, eventsSinceLastSnapshot })!;

        if (shouldSnapshot)
        {
            var aggStreamMethod = typeof(IEventStore)
                .GetMethods()
                .First(m => m.Name == nameof(IEventStore.AggregateStreamAsync) && m.GetParameters().Length == 3 && m.GetParameters()[0].ParameterType == typeof(string))
                .MakeGenericMethod(aggregateType);

            var rehydrateTask = (Task)aggStreamMethod.Invoke(EventStore, new object?[] { streamId, currentVersion, ct })!;
            await rehydrateTask.ConfigureAwait(false);

            var rehydrateResultProp = rehydrateTask.GetType().GetProperty("Result")!;
            var aggregateInstance = rehydrateResultProp.GetValue(rehydrateTask);

            if (aggregateInstance != null)
            {
                var saveSnapshotMethod = typeof(IEventStorageProvider)
                    .GetMethod(nameof(IEventStorageProvider.SaveSnapshotAsync))!
                    .MakeGenericMethod(aggregateType);

                var saveTask = (Task)saveSnapshotMethod.Invoke(EventStorage, new object[] { streamId, currentVersion, aggregateInstance, TenantId, ct })!;
                await saveTask.ConfigureAwait(false);
            }
        }
    }

    private void DetectAndQueueDirtyEntities()
    {
        var trackedEntities = InnerIdentityMap.GetTrackedEntities();
        foreach (var tracked in trackedEntities)
        {
            if (tracked.Snapshot == null) continue;

            var currentBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(tracked.Entity, tracked.EntityType);

            if (!currentBytes.AsSpan().SequenceEqual(tracked.Snapshot.AsSpan()))
            {
                bool alreadyPending = _pendingOperations.Any(op => op.Id == tracked.Id && op.DocType == tracked.EntityType.Name);
                if (!alreadyPending)
                {
                    var (pk, envelopeObj) = CreateEnvelopeForEntity(tracked);
                    _pendingOperations.Add(new StorageOperation
                    {
                        OperationType = StorageOperationType.Upsert,
                        Id = tracked.Id,
                        PartitionKey = pk,
                        DocType = tracked.EntityType.Name,
                        Document = envelopeObj
                    });
                }

                InnerIdentityMap.UpdateSnapshot(tracked.EntityType, tracked.Id, currentBytes);
            }
        }
    }

    // Performance Optimization: Cache generic MethodInfo per entity type to eliminate reflection lookup
    // overhead during dirty entity change detection on SaveChangesAsync.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, MethodInfo> _createEnvelopeMethodCache = new();

    private (string PartitionKey, object EnvelopeObject) CreateEnvelopeForEntity(TrackedEntity tracked)
    {
        var method = _createEnvelopeMethodCache.GetOrAdd(tracked.EntityType, static entityType =>
            typeof(DocumentSession).GetMethod(nameof(CreateEnvelopeGeneric), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(entityType));

        return ((string, object))method.Invoke(this, new[] { tracked.Id, tracked.Entity, tracked.Envelope })!;
    }

    private (string PartitionKey, object EnvelopeObject) CreateEnvelopeGeneric<T>(string id, T entity, object? existingEnvelopeObj) where T : class
    {
        var mapping = Options.Schema.For<T>();
        var pk = mapping.PartitionKeySelector(entity);
        var docType = typeof(T).Name;
        var snapshot = SnapshotDocument(entity);

        var existingEnv = existingEnvelopeObj as DocumentEnvelope<T>;

        var envelope = new DocumentEnvelope<T>
        {
            Id = id,
            PartitionKey = existingEnv?.PartitionKey ?? pk,
            DocType = docType,
            TenantId = TenantId,
            IsDeleted = false,
            Version = Guid.NewGuid().ToString(),
            Data = snapshot
        };

        return (envelope.PartitionKey, envelope);
    }
}
