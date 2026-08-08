using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Cosmos.Storage;

namespace Aquila.Cosmos.Events;

public sealed class CosmosEventStore : IEventStore
{
    private readonly CoreEventStore _innerStore;

    public CosmosEventStore(IAquilaStorageProvider storageProvider, string tenantId)
    {
        ArgumentNullException.ThrowIfNull(storageProvider);
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        _innerStore = new CoreEventStore(storageProvider, tenantId);
    }

    public CosmosEventStore(Container container, string tenantId)
        : this(new CosmosStorageProvider(container.Database.Client), tenantId)
    {
    }

    public IReadOnlyList<IEvent> UncommittedEvents => _innerStore.UncommittedEvents;

    public void StartStream<TAggregate>(Guid streamId, params object[] events) where TAggregate : class =>
        _innerStore.StartStream<TAggregate>(streamId, events);

    public void StartStream<TAggregate>(string streamId, params object[] events) where TAggregate : class =>
        _innerStore.StartStream<TAggregate>(streamId, events);

    public void Append(Guid streamId, params object[] events) =>
        _innerStore.Append(streamId, events);

    public void Append(string streamId, params object[] events) =>
        _innerStore.Append(streamId, events);

    public void Append(Guid streamId, long expectedVersion, params object[] events) =>
        _innerStore.Append(streamId, expectedVersion, events);

    public void Append(string streamId, long expectedVersion, params object[] events) =>
        _innerStore.Append(streamId, expectedVersion, events);

    public Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long fromVersion = 0, CancellationToken ct = default) =>
        _innerStore.FetchStreamAsync(streamId, fromVersion, ct);

    public Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamId, long fromVersion = 0, CancellationToken ct = default) =>
        _innerStore.FetchStreamAsync(streamId, fromVersion, ct);

    public Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, CancellationToken ct = default) =>
        _innerStore.FetchGlobalEventsAsync(fromGlobalSequence, batchSize, ct);

    public Task<TAggregate?> AggregateStreamAsync<TAggregate>(Guid streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new() =>
        _innerStore.AggregateStreamAsync<TAggregate>(streamId, version, ct);

    public Task<TAggregate?> AggregateStreamAsync<TAggregate>(string streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new() =>
        _innerStore.AggregateStreamAsync<TAggregate>(streamId, version, ct);

    public void ClearUncommittedEvents() =>
        _innerStore.ClearUncommittedEvents();
}
