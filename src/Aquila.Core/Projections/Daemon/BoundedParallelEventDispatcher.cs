using System.Collections.Concurrent;
using System.Reflection;
using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Dispatches event batches to a projection concurrently across distinct aggregate/document identities
/// while strictly preserving intra-identity GlobalSequence ordering.
/// </summary>
public static class BoundedParallelEventDispatcher
{
    private static readonly ConcurrentDictionary<Type, MethodInfo> _singleStreamMethodCache = new();

    /// <summary>
    /// Dispatches a batch of events to the specified projection using bounded parallelism grouped by target identity.
    /// </summary>
    public static async Task DispatchAsync(
        IDocumentStore documentStore,
        IProjection projection,
        IReadOnlyList<IEvent> events,
        int maxConcurrency,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(documentStore);
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0) return;

        var degreeOfParallelism = Math.Max(1, maxConcurrency);

        if (projection is IMultiStreamProjection multiProj)
        {
            await DispatchMultiStreamAsync(documentStore, multiProj, events, degreeOfParallelism, ct).ConfigureAwait(false);
        }
        else
        {
            var method = _singleStreamMethodCache.GetOrAdd(projection.AggregateType, t =>
                typeof(BoundedParallelEventDispatcher).GetMethod(nameof(DispatchSingleStreamAsync), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(t));

            var task = (Task)method.Invoke(null, new object[] { documentStore, projection, events, degreeOfParallelism, ct })!;
            await task.ConfigureAwait(false);
        }
    }

    private static async Task DispatchMultiStreamAsync(
        IDocumentStore documentStore,
        IMultiStreamProjection multiProj,
        IReadOnlyList<IEvent> events,
        int maxConcurrency,
        CancellationToken ct)
    {
        var groups = events
            .GroupBy(e => multiProj.GetIdentity(e)?.ToString())
            .Where(g => !string.IsNullOrWhiteSpace(g.Key))
            .ToList();

        if (groups.Count == 0) return;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxConcurrency,
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(groups, parallelOptions, async (group, token) =>
        {
            using var session = (DocumentSession)documentStore.OpenSession();
            var orderedEvents = group.OrderBy(e => e.GlobalSequence);

            foreach (var evt in orderedEvents)
            {
                await multiProj.ProcessEventAsync(session, evt, token).ConfigureAwait(false);
            }

            await session.SaveChangesAsync(token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    private static async Task DispatchSingleStreamAsync<TAggregate>(
        IDocumentStore documentStore,
        IProjection projection,
        IReadOnlyList<IEvent> events,
        int maxConcurrency,
        CancellationToken ct) where TAggregate : class
    {
        var groups = events
            .GroupBy(e => e.StreamId)
            .Where(g => !string.IsNullOrWhiteSpace(g.Key))
            .ToList();

        if (groups.Count == 0) return;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxConcurrency,
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(groups, parallelOptions, async (group, token) =>
        {
            using var session = (DocumentSession)documentStore.OpenSession();
            var streamId = group.Key!;
            var existingAggregate = await session.LoadAsync<TAggregate>(streamId, streamId, token).ConfigureAwait(false)
                                     ?? (TAggregate)Activator.CreateInstance(typeof(TAggregate))!;

            var orderedEvents = group.OrderBy(e => e.GlobalSequence);
            foreach (var evt in orderedEvents)
            {
                projection.ApplyEvent(evt, existingAggregate);
            }

            var envelope = new DocumentEnvelope<TAggregate>
            {
                Id = streamId,
                PartitionKey = streamId,
                DocType = typeof(TAggregate).Name,
                TenantId = session.TenantId,
                IsDeleted = false,
                Data = existingAggregate
            };

            await documentStore.Options.DocumentStorage.UpsertDocumentAsync(envelope, token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }
}
