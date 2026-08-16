using Aquila.Core.Abstractions;
using Aquila.Core.Events;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Dispatches event batches to a projection concurrently across distinct aggregate/document identities
/// while strictly preserving intra-identity GlobalSequence ordering.
/// </summary>
public static class BoundedParallelEventDispatcher
{
    /// <summary>
    /// Dispatches a batch of events to the specified projection using bounded parallelism grouped by target identity.
    /// </summary>
    public static Task DispatchAsync(
        IDocumentStore documentStore,
        IProjection projection,
        IReadOnlyList<IEvent> events,
        int maxConcurrency,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(documentStore);
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0) return Task.CompletedTask;

        var degreeOfParallelism = Math.Max(1, maxConcurrency);
        return projection.DispatchBatchAsync(documentStore, events, degreeOfParallelism, ct);
    }
}
