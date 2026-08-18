using Aquila.Core.Abstractions;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;

namespace Aquila.Core.Projections;

/// <summary>
/// Non-generic interface for multi-stream projections used internally by session execution.
/// </summary>
public interface IMultiStreamProjection : IProjection
{
    Type ReadModelType { get; }
    object? GetIdentity(IEvent @event);
    Task ProcessEventAsync(DocumentSession session, IEvent @event, CancellationToken ct);
}

/// <summary>
/// Abstract base class for projections that aggregate events across multiple streams into a single read model document.
/// </summary>
/// <typeparam name="TDoc">The read model document type.</typeparam>
/// <typeparam name="TId">The identity type used to address target read model documents.</typeparam>
public abstract class MultiStreamProjection<TDoc, TId> : IMultiStreamProjection
    where TDoc : class, new()
    where TId : notnull
{
    public ProjectionLifecycle Lifecycle { get; set; } = ProjectionLifecycle.Inline;
    public Type ReadModelType => typeof(TDoc);
    public Type AggregateType => typeof(TDoc);
    public string Name => GetType().Name;

    protected abstract TId Identity(IEvent @event);

    public object? GetIdentity(IEvent @event) => Identity(@event);

    public abstract bool Apply(IEvent @event, TDoc document);

    public void ApplyEvent(IEvent @event, object aggregate)
    {
        ArgumentNullException.ThrowIfNull(@event);
        ArgumentNullException.ThrowIfNull(aggregate);

        if (aggregate is TDoc doc)
        {
            Apply(@event, doc);
        }
    }

    public virtual async Task ProcessEventAsync(DocumentSession session, IEvent @event, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(@event);

        var id = Identity(@event);
        if (id == null) return;

        var docId = id.ToString();
        if (string.IsNullOrWhiteSpace(docId)) return;

        var mapping = session.StoreOptions.Schema.For<TDoc>();
        var doc = await session.LoadAsync<TDoc>(docId, ct: ct) ?? new TDoc();

        bool keep = Apply(@event, doc);
        var pk = mapping.PartitionKeySelector(doc);
        if (string.IsNullOrWhiteSpace(pk))
        {
            pk = typeof(TDoc).Name;
        }

        if (keep)
        {
            var envelope = new DocumentEnvelope<TDoc>
            {
                Id = docId,
                PartitionKey = pk,
                DocType = typeof(TDoc).Name,
                TenantId = session.TenantId,
                IsDeleted = false,
                Data = doc
            };

            await session.ProjectionStorage.UpsertDocumentAsync(envelope, ct).ConfigureAwait(false);
            // Performance Optimization: Bypass IdentityMap tracking overhead in lightweight projection sessions
            if (session.TrackingMode != TrackingMode.Lightweight)
            {
                session.IdentityMap.Track(docId, doc, envelope);
            }
        }
        else
        {
            await session.ProjectionStorage.DeleteDocumentAsync<TDoc>(docId, pk, ct).ConfigureAwait(false);
            // Performance Optimization: Bypass IdentityMap untracking overhead in lightweight projection sessions
            if (session.TrackingMode != TrackingMode.Lightweight)
            {
                session.IdentityMap.Untrack<TDoc>(docId);
            }
        }
    }

    public virtual async Task DispatchBatchAsync(IDocumentStore documentStore, IReadOnlyList<IEvent> events, int maxConcurrency, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(documentStore);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0) return;

        var groups = new Dictionary<string, List<IEvent>>();
        for (int i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            var idObj = GetIdentity(evt);
            var identity = idObj?.ToString();
            if (string.IsNullOrWhiteSpace(identity)) continue;

            if (!groups.TryGetValue(identity, out var list))
            {
                list = new List<IEvent>();
                groups[identity] = list;
            }
            list.Add(evt);
        }

        if (groups.Count == 0) return;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, maxConcurrency),
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(groups, parallelOptions, async (group, token) =>
        {
            using var session = (DocumentSession)documentStore.OpenSession();
            var streamEvents = group.Value;
            streamEvents.Sort(static (a, b) => a.GlobalSequence.CompareTo(b.GlobalSequence));

            for (int i = 0; i < streamEvents.Count; i++)
            {
                await ProcessEventAsync(session, streamEvents[i], token).ConfigureAwait(false);
            }

            await session.SaveChangesAsync(token).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }
}
