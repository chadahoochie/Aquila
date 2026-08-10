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

            await session.DocumentStorage.UpsertDocumentAsync(envelope, ct);
            session.IdentityMap.Track(docId, doc, envelope);
        }
        else
        {
            await session.DocumentStorage.DeleteDocumentAsync<TDoc>(docId, pk, ct);
            session.IdentityMap.Untrack<TDoc>(docId);
        }
    }
}
