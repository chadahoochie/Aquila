using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Aquila.Core.Events;

public sealed class UpcasterRegistry
{
    private readonly ConcurrentDictionary<Type, IEventUpcaster> _upcasters = new();
    // Performance Optimization: Cache pre-composed upcaster pipeline delegates keyed by source payload type
    // to eliminate repetitive dictionary lookups and loop dispatch overhead during chained upcasting (e.g. V1 -> V2 -> V3).
    private readonly ConcurrentDictionary<Type, Func<object, (object FinalData, bool HasChanged)>> _pipelineCache = new();
    private static readonly ConcurrentDictionary<Type, Func<IEvent, object, IEvent>> _envelopeUpcastFactories = new();

    public bool IsEmpty => _upcasters.IsEmpty;

    public void Register(IEventUpcaster upcaster)
    {
        ArgumentNullException.ThrowIfNull(upcaster);
        _upcasters[upcaster.SourceType] = upcaster;
        // Invalidate cached composed pipelines on new upcaster registration
        _pipelineCache.Clear();
    }

    public void Register<TUpcaster>() where TUpcaster : IEventUpcaster, new()
    {
        Register(new TUpcaster());
    }

    public IEvent Upcast(IEvent @event)
    {
        if (@event == null || @event.Data == null || _upcasters.IsEmpty)
        {
            return @event!;
        }

        var currentData = @event.Data;
        var currentType = currentData.GetType();

        // Retrieve or compile the linear upcast execution delegate for the event payload type
        var pipeline = _pipelineCache.GetOrAdd(currentType, BuildPipeline);
        var (finalData, hasChanged) = pipeline(currentData);

        if (!hasChanged)
        {
            return @event;
        }

        return CreateUpcastEnvelope(@event, finalData);
    }

    /// <summary>
    /// Traverses the type-migration graph and pre-assembles a single callable pipeline delegate for the source type.
    /// </summary>
    private Func<object, (object FinalData, bool HasChanged)> BuildPipeline(Type sourceType)
    {
        var steps = new List<IEventUpcaster>();
        var curr = sourceType;
        var visited = new HashSet<Type>();

        // Follow the transformation chain from source type to the latest target type
        while (curr != null && _upcasters.TryGetValue(curr, out var upcaster) && visited.Add(curr))
        {
            steps.Add(upcaster);
            curr = upcaster.TargetType;
        }

        if (steps.Count == 0)
        {
            return static data => (data, false);
        }

        // Fast-path: single step migration without array allocation or loop overhead
        if (steps.Count == 1)
        {
            var single = steps[0];
            return data =>
            {
                var result = single.Upcast(data);
                if (result == null)
                {
                    throw new InvalidOperationException($"Upcaster '{single.GetType().FullName}' returned null when upcasting source type '{data.GetType().FullName}'.");
                }
                return (result, true);
            };
        }

        // Multi-step chained migration (e.g. V1 -> V2 -> V3) using pre-baked step array
        var stepArray = steps.ToArray();
        return data =>
        {
            var cur = data;
            for (int i = 0; i < stepArray.Length; i++)
            {
                var u = stepArray[i];
                var next = u.Upcast(cur);
                if (next == null)
                {
                    throw new InvalidOperationException($"Upcaster '{u.GetType().FullName}' returned null when upcasting source type '{cur.GetType().FullName}'.");
                }
                cur = next;
            }
            return (cur, true);
        };
    }

    private static IEvent CreateUpcastEnvelope(IEvent originalEvent, object newPayload)
    {
        var targetType = newPayload.GetType();
        var factory = _envelopeUpcastFactories.GetOrAdd(targetType, t =>
        {
            var origParam = Expression.Parameter(typeof(IEvent), "orig");
            var payloadParam = Expression.Parameter(typeof(object), "payload");

            var envelopeType = typeof(EventEnvelope<>).MakeGenericType(t);
            var ctor = Expression.New(envelopeType);
            var envVar = Expression.Variable(envelopeType, "env");

            var idProp = envelopeType.GetProperty(nameof(IEvent.Id))!;
            var streamIdProp = envelopeType.GetProperty(nameof(IEvent.StreamId))!;
            var versionProp = envelopeType.GetProperty(nameof(IEvent.Version))!;
            var sequenceProp = envelopeType.GetProperty(nameof(IEvent.Sequence))!;
            var globalSequenceProp = envelopeType.GetProperty(nameof(IEvent.GlobalSequence))!;
            var timestampProp = envelopeType.GetProperty(nameof(IEvent.Timestamp))!;
            var eventTypeProp = envelopeType.GetProperty(nameof(IEvent.EventType))!;
            var dataProp = envelopeType.GetProperty(nameof(IEvent.Data))!;
            var tenantIdProp = envelopeType.GetProperty(nameof(IEvent.TenantId))!;
            var correlationIdProp = envelopeType.GetProperty(nameof(IEvent.CorrelationId))!;
            var causationIdProp = envelopeType.GetProperty(nameof(IEvent.CausationId))!;
            var headersProp = envelopeType.GetProperty(nameof(IEvent.Headers))!;

            var castPayload = Expression.Convert(payloadParam, t);
            var eventTypeConst = Expression.Constant(t.FullName ?? t.Name);

            var block = Expression.Block(
                new[] { envVar },
                Expression.Assign(envVar, ctor),
                Expression.Call(envVar, idProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.Id))),
                Expression.Call(envVar, streamIdProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.StreamId))),
                Expression.Call(envVar, versionProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.Version))),
                Expression.Call(envVar, sequenceProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.Sequence))),
                Expression.Call(envVar, globalSequenceProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.GlobalSequence))),
                Expression.Call(envVar, timestampProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.Timestamp))),
                Expression.Call(envVar, eventTypeProp.SetMethod!, eventTypeConst),
                Expression.Call(envVar, dataProp.SetMethod!, castPayload),
                Expression.Call(envVar, tenantIdProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.TenantId))),
                Expression.Call(envVar, correlationIdProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.CorrelationId))),
                Expression.Call(envVar, causationIdProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.CausationId))),
                Expression.Call(envVar, headersProp.SetMethod!, Expression.Property(origParam, nameof(IEvent.Headers))),
                Expression.Convert(envVar, typeof(IEvent))
            );

            return Expression.Lambda<Func<IEvent, object, IEvent>>(block, origParam, payloadParam).Compile();
        });

        return factory(originalEvent, newPayload);
    }
}
