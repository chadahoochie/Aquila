using Aquila.Core.Events;

namespace Aquila.Core.Projections;

/// <summary>
/// Execution mode for projections.
/// </summary>
public enum ProjectionLifecycle
{
    /// <summary>
    /// Projection runs inline inside the session transaction during SaveChangesAsync.
    /// </summary>
    Inline,

    /// <summary>
    /// Projection runs asynchronously in the background via Change Feed Processor.
    /// </summary>
    Async,

    /// <summary>
    /// Projection is evaluated live on-the-fly during query read operations without persistence.
    /// </summary>
    Live
}

/// <summary>
/// Interface implemented by all projection definitions.
/// </summary>
public interface IProjection
{
    ProjectionLifecycle Lifecycle { get; set; }
    Type AggregateType { get; }
    string Name => GetType().Name;
    void ApplyEvent(IEvent @event, object aggregate);
}

/// <summary>
/// Single stream projection that transforms events from a single stream into a read-model document.
/// </summary>
public abstract class SingleStreamProjection<TAggregate> : IProjection where TAggregate : class, new()
{
    private readonly Dictionary<Type, Action<object, TAggregate>> _handlers = new();

    public ProjectionLifecycle Lifecycle { get; set; } = ProjectionLifecycle.Inline;
    public Type AggregateType => typeof(TAggregate);
    public string Name => GetType().Name;

    protected void CreateEvent<TEvent>(Func<TEvent, TAggregate> creator)
    {
        ArgumentNullException.ThrowIfNull(creator);
        _handlers[typeof(TEvent)] = (evt, aggregate) =>
        {
            var result = creator((TEvent)evt);
            CopyProperties(result, aggregate);
        };
    }

    protected void ProjectEvent<TEvent>(Action<TEvent, TAggregate> applier)
    {
        ArgumentNullException.ThrowIfNull(applier);
        _handlers[typeof(TEvent)] = (evt, aggregate) => applier((TEvent)evt, aggregate);
    }

    public void ApplyEvent(IEvent @event, object aggregate)
    {
        ArgumentNullException.ThrowIfNull(@event);
        ArgumentNullException.ThrowIfNull(aggregate);

        if (aggregate is not TAggregate typedAggregate) return;

        var eventData = @event.Data;
        if (eventData != null && _handlers.TryGetValue(eventData.GetType(), out var handler))
        {
            handler(eventData, typedAggregate);
        }
    }

    private static void CopyProperties(TAggregate source, TAggregate target)
    {
        PropertyCopier<TAggregate>.Copy(source, target);
    }

    private static class PropertyCopier<T>
    {
        public static readonly Action<T, T> Copy = CompileCopy();

        private static Action<T, T> CompileCopy()
        {
            var sourceParam = System.Linq.Expressions.Expression.Parameter(typeof(T), "source");
            var targetParam = System.Linq.Expressions.Expression.Parameter(typeof(T), "target");
            var expressions = new List<System.Linq.Expressions.Expression>();

            foreach (var prop in typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
            {
                if (prop.CanRead && prop.CanWrite)
                {
                    var getExpr = System.Linq.Expressions.Expression.Property(sourceParam, prop);
                    var setExpr = System.Linq.Expressions.Expression.Assign(System.Linq.Expressions.Expression.Property(targetParam, prop), getExpr);
                    expressions.Add(setExpr);
                }
            }

            if (expressions.Count == 0) return (_, _) => { };

            var block = System.Linq.Expressions.Expression.Block(expressions);
            return System.Linq.Expressions.Expression.Lambda<Action<T, T>>(block, sourceParam, targetParam).Compile();
        }
    }
}
