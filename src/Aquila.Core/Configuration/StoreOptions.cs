using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Storage;

namespace Aquila.Core.Configuration;

public sealed class DocumentMapping<T> where T : class
{
    private static readonly Func<T, string> DefaultIdSelector = CompileDefaultIdSelector();

    private static Func<T, string> CompileDefaultIdSelector()
    {
        var prop = typeof(T).GetProperty("Id") ?? typeof(T).GetProperty("id");
        if (prop != null && prop.CanRead)
        {
            var param = System.Linq.Expressions.Expression.Parameter(typeof(T), "doc");
            var propExpr = System.Linq.Expressions.Expression.Property(param, prop);
            var toStringCall = System.Linq.Expressions.Expression.Call(propExpr, typeof(object).GetMethod("ToString")!);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, string>>(toStringCall, param);
            var compiled = lambda.Compile();
            return doc =>
            {
                ArgumentNullException.ThrowIfNull(doc);
                return compiled(doc) ?? Guid.NewGuid().ToString();
            };
        }

        return doc =>
        {
            ArgumentNullException.ThrowIfNull(doc);
            return Guid.NewGuid().ToString();
        };
    }

    public Func<T, string> IdSelector { get; private set; } = DefaultIdSelector;

    public Func<T, string> PartitionKeySelector { get; private set; } = doc =>
    {
        ArgumentNullException.ThrowIfNull(doc);
        return typeof(T).Name;
    };

    public bool UseSoftDeletes { get; private set; }
    public bool OptimisticConcurrencyEnabled { get; private set; }

    public DocumentMapping<T> Identity(Expression<Func<T, object>> idProperty)
    {
        ArgumentNullException.ThrowIfNull(idProperty);
        var compiled = idProperty.Compile();
        IdSelector = doc =>
        {
            ArgumentNullException.ThrowIfNull(doc);
            return compiled(doc)?.ToString() ?? Guid.NewGuid().ToString();
        };
        return this;
    }

    public DocumentMapping<T> PartitionKey(Expression<Func<T, object>> partitionKeyProperty)
    {
        ArgumentNullException.ThrowIfNull(partitionKeyProperty);
        var compiled = partitionKeyProperty.Compile();
        PartitionKeySelector = doc =>
        {
            ArgumentNullException.ThrowIfNull(doc);
            return compiled(doc)?.ToString() ?? typeof(T).Name;
        };
        return this;
    }

    public DocumentMapping<T> SoftDeleted()
    {
        UseSoftDeletes = true;
        return this;
    }

    public DocumentMapping<T> UseOptimisticConcurrency(bool enabled = true)
    {
        OptimisticConcurrencyEnabled = enabled;
        return this;
    }
}

public sealed class SchemaPolicy
{
    private readonly Dictionary<Type, object> _mappings = new();

    public DocumentMapping<T> For<T>() where T : class
    {
        if (!_mappings.TryGetValue(typeof(T), out var mapping))
        {
            var newMapping = new DocumentMapping<T>();
            _mappings[typeof(T)] = newMapping;
            return newMapping;
        }

        return (DocumentMapping<T>)mapping;
    }
}

public sealed class ProjectionRegistration
{
    private readonly List<IProjection> _projections = new();
    private readonly ConcurrentDictionary<Type, IProjection?> _typeCache = new();

    public List<IProjection> Projections => _projections;

    public void Add<TProjection>(ProjectionLifecycle lifecycle = ProjectionLifecycle.Inline) where TProjection : IProjection, new()
    {
        var projection = new TProjection();
        projection.Lifecycle = lifecycle;
        Add(projection, lifecycle);
    }

    public void Add(IProjection projection, ProjectionLifecycle lifecycle = ProjectionLifecycle.Inline)
    {
        ArgumentNullException.ThrowIfNull(projection);
        projection.Lifecycle = lifecycle;
        _projections.Add(projection);
        _typeCache.Clear();
    }

    public IProjection? ForType(Type aggregateType)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        return _typeCache.GetOrAdd(aggregateType, t => _projections.FirstOrDefault(p => p.AggregateType == t));
    }
}

public sealed class EventRegistration
{
    public UpcasterRegistry Upcasters { get; } = new();

    public void RegisterUpcaster<TUpcaster>() where TUpcaster : IEventUpcaster, new()
    {
        Upcasters.Register<TUpcaster>();
    }

    public void RegisterUpcaster(IEventUpcaster upcaster)
    {
        ArgumentNullException.ThrowIfNull(upcaster);
        Upcasters.Register(upcaster);
    }
}

public sealed class StoreOptions
{
    public string DefaultTenantId { get; set; } = "default";
    public IAquilaStorageProvider StorageProvider { get; set; } = new InMemoryStorageProvider();

    public SchemaPolicy Schema { get; } = new();
    public ProjectionRegistration Projections { get; } = new();
    public EventRegistration Events { get; } = new();

    public void UseStorageProvider(IAquilaStorageProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        StorageProvider = provider;
    }

    public void UseInMemoryStorage()
    {
        StorageProvider = new InMemoryStorageProvider();
    }
}
