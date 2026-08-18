using System.Collections.Concurrent;
using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Storage;

namespace Aquila.Core.Configuration;

public sealed class DocumentMapping<T> : IDocumentMappingInfo where T : class
{
    public Type DocumentType => typeof(T);
    public string DocTypeName => typeof(T).Name;
    public bool SoftDeletesEnabled => UseSoftDeletes;

    private static readonly Func<T, string> DefaultIdSelector = CompileDefaultIdSelector();

    private static Func<T, string> CompileDefaultIdSelector()
    {
        var prop = typeof(T).GetProperty("Id") ?? typeof(T).GetProperty("id");
        if (prop != null && prop.CanRead)
        {
            var param = Expression.Parameter(typeof(T), "doc");
            var propExpr = Expression.Property(param, prop);
            var toStringCall = Expression.Call(propExpr, typeof(object).GetMethod("ToString")!);
            var lambda = Expression.Lambda<Func<T, string>>(toStringCall, param);
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

    public string IdentityPropertyName { get; private set; } = GetDefaultIdentityPropertyName();
    public string PartitionKeyPropertyName { get; private set; } = string.Empty;

    private static string GetDefaultIdentityPropertyName()
    {
        var prop = typeof(T).GetProperty("Id") ?? typeof(T).GetProperty("id");
        return prop?.Name ?? "Id";
    }

    private static string? ExtractPropertyName(LambdaExpression expression)
    {
        if (expression == null) return null;
        var body = expression.Body;
        while (body is UnaryExpression unary && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            body = unary.Operand;
        }

        if (body is MemberExpression member)
        {
            return member.Member.Name;
        }

        return null;
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
        IdentityPropertyName = ExtractPropertyName(idProperty) ?? "Id";
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
        PartitionKeyPropertyName = ExtractPropertyName(partitionKeyProperty) ?? string.Empty;
        var compiled = partitionKeyProperty.Compile();
        PartitionKeySelector = doc =>
        {
            ArgumentNullException.ThrowIfNull(doc);
            return compiled(doc)?.ToString() ?? typeof(T).Name;
        };
        return this;
    }

    public DocumentMapping<T> UseIdentityAsPartitionKey()
    {
        PartitionKeyPropertyName = IdentityPropertyName;
        PartitionKeySelector = IdSelector;
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
    private readonly ConcurrentDictionary<Type, object> _mappings = new();

    public IReadOnlyDictionary<Type, object> Mappings => _mappings;

    /// <summary>
    /// When true, mappings without an explicit PartitionKey configuration default to using the document identity property (Id)
    /// as the partition key, ensuring uniform write distribution and avoiding the 20GB logical partition limit.
    /// </summary>
    public bool UseIdentityAsDefaultPartitionKey { get; set; }

    public DocumentMapping<T> For<T>() where T : class
    {
        return (DocumentMapping<T>)_mappings.GetOrAdd(typeof(T), _ =>
        {
            var mapping = new DocumentMapping<T>();
            if (UseIdentityAsDefaultPartitionKey)
            {
                mapping.UseIdentityAsPartitionKey();
            }
            return mapping;
        });
    }
}

public sealed class ProjectionRegistration
{
    private readonly List<IProjection> _projections = new();
    private readonly ConcurrentDictionary<Type, IProjection?> _typeCache = new();

    public IReadOnlyList<IProjection> Projections => _projections;

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
    private readonly ConcurrentDictionary<Type, object> _snapshotStrategies = new();

    public IReadOnlyDictionary<Type, object> SnapshotStrategies => _snapshotStrategies;

    public void RegisterUpcaster<TUpcaster>() where TUpcaster : IEventUpcaster, new()
    {
        Upcasters.Register<TUpcaster>();
    }

    public void RegisterUpcaster(IEventUpcaster upcaster)
    {
        ArgumentNullException.ThrowIfNull(upcaster);
        Upcasters.Register(upcaster);
    }

    public void SnapshotEvery<TAggregate>(int threshold) where TAggregate : class
    {
        if (threshold <= 0) throw new ArgumentOutOfRangeException(nameof(threshold), "Threshold must be greater than zero.");
        _snapshotStrategies[typeof(TAggregate)] = new DefaultSnapshotStrategy<TAggregate>(threshold);
    }

    public void RegisterSnapshotStrategy<TAggregate>(ISnapshotStrategy<TAggregate> strategy) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _snapshotStrategies[typeof(TAggregate)] = strategy;
    }

    public ISnapshotStrategy<TAggregate>? GetSnapshotStrategy<TAggregate>() where TAggregate : class
    {
        return _snapshotStrategies.TryGetValue(typeof(TAggregate), out var strategy)
            ? (ISnapshotStrategy<TAggregate>)strategy
            : null;
    }

    public object? GetSnapshotStrategy(Type aggregateType)
    {
        ArgumentNullException.ThrowIfNull(aggregateType);
        return _snapshotStrategies.TryGetValue(aggregateType, out var strategy) ? strategy : null;
    }
}

public sealed class StoreOptions
{
    private string _defaultTenantId = "default";
    private IDocumentStorageProvider _documentStorage = new InMemoryStorageProvider();
    private IEventStorageProvider _eventStorage = new InMemoryStorageProvider();
    private bool _isFrozen;

    public bool IsReadOnly => _isFrozen;

    public void Freeze()
    {
        _isFrozen = true;
    }

    private void AssertNotFrozen()
    {
        if (_isFrozen)
        {
            throw new InvalidOperationException("StoreOptions is frozen and cannot be modified after DocumentStore has been initialized.");
        }
    }

    public string DefaultTenantId
    {
        get => _defaultTenantId;
        set
        {
            AssertNotFrozen();
            _defaultTenantId = value;
        }
    }

    public IDocumentStorageProvider DocumentStorage
    {
        get => _documentStorage;
        set
        {
            AssertNotFrozen();
            _documentStorage = value;
        }
    }

    public IEventStorageProvider EventStorage
    {
        get => _eventStorage;
        set
        {
            AssertNotFrozen();
            _eventStorage = value;
        }
    }

    public SchemaPolicy Schema { get; } = new();
    public ProjectionRegistration Projections { get; } = new();
    public EventRegistration Events { get; } = new();

    public void UseStorageProvider(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage)
    {
        AssertNotFrozen();
        ArgumentNullException.ThrowIfNull(documentStorage);
        ArgumentNullException.ThrowIfNull(eventStorage);
        DocumentStorage = documentStorage;
        EventStorage = eventStorage;
    }

    public void UseStorageProvider(object provider)
    {
        AssertNotFrozen();
        ArgumentNullException.ThrowIfNull(provider);
        if (provider is IDocumentStorageProvider docStorage)
        {
            DocumentStorage = docStorage;
        }
        if (provider is IEventStorageProvider evtStorage)
        {
            EventStorage = evtStorage;
        }
    }

    public void UseInMemoryStorage()
    {
        AssertNotFrozen();
        var provider = new InMemoryStorageProvider();
        DocumentStorage = provider;
        EventStorage = provider;
    }
}
