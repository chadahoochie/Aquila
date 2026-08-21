using System.Collections.Concurrent;
using System.Collections.Frozen;
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

    // A single default provider backs all three SPI roles. Distinct instances would make the
    // store look polyglot to Freeze() and would leave a caller who overrides only DocumentStorage
    // with an event store and a projection store pointed at other, empty instances.
    private readonly InMemoryStorageProvider _defaultStorage = new();
    private IDocumentStorageProvider _documentStorage;
    private IEventStorageProvider _eventStorage;
    private IProjectionStorageProvider _projectionStorage;
    private bool _projectionStorageConfigured;
    private FrozenSet<Type>? _projectionReadModelTypes;
    private bool _isFrozen;

    public StoreOptions()
    {
        _documentStorage = _defaultStorage;
        _eventStorage = _defaultStorage;
        _projectionStorage = _defaultStorage;
    }

    public bool IsReadOnly => _isFrozen;

    public void Freeze()
    {
        if (_isFrozen) return;

        // 1. Precompute immutable projection type registry for O(1) routing
        var types = new HashSet<Type>();
        foreach (var proj in Projections.Projections)
        {
            if (proj is IMultiStreamProjection multi)
            {
                types.Add(multi.ReadModelType);
            }
            types.Add(proj.AggregateType);
        }
        _projectionReadModelTypes = types.ToFrozenSet();

        // 2. Fail-Fast Polyglot Inline Validation
        if (IsPolyglot)
        {
            foreach (var proj in Projections.Projections)
            {
                if (proj.Lifecycle == ProjectionLifecycle.Inline)
                {
                    throw new InvalidOperationException(
                        $"Projection '{proj.Name}' is registered with ProjectionLifecycle.Inline, but ProjectionStorage ({ProjectionStorage.ProviderName}) " +
                        $"and EventStorage ({EventStorage.ProviderName}) are different physical providers. " +
                        "Polyglot projections must use ProjectionLifecycle.Async or ProjectionLifecycle.Live to prevent distributed partial-failure dual writes without 2PC.");
                }
            }
        }

        _isFrozen = true;
    }

    /// <summary>
    /// True when projection read models and events live in different physical backends, so an
    /// <see cref="ProjectionLifecycle.Inline"/> projection would have to dual-write across stores
    /// without a distributed transaction.
    /// </summary>
    /// <remarks>
    /// Compared by <c>ProviderName</c> rather than reference identity. A composite provider serving
    /// several roles is one object, but the segregated extensions (<c>UseCosmosDocuments</c> +
    /// <c>UseCosmosEvents</c>) produce distinct provider instances over the same Cosmos account —
    /// reference inequality would reject that valid configuration at startup. The trade is a false
    /// negative for two accounts of the same provider family; a false positive is worse, since it
    /// blocks a working configuration outright.
    /// </remarks>
    public bool IsPolyglot
    {
        get
        {
            var projectionStorage = EffectiveProjectionStorage;
            if (ReferenceEquals(projectionStorage, _eventStorage)) return false;

            return !string.Equals(projectionStorage.ProviderName, _eventStorage.ProviderName, StringComparison.Ordinal);
        }
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
            _documentStorage = value ?? throw new ArgumentNullException(nameof(value));
        }
    }

    public IEventStorageProvider EventStorage
    {
        get => _eventStorage;
        set
        {
            AssertNotFrozen();
            _eventStorage = value ?? throw new ArgumentNullException(nameof(value));
        }
    }

    /// <summary>
    /// Storage for materialized projection read models. When not explicitly configured, projections
    /// live alongside documents in <see cref="DocumentStorage"/> — a store is only polyglot because
    /// the caller made it so, never by omission.
    /// </summary>
    public IProjectionStorageProvider ProjectionStorage
    {
        get
        {
            if (_projectionStorageConfigured) return _projectionStorage;
            return _documentStorage as IProjectionStorageProvider ?? _projectionStorage;
        }
        set
        {
            AssertNotFrozen();
            _projectionStorage = value ?? throw new ArgumentNullException(nameof(value));
            _projectionStorageConfigured = true;
        }
    }

    /// <summary>
    /// The provider that read models are actually routed to, as an <see cref="IDocumentStorageProvider"/>.
    /// Differs from <see cref="ProjectionStorage"/> only when projection storage was left unconfigured and
    /// <see cref="DocumentStorage"/> does not implement <see cref="IProjectionStorageProvider"/> — in which
    /// case read models still belong with the documents.
    /// </summary>
    private IDocumentStorageProvider EffectiveProjectionStorage =>
        _projectionStorageConfigured ? _projectionStorage : _documentStorage;

    /// <summary>
    /// True when <paramref name="type"/> is a registered projection read model and therefore
    /// belongs in <see cref="ProjectionStorage"/> rather than <see cref="DocumentStorage"/>.
    /// </summary>
    /// <remarks>
    /// The routing registry is built by <see cref="Freeze"/>. Querying before then cannot return a
    /// meaningful answer, and silently answering <c>false</c> would route every read model to the
    /// document store while the projection writers targeted the projection store — so this throws
    /// instead of guessing.
    /// </remarks>
    public bool IsProjectionReadModel(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        var registry = _projectionReadModelTypes
            ?? throw new InvalidOperationException(
                "Storage routing was queried before StoreOptions.Freeze() ran, so the projection read-model " +
                "registry does not exist yet. Freeze() is called by the DocumentStore constructor and by " +
                "AddAquila(); if you construct StoreOptions directly, call Freeze() once configuration is complete.");

        return registry.Contains(type);
    }

    /// <summary>
    /// Resolves the storage provider that owns <paramref name="type"/>. This is the single routing
    /// decision point — sessions, projections and the daemon all resolve through it, so a read and a
    /// write of the same type can never disagree about which store holds it.
    /// </summary>
    public IDocumentStorageProvider GetStorageFor(Type type) =>
        IsProjectionReadModel(type) ? EffectiveProjectionStorage : _documentStorage;

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
        if (documentStorage is IProjectionStorageProvider projStorage)
        {
            ProjectionStorage = projStorage;
        }
    }

    public void UseStorageProvider(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, IProjectionStorageProvider projectionStorage)
    {
        AssertNotFrozen();
        ArgumentNullException.ThrowIfNull(documentStorage);
        ArgumentNullException.ThrowIfNull(eventStorage);
        ArgumentNullException.ThrowIfNull(projectionStorage);
        DocumentStorage = documentStorage;
        EventStorage = eventStorage;
        ProjectionStorage = projectionStorage;
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
        if (provider is IProjectionStorageProvider projStorage)
        {
            ProjectionStorage = projStorage;
        }
    }

    public void UseInMemoryStorage()
    {
        AssertNotFrozen();
        var provider = new InMemoryStorageProvider();
        DocumentStorage = provider;
        EventStorage = provider;
        ProjectionStorage = provider;
    }
}
