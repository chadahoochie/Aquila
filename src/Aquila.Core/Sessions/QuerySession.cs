using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Queries;
using Aquila.Core.Serialization;
using Aquila.Core.Storage;

namespace Aquila.Core.Sessions;

public sealed class CoreEventStore : IEventStore
{
    private readonly IEventStorageProvider _storage;
    private readonly string _tenantId;
    private readonly UpcasterRegistry? _upcasters;
    private readonly List<IEvent> _uncommittedEvents = new();
    private readonly Dictionary<string, long> _streamExpectedVersions = new();
    private readonly ConcurrentDictionary<string, Type> _streamAggregateTypes = new();

    private static readonly ConcurrentDictionary<Type, Func<string, long, object, string, IEvent>> _envelopeFactories = new();
    private static readonly ConcurrentDictionary<(Type AggregateType, Type EventType), Action<object, object>?> _applyMethodCache = new();

    private readonly Func<(string? CorrelationId, string? CausationId, IReadOnlyDictionary<string, object> Headers)>? _headerProvider;

    public CoreEventStore(
        IEventStorageProvider storage,
        string tenantId,
        UpcasterRegistry? upcasters = null,
        Func<(string? CorrelationId, string? CausationId, IReadOnlyDictionary<string, object> Headers)>? headerProvider = null)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        _storage = storage;
        _tenantId = tenantId;
        _upcasters = upcasters;
        _headerProvider = headerProvider;
    }

    public CoreEventStore(
        IEventStorageProvider storage,
        StoreOptions options,
        string tenantId,
        Func<(string? CorrelationId, string? CausationId, IReadOnlyDictionary<string, object> Headers)>? headerProvider = null)
        : this(storage, tenantId, options?.Events?.Upcasters, headerProvider)
    {
    }

    public IReadOnlyList<IEvent> UncommittedEvents => _uncommittedEvents;
    public IReadOnlyDictionary<string, long> StreamExpectedVersions => _streamExpectedVersions;
    public IReadOnlyDictionary<string, Type> StreamAggregateTypes => _streamAggregateTypes;

    public void StartStream<TAggregate>(Guid streamId, params object[] events) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(events);
        StartStream<TAggregate>(streamId.ToString(), events);
    }

    public void StartStream<TAggregate>(string streamId, params object[] events) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);

        _streamAggregateTypes[streamId] = typeof(TAggregate);

        long version = 0;
        foreach (var evt in events)
        {
            ArgumentNullException.ThrowIfNull(evt);
            version++;
            var envelope = CreateEnvelope(evt.GetType(), streamId, version, evt, _tenantId);
            ApplyHeaders(envelope, evt);
            _uncommittedEvents.Add(envelope);
        }
    }

    public void Append(Guid streamId, params object[] events)
    {
        ArgumentNullException.ThrowIfNull(events);
        Append(streamId.ToString(), -1, events);
    }

    public void Append(string streamId, params object[] events)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);
        Append(streamId, -1, events);
    }

    public void Append(Guid streamId, long expectedVersion, params object[] events)
    {
        ArgumentNullException.ThrowIfNull(events);
        Append(streamId.ToString(), expectedVersion, events);
    }

    public void Append(string streamId, long expectedVersion, params object[] events)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);

        if (!_streamExpectedVersions.ContainsKey(streamId))
        {
            _streamExpectedVersions[streamId] = expectedVersion;
        }

        long version = expectedVersion > 0 ? expectedVersion : 0;
        foreach (var evt in events)
        {
            ArgumentNullException.ThrowIfNull(evt);
            version++;
            var envelope = CreateEnvelope(evt.GetType(), streamId, version, evt, _tenantId);
            ApplyHeaders(envelope, evt);
            _uncommittedEvents.Add(envelope);
        }
    }

    public void Append<TAggregate>(Guid streamId, params object[] events) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(events);
        Append<TAggregate>(streamId.ToString(), -1, events);
    }

    public void Append<TAggregate>(string streamId, params object[] events) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);
        _streamAggregateTypes[streamId] = typeof(TAggregate);
        Append(streamId, -1, events);
    }

    public void Append<TAggregate>(Guid streamId, long expectedVersion, params object[] events) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(events);
        Append<TAggregate>(streamId.ToString(), expectedVersion, events);
    }

    public void Append<TAggregate>(string streamId, long expectedVersion, params object[] events) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);
        _streamAggregateTypes[streamId] = typeof(TAggregate);
        Append(streamId, expectedVersion, events);
    }

    private void ApplyHeaders(IEvent envelope, object sourceEvt)
    {
        var existingEvt = sourceEvt as IEvent;
        if (_headerProvider != null)
        {
            var (correlationId, causationId, headers) = _headerProvider();
            envelope.CorrelationId = correlationId ?? existingEvt?.CorrelationId;
            envelope.CausationId = causationId ?? existingEvt?.CausationId;
            if (headers != null && headers.Count > 0)
            {
                var combinedHeaders = new Dictionary<string, object>(headers);
                if (existingEvt?.Headers != null)
                {
                    foreach (var kvp in existingEvt.Headers)
                    {
                        if (!combinedHeaders.ContainsKey(kvp.Key))
                        {
                            combinedHeaders[kvp.Key] = kvp.Value;
                        }
                    }
                }
                envelope.Headers = new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(combinedHeaders);
            }
            else if (existingEvt?.Headers != null && existingEvt.Headers.Count > 0)
            {
                envelope.Headers = existingEvt.Headers;
            }
        }
        else if (existingEvt != null)
        {
            envelope.CorrelationId = existingEvt.CorrelationId;
            envelope.CausationId = existingEvt.CausationId;
            if (existingEvt.Headers != null && existingEvt.Headers.Count > 0)
            {
                envelope.Headers = existingEvt.Headers;
            }
        }
    }

    public Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long fromVersion = 0, CancellationToken ct = default)
    {
        return FetchStreamAsync(streamId.ToString(), fromVersion, ct);
    }

    public async Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamId, long fromVersion = 0, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        var events = await _storage.FetchEventsAsync(streamId, _tenantId, fromVersion, ct);
        if (_upcasters == null || _upcasters.IsEmpty)
        {
            return events;
        }

        var upcastEvents = new List<IEvent>(events.Count);
        foreach (var evt in events)
        {
            upcastEvents.Add(_upcasters.Upcast(evt));
        }
        return upcastEvents;
    }

    public async Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, CancellationToken ct = default)
    {
        var events = await _storage.FetchGlobalEventsAsync(fromGlobalSequence, batchSize, _tenantId, ct);
        if (_upcasters == null || _upcasters.IsEmpty)
        {
            return events;
        }

        var upcastEvents = new List<IEvent>(events.Count);
        foreach (var evt in events)
        {
            upcastEvents.Add(_upcasters.Upcast(evt));
        }
        return upcastEvents;
    }

    public Task<TAggregate?> AggregateStreamAsync<TAggregate>(Guid streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new()
    {
        return AggregateStreamAsync<TAggregate>(streamId.ToString(), version, ct);
    }

    public async Task<TAggregate?> AggregateStreamAsync<TAggregate>(string streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        (TAggregate? snapshot, long snapshotVersion) = await _storage.GetSnapshotAsync<TAggregate>(streamId, _tenantId, ct);

        TAggregate? aggregate;
        long fromVersion;

        if (snapshot != null && snapshotVersion > 0 && (version == 0 || snapshotVersion <= version))
        {
            aggregate = snapshot;
            fromVersion = snapshotVersion + 1;
        }
        else
        {
            aggregate = null;
            fromVersion = 0;
        }

        var events = await FetchStreamAsync(streamId, fromVersion, ct);
        if (aggregate == null)
        {
            if (events.Count == 0) return null;
            aggregate = new TAggregate();
        }

        foreach (var @evt in events)
        {
            if (version > 0 && @evt.Version > version) break;
            ApplyEventToAggregate(aggregate, @evt);
        }

        return aggregate;
    }

    public void ClearUncommittedEvents()
    {
        _uncommittedEvents.Clear();
        _streamExpectedVersions.Clear();
    }

    private static IEvent CreateEnvelope(Type eventType, string streamId, long version, object data, string tenantId)
    {
        var factory = _envelopeFactories.GetOrAdd(eventType, t =>
        {
            var streamIdParam = Expression.Parameter(typeof(string), "streamId");
            var versionParam = Expression.Parameter(typeof(long), "version");
            var dataParam = Expression.Parameter(typeof(object), "data");
            var tenantIdParam = Expression.Parameter(typeof(string), "tenantId");

            var envelopeType = typeof(EventEnvelope<>).MakeGenericType(t);
            var ctor = Expression.New(envelopeType);
            var envelopeVar = Expression.Variable(envelopeType, "env");

            var idProp = envelopeType.GetProperty("Id")!;
            var streamIdProp = envelopeType.GetProperty("StreamId")!;
            var versionProp = envelopeType.GetProperty("Version")!;
            var sequenceProp = envelopeType.GetProperty("Sequence")!;
            var eventTypeProp = envelopeType.GetProperty("EventType")!;
            var dataProp = envelopeType.GetProperty("Data")!;
            var tenantIdProp = envelopeType.GetProperty("TenantId")!;

            var newGuidCall = Expression.Call(typeof(Guid), nameof(Guid.NewGuid), Type.EmptyTypes);
            var castData = Expression.Convert(dataParam, t);
            var eventTypeConst = Expression.Constant(t.FullName ?? t.Name);

            var block = Expression.Block(
                new[] { envelopeVar },
                Expression.Assign(envelopeVar, ctor),
                Expression.Call(envelopeVar, idProp.SetMethod!, newGuidCall),
                Expression.Call(envelopeVar, streamIdProp.SetMethod!, streamIdParam),
                Expression.Call(envelopeVar, versionProp.SetMethod!, versionParam),
                Expression.Call(envelopeVar, sequenceProp.SetMethod!, versionParam),
                Expression.Call(envelopeVar, eventTypeProp.SetMethod!, eventTypeConst),
                Expression.Call(envelopeVar, dataProp.SetMethod!, castData),
                Expression.Call(envelopeVar, tenantIdProp.SetMethod!, tenantIdParam),
                Expression.Convert(envelopeVar, typeof(IEvent))
            );

            return Expression.Lambda<Func<string, long, object, string, IEvent>>(
                block, streamIdParam, versionParam, dataParam, tenantIdParam).Compile();
        });

        return factory(streamId, version, data, tenantId);
    }

    internal static void ApplyEventToAggregate(object aggregate, object eventData)
    {
        ArgumentNullException.ThrowIfNull(aggregate);
        ArgumentNullException.ThrowIfNull(eventData);

        string eventTypeName = string.Empty;
        object payload = eventData;

        if (eventData is IEvent ie)
        {
            eventTypeName = ie.EventType;
            payload = ie.Data;
        }

        var aggType = aggregate.GetType();
        var evtType = payload.GetType();

        var directKey = (aggType, evtType);
        var directInvoker = _applyMethodCache.GetOrAdd(directKey, k =>
        {
            var method = k.AggregateType.GetMethod("Apply", new[] { k.EventType });
            if (method == null) return null;

            var aggParam = Expression.Parameter(typeof(object), "agg");
            var evtParam = Expression.Parameter(typeof(object), "evt");

            var castAgg = Expression.Convert(aggParam, k.AggregateType);
            var castEvt = Expression.Convert(evtParam, k.EventType);
            var call = Expression.Call(castAgg, method, castEvt);

            return Expression.Lambda<Action<object, object>>(call, aggParam, evtParam).Compile();
        });

        if (directInvoker != null)
        {
            directInvoker.Invoke(aggregate, payload);
            return;
        }

        var applyMethods = aggType.GetMethods()
            .Where(m => m.Name == "Apply" && m.GetParameters().Length == 1)
            .ToList();

        foreach (var m in applyMethods)
        {
            var targetParamType = m.GetParameters()[0].ParameterType;
            if (!string.IsNullOrEmpty(eventTypeName) &&
                (targetParamType.Name == eventTypeName || targetParamType.FullName == eventTypeName || targetParamType.AssemblyQualifiedName == eventTypeName))
            {
                if (payload is Newtonsoft.Json.Linq.JObject jobj)
                {
                    var deserialized = jobj.ToObject(targetParamType, JsonSerializer.Create(PrivateConstructorContractResolver.Settings));
                    if (deserialized != null)
                    {
                        m.Invoke(aggregate, new[] { deserialized });
                        return;
                    }
                }
                else
                {
                    var json = payload.ToString();
                    if (!string.IsNullOrEmpty(json))
                    {
                        var deserialized = JsonConvert.DeserializeObject(json, targetParamType, PrivateConstructorContractResolver.Settings);
                        if (deserialized != null)
                        {
                            m.Invoke(aggregate, new[] { deserialized });
                            return;
                        }
                    }
                }
            }
        }
    }
}

public abstract class QuerySessionBase : IQuerySession
{
    public IDocumentStorageProvider DocumentStorage { get; }
    public IEventStorageProvider EventStorage { get; }
    protected readonly StoreOptions Options;
    protected readonly CoreEventStore EventStore;
    protected readonly IIdentityMap InnerIdentityMap;

    public string TenantId { get; }
    public TrackingMode TrackingMode { get; }
    public IIdentityMap IdentityMap => InnerIdentityMap;
    internal StoreOptions StoreOptions => Options;

    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }

    private Dictionary<string, object>? _headers;
    public IReadOnlyDictionary<string, object> Headers =>
        _headers != null
            ? new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(_headers)
            : System.Collections.ObjectModel.ReadOnlyDictionary<string, object>.Empty;

    public void SetHeader(string key, object value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);

        _headers ??= new Dictionary<string, object>();
        _headers[key] = value;
    }

    protected QuerySessionBase(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, StoreOptions options, TrackingMode trackingMode = TrackingMode.DirtyTracking, string? tenantId = null)
    {
        ArgumentNullException.ThrowIfNull(documentStorage);
        ArgumentNullException.ThrowIfNull(eventStorage);
        ArgumentNullException.ThrowIfNull(options);
        if (tenantId != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        }

        DocumentStorage = documentStorage;
        EventStorage = eventStorage;
        Options = options;
        TrackingMode = trackingMode;
        TenantId = tenantId ?? options.DefaultTenantId;
        EventStore = new CoreEventStore(eventStorage, options, TenantId, () => (CorrelationId, CausationId, Headers));
        InnerIdentityMap = trackingMode == TrackingMode.Lightweight ? NoIdentityMap.Instance : new IdentityMap();
    }

    protected QuerySessionBase(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, StoreOptions options, string? tenantId)
        : this(documentStorage, eventStorage, options, TrackingMode.DirtyTracking, tenantId)
    {
    }

    public IEventStore Events => EventStore;

    public Task<T?> LoadAsync<T>(Guid id, string? partitionKey = null, CancellationToken ct = default) where T : class
    {
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }
        return LoadAsync<T>(id.ToString(), partitionKey, ct);
    }

    public async Task<T?> LoadAsync<T>(string id, string? partitionKey = null, CancellationToken ct = default) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (partitionKey != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        }

        if (TrackingMode != TrackingMode.Lightweight && InnerIdentityMap.TryGet<T>(id, out var cachedEntity))
        {
            return cachedEntity;
        }

        var pk = partitionKey ?? typeof(T).Name;
        var envelope = await DocumentStorage.ReadDocumentAsync<T>(id, pk, ct);
        if (envelope == null || envelope.IsDeleted) return null;
        if (envelope.TenantId != TenantId) return null;

        if (TrackingMode == TrackingMode.Lightweight)
        {
            return SnapshotDocument(envelope.Data);
        }

        var (loadedData, snapshotBytes) = CloneAndSnapshotDocument(envelope.Data);
        byte[]? snapshot = TrackingMode == TrackingMode.DirtyTracking ? snapshotBytes : null;
        InnerIdentityMap.Track(id, loadedData, envelope, snapshot);
        return loadedData;
    }

    public async Task<IReadOnlyList<T>> LoadManyAsync<T>(IEnumerable<string> ids, CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(ids);
        var idList = ids.ToList();
        foreach (var id in idList)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(id);
        }

        var results = new List<T>(idList.Count);
        var missingIds = new List<string>();

        if (TrackingMode != TrackingMode.Lightweight)
        {
            foreach (var id in idList)
            {
                if (InnerIdentityMap.TryGet<T>(id, out var cached))
                {
                    results.Add(cached!);
                }
                else
                {
                    missingIds.Add(id);
                }
            }
        }
        else
        {
            missingIds.AddRange(idList);
        }

        if (missingIds.Count > 0)
        {
            var idSet = missingIds.ToHashSet();

            var envelopes = await DocumentStorage.QueryDocumentsAsync<T>(
                x => x.TenantId == TenantId && idSet.Contains(x.Id),
                null,
                ct);

            foreach (var envelope in envelopes)
            {
                if (TrackingMode == TrackingMode.Lightweight)
                {
                    results.Add(SnapshotDocument(envelope.Data));
                }
                else
                {
                    var (loadedData, snapshotBytes) = CloneAndSnapshotDocument(envelope.Data);
                    byte[]? snapshot = TrackingMode == TrackingMode.DirtyTracking ? snapshotBytes : null;
                    InnerIdentityMap.Track(envelope.Id, loadedData, envelope, snapshot);
                    results.Add(loadedData);
                }
            }
        }

        return results;
    }

    [Obsolete("Use QueryAsync<T>() to avoid sync-over-async thread pool starvation.")]
    public IQueryable<T> Query<T>() where T : class
    {
        throw new NotSupportedException("Synchronous Query<T>() is disabled to prevent sync-over-async thread pool starvation. Use QueryAsync<T>() instead.");
    }

    public Task<IReadOnlyList<T>> QueryAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, CancellationToken ct = default) where T : class
    {
        return QueryAsync<T>(predicate, (QueryOptions?)null, ct);
    }

    public Task<IReadOnlyList<T>> QueryAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        Expression<Func<DocumentEnvelope<T>, object?>> orderBy,
        SortOrder sortOrder = SortOrder.Ascending,
        CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(orderBy);
        var options = new QueryOptions();
        options.OrderBy(orderBy, sortOrder);
        return QueryAsync<T>(predicate, options, ct);
    }

    public Task<IReadOnlyList<T>> QueryAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        IEnumerable<SortOrderDefinition<T>> orderings,
        CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(orderings);
        var options = new QueryOptions();
        foreach (var ordering in orderings)
        {
            if (ordering != null)
            {
                options.Orderings.Add(ordering.ToDescriptor());
            }
        }
        return QueryAsync<T>(predicate, options, ct);
    }

    public async Task<IReadOnlyList<T>> QueryAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        QueryOptions? options,
        CancellationToken ct = default) where T : class
    {
        var fullPredicate = CombineWithTenantId(predicate);
        var envelopes = await DocumentStorage.QueryDocumentsAsync(fullPredicate, options, ct).ConfigureAwait(false);
        return TrackAndUnwrap(envelopes);
    }

    private IReadOnlyList<T> TrackAndUnwrap<T>(IEnumerable<DocumentEnvelope<T>> envelopes) where T : class
    {
        var results = new List<T>();
        foreach (var envelope in envelopes)
        {
            if (TrackingMode == TrackingMode.Lightweight)
            {
                results.Add(SnapshotDocument(envelope.Data));
            }
            else if (InnerIdentityMap.TryGet<T>(envelope.Id, out var cached))
            {
                results.Add(cached!);
            }
            else
            {
                var (loadedData, snapshotBytes) = CloneAndSnapshotDocument(envelope.Data);
                byte[]? snapshot = TrackingMode == TrackingMode.DirtyTracking ? snapshotBytes : null;
                InnerIdentityMap.Track(envelope.Id, loadedData, envelope, snapshot);
                results.Add(loadedData);
            }
        }

        return results;
    }

    // Performance Optimization: Cache combined tenant-filtered LINQ expression lambdas per (Type, TenantId, Predicate)
    // to avoid allocating ParameterExpression, BinaryExpression, ParameterReplaceVisitor, and LambdaExpression trees per query.
    private static readonly ConcurrentDictionary<(Type DocType, string? TenantId, object? Predicate), object> _tenantPredicateCache = new();

    private Expression<Func<DocumentEnvelope<T>, bool>> CombineWithTenantId<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate)
    {
        return (Expression<Func<DocumentEnvelope<T>, bool>>)_tenantPredicateCache.GetOrAdd((typeof(T), TenantId, predicate), _ =>
        {
            var param = Expression.Parameter(typeof(DocumentEnvelope<T>), "x");
            var tenantCheck = Expression.Equal(
                Expression.Property(param, nameof(DocumentEnvelope<T>.TenantId)),
                Expression.Constant(TenantId));

            if (predicate == null)
            {
                return Expression.Lambda<Func<DocumentEnvelope<T>, bool>>(tenantCheck, param);
            }

            var visitor = new ParameterReplaceVisitor(predicate.Parameters[0], param);
            var rewrittenBody = visitor.Visit(predicate.Body);

            var combined = Expression.AndAlso(tenantCheck, rewrittenBody);
            return Expression.Lambda<Func<DocumentEnvelope<T>, bool>>(combined, param);
        });
    }

    private sealed class ParameterReplaceVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _from;
        private readonly ParameterExpression _to;

        public ParameterReplaceVisitor(ParameterExpression from, ParameterExpression to)
        {
            _from = from;
            _to = to;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            return node == _from ? _to : base.VisitParameter(node);
        }
    }

    public async Task<TResult> QueryAsync<TDoc, TResult>(ICompiledQuery<TDoc, TResult> query, CancellationToken ct = default) where TDoc : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var documents = await QueryAsync((Expression<Func<DocumentEnvelope<TDoc>, bool>>?)null, ct);
        var queryable = documents.AsQueryable();

        return CompiledQueryCache.Execute(queryable, query);
    }

    public Task<PagedResult<T>> QueryPagedAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        int pageSize = 20,
        string? continuationToken = null,
        string? partitionKey = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var options = new QueryOptions
        {
            PartitionKey = partitionKey,
            MaxItemCount = pageSize,
            ContinuationToken = string.IsNullOrWhiteSpace(continuationToken) ? null : continuationToken
        };

        return QueryPagedAsync<T>(predicate, options, ct);
    }

    public Task<PagedResult<T>> QueryPagedAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        Expression<Func<DocumentEnvelope<T>, object?>> orderBy,
        SortOrder sortOrder = SortOrder.Ascending,
        int pageSize = 20,
        string? continuationToken = null,
        string? partitionKey = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(orderBy);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var options = new QueryOptions
        {
            PartitionKey = partitionKey,
            MaxItemCount = pageSize,
            ContinuationToken = string.IsNullOrWhiteSpace(continuationToken) ? null : continuationToken
        };
        options.OrderBy(orderBy, sortOrder);

        return QueryPagedAsync<T>(predicate, options, ct);
    }

    public Task<PagedResult<T>> QueryPagedAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        IEnumerable<SortOrderDefinition<T>> orderings,
        int pageSize = 20,
        string? continuationToken = null,
        string? partitionKey = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(orderings);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var options = new QueryOptions
        {
            PartitionKey = partitionKey,
            MaxItemCount = pageSize,
            ContinuationToken = string.IsNullOrWhiteSpace(continuationToken) ? null : continuationToken
        };
        foreach (var ordering in orderings)
        {
            if (ordering != null)
            {
                options.Orderings.Add(ordering.ToDescriptor());
            }
        }

        return QueryPagedAsync<T>(predicate, options, ct);
    }

    public async Task<PagedResult<T>> QueryPagedAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        QueryOptions options,
        CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);

        var fullPredicate = CombineWithTenantId(predicate);
        var result = await DocumentStorage.QueryPagedDocumentsAsync(fullPredicate, options, ct).ConfigureAwait(false);
        var unwrappedItems = TrackAndUnwrap(result.Documents);

        int pageSize = options.MaxItemCount ?? unwrappedItems.Count;
        if (options.Skip.HasValue)
        {
            int pageNumber = pageSize > 0 ? (options.Skip.Value / pageSize) + 1 : 1;
            return new PagedResult<T>(unwrappedItems, pageNumber, pageSize, result.TotalCount);
        }

        return new PagedResult<T>(unwrappedItems, result.ContinuationToken, pageSize)
        {
            TotalCount = result.TotalCount
        };
    }

    public Task<PagedResult<T>> QueryPagedByOffsetAsync<T>(
        int pageNumber,
        int pageSize,
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        string? partitionKey = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageNumber);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        int skip = (pageNumber - 1) * pageSize;
        var options = new QueryOptions
        {
            PartitionKey = partitionKey,
            MaxItemCount = pageSize,
            Skip = skip
        };

        return QueryPagedAsync<T>(predicate, options, ct);
    }

    public Task<PagedResult<T>> QueryPagedByOffsetAsync<T>(
        int pageNumber,
        int pageSize,
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        Expression<Func<DocumentEnvelope<T>, object?>> orderBy,
        SortOrder sortOrder = SortOrder.Ascending,
        string? partitionKey = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageNumber);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        ArgumentNullException.ThrowIfNull(orderBy);

        int skip = (pageNumber - 1) * pageSize;
        var options = new QueryOptions
        {
            PartitionKey = partitionKey,
            MaxItemCount = pageSize,
            Skip = skip
        };
        options.OrderBy(orderBy, sortOrder);

        return QueryPagedAsync<T>(predicate, options, ct);
    }

    public Task<PagedResult<T>> QueryPagedByOffsetAsync<T>(
        int pageNumber,
        int pageSize,
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        IEnumerable<SortOrderDefinition<T>> orderings,
        string? partitionKey = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageNumber);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        ArgumentNullException.ThrowIfNull(orderings);

        int skip = (pageNumber - 1) * pageSize;
        var options = new QueryOptions
        {
            PartitionKey = partitionKey,
            MaxItemCount = pageSize,
            Skip = skip
        };
        foreach (var ordering in orderings)
        {
            if (ordering != null)
            {
                options.Orderings.Add(ordering.ToDescriptor());
            }
        }

        return QueryPagedAsync<T>(predicate, options, ct);
    }

    public async Task<PagedResult<TDoc>> QueryPagedAsync<TDoc>(
        ICompiledPagedQuery<TDoc> query,
        CancellationToken ct = default) where TDoc : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(query.PageSize);

        var predicate = CompiledQueryCache.ExtractPredicate(query);
        var orderings = CompiledQueryCache.ExtractOrderings(query);

        var options = new QueryOptions
        {
            PartitionKey = query.PartitionKey,
            MaxItemCount = query.PageSize,
            ContinuationToken = string.IsNullOrWhiteSpace(query.ContinuationToken) ? null : query.ContinuationToken
        };

        if (orderings != null)
        {
            foreach (var ord in orderings)
            {
                options.Orderings.Add(ord);
            }
        }

        return await QueryPagedAsync(predicate, options, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<PagedResult<T>> StreamPagesAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        string? partitionKey = null,
        int pageSize = 100,
        string? initialContinuationToken = null,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        string? currentToken = string.IsNullOrWhiteSpace(initialContinuationToken) ? null : initialContinuationToken;
        bool isFirstPage = true;

        while ((isFirstPage || !string.IsNullOrWhiteSpace(currentToken)) && !ct.IsCancellationRequested)
        {
            isFirstPage = false;
            var page = await QueryPagedAsync<T>(predicate, pageSize, currentToken, partitionKey, ct).ConfigureAwait(false);
            yield return page;
            currentToken = page.ContinuationToken;
        }
    }

    public async IAsyncEnumerable<PagedResult<T>> StreamPagesAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        Expression<Func<DocumentEnvelope<T>, object?>> orderBy,
        SortOrder sortOrder = SortOrder.Ascending,
        string? partitionKey = null,
        int pageSize = 100,
        string? initialContinuationToken = null,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(orderBy);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        string? currentToken = string.IsNullOrWhiteSpace(initialContinuationToken) ? null : initialContinuationToken;
        bool isFirstPage = true;

        while ((isFirstPage || !string.IsNullOrWhiteSpace(currentToken)) && !ct.IsCancellationRequested)
        {
            isFirstPage = false;
            var page = await QueryPagedAsync<T>(predicate, orderBy, sortOrder, pageSize, currentToken, partitionKey, ct).ConfigureAwait(false);
            yield return page;
            currentToken = page.ContinuationToken;
        }
    }

    public async IAsyncEnumerable<PagedResult<T>> StreamPagesAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        IEnumerable<SortOrderDefinition<T>> orderings,
        string? partitionKey = null,
        int pageSize = 100,
        string? initialContinuationToken = null,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(orderings);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        string? currentToken = string.IsNullOrWhiteSpace(initialContinuationToken) ? null : initialContinuationToken;
        bool isFirstPage = true;

        while ((isFirstPage || !string.IsNullOrWhiteSpace(currentToken)) && !ct.IsCancellationRequested)
        {
            isFirstPage = false;
            var page = await QueryPagedAsync<T>(predicate, orderings, pageSize, currentToken, partitionKey, ct).ConfigureAwait(false);
            yield return page;
            currentToken = page.ContinuationToken;
        }
    }

    public async IAsyncEnumerable<T> StreamAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null,
        string? partitionKey = null,
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        await foreach (var page in StreamPagesAsync<T>(predicate, partitionKey, batchSize, null, ct).ConfigureAwait(false))
        {
            foreach (var item in page.Items)
            {
                ct.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    public async IAsyncEnumerable<T> StreamAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        Expression<Func<DocumentEnvelope<T>, object?>> orderBy,
        SortOrder sortOrder = SortOrder.Ascending,
        string? partitionKey = null,
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        await foreach (var page in StreamPagesAsync<T>(predicate, orderBy, sortOrder, partitionKey, batchSize, null, ct).ConfigureAwait(false))
        {
            foreach (var item in page.Items)
            {
                ct.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    public async IAsyncEnumerable<T> StreamAsync<T>(
        Expression<Func<DocumentEnvelope<T>, bool>>? predicate,
        IEnumerable<SortOrderDefinition<T>> orderings,
        string? partitionKey = null,
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        await foreach (var page in StreamPagesAsync<T>(predicate, orderings, partitionKey, batchSize, null, ct).ConfigureAwait(false))
        {
            foreach (var item in page.Items)
            {
                ct.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    public Task<TDoc?> LiveStreamAsync<TDoc>(Guid streamId, CancellationToken ct = default) where TDoc : class, new()
    {
        return LiveStreamAsync<TDoc>(streamId.ToString(), null, ct);
    }

    public Task<TDoc?> LiveStreamAsync<TDoc>(Guid streamId, string? tenantId, CancellationToken ct = default) where TDoc : class, new()
    {
        return LiveStreamAsync<TDoc>(streamId.ToString(), tenantId, ct);
    }

    public Task<TDoc?> LiveStreamAsync<TDoc>(string streamId, CancellationToken ct = default) where TDoc : class, new()
    {
        return LiveStreamAsync<TDoc>(streamId, null, ct);
    }

    public async Task<TDoc?> LiveStreamAsync<TDoc>(string streamId, string? tenantId, CancellationToken ct = default) where TDoc : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var targetTenant = tenantId ?? TenantId;
        var events = await EventStorage.FetchEventsAsync(streamId, targetTenant, 0, ct);
        if (events == null || events.Count == 0)
        {
            return null;
        }

        var doc = new TDoc();
        var projection = Options.Projections.ForType(typeof(TDoc));

        if (projection != null)
        {
            foreach (var @evt in events)
            {
                projection.ApplyEvent(@evt, doc);
            }
        }
        else
        {
            foreach (var @evt in events)
            {
                CoreEventStore.ApplyEventToAggregate(doc, @evt);
            }
        }

        return doc;
    }

    // Performance Optimization: Use UTF-8 byte serialization directly instead of UTF-16 JSON strings
    // to reduce memory allocations and eliminate intermediate string object creation.
    protected static T SnapshotDocument<T>(T document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);
        var type = document.GetType();
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(document, type);
        return (T)System.Text.Json.JsonSerializer.Deserialize(bytes, type)!;
    }

    // Performance Optimization: Produce a fresh cloned entity and its UTF-8 snapshot bytes in a single pass,
    // allowing the DirtyTracking pipeline to reuse the serialized bytes without re-serializing.
    protected static (T ClonedData, byte[] SnapshotBytes) CloneAndSnapshotDocument<T>(T document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);
        var type = document.GetType();
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(document, type);
        var clone = (T)System.Text.Json.JsonSerializer.Deserialize(bytes, type)!;
        return (clone, bytes);
    }

    public void Clear()
    {
        InnerIdentityMap.Clear();
    }

    public void Dispose()
    {
        InnerIdentityMap.Clear();
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        InnerIdentityMap.Clear();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}

public sealed class QuerySession : QuerySessionBase
{
    public QuerySession(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, StoreOptions options, string? tenantId = null)
        : base(documentStorage, eventStorage, options, TrackingMode.DirtyTracking, tenantId)
    {
    }

    public QuerySession(IDocumentStorageProvider documentStorage, IEventStorageProvider eventStorage, StoreOptions options, TrackingMode trackingMode, string? tenantId = null)
        : base(documentStorage, eventStorage, options, trackingMode, tenantId)
    {
    }
}
