using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Queries;
using Aquila.Core.Storage;

namespace Aquila.Core.Sessions;

public sealed class CoreEventStore : IEventStore
{
    private readonly IAquilaStorageProvider _storage;
    private readonly string _tenantId;
    private readonly List<IEvent> _uncommittedEvents = new();
    private readonly Dictionary<string, long> _streamExpectedVersions = new();

    private static readonly ConcurrentDictionary<Type, Func<string, long, object, string, IEvent>> _envelopeFactories = new();
    private static readonly ConcurrentDictionary<(Type AggregateType, Type EventType), Action<object, object>?> _applyMethodCache = new();

    public CoreEventStore(IAquilaStorageProvider storage, string tenantId)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        _storage = storage;
        _tenantId = tenantId;
    }

    public IReadOnlyList<IEvent> UncommittedEvents => _uncommittedEvents;
    public IReadOnlyDictionary<string, long> StreamExpectedVersions => _streamExpectedVersions;

    public void StartStream<TAggregate>(Guid streamId, params object[] events) where TAggregate : class
    {
        ArgumentNullException.ThrowIfNull(events);
        StartStream<TAggregate>(streamId.ToString(), events);
    }

    public void StartStream<TAggregate>(string streamId, params object[] events) where TAggregate : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(events);

        long version = 0;
        foreach (var evt in events)
        {
            ArgumentNullException.ThrowIfNull(evt);
            version++;
            var envelope = CreateEnvelope(evt.GetType(), streamId, version, evt, _tenantId);
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
            _uncommittedEvents.Add(envelope);
        }
    }

    public Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long fromVersion = 0, CancellationToken ct = default)
    {
        return FetchStreamAsync(streamId.ToString(), fromVersion, ct);
    }

    public async Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamId, long fromVersion = 0, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        return await _storage.Events.FetchEventsAsync(streamId, _tenantId, fromVersion, ct);
    }

    public async Task<IReadOnlyList<IEvent>> FetchGlobalEventsAsync(long fromGlobalSequence, int batchSize = 1000, CancellationToken ct = default)
    {
        return await _storage.Events.FetchGlobalEventsAsync(fromGlobalSequence, batchSize, _tenantId, ct);
    }

    public Task<TAggregate?> AggregateStreamAsync<TAggregate>(Guid streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new()
    {
        return AggregateStreamAsync<TAggregate>(streamId.ToString(), version, ct);
    }

    public async Task<TAggregate?> AggregateStreamAsync<TAggregate>(string streamId, long version = 0, CancellationToken ct = default) where TAggregate : class, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        (TAggregate? snapshot, long snapshotVersion) = await _storage.Events.GetSnapshotAsync<TAggregate>(streamId, _tenantId, ct);

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
                    var deserialized = jobj.ToObject(targetParamType);
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
                        var deserialized = JsonConvert.DeserializeObject(json, targetParamType);
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
    protected readonly IAquilaStorageProvider Storage;
    protected readonly StoreOptions Options;
    protected readonly CoreEventStore EventStore;
    protected readonly IIdentityMap InnerIdentityMap;

    public string TenantId { get; }
    public TrackingMode TrackingMode { get; }
    public IIdentityMap IdentityMap => InnerIdentityMap;
    internal IAquilaStorageProvider StorageProvider => Storage;
    internal StoreOptions StoreOptions => Options;

    protected QuerySessionBase(IAquilaStorageProvider storage, StoreOptions options, TrackingMode trackingMode = TrackingMode.DirtyTracking, string? tenantId = null)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(options);
        if (tenantId != null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        }

        Storage = storage;
        Options = options;
        TrackingMode = trackingMode;
        TenantId = tenantId ?? options.DefaultTenantId;
        EventStore = new CoreEventStore(storage, TenantId);
        InnerIdentityMap = trackingMode == TrackingMode.Lightweight ? NoIdentityMap.Instance : new IdentityMap();
    }

    protected QuerySessionBase(IAquilaStorageProvider storage, StoreOptions options, string? tenantId)
        : this(storage, options, TrackingMode.DirtyTracking, tenantId)
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
        var envelope = await Storage.Documents.ReadDocumentAsync<T>(id, pk, ct);
        if (envelope == null || envelope.IsDeleted) return null;
        if (envelope.TenantId != TenantId) return null;

        var loadedData = SnapshotDocument(envelope.Data);

        if (TrackingMode != TrackingMode.Lightweight)
        {
            bool recordSnapshot = TrackingMode == TrackingMode.DirtyTracking;
            InnerIdentityMap.Track(id, loadedData, envelope, recordSnapshot);
        }
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
            var docType = typeof(T).Name;

            var envelopes = await Storage.Documents.QueryDocumentsAsync<T>(
                x => x.DocType == docType && !x.IsDeleted && x.TenantId == TenantId && idSet.Contains(x.Id),
                ct);

            foreach (var envelope in envelopes)
            {
                var loadedData = SnapshotDocument(envelope.Data);
                if (TrackingMode != TrackingMode.Lightweight)
                {
                    bool recordSnapshot = TrackingMode == TrackingMode.DirtyTracking;
                    InnerIdentityMap.Track(envelope.Id, loadedData, envelope, recordSnapshot);
                }
                results.Add(loadedData);
            }
        }

        return results;
    }

    [Obsolete("Use QueryAsync<T>() to avoid sync-over-async thread pool starvation.")]
    public IQueryable<T> Query<T>() where T : class
    {
        throw new NotSupportedException("Synchronous Query<T>() is disabled to prevent sync-over-async thread pool starvation. Use QueryAsync<T>() instead.");
    }

    public async Task<IReadOnlyList<T>> QueryAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, CancellationToken ct = default) where T : class
    {
        var docType = typeof(T).Name;
        var compiled = predicate?.Compile();
        var envelopes = await Storage.Documents.QueryDocumentsAsync<T>(
            x => x.DocType == docType && !x.IsDeleted && x.TenantId == TenantId,
            ct);

        var matchingEnvelopes = compiled != null ? envelopes.Where(compiled) : envelopes;
        var results = new List<T>();

        foreach (var envelope in matchingEnvelopes)
        {
            if (TrackingMode != TrackingMode.Lightweight && InnerIdentityMap.TryGet<T>(envelope.Id, out var cached))
            {
                results.Add(cached!);
            }
            else
            {
                var loadedData = SnapshotDocument(envelope.Data);
                if (TrackingMode != TrackingMode.Lightweight)
                {
                    bool recordSnapshot = TrackingMode == TrackingMode.DirtyTracking;
                    InnerIdentityMap.Track(envelope.Id, loadedData, envelope, recordSnapshot);
                }
                results.Add(loadedData);
            }
        }

        return results;
    }

    public async Task<TResult> QueryAsync<TDoc, TResult>(ICompiledQuery<TDoc, TResult> query, CancellationToken ct = default) where TDoc : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var documents = await QueryAsync<TDoc>((Expression<Func<DocumentEnvelope<TDoc>, bool>>?)null, ct);
        var queryable = documents.AsQueryable();

        return CompiledQueryCache.Execute(queryable, query);
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
        var events = await Storage.Events.FetchEventsAsync(streamId, targetTenant, 0, ct);
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

    protected static T SnapshotDocument<T>(T document) where T : class
    {
        ArgumentNullException.ThrowIfNull(document);
        var type = document.GetType();
        var json = System.Text.Json.JsonSerializer.Serialize(document, type);
        return (T)System.Text.Json.JsonSerializer.Deserialize(json, type)!;
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
    public QuerySession(IAquilaStorageProvider storage, StoreOptions options, string? tenantId = null)
        : base(storage, options, TrackingMode.DirtyTracking, tenantId)
    {
    }

    public QuerySession(IAquilaStorageProvider storage, StoreOptions options, TrackingMode trackingMode, string? tenantId = null)
        : base(storage, options, trackingMode, tenantId)
    {
    }
}
