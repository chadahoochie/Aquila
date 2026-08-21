using System.Linq.Expressions;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Queries;
using Aquila.Core.Storage;
using Shouldly;

namespace Aquila.Core.Tests.Projections;

public class PolyglotProjectionValidationTests
{
    public class SampleEvent
    {
        public string Id { get; set; } = string.Empty;
    }

    public class SampleReadModel
    {
        public string Id { get; set; } = string.Empty;
    }

    public class InlineSampleProjection : SingleStreamProjection<SampleReadModel>
    {
        public InlineSampleProjection()
        {
            Lifecycle = ProjectionLifecycle.Inline;
            CreateEvent<SampleEvent>(e => new SampleReadModel { Id = e.Id });
        }
    }

    public class AsyncSampleProjection : SingleStreamProjection<SampleReadModel>
    {
        public AsyncSampleProjection()
        {
            Lifecycle = ProjectionLifecycle.Async;
            CreateEvent<SampleEvent>(e => new SampleReadModel { Id = e.Id });
        }
    }

    public class LiveSampleProjection : SingleStreamProjection<SampleReadModel>
    {
        public LiveSampleProjection()
        {
            Lifecycle = ProjectionLifecycle.Live;
            CreateEvent<SampleEvent>(e => new SampleReadModel { Id = e.Id });
        }
    }

    /// <summary>
    /// Stands in for a genuinely different storage backend. Polyglot detection compares
    /// <c>ProviderName</c>, so a distinct in-memory instance is not sufficient to model one.
    /// </summary>
    private sealed class ForeignProjectionStorage : IProjectionStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();

        public string ProviderName => "Redis";
        public double LastRequestCharge => _inner.LastRequestCharge;
        public double CumulativeRequestCharge => _inner.CumulativeRequestCharge;

        public Task InitializeAsync(CancellationToken ct = default) => _inner.InitializeAsync(ct);

        public Task<DocumentEnvelope<T>?> ReadDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
            _inner.ReadDocumentAsync<T>(id, partitionKey, ct);

        public Task<IReadOnlyList<DocumentEnvelope<T>>> QueryDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
            _inner.QueryDocumentsAsync(predicate, options, ct);

        public Task<StorageQueryResult<T>> QueryPagedDocumentsAsync<T>(Expression<Func<DocumentEnvelope<T>, bool>>? predicate = null, QueryOptions? options = null, CancellationToken ct = default) where T : class =>
            _inner.QueryPagedDocumentsAsync(predicate, options, ct);

        public Task UpsertDocumentAsync<T>(DocumentEnvelope<T> envelope, CancellationToken ct = default) where T : class =>
            _inner.UpsertDocumentAsync(envelope, ct);

        public Task DeleteDocumentAsync<T>(string id, string partitionKey, CancellationToken ct = default) where T : class =>
            _inner.DeleteDocumentAsync<T>(id, partitionKey, ct);

        public Task ExecuteBatchAsync(IEnumerable<StorageOperation> operations, CancellationToken ct = default) =>
            _inner.ExecuteBatchAsync(operations, ct);

        public Task PurgeProjectionAsync(string projectionName, Type readModelType, CancellationToken ct = default) =>
            _inner.PurgeProjectionAsync(projectionName, readModelType, ct);

        public void Dispose() => _inner.Dispose();
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    [Fact]
    public void Freeze_ThrowsInvalidOperationException_WhenInlineProjectionUsedInPolyglotSetup()
    {
        var options = new StoreOptions();
        options.EventStorage = new InMemoryStorageProvider();
        options.ProjectionStorage = new ForeignProjectionStorage();
        options.Projections.Add<InlineSampleProjection>(ProjectionLifecycle.Inline);

        options.IsPolyglot.ShouldBeTrue();

        var ex = Should.Throw<InvalidOperationException>(() => options.Freeze());
        ex.Message.ShouldContain("is registered with ProjectionLifecycle.Inline");
        ex.Message.ShouldContain("Polyglot projections must use ProjectionLifecycle.Async or ProjectionLifecycle.Live");
    }

    [Fact]
    public void Freeze_Succeeds_WhenAsyncOrLiveProjectionsUsedInPolyglotSetup()
    {
        var options = new StoreOptions();
        options.EventStorage = new InMemoryStorageProvider();
        options.ProjectionStorage = new ForeignProjectionStorage();
        options.Projections.Add<AsyncSampleProjection>(ProjectionLifecycle.Async);
        options.Projections.Add<LiveSampleProjection>(ProjectionLifecycle.Live);

        Should.NotThrow(() => options.Freeze());
        options.IsReadOnly.ShouldBeTrue();
    }

    [Fact]
    public void Freeze_Succeeds_WhenInlineProjectionUsedInMonoStoreSetup()
    {
        var sharedProvider = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.EventStorage = sharedProvider;
        options.ProjectionStorage = sharedProvider; // same instance
        options.Projections.Add<InlineSampleProjection>(ProjectionLifecycle.Inline);

        Should.NotThrow(() => options.Freeze());
        options.IsReadOnly.ShouldBeTrue();
    }

    [Fact]
    public void Freeze_Succeeds_WhenStorageIsLeftAtItsDefaults()
    {
        // A default StoreOptions is not polyglot. Backing the three SPI roles with distinct
        // instances previously made it appear so, and rejected the default Inline lifecycle
        // with a message that named the same provider on both sides of the comparison.
        var options = new StoreOptions();
        options.Projections.Add<InlineSampleProjection>(); // default lifecycle is Inline

        options.IsPolyglot.ShouldBeFalse();
        Should.NotThrow(() => options.Freeze());
    }

    [Fact]
    public void Freeze_Succeeds_WhenSeparateProviderInstancesShareOneBackend()
    {
        // Segregated registration (UseCosmosDocuments + UseCosmosEvents) yields distinct provider
        // instances over one physical account. Detecting polyglot by reference inequality would
        // reject that valid configuration, so detection compares ProviderName.
        var options = new StoreOptions();
        options.DocumentStorage = new InMemoryStorageProvider();
        options.EventStorage = new InMemoryStorageProvider();
        options.ProjectionStorage = new InMemoryStorageProvider();
        options.Projections.Add<InlineSampleProjection>(ProjectionLifecycle.Inline);

        options.IsPolyglot.ShouldBeFalse();
        Should.NotThrow(() => options.Freeze());
    }

    [Fact]
    public void ProjectionStorage_FallsBackToDocumentStorage_WhenNotExplicitlyConfigured()
    {
        // Omitting projection storage must not silently strand read models in an unrelated
        // provider — they belong with the documents until the caller says otherwise.
        var documents = new InMemoryStorageProvider();
        var options = new StoreOptions();
        options.DocumentStorage = documents;
        options.EventStorage = new InMemoryStorageProvider();
        options.Projections.Add<AsyncSampleProjection>(ProjectionLifecycle.Async);
        options.Freeze();

        options.ProjectionStorage.ShouldBeSameAs(documents);
        options.GetStorageFor(typeof(SampleReadModel)).ShouldBeSameAs(documents);
    }
}
