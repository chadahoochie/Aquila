using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
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

    [Fact]
    public void Freeze_ThrowsInvalidOperationException_WhenInlineProjectionUsedInPolyglotSetup()
    {
        var options = new StoreOptions();
        options.EventStorage = new InMemoryStorageProvider();
        options.ProjectionStorage = new InMemoryStorageProvider(); // distinct instance
        options.Projections.Add<InlineSampleProjection>(ProjectionLifecycle.Inline);

        var ex = Should.Throw<InvalidOperationException>(() => options.Freeze());
        ex.Message.ShouldContain("is registered with ProjectionLifecycle.Inline");
        ex.Message.ShouldContain("Polyglot projections must use ProjectionLifecycle.Async or ProjectionLifecycle.Live");
    }

    [Fact]
    public void Freeze_Succeeds_WhenAsyncOrLiveProjectionsUsedInPolyglotSetup()
    {
        var options = new StoreOptions();
        options.EventStorage = new InMemoryStorageProvider();
        options.ProjectionStorage = new InMemoryStorageProvider(); // distinct instance
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
}
