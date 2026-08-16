using Shouldly;
using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Storage;

namespace Aquila.Core.Tests.Configuration;

public sealed class StoreOptionsTests
{
    private sealed record TestAggregate(string Id);
    private sealed record OldEvent(string Data);
    private sealed record NewEvent(string Data, int Version);

    private sealed class TestEventUpcaster : IEventUpcaster
    {
        public Type SourceType => typeof(OldEvent);
        public Type TargetType => typeof(NewEvent);
        public object Upcast(object oldEvent)
        {
            var old = (OldEvent)oldEvent;
            return new NewEvent(old.Data, 2);
        }
    }

    private sealed class CustomSnapshotStrategy : ISnapshotStrategy<TestAggregate>
    {
        public bool ShouldSnapshot(long currentVersion, int eventsSinceLastSnapshot) => true;
    }

    [Fact]
    public void EventRegistration_SnapshotEvery_Registers_Default_Strategy()
    {
        var reg = new EventRegistration();
        reg.SnapshotEvery<TestAggregate>(5);

        var strategy = reg.GetSnapshotStrategy<TestAggregate>();
        strategy.ShouldNotBeNull();
        strategy.ShouldSnapshot(currentVersion: 10, eventsSinceLastSnapshot: 5).ShouldBeTrue();
        strategy.ShouldSnapshot(currentVersion: 10, eventsSinceLastSnapshot: 4).ShouldBeFalse();

        var nonGeneric = reg.GetSnapshotStrategy(typeof(TestAggregate));
        nonGeneric.ShouldNotBeNull();
        nonGeneric.ShouldBe(strategy);

        reg.SnapshotStrategies.Count.ShouldBe(1);
    }

    [Fact]
    public void EventRegistration_SnapshotEvery_Throws_On_Zero_Or_Negative_Threshold()
    {
        var reg = new EventRegistration();
        Should.Throw<ArgumentOutOfRangeException>(() => reg.SnapshotEvery<TestAggregate>(0));
        Should.Throw<ArgumentOutOfRangeException>(() => reg.SnapshotEvery<TestAggregate>(-1));
    }

    [Fact]
    public void EventRegistration_RegisterSnapshotStrategy_Registers_Custom_Strategy()
    {
        var reg = new EventRegistration();
        var custom = new CustomSnapshotStrategy();

        reg.RegisterSnapshotStrategy(custom);

        var retrieved = reg.GetSnapshotStrategy<TestAggregate>();
        retrieved.ShouldBeSameAs(custom);

        Should.Throw<ArgumentNullException>(() => reg.RegisterSnapshotStrategy<TestAggregate>(null!));
        Should.Throw<ArgumentNullException>(() => reg.GetSnapshotStrategy((Type)null!));
    }

    [Fact]
    public void EventRegistration_GetSnapshotStrategy_Returns_Null_When_Not_Configured()
    {
        var reg = new EventRegistration();
        reg.GetSnapshotStrategy<TestAggregate>().ShouldBeNull();
        reg.GetSnapshotStrategy(typeof(TestAggregate)).ShouldBeNull();
    }

    [Fact]
    public void EventRegistration_RegisterUpcaster_Registers_Successfully()
    {
        var reg = new EventRegistration();
        reg.Upcasters.IsEmpty.ShouldBeTrue();

        reg.RegisterUpcaster<TestEventUpcaster>();
        reg.Upcasters.IsEmpty.ShouldBeFalse();

        var evt = new EventEnvelope<OldEvent> { Data = new OldEvent("test-data") };
        var upcasted = reg.Upcasters.Upcast(evt);
        upcasted.ShouldNotBeNull();
        upcasted.Data.ShouldBeOfType<NewEvent>();
        ((NewEvent)upcasted.Data).Version.ShouldBe(2);

        var reg2 = new EventRegistration();
        var instance = new TestEventUpcaster();
        reg2.RegisterUpcaster(instance);
        var upcasted2 = reg2.Upcasters.Upcast(evt);
        upcasted2.ShouldNotBeNull();
        upcasted2.Data.ShouldBeOfType<NewEvent>();

        Should.Throw<ArgumentNullException>(() => reg2.RegisterUpcaster(null!));
    }

    [Fact]
    public void StoreOptions_UseStorageProvider_With_Separate_Providers()
    {
        var options = new StoreOptions();
        var docStorage = new InMemoryStorageProvider();
        var evtStorage = new InMemoryStorageProvider();

        options.UseStorageProvider(docStorage, evtStorage);

        options.DocumentStorage.ShouldBeSameAs(docStorage);
        options.EventStorage.ShouldBeSameAs(evtStorage);

        Should.Throw<ArgumentNullException>(() => options.UseStorageProvider((IDocumentStorageProvider)null!, evtStorage));
        Should.Throw<ArgumentNullException>(() => options.UseStorageProvider(docStorage, (IEventStorageProvider)null!));
    }

    [Fact]
    public void StoreOptions_UseStorageProvider_With_Combined_Object()
    {
        var options = new StoreOptions();
        var combined = new InMemoryStorageProvider();

        options.UseStorageProvider((object)combined);

        options.DocumentStorage.ShouldBeSameAs(combined);
        options.EventStorage.ShouldBeSameAs(combined);

        Should.Throw<ArgumentNullException>(() => options.UseStorageProvider((object)null!));
    }

    [Fact]
    public void StoreOptions_UseInMemoryStorage_Configures_Defaults()
    {
        var options = new StoreOptions();
        options.UseInMemoryStorage();

        options.DocumentStorage.ShouldBeOfType<InMemoryStorageProvider>();
        options.EventStorage.ShouldBeOfType<InMemoryStorageProvider>();
    }

    [Fact]
    public void StoreOptions_Freeze_Prevents_Further_Mutations()
    {
        var options = new StoreOptions();
        options.IsReadOnly.ShouldBeFalse();

        options.DefaultTenantId = "tenant-1";
        options.Freeze();

        options.IsReadOnly.ShouldBeTrue();
        Should.Throw<InvalidOperationException>(() => options.DefaultTenantId = "tenant-2");
        Should.Throw<InvalidOperationException>(() => options.DocumentStorage = new InMemoryStorageProvider());
        Should.Throw<InvalidOperationException>(() => options.EventStorage = new InMemoryStorageProvider());
        Should.Throw<InvalidOperationException>(() => options.UseInMemoryStorage());
        Should.Throw<InvalidOperationException>(() => options.UseStorageProvider(new InMemoryStorageProvider(), new InMemoryStorageProvider()));
        Should.Throw<InvalidOperationException>(() => options.UseStorageProvider((object)new InMemoryStorageProvider()));
    }
}
