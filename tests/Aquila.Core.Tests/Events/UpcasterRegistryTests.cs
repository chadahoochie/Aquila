using System;
using System.Collections.Generic;
using Shouldly;
using Xunit;
using Aquila.Core.Events;

namespace Aquila.Core.Tests.Events;

public class UpcasterRegistryTests
{
    public record UpcastV1(string Name);
    public record UpcastV2(string Name, int Version);
    public record UpcastV3(string Name, int Version, bool IsActive);

    public class V1ToV2Upcaster : EventUpcaster<UpcastV1, UpcastV2>
    {
        public override UpcastV2 Upcast(UpcastV1 oldEvent)
        {
            return new UpcastV2(oldEvent.Name, 2);
        }
    }

    public class V2ToV3Upcaster : EventUpcaster<UpcastV2, UpcastV3>
    {
        public override UpcastV3 Upcast(UpcastV2 oldEvent)
        {
            return new UpcastV3(oldEvent.Name, oldEvent.Version + 1, true);
        }
    }

    public class NullReturningUpcaster : EventUpcaster<UpcastV1, UpcastV2>
    {
        public override UpcastV2 Upcast(UpcastV1 oldEvent) => null!;
    }

    [Fact]
    public void Upcast_NullEventOrNullDataOrEmptyRegistry_ReturnsInput()
    {
        var registry = new UpcasterRegistry();
        registry.IsEmpty.ShouldBeTrue();

        registry.Upcast(null!).ShouldBeNull();

        var nullDataEnv = new EventEnvelope<object> { Data = null! };
        registry.Upcast(nullDataEnv).ShouldBeSameAs(nullDataEnv);

        var validEnv = new EventEnvelope<UpcastV1> { Data = new UpcastV1("test") };
        registry.Upcast(validEnv).ShouldBeSameAs(validEnv);
    }

    [Fact]
    public void Upcast_NoMatchingUpcasterFound_ReturnsOriginalEvent()
    {
        var registry = new UpcasterRegistry();
        registry.Register<V1ToV2Upcaster>();
        registry.IsEmpty.ShouldBeFalse();

        var unmappedEnv = new EventEnvelope<UpcastV3> { Data = new UpcastV3("unmapped", 3, true) };
        var result = registry.Upcast(unmappedEnv);
        result.ShouldBeSameAs(unmappedEnv);
    }

    [Fact]
    public void Upcast_ChainedUpcasters_TransformsToFinalVersionAndPreservesHeaders()
    {
        var registry = new UpcasterRegistry();
        registry.Register(new V1ToV2Upcaster());
        registry.Register(new V2ToV3Upcaster());

        var origId = Guid.NewGuid();
        var origTime = DateTimeOffset.UtcNow.AddMinutes(-5);

        var origEnvelope = new EventEnvelope<UpcastV1>
        {
            Id = origId,
            StreamId = "stream-chain",
            Version = 10,
            Sequence = 10,
            GlobalSequence = 100,
            Timestamp = origTime,
            TenantId = "tenant-a",
            CorrelationId = "corr-chain",
            CausationId = "cause-chain",
            Headers = new Dictionary<string, object> { ["Key"] = "Val" },
            Data = new UpcastV1("ChainTest")
        };

        var result = registry.Upcast(origEnvelope);

        result.ShouldNotBeNull();
        result.ShouldNotBeSameAs(origEnvelope);
        result.Data.ShouldBeOfType<UpcastV3>();

        var v3Data = (UpcastV3)result.Data;
        v3Data.Name.ShouldBe("ChainTest");
        v3Data.Version.ShouldBe(3);
        v3Data.IsActive.ShouldBeTrue();

        result.Id.ShouldBe(origId);
        result.StreamId.ShouldBe("stream-chain");
        result.Version.ShouldBe(10);
        result.Sequence.ShouldBe(10);
        result.GlobalSequence.ShouldBe(100);
        result.Timestamp.ShouldBe(origTime);
        result.TenantId.ShouldBe("tenant-a");
        result.CorrelationId.ShouldBe("corr-chain");
        result.CausationId.ShouldBe("cause-chain");
        result.Headers["Key"].ShouldBe("Val");
    }

    [Fact]
    public void Upcast_UpcasterReturnsNull_ThrowsInvalidOperationException()
    {
        var registry = new UpcasterRegistry();
        registry.Register(new NullReturningUpcaster());

        var envelope = new EventEnvelope<UpcastV1> { Data = new UpcastV1("NullTest") };

        var ex = Should.Throw<InvalidOperationException>(() => registry.Upcast(envelope));
        ex.Message.ShouldContain("returned null when upcasting source type");
    }

    [Fact]
    public void Register_NullUpcaster_ThrowsArgumentNullException()
    {
        var registry = new UpcasterRegistry();
        Should.Throw<ArgumentNullException>(() => registry.Register((IEventUpcaster)null!));
    }
}
