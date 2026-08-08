using System;
using System.Threading.Tasks;
using Aquila.Core.Events;
using Aquila.Core.Sessions;
using Shouldly;
using Xunit;

namespace Aquila.Core.Tests;

public class UpcasterTests
{
    public record V1OrderPlaced(string OrderId, string CustomerName);
    public record V2OrderPlaced(string OrderId, string CustomerName, string Email);

    public class V1ToV2OrderPlacedUpcaster : EventUpcaster<V1OrderPlaced, V2OrderPlaced>
    {
        public override V2OrderPlaced Upcast(V1OrderPlaced oldEvent)
        {
            return new V2OrderPlaced(oldEvent.OrderId, oldEvent.CustomerName, $"{oldEvent.CustomerName.ToLower()}@example.com");
        }
    }

    public class OrderAggregate
    {
        public string OrderId { get; private set; } = string.Empty;
        public string CustomerName { get; private set; } = string.Empty;
        public string Email { get; private set; } = string.Empty;

        public void Apply(V2OrderPlaced e)
        {
            OrderId = e.OrderId;
            CustomerName = e.CustomerName;
            Email = e.Email;
        }
    }

    public record V1UserCreated(string UserId, string Name);
    public record V2UserCreated(string UserId, string FirstName, string LastName);
    public record V3UserCreated(string UserId, string FirstName, string LastName, string Status);

    public class V1ToV2UserUpcaster : EventUpcaster<V1UserCreated, V2UserCreated>
    {
        public override V2UserCreated Upcast(V1UserCreated oldEvent)
        {
            var parts = oldEvent.Name.Split(' ');
            return new V2UserCreated(oldEvent.UserId, parts[0], parts.Length > 1 ? parts[1] : string.Empty);
        }
    }

    public class V2ToV3UserUpcaster : EventUpcaster<V2UserCreated, V3UserCreated>
    {
        public override V3UserCreated Upcast(V2UserCreated oldEvent)
        {
            return new V3UserCreated(oldEvent.UserId, oldEvent.FirstName, oldEvent.LastName, "Active");
        }
    }

    [Fact]
    public async Task EventUpcaster_Transforms_Legacy_Event_Payload()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.UseInMemoryStorage();
            opts.Events.RegisterUpcaster<V1ToV2OrderPlacedUpcaster>();
        });

        using var session = store.OpenSession();
        var streamId = "order-" + Guid.NewGuid();
        session.Events.Append(streamId, new V1OrderPlaced(streamId, "Alice"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var events = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        Assert.Single(events);
        Assert.IsType<V2OrderPlaced>(events[0].Data);

        var v2 = (V2OrderPlaced)events[0].Data;
        Assert.Equal("Alice", v2.CustomerName);
        Assert.Equal("alice@example.com", v2.Email);

        var aggregate = await session.Events.AggregateStreamAsync<OrderAggregate>(streamId, ct: TestContext.Current.CancellationToken);
        Assert.NotNull(aggregate);
        Assert.Equal("Alice", aggregate.CustomerName);
        Assert.Equal("alice@example.com", aggregate.Email);
    }

    [Fact]
    public async Task EventUpcaster_Supports_Chained_Upcasting()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.UseInMemoryStorage();
            opts.Events.RegisterUpcaster(new V1ToV2UserUpcaster());
            opts.Events.RegisterUpcaster(new V2ToV3UserUpcaster());
        });

        using var session = store.OpenSession();
        var streamId = "user-" + Guid.NewGuid();
        session.Events.Append(streamId, new V1UserCreated(streamId, "John Doe"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var events = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        Assert.Single(events);
        Assert.IsType<V3UserCreated>(events[0].Data);

        var v3 = (V3UserCreated)events[0].Data;
        Assert.Equal("John", v3.FirstName);
        Assert.Equal("Doe", v3.LastName);
        Assert.Equal("Active", v3.Status);
    }

    [Fact]
    public async Task EventUpcaster_FetchGlobalEventsAsync_Upcasts_Events()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.UseInMemoryStorage();
            opts.Events.RegisterUpcaster<V1ToV2OrderPlacedUpcaster>();
        });

        using var session = store.OpenSession();
        var streamId = "order-" + Guid.NewGuid();
        session.Events.Append(streamId, new V1OrderPlaced(streamId, "Bob"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var globalEvents = await session.Events.FetchGlobalEventsAsync(0, ct: TestContext.Current.CancellationToken);
        Assert.NotEmpty(globalEvents);
        var targetEvent = Assert.Single(globalEvents, e => e.StreamId == streamId);
        Assert.IsType<V2OrderPlaced>(targetEvent.Data);
        Assert.Equal("bob@example.com", ((V2OrderPlaced)targetEvent.Data).Email);
    }

    [Fact]
    public async Task UpcasterRegistry_Returns_Original_Event_When_No_Upcaster_Registered()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.UseInMemoryStorage();
        });

        using var session = store.OpenSession();
        var streamId = "order-" + Guid.NewGuid();
        session.Events.Append(streamId, new V1OrderPlaced(streamId, "Charlie"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var events = await session.Events.FetchStreamAsync(streamId, ct: TestContext.Current.CancellationToken);
        Assert.Single(events);
        Assert.IsType<V1OrderPlaced>(events[0].Data);
    }

    [Fact]
    public void EventUpcaster_SourceType_And_TargetType_Reflect_Generic_Arguments()
    {
        IEventUpcaster upcaster = new V1ToV2OrderPlacedUpcaster();

        upcaster.SourceType.ShouldBe(typeof(V1OrderPlaced));
        upcaster.TargetType.ShouldBe(typeof(V2OrderPlaced));
    }
}

