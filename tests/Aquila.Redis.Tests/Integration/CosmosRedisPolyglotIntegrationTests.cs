using Aquila.Core.Configuration;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Projections.Daemon;
using Aquila.Core.Sessions;
using Aquila.Core.Storage;
using Aquila.Redis.Configuration;
using Aquila.Redis.Extensions;
using Aquila.Redis.Storage;
using Aquila.Redis.Tests.Fixtures;
using Shouldly;

namespace Aquila.Redis.Tests.Integration;

public class CosmosRedisPolyglotIntegrationTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fixture;

    public CosmosRedisPolyglotIntegrationTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    public record OrderPlaced(string OrderId, string CustomerId, decimal Amount);
    public record OrderItemAdded(string OrderId, string Sku, decimal Price);

    public class CustomerDocument
    {
        public string Id { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
    }

    public class OrderSummaryReadModel
    {
        public string Id { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
        public int ItemCount { get; set; }
    }

    public class OrderSummaryProjection : SingleStreamProjection<OrderSummaryReadModel>
    {
        public OrderSummaryProjection()
        {
            Lifecycle = ProjectionLifecycle.Async;
            CreateEvent<OrderPlaced>(e => new OrderSummaryReadModel
            {
                Id = e.OrderId,
                TotalAmount = e.Amount,
                ItemCount = 1
            });
            ProjectEvent<OrderItemAdded>((e, summary) =>
            {
                summary.TotalAmount += e.Price;
                summary.ItemCount++;
            });
        }
    }

    public class CustomerOrdersSummaryReadModel
    {
        public string Id { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public decimal TotalSpent { get; set; }
        public int TotalOrders { get; set; }
    }

    public class CustomerOrdersSummaryProjection : MultiStreamProjection<CustomerOrdersSummaryReadModel, string>
    {
        public CustomerOrdersSummaryProjection()
        {
            Lifecycle = ProjectionLifecycle.Async;
        }

        protected override string Identity(IEvent @event)
        {
            return @event.Data switch
            {
                OrderPlaced placed => placed.CustomerId,
                _ => string.Empty
            };
        }

        public override bool Apply(IEvent @event, CustomerOrdersSummaryReadModel document)
        {
            if (@event.Data is OrderPlaced placed)
            {
                document.Id = placed.CustomerId;
                document.TotalSpent += placed.Amount;
                document.TotalOrders++;
            }
            return true;
        }
    }

    [Fact]
    public async Task EndToEnd_PolyglotStore_AppendsEvents_ProjectsToRedis_AndSupportsRebuild()
    {
        var testId = Guid.NewGuid().ToString("N");
        var redisOptions = new RedisStorageOptions
        {
            KeyPrefix = $"aquila:polyglot:{testId}:",
            Database = 0
        };

        var options = new StoreOptions();
        options.UseInMemoryStorage(); // In-memory events & primary docs for test isolation
        options.UseRedisProjections(_fixture.Multiplexer, opt =>
        {
            opt.KeyPrefix = redisOptions.KeyPrefix;
            opt.Database = redisOptions.Database;
        });

        options.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);
        options.Projections.Add<CustomerOrdersSummaryProjection>(ProjectionLifecycle.Async);

        using var store = new DocumentStore(options);
        await store.InitializeAsync(TestContext.Current.CancellationToken);

        var checkpointStore = new RedisProjectionCheckpointStore(
            _fixture.Multiplexer,
            keyPrefix: $"aquila:chk:{testId}:");

        var daemon = new ProjectionDaemon(store, checkpointStore);

        // 1. Store a customer document in DocumentStorage
        using (var session = store.OpenSession())
        {
            session.Store(new CustomerDocument { Id = "CUST-1", CustomerName = "Acme Corp" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // 2. Start event streams
        using (var session = store.OpenSession())
        {
            session.Events.StartStream<OrderSummaryReadModel>("ORD-1",
                new OrderPlaced("ORD-1", "CUST-1", 100m),
                new OrderItemAdded("ORD-1", "SKU-A", 25m)
            );
            session.Events.StartStream<OrderSummaryReadModel>("ORD-2",
                new OrderPlaced("ORD-2", "CUST-1", 50m)
            );
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // 3. Catch up daemon -> materializes read models in Redis
        await daemon.CatchUpAsync(TestContext.Current.CancellationToken);

        // 4. Verify read models in Redis via session.LoadAsync
        using (var session = store.OpenSession())
        {
            var summary1 = await session.LoadAsync<OrderSummaryReadModel>("ORD-1", "ORD-1", TestContext.Current.CancellationToken);
            summary1.ShouldNotBeNull();
            summary1.TotalAmount.ShouldBe(125m);
            summary1.ItemCount.ShouldBe(2);

            var summary2 = await session.LoadAsync<OrderSummaryReadModel>("ORD-2", "ORD-2", TestContext.Current.CancellationToken);
            summary2.ShouldNotBeNull();
            summary2.TotalAmount.ShouldBe(50m);
            summary2.ItemCount.ShouldBe(1);

            var custSummary = await session.LoadAsync<CustomerOrdersSummaryReadModel>("CUST-1", partitionKey: null, TestContext.Current.CancellationToken);
            custSummary.ShouldNotBeNull();
            custSummary.TotalSpent.ShouldBe(150m);
            custSummary.TotalOrders.ShouldBe(2);
        }

        // 5. Zero-Downtime Rebuild of Single Stream Projection
        await daemon.RebuildProjectionAsync<OrderSummaryProjection>(TestContext.Current.CancellationToken);

        // Verify rebuilt projection state in Redis is identical
        using (var session = store.OpenSession())
        {
            var summary1 = await session.LoadAsync<OrderSummaryReadModel>("ORD-1", "ORD-1", TestContext.Current.CancellationToken);
            summary1.ShouldNotBeNull();
            summary1.TotalAmount.ShouldBe(125m);
            summary1.ItemCount.ShouldBe(2);
        }
    }
}
