using Microsoft.Extensions.DependencyInjection;
using Aquila.Core.Abstractions;
using Aquila.Core.Configuration;
using Aquila.Core.Projections;
using Aquila.Cosmos.Extensions;

namespace Aquila.Samples;

// Document model
public class Customer
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTimeOffset JoinedAt { get; set; } = DateTimeOffset.UtcNow;
}

// Event Store aggregate and events
public record CustomerRegistered(string CustomerId, string Name, string Email);
public record MembershipUpgraded(string CustomerId, string TierLevel, decimal DiscountPercent);
public record PurchaseMade(string CustomerId, string OrderId, decimal Amount);

public class CustomerAggregate
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string MembershipTier { get; set; } = "Standard";
    public decimal TotalSpent { get; set; }
    public int PurchaseCount { get; set; }

    public void Apply(CustomerRegistered @event)
    {
        Id = @event.CustomerId;
        Name = @event.Name;
        Email = @event.Email;
    }

    public void Apply(MembershipUpgraded @event)
    {
        MembershipTier = @event.TierLevel;
    }

    public void Apply(PurchaseMade @event)
    {
        TotalSpent += @event.Amount;
        PurchaseCount++;
    }
}

public class CustomerProjection : SingleStreamProjection<CustomerAggregate>
{
    public CustomerProjection()
    {
        CreateEvent<CustomerRegistered>(e => new CustomerAggregate
        {
            Id = e.CustomerId,
            Name = e.Name,
            Email = e.Email,
            MembershipTier = "Standard"
        });

        ProjectEvent<MembershipUpgraded>((e, agg) => agg.MembershipTier = e.TierLevel);
        ProjectEvent<PurchaseMade>((e, agg) =>
        {
            agg.TotalSpent += e.Amount;
            agg.PurchaseCount++;
        });
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("==========================================================");
        Console.WriteLine("  Aquila: MartenDB Clone with Pluggable Storage (Demo)");
        Console.WriteLine("==========================================================");

        var services = new ServiceCollection();

        // Register Aquila Document & Event Store with InMemory provider for fast demo
        services.AddAquila(options =>
        {
            options.UseInMemoryStorage();

            options.Schema.For<Customer>()
                .Identity(c => c.Id)
                .PartitionKey(c => c.Name);

            options.Projections.Add<CustomerProjection>(ProjectionLifecycle.Inline);
        });

        var serviceProvider = services.BuildServiceProvider();

        Console.WriteLine("\n[1] StoreOptions and Schema Policy configured successfully.");
        var options = serviceProvider.GetRequiredService<StoreOptions>();
        Console.WriteLine($"    - Storage Provider: {options.DocumentStorage.ProviderName}");
        Console.WriteLine($"    - Registered Projections: {options.Projections.Projections.Count}");

        Console.WriteLine("\n[2] Executing Document Storage & Event Sourcing Session...");
        var store = serviceProvider.GetRequiredService<IDocumentStore>();
        using var session = store.OpenSession();

        var streamId = Guid.NewGuid().ToString();
        session.Events.StartStream<CustomerAggregate>(streamId,
            new CustomerRegistered(streamId, "Alice Smith", "alice@example.com"),
            new MembershipUpgraded(streamId, "VIP Premium", 15.0m),
            new PurchaseMade(streamId, "ORD-9001", 249.99m)
        );

        await session.SaveChangesAsync();

        Console.WriteLine("\n[3] Rehydrating Aggregate from Event Store Stream...");
        var aggregate = await session.Events.AggregateStreamAsync<CustomerAggregate>(streamId);

        if (aggregate != null)
        {
            Console.WriteLine($"    - Stream ID: {aggregate.Id}");
            Console.WriteLine($"    - Customer Name: {aggregate.Name}");
            Console.WriteLine($"    - Membership Tier: {aggregate.MembershipTier}");
            Console.WriteLine($"    - Purchases Made: {aggregate.PurchaseCount}");
            Console.WriteLine($"    - Total Spent: ${aggregate.TotalSpent}");
        }

        Console.WriteLine("\n==========================================================");
        Console.WriteLine("  Aquila pluggable storage provider validation COMPLETE!");
        Console.WriteLine("==========================================================");

        await Task.CompletedTask;
    }
}
