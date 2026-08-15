using System.Linq.Expressions;
using Aquila.Core.Events;
using Aquila.Core.Projections;
using Aquila.Core.Queries;

namespace Aquila.Benchmarks.Models;

#region Documents

public sealed class CustomerProfileDocument
{
    public string Id { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Region { get; set; } = "US-East";
    public string Tier { get; set; } = "Standard";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public bool IsActive { get; set; } = true;
    public int LoginCount { get; set; }
    public List<string> Tags { get; set; } = new();
}

public sealed class Address
{
    public string Street { get; set; } = "123 Technology Drive";
    public string City { get; set; } = "Redmond";
    public string State { get; set; } = "WA";
    public string PostalCode { get; set; } = "98052";
    public string Country { get; set; } = "USA";
}

public sealed class OrderLineItem
{
    public string Sku { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; } = 1;
    public decimal UnitPrice { get; set; }
    public decimal Subtotal => Quantity * UnitPrice;
}

public sealed class OrderDocument
{
    public string Id { get; set; } = string.Empty;
    public string OrderNumber { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public string Region { get; set; } = "US-East";
    public string Status { get; set; } = "Pending";
    public decimal TotalAmount { get; set; }
    public decimal DiscountAmount { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public Address ShippingAddress { get; set; } = new();
    public List<OrderLineItem> Items { get; set; } = new();
    public List<string> Tags { get; set; } = new();
    public Dictionary<string, string> Metadata { get; set; } = new();
}

#endregion

#region Events

public sealed record OrderCreated(
    string OrderId,
    string CustomerId,
    decimal TotalAmount,
    string Region,
    DateTime CreatedAt);

public sealed record OrderLineItemAdded(
    string OrderId,
    string Sku,
    string ProductName,
    int Quantity,
    decimal Price);

public sealed record OrderDiscountApplied(
    string OrderId,
    decimal DiscountAmount,
    string DiscountCode);

public sealed record OrderStatusUpdated(
    string OrderId,
    string Status);

// Schema evolution / upcasting events
public sealed record OrderCreatedV1(
    string OrderId,
    string CustomerId,
    decimal TotalAmount);

public sealed record OrderCreatedV2(
    string OrderId,
    string CustomerId,
    decimal TotalAmount,
    string Currency);

public sealed record OrderCreatedV3(
    string OrderId,
    string CustomerId,
    decimal TotalAmount,
    string Currency,
    string Channel);

public sealed class OrderCreatedV1ToV2Upcaster : EventUpcaster<OrderCreatedV1, OrderCreatedV2>
{
    public override OrderCreatedV2 Upcast(OrderCreatedV1 oldEvent)
    {
        return new OrderCreatedV2(oldEvent.OrderId, oldEvent.CustomerId, oldEvent.TotalAmount, "USD");
    }
}

public sealed class OrderCreatedV2ToV3Upcaster : EventUpcaster<OrderCreatedV2, OrderCreatedV3>
{
    public override OrderCreatedV3 Upcast(OrderCreatedV2 oldEvent)
    {
        return new OrderCreatedV3(oldEvent.OrderId, oldEvent.CustomerId, oldEvent.TotalAmount, oldEvent.Currency, "Web");
    }
}

#endregion

#region Aggregates

public sealed class OrderAggregate
{
    public string Id { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public string Status { get; set; } = string.Empty;
    public List<OrderLineItem> Items { get; set; } = new();

    public void Apply(OrderCreated e)
    {
        Id = e.OrderId;
        CustomerId = e.CustomerId;
        Total = e.TotalAmount;
        Status = "Created";
    }

    public void Apply(OrderLineItemAdded e)
    {
        Items.Add(new OrderLineItem
        {
            Sku = e.Sku,
            ProductName = e.ProductName,
            Quantity = e.Quantity,
            UnitPrice = e.Price
        });
        Total += e.Price * e.Quantity;
    }

    public void Apply(OrderDiscountApplied e)
    {
        Total -= e.DiscountAmount;
    }

    public void Apply(OrderStatusUpdated e)
    {
        Status = e.Status;
    }
}

#endregion

#region Projections

public sealed class OrderSummaryProjection : SingleStreamProjection<OrderAggregate>
{
    public OrderSummaryProjection()
    {
        CreateEvent<OrderCreated>(e => new OrderAggregate
        {
            Id = e.OrderId,
            CustomerId = e.CustomerId,
            Total = e.TotalAmount,
            Status = "Created"
        });

        ProjectEvent<OrderLineItemAdded>((e, agg) =>
        {
            agg.Items.Add(new OrderLineItem
            {
                Sku = e.Sku,
                ProductName = e.ProductName,
                Quantity = e.Quantity,
                UnitPrice = e.Price
            });
            agg.Total += e.Price * e.Quantity;
        });

        ProjectEvent<OrderDiscountApplied>((e, agg) =>
        {
            agg.Total -= e.DiscountAmount;
        });

        ProjectEvent<OrderStatusUpdated>((e, agg) =>
        {
            agg.Status = e.Status;
        });
    }
}

public sealed class UserOrdersSummary
{
    public string CustomerId { get; set; } = string.Empty;
    public int TotalOrders { get; set; }
    public decimal TotalSpent { get; set; }
}

public sealed class UserOrdersProjection : MultiStreamProjection<UserOrdersSummary, string>
{
    protected override string Identity(IEvent @event) =>
        @event.Data switch
        {
            OrderCreated e => e.CustomerId,
            _ => string.Empty
        };

    public override bool Apply(IEvent @event, UserOrdersSummary doc)
    {
        if (@event.Data is OrderCreated e)
        {
            doc.CustomerId = e.CustomerId;
            doc.TotalOrders++;
            doc.TotalSpent += e.TotalAmount;
        }
        return true;
    }
}

#endregion

#region Compiled Queries

public sealed class ActiveOrdersByCustomerQuery : ICompiledQuery<OrderDocument, IQueryable<OrderDocument>>
{
    public string CustomerId { get; }
    public string Status { get; }

    public ActiveOrdersByCustomerQuery(string customerId, string status = "Pending")
    {
        CustomerId = customerId;
        Status = status;
    }

    public Expression<Func<IQueryable<OrderDocument>, IQueryable<OrderDocument>>> QueryIs() =>
        orders => orders.Where(o => o.CustomerId == CustomerId && o.Status == Status);
}

public sealed class OrdersByRegionAndStatusQuery : ICompiledQuery<OrderDocument, IQueryable<OrderDocument>>
{
    public string Region { get; }
    public string Status { get; }

    public OrdersByRegionAndStatusQuery(string region, string status = "Completed")
    {
        Region = region;
        Status = status;
    }

    public Expression<Func<IQueryable<OrderDocument>, IQueryable<OrderDocument>>> QueryIs() =>
        orders => orders.Where(o => o.Region == Region && o.Status == Status);
}

#endregion

#region Benchmark Data Generator

public static class BenchmarkDataGenerator
{
    public static CustomerProfileDocument CreateCustomer(int index)
    {
        return new CustomerProfileDocument
        {
            Id = $"CUST-{index:D6}",
            CustomerId = $"CUST-{index:D6}",
            Email = $"customer{index}@example.com",
            Name = $"Customer Name {index}",
            Region = index % 2 == 0 ? "US-East" : "US-West",
            Tier = index % 5 == 0 ? "VIP" : "Standard",
            CreatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMinutes(index),
            IsActive = true,
            LoginCount = index % 100,
            Tags = new List<string> { "retail", "active", $"cohort-{index % 10}" }
        };
    }

    public static OrderDocument CreateOrder(int index, int itemCount = 3)
    {
        var items = new List<OrderLineItem>(itemCount);
        decimal total = 0;
        for (int i = 1; i <= itemCount; i++)
        {
            var price = 19.99m * i;
            items.Add(new OrderLineItem
            {
                Sku = $"SKU-{i:D4}",
                ProductName = $"Product Item {i}",
                Quantity = i,
                UnitPrice = price
            });
            total += price * i;
        }

        return new OrderDocument
        {
            Id = $"ORD-{index:D6}",
            OrderNumber = $"ORD-{index:D6}",
            CustomerId = $"CUST-{(index % 100):D6}",
            Region = index % 2 == 0 ? "US-East" : "US-West",
            Status = index % 3 == 0 ? "Completed" : "Pending",
            TotalAmount = total,
            DiscountAmount = index % 5 == 0 ? 10.00m : 0m,
            CreatedAt = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc).AddMinutes(index),
            ShippingAddress = new Address
            {
                Street = $"{100 + index} Main Street",
                City = "Seattle",
                State = "WA",
                PostalCode = "98101",
                Country = "USA"
            },
            Items = items,
            Tags = new List<string> { "express", "b2c", $"channel-{index % 4}" },
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "web",
                ["currency"] = "USD",
                ["ip"] = $"192.168.1.{index % 250}"
            }
        };
    }

    public static List<OrderDocument> CreateOrders(int count, int itemsPerOrder = 3)
    {
        var list = new List<OrderDocument>(count);
        for (int i = 0; i < count; i++)
        {
            list.Add(CreateOrder(i, itemsPerOrder));
        }
        return list;
    }

    public static List<CustomerProfileDocument> CreateCustomers(int count)
    {
        var list = new List<CustomerProfileDocument>(count);
        for (int i = 0; i < count; i++)
        {
            list.Add(CreateCustomer(i));
        }
        return list;
    }

    public static List<object> CreateEventSequence(string orderId, string customerId, int count)
    {
        var events = new List<object>(count);
        events.Add(new OrderCreated(orderId, customerId, 100m, "US-East", DateTime.UtcNow));

        for (int i = 1; i < count; i++)
        {
            switch (i % 3)
            {
                case 1:
                    events.Add(new OrderLineItemAdded(orderId, $"SKU-{i:D4}", $"Product {i}", 1, 25.00m));
                    break;
                case 2:
                    events.Add(new OrderDiscountApplied(orderId, 5.00m, "SUMMER5"));
                    break;
                case 0:
                    events.Add(new OrderStatusUpdated(orderId, "Processing"));
                    break;
            }
        }
        return events;
    }
}

#endregion
