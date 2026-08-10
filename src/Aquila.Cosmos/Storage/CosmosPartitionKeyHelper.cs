using Microsoft.Azure.Cosmos;

namespace Aquila.Cosmos.Storage;

public static class CosmosPartitionKeyHelper
{
    public static PartitionKey CreatePartitionKey(string partitionKey)
    {
        if (string.IsNullOrEmpty(partitionKey))
        {
            return PartitionKey.Null;
        }

        if (partitionKey.Contains('|'))
        {
            var parts = partitionKey.Split('|', StringSplitOptions.RemoveEmptyEntries);
            var builder = new PartitionKeyBuilder();
            foreach (var part in parts)
            {
                builder.Add(part);
            }
            return builder.Build();
        }

        return new PartitionKey(partitionKey);
    }
}
