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

        int pipeIndex = partitionKey.IndexOf('|');
        if (pipeIndex < 0)
        {
            return new PartitionKey(partitionKey);
        }

        // Performance Optimization: Use ReadOnlySpan<char> slicing instead of string.Split('|')
        // to build hierarchical partition keys without string[] array allocations.
        var builder = new PartitionKeyBuilder();
        var span = partitionKey.AsSpan();
        while (!span.IsEmpty)
        {
            int idx = span.IndexOf('|');
            if (idx == -1)
            {
                if (!span.IsEmpty)
                {
                    builder.Add(span.ToString());
                }
                break;
            }

            var segment = span.Slice(0, idx);
            if (!segment.IsEmpty)
            {
                builder.Add(segment.ToString());
            }
            span = span.Slice(idx + 1);
        }

        return builder.Build();
    }
}
