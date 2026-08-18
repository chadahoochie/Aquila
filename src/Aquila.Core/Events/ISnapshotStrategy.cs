namespace Aquila.Core.Events;

/// <summary>
/// Strategy for determining when a snapshot should be taken for an aggregate root.
/// </summary>
/// <typeparam name="TAggregate">The aggregate type.</typeparam>
public interface ISnapshotStrategy<TAggregate> where TAggregate : class
{
    /// <summary>
    /// Evaluates whether a snapshot should be saved based on the aggregate version and events processed.
    /// </summary>
    bool ShouldSnapshot(long currentVersion, int eventsSinceLastSnapshot);
}

/// <summary>
/// Default threshold-based snapshot strategy.
/// </summary>
/// <typeparam name="TAggregate">The aggregate type.</typeparam>
public sealed class DefaultSnapshotStrategy<TAggregate> : ISnapshotStrategy<TAggregate> where TAggregate : class
{
    private readonly int _threshold;

    public DefaultSnapshotStrategy(int threshold = 100)
    {
        _threshold = threshold;
    }

    public bool ShouldSnapshot(long currentVersion, int eventsSinceLastSnapshot) => eventsSinceLastSnapshot >= _threshold;
}
