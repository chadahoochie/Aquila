namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Durable checkpoint entity tracking progress for an asynchronous projection.
/// </summary>
public sealed class ProjectionCheckpoint
{
    public string ProjectionName { get; set; } = string.Empty;
    public long LastCompletedSequence { get; set; }
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
}
