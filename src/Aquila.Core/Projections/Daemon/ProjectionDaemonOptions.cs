namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Configuration options for projection daemons controlling batch size, polling intervals, and concurrency limits.
/// </summary>
public sealed class ProjectionDaemonOptions
{
    /// <summary>
    /// Maximum number of events to fetch in a single global sequence batch. Defaults to 1000.
    /// </summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>
    /// Polling interval in milliseconds when active events are being processed or between poll iterations. Defaults to 100.
    /// </summary>
    public int PollingIntervalMs { get; set; } = 100;

    /// <summary>
    /// Polling interval in milliseconds when no events were found or no active projections are registered. Defaults to 500.
    /// </summary>
    public int IdlePollingIntervalMs { get; set; } = 500;

    /// <summary>
    /// Maximum number of projections to dispatch in parallel. Defaults to Environment.ProcessorCount.
    /// </summary>
    public int MaxProjectionConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Maximum number of independent event groups (by aggregate or identity) to process in parallel within a projection. Defaults to Environment.ProcessorCount * 2.
    /// </summary>
    public int MaxEventGroupConcurrency { get; set; } = Environment.ProcessorCount * 2;
}
