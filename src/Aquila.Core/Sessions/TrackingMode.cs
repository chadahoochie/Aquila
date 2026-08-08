namespace Aquila.Core.Sessions;

/// <summary>
/// Session tracking modes controlling IdentityMap and automatic dirty checking behavior.
/// </summary>
public enum TrackingMode
{
    /// <summary>
    /// Lightweight session without identity map caching or dirty checking.
    /// </summary>
    Lightweight,

    /// <summary>
    /// Identity map enabled session; explicit Store() call required to queue dirty documents.
    /// </summary>
    IdentityMap,

    /// <summary>
    /// Identity map enabled session with automatic JSON snapshot dirty checking on SaveChangesAsync.
    /// </summary>
    DirtyTracking
}
