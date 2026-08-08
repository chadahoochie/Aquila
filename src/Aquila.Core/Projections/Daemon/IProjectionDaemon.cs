using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Hosted service interface for managing background asynchronous projection processing.
/// </summary>
public interface IProjectionDaemon : IHostedService
{
    Task StartProjectionAsync(string projectionName, CancellationToken ct = default);
    Task StopProjectionAsync(string projectionName, CancellationToken ct = default);
    Task RebuildProjectionAsync<TProjection>(CancellationToken ct = default) where TProjection : IProjection;
    Task RebuildProjectionAsync(string projectionName, CancellationToken ct = default);
    Task CatchUpAsync(CancellationToken ct = default);
}
