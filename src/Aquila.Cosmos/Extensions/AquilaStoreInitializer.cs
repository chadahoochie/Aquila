using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Aquila.Core.Abstractions;

namespace Aquila.Cosmos.Extensions;

/// <summary>
/// Runs <see cref="IDocumentStore.InitializeAsync"/> once during host startup.
/// </summary>
/// <remarks>
/// <para>
/// <c>AddAquila</c> cannot await initialization from a synchronous registration method, so before
/// this existed nothing in the DI path ever called <c>InitializeAsync</c>. Two things depend on it:
/// container and index provisioning, and seeding the event store's global sequence from the highest
/// sequence already in storage. Without the seed, every process start resumes the counter at 0 and
/// re-issues sequence numbers that existing events already hold, which makes the projection daemon —
/// whose cursor is that sequence — skip or re-apply events.
/// </para>
/// <para>
/// Startup fails if initialization fails. A store whose containers do not exist, or whose sequence
/// silently restarted, produces data corruption rather than an outage, and an outage is the better
/// of the two.
/// </para>
/// </remarks>
public sealed class AquilaStoreInitializer : IHostedService
{
    private readonly IDocumentStore _store;
    private readonly ILogger<AquilaStoreInitializer>? _logger;

    public AquilaStoreInitializer(IDocumentStore store, ILogger<AquilaStoreInitializer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger?.LogDebug("Initializing Aquila document store.");
        await _store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogDebug("Aquila document store initialized.");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
