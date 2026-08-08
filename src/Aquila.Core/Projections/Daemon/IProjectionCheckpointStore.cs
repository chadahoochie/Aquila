using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Aquila.Core.Storage;

namespace Aquila.Core.Projections.Daemon;

/// <summary>
/// Service Provider Interface (SPI) for reading and writing durable projection checkpoints.
/// </summary>
public interface IProjectionCheckpointStore
{
    Task<long> GetCheckpointAsync(string projectionName, CancellationToken ct = default);
    Task SaveCheckpointAsync(string projectionName, long sequence, CancellationToken ct = default);
}

/// <summary>
/// Persistence implementation of <see cref="IProjectionCheckpointStore"/> using <see cref="IDocumentStorageProvider"/>.
/// </summary>
public class DocumentStorageProjectionCheckpointStore : IProjectionCheckpointStore
{
    private readonly IDocumentStorageProvider _storageProvider;

    public DocumentStorageProjectionCheckpointStore(IDocumentStorageProvider storageProvider)
    {
        _storageProvider = storageProvider ?? throw new ArgumentNullException(nameof(storageProvider));
    }

    public async Task<long> GetCheckpointAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var envelope = await _storageProvider.ReadDocumentAsync<ProjectionCheckpoint>(projectionName, projectionName, ct).ConfigureAwait(false);
        return envelope?.Data?.LastCompletedSequence ?? 0;
    }

    public async Task SaveCheckpointAsync(string projectionName, long sequence, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var checkpoint = new ProjectionCheckpoint
        {
            ProjectionName = projectionName,
            LastCompletedSequence = sequence,
            UpdatedAt = DateTimeOffset.UtcNow
        };

        var envelope = new DocumentEnvelope<ProjectionCheckpoint>
        {
            Id = projectionName,
            PartitionKey = projectionName,
            DocType = "_checkpoint",
            TenantId = "default",
            IsDeleted = false,
            Data = checkpoint
        };

        await _storageProvider.UpsertDocumentAsync(envelope, ct).ConfigureAwait(false);
    }
}

/// <summary>
/// In-memory implementation of <see cref="IProjectionCheckpointStore"/> for testing and lightweight deployments.
/// </summary>
public class InMemoryProjectionCheckpointStore : IProjectionCheckpointStore
{
    private readonly ConcurrentDictionary<string, ProjectionCheckpoint> _checkpoints = new();

    public Task<long> GetCheckpointAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        if (_checkpoints.TryGetValue(projectionName, out var checkpoint))
        {
            return Task.FromResult(checkpoint.LastCompletedSequence);
        }
        return Task.FromResult(0L);
    }

    public Task SaveCheckpointAsync(string projectionName, long sequence, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        _checkpoints[projectionName] = new ProjectionCheckpoint
        {
            ProjectionName = projectionName,
            LastCompletedSequence = sequence,
            UpdatedAt = DateTimeOffset.UtcNow
        };
        return Task.CompletedTask;
    }
}
