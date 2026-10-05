using Aquila.Core.Events;
using Aquila.Core.Exceptions;
using Aquila.Cosmos.Storage;
using Shouldly;

namespace Aquila.Cosmos.Tests;

public sealed record ConcurrencyTestEvent(string OrderId, decimal Amount);

/// <summary>
/// Optimistic concurrency on the event append path, exercised against the emulator.
/// </summary>
/// <remarks>
/// These cannot be written against a mocked <c>Container</c>: the defect they cover was that the
/// version check was a check-then-act with no precondition on the write, which a mock happily
/// accepts because it never enforces id uniqueness or ETag matching. Only a real store can fail
/// the second writer.
/// </remarks>
[Collection("CosmosIntegration")]
public sealed class CosmosEventConcurrencyIntegrationTests
{
    private readonly CosmosContainerFixture _fixture;

    public CosmosEventConcurrencyIntegrationTests(CosmosContainerFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<CosmosStorageProvider> CreateProviderAsync(string containerName, CancellationToken ct)
    {
        var provider = new CosmosStorageProvider(_fixture.Client, "IntegrationDb", containerName);
        await provider.InitializeAsync(ct);
        return provider;
    }

    private static EventEnvelope<ConcurrencyTestEvent> Event(string streamId, string orderId, decimal amount) =>
        new()
        {
            StreamId = streamId,
            TenantId = "concurrency-tenant",
            Data = new ConcurrencyTestEvent(orderId, amount)
        };

    [Fact]
    public async Task ConcurrentAppends_AtTheSameExpectedVersion_AdmitExactlyOneWriter()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = await CreateProviderAsync($"events-race-{Guid.NewGuid():N}", ct);
        var streamId = $"order-{Guid.NewGuid():N}";

        // Establish the stream at version 1.
        await provider.AppendEventsAsync(streamId, new[] { Event(streamId, "seed", 10m) }, expectedVersion: 0, ct);

        // Several writers all observe version 1 and all try to write version 2. Two writers are not
        // enough to force the interleaving reliably -- the first can finish reading and writing
        // before the second reads, in which case the plain version check catches it and the race
        // never happens. A wider fan-out makes at least one genuine overlap near-certain.
        const int racers = 8;
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<Exception?> Contend(string marker)
        {
            await barrier.Task.ConfigureAwait(false);
            try
            {
                await provider.AppendEventsAsync(streamId, new[] { Event(streamId, marker, 25m) }, expectedVersion: 1, ct)
                    .ConfigureAwait(false);
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }

        var contenders = Enumerable.Range(0, racers).Select(i => Contend($"writer-{i}")).ToArray();
        barrier.SetResult();

        var outcomes = await Task.WhenAll(contenders);

        var winners = outcomes.Count(o => o == null);
        winners.ShouldBe(1, "exactly one writer may claim version 2");
        outcomes.Where(o => o != null).ShouldAllBe(o => o is AquilaConcurrencyException);

        // Upserting at a deterministic event id let later writers overwrite earlier ones, so the
        // stream still looked well-formed -- one event at version 2 -- while appends that had been
        // reported as successful were gone.
        var events = await provider.FetchEventsAsync(streamId, "concurrency-tenant", 0, ct);
        events.Count.ShouldBe(2, "the seed event plus exactly one contended append");

        var header = await provider.GetStreamHeaderAsync(streamId, "concurrency-tenant", ct);
        header.ShouldNotBeNull();
        header.Version.ShouldBe(2);
    }

    [Fact]
    public async Task AppendAtAStaleExpectedVersion_IsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = await CreateProviderAsync($"events-stale-{Guid.NewGuid():N}", ct);
        var streamId = $"order-{Guid.NewGuid():N}";

        await provider.AppendEventsAsync(streamId, new[] { Event(streamId, "e1", 10m) }, expectedVersion: 0, ct);
        await provider.AppendEventsAsync(streamId, new[] { Event(streamId, "e2", 20m) }, expectedVersion: 1, ct);

        await Should.ThrowAsync<AquilaConcurrencyException>(
            provider.AppendEventsAsync(streamId, new[] { Event(streamId, "stale", 30m) }, expectedVersion: 1, ct));

        var events = await provider.FetchEventsAsync(streamId, "concurrency-tenant", 0, ct);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task ConcurrentStreamCreation_AdmitsExactlyOneWriter()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = await CreateProviderAsync($"events-create-{Guid.NewGuid():N}", ct);
        var streamId = $"order-{Guid.NewGuid():N}";

        // Neither writer sees an existing header, so both take the create path.
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<Exception?> Contend(string marker)
        {
            await barrier.Task.ConfigureAwait(false);
            try
            {
                await provider.AppendEventsAsync(streamId, new[] { Event(streamId, marker, 15m) }, expectedVersion: 0, ct)
                    .ConfigureAwait(false);
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }

        // Same reasoning as the append race: a wider fan-out is needed to make the interleaving
        // reliable rather than dependent on how quickly the first writer finishes.
        var contenders = Enumerable.Range(0, 8).Select(i => Contend($"creator-{i}")).ToArray();
        barrier.SetResult();

        var outcomes = await Task.WhenAll(contenders);

        outcomes.Count(o => o == null).ShouldBe(1, "exactly one writer may start the stream");
        outcomes.Where(o => o != null).ShouldAllBe(o => o is AquilaConcurrencyException);

        var events = await provider.FetchEventsAsync(streamId, "concurrency-tenant", 0, ct);
        events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task SequentialAppends_AtTheCorrectVersion_AllSucceed()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = await CreateProviderAsync($"events-happy-{Guid.NewGuid():N}", ct);
        var streamId = $"order-{Guid.NewGuid():N}";

        for (long version = 0; version < 5; version++)
        {
            await provider.AppendEventsAsync(streamId, new[] { Event(streamId, $"e{version}", 5m) }, version, ct);
        }

        var events = await provider.FetchEventsAsync(streamId, "concurrency-tenant", 0, ct);
        events.Count.ShouldBe(5);
        events.Select(e => e.Version).OrderBy(v => v).ShouldBe(new long[] { 1, 2, 3, 4, 5 });

        var header = await provider.GetStreamHeaderAsync(streamId, "concurrency-tenant", ct);
        header!.Version.ShouldBe(5);
    }
}
