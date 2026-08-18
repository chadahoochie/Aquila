using Aquila.Redis.Storage;
using Aquila.Redis.Tests.Fixtures;
using Shouldly;

namespace Aquila.Redis.Tests.Storage;

public class RedisProjectionCheckpointStoreTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _fixture;

    public RedisProjectionCheckpointStoreTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task GetCheckpointAsync_ReturnsZero_WhenNotFound()
    {
        var prefix = $"chk:{Guid.NewGuid():N}:";
        var store = new RedisProjectionCheckpointStore(_fixture.Multiplexer, prefix);

        var seq = await store.GetCheckpointAsync("UnknownProjection", TestContext.Current.CancellationToken);
        seq.ShouldBe(0L);
    }

    [Fact]
    public async Task SaveCheckpointAsync_PersistsSequence_AndAdvancesMonotonically()
    {
        var prefix = $"chk:{Guid.NewGuid():N}:";
        var store = new RedisProjectionCheckpointStore(_fixture.Multiplexer, prefix);
        var projName = "OrdersSummaryProjection";

        await store.SaveCheckpointAsync(projName, 100, TestContext.Current.CancellationToken);
        var seq1 = await store.GetCheckpointAsync(projName, TestContext.Current.CancellationToken);
        seq1.ShouldBe(100L);

        // Attempt to regress checkpoint to 50 -> Lua script prevents regression
        await store.SaveCheckpointAsync(projName, 50, TestContext.Current.CancellationToken);
        var seq2 = await store.GetCheckpointAsync(projName, TestContext.Current.CancellationToken);
        seq2.ShouldBe(100L);

        // Advance to 150 -> succeeds
        await store.SaveCheckpointAsync(projName, 150, TestContext.Current.CancellationToken);
        var seq3 = await store.GetCheckpointAsync(projName, TestContext.Current.CancellationToken);
        seq3.ShouldBe(150L);

        // Reset to 0 (rebuild) -> succeeds
        await store.SaveCheckpointAsync(projName, 0, TestContext.Current.CancellationToken);
        var seq4 = await store.GetCheckpointAsync(projName, TestContext.Current.CancellationToken);
        seq4.ShouldBe(0L);
    }
}
