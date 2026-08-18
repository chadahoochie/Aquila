using StackExchange.Redis;
using Aquila.Core.Projections.Daemon;

namespace Aquila.Redis.Storage;

/// <summary>
/// Redis-backed implementation of <see cref="IProjectionCheckpointStore"/> using atomic monotonic Lua script execution.
/// </summary>
public sealed class RedisProjectionCheckpointStore : IProjectionCheckpointStore
{
    private static readonly LuaScript MonotonicSaveScript = LuaScript.Prepare(@"
        local cur = redis.call('GET', @key)
        if not cur or tonumber(@seq) == 0 or tonumber(@seq) > tonumber(cur) then
            redis.call('SET', @key, @seq)
            return 1
        end
        return 0
    ");

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly string _keyPrefix;
    private readonly int _database;

    public RedisProjectionCheckpointStore(IConnectionMultiplexer multiplexer, string keyPrefix = "aquila:checkpoints:", int database = 0)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _keyPrefix = keyPrefix ?? "aquila:checkpoints:";
        _database = database;
    }

    public async Task<long> GetCheckpointAsync(string projectionName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var db = _multiplexer.GetDatabase(_database);
        var value = await db.StringGetAsync($"{_keyPrefix}{projectionName}").ConfigureAwait(false);
        return value.HasValue && long.TryParse((string?)value, out var seq) ? seq : 0L;
    }

    public async Task SaveCheckpointAsync(string projectionName, long sequence, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var db = _multiplexer.GetDatabase(_database);
        var key = (RedisKey)$"{_keyPrefix}{projectionName}";

        await db.ScriptEvaluateAsync(MonotonicSaveScript, new { key = key, seq = sequence }).ConfigureAwait(false);
    }
}
