# Optimize Projection Processing Throughput (Aquila)

## Context

Aquila is a bespoke .NET event-sourcing/document-store library (Marten-like) with a Cosmos DB backend, split into `src/Aquila.Core` (storage-agnostic core, including the `ProjectionDaemon` background service) and `src/Aquila.Cosmos` (Cosmos DB storage engine, including `CosmosProjectionDaemon`). The top priority is **projection processing throughput**. A direct audit of the source (not secondhand — every finding below was read and confirmed in the actual files) found one dominant, unbounded bottleneck plus several compounding inefficiencies on both the .NET-concurrency side and the Cosmos DB side. This plan fixes them in priority order: fix the worst-case-growing query first (biggest ROI, lowest risk), then add concurrency (biggest remaining ROI, moderate risk), then tune Cosmos provisioning/indexing/client config (low risk, meaningful RU savings), then batch-API correctness and rebuild-path cleanup, with Change Feed Processor scoped as a deliberately deferred follow-up.

No `appsettings.json` exists anywhere — this is a library; all config happens through `StoreOptions`/`CosmosStorageOptions`/`ProjectionStorageOptions` in code, so every new knob below is a new C# options property, not a config file change.

## Confirmed root causes (verified directly against source)

1. **`CosmosEventStorageProvider.FetchGlobalEventsAsync`** (`src/Aquila.Cosmos/Storage/CosmosEventStorageProvider.cs:272-330`) — the dominant bottleneck. Executes `SELECT * FROM c WHERE c._docType = '$event'` with **no `GlobalSequence` filter, no `MaxItemCount`**, buffers/deserializes **every** event document in the container into memory, then filters by `GlobalSequence > fromGlobalSequence` and sorts **client-side**. Every daemon poll (every ~100ms) rescans and redeserializes the entire event store — cost grows without bound as the event store grows. A composite index `(/_docType, /data/GlobalSequence)` already exists (`CosmosStorageProvider.cs:111-128`) specifically to support a server-side filtered+sorted query, but it's never used that way.

2. **Zero concurrency anywhere in the projection pipeline** (confirmed via repo-wide grep — zero hits for `Task.WhenAll`, `Parallel.`, `SemaphoreSlim`, `Channel.`, `MaxDegreeOfParallelism`, `AllowBulkExecution`). In `ProjectionDaemon.cs` and its near-duplicate `CosmosProjectionDaemon.cs`:
   - Checkpoints for all active projections are fetched **serially** (`ProcessNextBatchAsync`/`ProcessNextBatchFromStorageAsync`, ~lines 181-189/182-190).
   - Projections are processed **serially, one after another** (~lines 197-207/198-208).
   - Within a projection, events are processed **one at a time**; for single-stream projections, `ProcessSingleStreamEventsAsync<TAggregate>` (~lines 261-287/453-479) does a point-read (`session.LoadAsync`) then a point-write per event — **2 Cosmos round trips per event, serially**, even when consecutive events target completely independent aggregates/partitions.

3. **Hardcoded batch size of 100** for `FetchGlobalEventsAsync` (`ProjectionDaemon.cs:194,224`, `CosmosProjectionDaemon.cs:195`) — not configurable.

4. **No RU/throughput provisioning anywhere.** `CosmosStorageProvider.InitializeAsync` (`CosmosStorageProvider.cs:81-109`) calls `CreateContainerIfNotExistsAsync` with no `ThroughputProperties` for any container. This matters more now: the recently-merged options-segregation feature (`src/Aquila.Cosmos/Configuration/ProjectionStorageOptions.cs`, `StorageLocationOptions.cs`, `CosmosContainerResolver.cs`) lets a hot projection get its own dedicated container specifically to avoid RU contention, but there's no way to also give that container its own RU budget through the library.

5. **No indexing exclusions** — `CreateDefaultContainerProperties`/`CreateDefaultEventsContainerProperties` (`CosmosStorageProvider.cs:111-147`) only add composite indexes, never exclude paths, so every property of the `data` blob is indexed by default on every write.

6. **Under-tuned `CosmosClientOptions`** — only the connection-string constructor sets `ConnectionMode.Direct` (`CosmosStorageProvider.cs:33-37`); no `AllowBulkExecution`, no custom retry/backoff tuning anywhere.

7. **`CosmosDocumentStorageProvider.ExecuteBatchAsync`** (`CosmosDocumentStorageProvider.cs:217-292`) is misleadingly named — it's a serial `foreach` issuing one Cosmos call per operation, not grouped by partition key, not using `Container.CreateTransactionalBatch` (which IS used correctly in `CosmosEventStorageProvider.AppendEventsAsync`).

8. **Silent catch-all** around the transactional-batch path in `AppendEventsAsync` (`CosmosEventStorageProvider.cs:163-166`) falls through to slow sequential per-event writes on **any** exception, not just "emulator doesn't support batch" — this can silently mask real errors as a performance cliff under load.

9. **Rebuild-path inefficiency** (lower priority, not steady-state): `ClearProjectionDocumentsAsync` in both daemons deletes documents **one at a time, serially**.

10. **Change Feed Processor is not wired up** despite doc-comments implying it — `CosmosProjectionDaemon.ProcessChangeFeedBatchAsync` exists and is tested but nothing calls `container.GetChangeFeedProcessorBuilder(...)`. The real live loop is 100ms polling.

Additional verified detail affecting Phase 2 design: `MultiStreamProjection<TDoc,TId>.Identity(IEvent)` is a `protected abstract` method — **not exposed** on the non-generic `IMultiStreamProjection` interface, so a batch dispatcher outside the class can't group events by target identity without a small additive interface method. Also, `DocumentSession` (`src/Aquila.Core/Sessions/DocumentSession.cs`) holds mutable, non-thread-safe state (`_pendingOperations` list, `IdentityMap.Track/Untrack`) — **it is not safe to share one `DocumentSession` instance across concurrently-running parallel groups**; each parallel group must open its own session.

## Phased Plan

### Phase 1 — Fix the event-fetch query (highest ROI, lowest risk)

- Rewrite `FetchGlobalEventsAsync` in `src/Aquila.Cosmos/Storage/CosmosEventStorageProvider.cs` to issue a server-side filtered, sorted query using the existing composite index:
  `SELECT * FROM c WHERE c._docType = '$event' AND c.data.GlobalSequence > @fromGlobalSequence ORDER BY c.data.GlobalSequence` (append `AND c._tenantId = @tenantId` when a tenant is specified), with `QueryRequestOptions.MaxItemCount = batchSize`. Drop the client-side full-buffer/filter/sort — keep a defensive `Take(batchSize)` and the existing tenant post-filter for the raw-JSON fallback path.
- `GetMaxGlobalSequenceAsync` stays as-is (it's a one-time startup cost, not part of the hot loop — not in scope).
- Introduce `ProjectionDaemonOptions` (new file `src/Aquila.Core/Projections/Daemon/ProjectionDaemonOptions.cs`) with `BatchSize` (default 100) and `PollingIntervalMs`/`IdlePollingIntervalMs` (default 100). Add an optional trailing constructor parameter to `ProjectionDaemon` and `CosmosProjectionDaemon` (default `new ProjectionDaemonOptions()`), and replace the hardcoded `100` literals and `Task.Delay(100, ...)` calls with it. Purely additive — no existing constructor signature breaks.

**Files:** `CosmosEventStorageProvider.cs`, new `ProjectionDaemonOptions.cs`, `ProjectionDaemon.cs`, `CosmosProjectionDaemon.cs`.

### Phase 2 — Add concurrency to the projection pipeline

- **Parallel checkpoint fetch**: replace the serial `foreach` (`ProcessNextBatchAsync`/`ProcessNextBatchFromStorageAsync`) with a `Task.WhenAll` over independent, read-only checkpoint reads.
- **Bounded-parallel per-projection dispatch**: different async projections are independent consumers of the same event batch. Replace the serial per-projection `foreach` with `Parallel.ForEachAsync(projections, new ParallelOptions { MaxDegreeOfParallelism = _options.MaxProjectionConcurrency }, ...)`. Each projection still only saves its own checkpoint after its own writes complete — the "write before checkpoint" crash-safety invariant is preserved because it's still sequenced *within* each projection's own task, just running concurrently with *other* projections.
- **Bounded-parallel per-identity event dispatch (the highest-value piece)**: add a shared `BoundedParallelEventDispatcher` (new file `src/Aquila.Core/Projections/Daemon/BoundedParallelEventDispatcher.cs`) that groups a projection's event batch by target identity (single-stream: `evt.StreamId`; multi-stream: projection identity), processes each group **internally in strict `GlobalSequence` order**, but runs independent groups **concurrently** (bounded by `MaxEventGroupConcurrency`). This removes the "2 round trips per event, fully serial" bottleneck across independent aggregates while preserving intra-aggregate ordering. `Parallel.ForEachAsync` awaits full completion before returning, so the projection's checkpoint still only advances after every group in the batch finishes — no change to checkpoint-ordering semantics.
- **Per-group sessions, not a shared one**: since `DocumentSession` is not safe for concurrent use (mutable `_pendingOperations`/identity-map state), each parallel group must open its own `DocumentSession` rather than sharing the one currently opened per `ProcessEventsForProjectionAsync` call.
- **Small additive interface change**: add `object GetIdentity(IEvent @event)` to `IMultiStreamProjection` (`src/Aquila.Core/Projections/MultiStreamProjection.cs`), implemented on `MultiStreamProjection<TDoc,TId>` by calling the existing `protected abstract Identity(...)`. `MultiStreamProjection<TDoc,TId>` is the only implementer in the repo (verified via grep), so this is safe and non-breaking.
- New options on `ProjectionDaemonOptions`: `MaxProjectionConcurrency` (default `Environment.ProcessorCount`), `MaxEventGroupConcurrency` (default `Environment.ProcessorCount * 2`).

**Files:** `ProjectionDaemon.cs`, `CosmosProjectionDaemon.cs`, `ProjectionDaemonOptions.cs`, new `BoundedParallelEventDispatcher.cs`, `MultiStreamProjection.cs`.

**Risk note:** this is the riskiest phase — ordering correctness per aggregate/identity must be exact. New tests (below) specifically target this.

### Phase 3 — Cosmos DB provisioning, indexing, and client tuning

- **RU/throughput provisioning API**: add `ThroughputSettings` (manual RU or autoscale max-RU) to `StorageLocationOptions.cs`, with `WithManualThroughput(int)`/`WithAutoscaleThroughput(int)` builder methods, and thread it through `ProjectionStorageOptions` (`DedicatedContainer`/`AutoContainerPerProjection` modes and per-projection `.For<TProjection>()` overrides). Extend `CosmosContainerResolver.GetAllConfiguredContainers()` to surface resolved throughput per container, and pass it into `CreateContainerIfNotExistsAsync` in `CosmosStorageProvider.InitializeAsync`. Unset = `null` throughput arg = today's exact behavior (backward compatible).
- **Indexing exclusions**: in `CreateDefaultContainerProperties`/`CreateDefaultEventsContainerProperties`, exclude `/data/*` by default and explicitly re-include only the paths actually queried (`/_docType/?`, `/_tenantId/?`, `/data/GlobalSequence/?`, `/pk/?`, `/id/?`). Only affects newly-created containers (non-breaking for existing deployments).
- **`CosmosClientOptions` tuning**: in the connection-string constructor path, add `AllowBulkExecution = true` and explicit `MaxRetryAttemptsOnRateLimitedRequests`/`MaxRetryWaitTimeOnRateLimitedRequests`. Add a `CosmosStorageProvider.CreateDefaultClientOptions()` static helper for callers who inject their own `CosmosClient`.

**Files:** `StorageLocationOptions.cs`, `ProjectionStorageOptions.cs`, `CosmosContainerResolver.cs`, `CosmosStorageProvider.cs`.

### Phase 4 — Batch API correctness and error-handling hygiene

- **Fix `ExecuteBatchAsync`** (`CosmosDocumentStorageProvider.cs:217-292`): group operations by resolved `(Container, PartitionKey)`, use `Container.CreateTransactionalBatch` per group (mirroring the working pattern in `AppendEventsAsync`), chunk groups exceeding Cosmos's 100-op/2MB batch limit, and run independent partition-key groups concurrently (bounded, same pattern as Phase 2). Fall back to per-item writes only on a narrow, specific exception, not silently.
- **Narrow the silent catch** in `AppendEventsAsync` (`CosmosEventStorageProvider.cs:163-166`) to the specific "batch unsupported" condition; log at `Warning` (add optional `ILogger<CosmosEventStorageProvider>?`) whenever the fallback triggers so throttling/bugs aren't silently masked as slow writes; rethrow anything else.

**Files:** `CosmosDocumentStorageProvider.cs`, `CosmosEventStorageProvider.cs`.

### Phase 5 — Change Feed Processor (deferred, opt-in follow-up — design only, not implemented in this pass)

CFP is genuinely the highest-throughput mechanism Cosmos offers for this workload, and `ProcessChangeFeedBatchAsync` already exists/is tested as the processing half. However, wiring the subscription half (`GetChangeFeedProcessorBuilder`, lease containers, rebalance/redelivery semantics, multi-instance deployment implications) is materially riskier and orthogonal to Phases 1-3, which already remove the actual unbounded bottleneck. **Recommendation: do not implement in this pass.** Design note for later: it would replace only the *live-tail* ingestion path — rebuild/backfill should keep using the Phase-1-optimized pull-based `FetchGlobalEventsAsync`, added as a new opt-in `CosmosChangeFeedProjectionDaemon` type, defaulting off.

### Phase 6 — Rebuild-path bulk delete (lowest priority)

`ClearProjectionDocumentsAsync` in both daemons deletes rebuild documents one at a time. Once Phase 2's `BoundedParallelEventDispatcher`/`Parallel.ForEachAsync` pattern exists, reuse it here with bounded concurrency instead of a serial `foreach`. Rebuild-time only, no steady-state throughput impact — do last.

## Verification

**Existing tests that must keep passing:**
- `tests/Aquila.Core.Tests/Projections/*` (all `ProjectionDaemon` tests against `InMemoryStorageProvider`).
- `tests/Aquila.Cosmos.Tests/Projections/CosmosDaemonTests.cs` — especially the rebuild-ordering tests (`RebuildProjectionAsync_Resets_Checkpoint_And_Reprocesses_Events`, `RebuildProjectionAsync_Clears_Existing_ReadModel_Documents`), the stop/start gating test, and the exception-resilience test in the daemon loop.

**New tests required:**
- Phase 1: assert the exact `QueryDefinition` text/parameters issued by `FetchGlobalEventsAsync` (server-side filter+sort, `MaxItemCount` set) — e.g. against a `Container` test double, plus a large-synthetic-container test proving result correctness at scale.
- Phase 2 (most important): an **ordering-under-parallelism** test with interleaved events for the same aggregate mixed with other aggregates in one batch (e.g. `[a-1 v1, a-2 v1, a-1 v2, a-3 v1, a-1 v3]`), asserting `a-1`'s final state reflects v1→v2→v3 strictly in order despite cross-aggregate parallel dispatch. Also a cross-projection independence test (one slow projection must not block another's checkpoint) and a checkpoint-crash-safety test (a failure in one parallel group must not advance that projection's checkpoint past the last successful sequence).
- Phase 3: unit tests that `ThroughputSettings` resolves correctly per `ProjectionStorageMode` and flows into `CreateContainerIfNotExistsAsync`; assert default `IndexingPolicy.ExcludedPaths`/`IncludedPaths` shape.
- Phase 4: assert `ExecuteBatchAsync` issues one `TransactionalBatch.ExecuteAsync` per partition-key group rather than N individual calls; assert the narrowed catch in `AppendEventsAsync` rethrows unrelated exceptions.

**Throughput proof:** capture `RequestCharge` per operation in emulator-backed integration tests to show RU-per-poll no longer scales with total event count (Phase 1) and RU/wall-clock for a fixed synthetic workload (e.g. 10k events / 500 aggregates / 3 projections) before and after each phase, so gains are attributable per-phase rather than lumped together. Run these against the Cosmos emulator in CI for determinism; a final live-Azure RU-cost validation is recommended before rollout since emulator RU accounting can differ from production.
