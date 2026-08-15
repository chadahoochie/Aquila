# Aquila Framework — Performance Benchmarks Baseline

> **Aquila Framework** (.NET 10 Cosmos DB Native Document Store & Event Sourcing Engine)  
> **Generated**: August 14, 2026 22:03:15  
> **Environment**: BenchmarkDotNet v0.15.8, Linux Fedora Linux 44 (KDE Plasma Desktop Edition) | 11th Gen Intel Core i5-1135G7 2.40GHz (Max: 0.40GHz), 1 CPU, 8 logical and 4 physical cores | .NET SDK 10.0.302 | [Host]     : .NET 10.0.10 (10.0.10, 10.0.1026.32716), X64 RyuJIT x86-64-v4 | DefaultJob : .NET 10.0.10 (10.0.10, 10.0.1026.32716), X64 RyuJIT x86-64-v4  

---

## Executive Summary

The Aquila performance benchmarking suite evaluates latency, throughput, and memory allocations across all core subsystems:
1. **Cosmos DB Provider**: Partition key construction, LINQ envelope AST rewriting, and JSON serialization.
2. **Event Sourcing Engine**: Stream creation, event appending, stream fetching, upcasting evolutions, and snapshot-accelerated rehydration.
3. **Session & Change Tracking**: Lightweight, IdentityMap, and DirtyTracking modes, plus UTF-8 JSON snapshotting and delta calculation.
4. **Projections**: In-memory event application, single-stream aggregate folds, and multi-stream session batch processing.
5. **Compiled Queries & LINQ**: Cold compilation vs steady-state cached delegate execution.
6. **Patch API**: Single and compound JSON pointer patch operations.

---

## Cosmos DB Provider

### 1.1 Partition Key Construction (`CosmosPartitionKeyBenchmarks`)
Measures partition key generation for single-part and multi-part hierarchical partition keys.

| Method                             | Mean        | Error     | StdDev     | Rank | Gen0   | Allocated |
|----------------------------------- |------------:|----------:|-----------:|-----:|-------:|----------:|
| 'Empty / Null PartitionKey'        |   0.8000 ns | 0.0442 ns |  0.0414 ns |    1 |      - |         - |
| 'Single-Part PartitionKey'         |  34.5408 ns | 0.7193 ns |  0.7995 ns |    2 | 0.0363 |     152 B |
| 'Hierarchical 2-Part PartitionKey' | 146.5284 ns | 1.3211 ns |  1.1711 ns |    3 | 0.1280 |     536 B |
| 'Hierarchical 3-Part PartitionKey' | 202.5363 ns | 3.5737 ns |  3.3429 ns |    4 | 0.1605 |     672 B |
| 'Hierarchical 4-Part PartitionKey' | 257.2770 ns | 5.1503 ns | 12.6338 ns |    5 | 0.1931 |     808 B |

### 1.2 LINQ Expression Rewriter (`CosmosExpressionRewriterBenchmarks`)
Measures the AST visitor rewriting LINQ expressions from entity models to Cosmos document models.

| Method                                            | Mean       | Error    | StdDev   | Rank | Gen0   | Allocated |
|-------------------------------------------------- |-----------:|---------:|---------:|-----:|-------:|----------:|
| 'Rewrite Simple Single Property Predicate'        |   402.7 ns |  6.32 ns |  5.91 ns |    1 | 0.1144 |     480 B |
| 'Rewrite Two-Term And Predicate'                  |   609.4 ns |  4.17 ns |  3.70 ns |    2 | 0.1526 |     640 B |
| 'Rewrite Nested Property + Envelope Predicate'    |   794.1 ns |  5.74 ns |  5.37 ns |    3 | 0.1831 |     768 B |
| 'Rewrite Complex Composite Predicate (5 Clauses)' | 1,459.5 ns | 28.84 ns | 35.42 ns |    4 | 0.2880 |    1208 B |

### 1.3 JSON Serialization Roundtrips (`CosmosSerializationBenchmarks`)
Measures `AquilaCosmosJsonSerializer.ToStream<T>` and `FromStream<T>` UTF-8 stream serialization.

| Method                                      | Mean      | Error     | StdDev    | Rank | Gen0   | Allocated |
|-------------------------------------------- |----------:|----------:|----------:|-----:|-------:|----------:|
| 'ToStream (Small Document Envelope)'        |  1.731 μs | 0.0145 μs | 0.0135 μs |    1 | 1.5564 |   6.36 KB |
| 'FromStream (Small Document Envelope)'      |  2.499 μs | 0.0236 μs | 0.0221 μs |    2 | 1.6174 |   6.62 KB |
| 'Roundtrip SerDe (Small Document Envelope)' |  4.558 μs | 0.0415 μs | 0.0388 μs |    3 | 3.1586 |  12.91 KB |
| 'ToStream (Large Document Envelope)'        |  4.978 μs | 0.0448 μs | 0.0419 μs |    4 | 2.4414 |   9.98 KB |
| 'FromStream (Large Document Envelope)'      |  7.548 μs | 0.0872 μs | 0.0815 μs |    5 | 2.0752 |   8.48 KB |
| 'Roundtrip SerDe (Large Document Envelope)' | 13.035 μs | 0.0842 μs | 0.0746 μs |    6 | 4.5013 |   18.4 KB |


## Event Sourcing Engine

### 2.1 Event Store Appends (`EventStoreAppendBenchmarks`)
Measures session-level `StartStream` + `SaveChangesAsync`, stream append throughput, and direct SPI storage calls.

| Method                                     | BatchSize | Mean       | Error     | StdDev    | Median     | Rank | Gen0   | Gen1   | Allocated |
|------------------------------------------- |---------- |-----------:|----------:|----------:|-----------:|-----:|-------:|-------:|----------:|
| 'StorageProvider Direct AppendEventsAsync' | 5         |   3.721 μs | 0.1850 μs | 0.5278 μs |   3.597 μs |    1 | 0.0916 | 0.0458 |     584 B |
| 'StorageProvider Direct AppendEventsAsync' | 50        |   4.859 μs | 0.0955 μs | 0.1369 μs |   4.894 μs |    2 | 0.2975 | 0.0992 |    1912 B |
| 'StorageProvider Direct AppendEventsAsync' | 200       |   8.920 μs | 0.1740 μs | 0.2004 μs |   8.930 μs |    3 | 0.9918 | 0.3357 |    6232 B |
| 'StartStream + SaveChangesAsync'           | 5         |   9.523 μs | 0.1855 μs | 0.1985 μs |   9.523 μs |    4 | 0.5188 | 0.1373 |    3345 B |
| 'Append to Stream + SaveChangesAsync'      | 5         |  10.390 μs | 0.1962 μs | 0.2015 μs |  10.363 μs |    5 | 0.8087 | 0.2289 |    5161 B |
| 'Append to Stream + SaveChangesAsync'      | 50        |  52.713 μs | 1.0535 μs | 1.1272 μs |  52.664 μs |    6 | 2.3804 | 0.7935 |   15083 B |
| 'StartStream + SaveChangesAsync'           | 50        |  53.017 μs | 1.0556 μs | 2.3391 μs |  51.816 μs |    6 | 2.0752 | 1.0376 |   13090 B |
| 'StartStream + SaveChangesAsync'           | 200       | 181.222 μs | 0.7691 μs | 0.6422 μs | 181.195 μs |    7 | 7.0801 | 3.4180 |   45256 B |
| 'Append to Stream + SaveChangesAsync'      | 200       | 188.875 μs | 3.7069 μs | 4.9486 μs | 189.284 μs |    7 | 7.3242 | 3.6621 |   47248 B |

### 2.2 Aggregate Rehydration (`AggregateRehydrationBenchmarks`)
Compares full event stream replay vs snapshot-accelerated rehydration.

| Method                                             | StreamLength | Mean       | Error     | StdDev    | Median     | Rank | Gen0    | Gen1   | Allocated |
|--------------------------------------------------- |------------- |-----------:|----------:|----------:|-----------:|-----:|--------:|-------:|----------:|
| 'AggregateStreamAsync (Snapshot Accelerated)'      | 10           |   1.161 μs | 0.0123 μs | 0.0109 μs |   1.160 μs |    1 |  0.6771 |      - |   2.77 KB |
| 'AggregateStreamAsync (Full Replay, No Snapshots)' | 10           |   1.169 μs | 0.0061 μs | 0.0048 μs |   1.170 μs |    1 |  0.6771 |      - |   2.77 KB |
| 'AggregateStreamAsync (Full Replay, No Snapshots)' | 50           |   3.268 μs | 0.0368 μs | 0.0307 μs |   3.267 μs |    2 |  1.2550 |      - |   5.13 KB |
| 'AggregateStreamAsync (Full Replay, No Snapshots)' | 200          |  11.297 μs | 0.1988 μs | 0.4446 μs |  11.107 μs |    3 |  3.2959 |      - |  13.52 KB |
| 'AggregateStreamAsync (Snapshot Accelerated)'      | 50           |  13.091 μs | 0.1451 μs | 0.1727 μs |  13.045 μs |    4 |  2.1667 |      - |   8.95 KB |
| 'AggregateStreamAsync (Full Replay, No Snapshots)' | 500          |  27.251 μs | 0.5295 μs | 0.8551 μs |  27.279 μs |    5 |  7.1411 |      - |  29.21 KB |
| 'AggregateStreamAsync (Snapshot Accelerated)'      | 200          |  47.716 μs | 0.9698 μs | 2.8595 μs |  46.217 μs |    6 |  5.0049 |      - |  20.67 KB |
| 'AggregateStreamAsync (Snapshot Accelerated)'      | 500          | 117.588 μs | 2.3194 μs | 3.8752 μs | 115.904 μs |    7 | 10.4980 | 0.9766 |  43.01 KB |

### 2.3 Event Upcasting (`EventUpcastingBenchmarks`)
Evaluates schema migration pipeline overhead (identity no-op, single-step V1 -> V2, chained V1 -> V2 -> V3).

| Method                                                      | Mean        | Error     | StdDev    | Rank | Gen0   | Gen1   | Allocated |
|------------------------------------------------------------ |------------:|----------:|----------:|-----:|-------:|-------:|----------:|
| 'Registry Direct Upcast (No-Op Identity)'                   |    108.2 ns |   1.06 ns |   0.94 ns |    1 |      - |      - |         - |
| 'Registry Direct Upcast (Single Step V1 -> V2)'             |    370.9 ns |   2.63 ns |   2.33 ns |    2 | 0.0439 |      - |     184 B |
| 'Registry Direct Upcast (Chained V1 -> V2 -> V3)'           |    397.6 ns |   3.81 ns |   3.18 ns |    3 | 0.0591 |      - |     248 B |
| 'FetchStream 100 Events (No Upcasting)'                     |  2,196.3 ns |   8.90 ns |   7.43 ns |    4 | 1.1139 |      - |    4672 B |
| 'FetchStream 100 Events (Single Step Upcasting V1 -> V2)'   | 41,183.7 ns | 188.19 ns | 166.82 ns |    5 | 5.6763 | 0.0610 |   23930 B |
| 'FetchStream 100 Events (Chained Upcasting V1 -> V2 -> V3)' | 44,865.8 ns | 834.26 ns | 780.37 ns |    6 | 7.2021 | 0.0610 |   30330 B |


## Session & Change Tracking

### 3.1 Session Tracking Modes (`SessionTrackingModeBenchmarks`)
Evaluates throughput and allocation profiles of `Lightweight`, `IdentityMap`, and `DirtyTracking` modes.

| Method                                      | BatchSize | Mode          | Mean         | Error      | StdDev     | Median       | Rank | Gen0     | Gen1     | Allocated |
|-------------------------------------------- |---------- |-------------- |-------------:|-----------:|-----------:|-------------:|-----:|---------:|---------:|----------:|
| 'LoadAsync Cold (From Storage)'             | 1         | Lightweight   |     5.535 μs |  0.0588 μs |  0.0491 μs |     5.526 μs |    1 |   1.3657 |        - |    5.6 KB |
| 'LoadAsync Cold (From Storage)'             | 1         | IdentityMap   |     5.870 μs |  0.1156 μs |  0.1899 μs |     5.798 μs |    2 |   1.6785 |        - |   6.87 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 1         | IdentityMap   |     6.190 μs |  0.0894 μs |  0.0836 μs |     6.166 μs |    2 |   1.6937 |        - |   6.94 KB |
| 'Store + SaveChangesAsync Batch'            | 1         | Lightweight   |     6.279 μs |  0.0430 μs |  0.0359 μs |     6.263 μs |    2 |   1.4572 |        - |   5.98 KB |
| 'Store + SaveChangesAsync Batch'            | 1         | IdentityMap   |     6.963 μs |  0.1377 μs |  0.2184 μs |     6.930 μs |    3 |   1.7700 |        - |   7.24 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 1         | DirtyTracking |     7.771 μs |  0.1417 μs |  0.2206 μs |     7.654 μs |    3 |   1.9379 |        - |   7.96 KB |
| 'LoadAsync Cold (From Storage)'             | 1         | DirtyTracking |     7.826 μs |  0.1553 μs |  0.2324 μs |     7.800 μs |    3 |   1.9226 |        - |   7.89 KB |
| 'Store + SaveChangesAsync Batch'            | 1         | DirtyTracking |    10.900 μs |  0.2085 μs |  0.2318 μs |    10.816 μs |    4 |   2.3346 |        - |   9.58 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 1         | Lightweight   |    11.483 μs |  0.2281 μs |  0.2441 μs |    11.494 μs |    4 |   2.4261 |        - |   9.95 KB |
| 'LoadAsync Cold (From Storage)'             | 10        | Lightweight   |    51.954 μs |  0.9864 μs |  1.1359 μs |    51.583 μs |    5 |  10.9253 |        - |   44.7 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 10        | IdentityMap   |    55.604 μs |  1.0717 μs |  1.2342 μs |    55.703 μs |    6 |  11.6577 |   0.1221 |  47.66 KB |
| 'LoadAsync Cold (From Storage)'             | 10        | IdentityMap   |    57.662 μs |  1.1267 μs |  1.1066 μs |    57.465 μs |    6 |  11.4746 |        - |  46.95 KB |
| 'Store + SaveChangesAsync Batch'            | 10        | Lightweight   |    61.166 μs |  0.6545 μs |  0.5110 μs |    61.278 μs |    7 |  11.4746 |   0.3662 |  47.28 KB |
| 'Store + SaveChangesAsync Batch'            | 10        | IdentityMap   |    62.891 μs |  0.9677 μs |  0.9051 μs |    63.155 μs |    7 |  12.0850 |        - |  49.53 KB |
| 'LoadAsync Cold (From Storage)'             | 10        | DirtyTracking |    74.544 μs |  1.4356 μs |  1.7630 μs |    74.556 μs |    8 |  13.9160 |        - |  57.19 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 10        | DirtyTracking |    75.761 μs |  1.1919 μs |  1.1149 μs |    75.809 μs |    8 |  14.1602 |        - |  57.89 KB |
| 'Store + SaveChangesAsync Batch'            | 10        | DirtyTracking |   100.725 μs |  1.7162 μs |  1.6053 μs |   100.213 μs |    9 |  17.3340 |        - |  71.13 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 10        | Lightweight   |   108.199 μs |  2.1012 μs |  2.5804 μs |   108.060 μs |   10 |  21.4844 |        - |  88.08 KB |
| 'LoadAsync Cold (From Storage)'             | 100       | Lightweight   |   550.094 μs |  9.5854 μs |  9.4142 μs |   547.872 μs |   11 | 102.5391 |  13.6719 | 435.78 KB |
| 'LoadAsync Cold (From Storage)'             | 100       | IdentityMap   |   569.085 μs | 11.1806 μs | 19.8735 μs |   570.370 μs |   11 | 106.4453 |  12.6953 | 456.06 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 100       | IdentityMap   |   581.474 μs | 11.2367 μs | 10.5108 μs |   577.246 μs |   11 | 112.3047 |   3.9063 | 461.78 KB |
| 'Store + SaveChangesAsync Batch'            | 100       | Lightweight   |   632.201 μs | 12.1949 μs | 14.5171 μs |   628.516 μs |   12 |  98.6328 |  44.9219 | 459.87 KB |
| 'Store + SaveChangesAsync Batch'            | 100       | IdentityMap   |   666.993 μs | 13.2747 μs | 14.7547 μs |   664.242 μs |   12 |  96.6797 |  48.8281 | 488.77 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 100       | DirtyTracking |   753.452 μs | 14.3663 μs | 14.7531 μs |   745.410 μs |   13 | 130.8594 |  38.0859 | 563.95 KB |
| 'LoadAsync Cold (From Storage)'             | 100       | DirtyTracking |   778.353 μs | 11.3642 μs | 10.0741 μs |   778.742 μs |   13 | 125.9766 |  41.9922 | 565.98 KB |
| 'LoadAsync Warm Repeated (IdentityMap Hit)' | 100       | Lightweight   | 1,061.154 μs | 16.3539 μs | 14.4973 μs | 1,054.954 μs |   14 | 210.9375 |  37.1094 | 869.53 KB |
| 'Store + SaveChangesAsync Batch'            | 100       | DirtyTracking | 1,066.743 μs | 21.3282 μs | 29.1943 μs | 1,072.576 μs |   14 | 121.0938 | 107.4219 | 693.57 KB |

### 3.2 Dirty Checking & JSON Snapshots (`DirtyCheckingBenchmarks`)
Measures UTF-8 snapshotting cost and change detection diffing across entity mutation ratios.

| Method                                                  | Mean           | Error        | StdDev       | Rank | Gen0     | Gen1     | Allocated |
|-------------------------------------------------------- |---------------:|-------------:|-------------:|-----:|---------:|---------:|----------:|
| 'Snapshot Baseline (Small Document UTF8)'               |       495.1 ns |      1.82 ns |      1.52 ns |    1 |   0.1392 |        - |     584 B |
| 'Snapshot Baseline (Large Document UTF8)'               |     2,212.6 ns |     41.41 ns |     76.76 ns |    2 |   0.2975 |        - |    1248 B |
| 'Dirty Diff Check (100 Tracked Entities, 100% Mutated)' |    55,523.2 ns |    155.33 ns |    137.70 ns |    3 |  15.6860 |        - |   65744 B |
| 'Dirty Diff Check (100 Tracked Entities, 0% Mutated)'   |    56,123.7 ns |    306.54 ns |    271.74 ns |    3 |  15.6860 |        - |   65744 B |
| 'Dirty Diff Check (100 Tracked Entities, 50% Mutated)'  |    57,643.8 ns |    261.82 ns |    244.91 ns |    3 |  15.6860 |        - |   65744 B |
| 'Session DirtyTracking SaveChangesAsync (0% Mutated)'   | 1,003,787.6 ns | 11,673.65 ns | 10,919.54 ns |    4 | 132.8125 |  85.9375 |  719376 B |
| 'Session DirtyTracking SaveChangesAsync (50% Mutated)'  | 1,052,957.4 ns |  8,243.39 ns |  7,307.55 ns |    5 | 125.0000 | 101.5625 |  718000 B |
| 'Session DirtyTracking SaveChangesAsync (100% Mutated)' | 1,074,615.0 ns |  6,281.20 ns |  5,245.08 ns |    5 | 132.8125 |  93.7500 |  734944 B |


## Projections & Queries

### 4.1 Projection Execution (`ProjectionExecutionBenchmarks`)
Measures in-memory folding and read-model persistence for single-stream and multi-stream projections.

| Method                                                             | Mean         | Error        | StdDev     | Rank | Gen0    | Allocated |
|------------------------------------------------------------------- |-------------:|-------------:|-----------:|-----:|--------:|----------:|
| 'MultiStreamProjection Apply In-Memory (Single Event)'             |     10.77 ns |     0.283 ns |   0.265 ns |    1 |  0.0115 |      48 B |
| 'SingleStreamProjection ApplyEvent (Single Event)'                 |    126.10 ns |     2.557 ns |   2.843 ns |    2 |  0.0725 |     304 B |
| 'MultiStreamProjection ProcessEventAsync (Full Session Execution)' |    815.71 ns |     9.636 ns |   8.542 ns |    3 |  0.3786 |    1584 B |
| 'SingleStreamProjection Fold (100 Events)'                         |  2,183.73 ns |    43.679 ns |  99.480 ns |    4 |  0.7515 |    3152 B |
| 'MultiStreamProjection ProcessEventAsync (100 Events Batch)'       | 58,376.15 ns | 1,091.274 ns | 967.386 ns |    5 | 10.5591 |   44352 B |

### 4.2 Compiled Queries & Dynamic LINQ (`CompiledQueryBenchmarks`)
Compares `CompiledQueryCache` compilation vs cached delegate execution and ad-hoc LINQ expression queries.

| Method                                                      | Mean       | Error    | StdDev   | Rank | Gen0     | Gen1     | Allocated  |
|------------------------------------------------------------ |-----------:|---------:|---------:|-----:|---------:|---------:|-----------:|
| 'Session.QueryAsync (Ad-Hoc Predicate)'                     |   276.6 μs |  3.49 μs |  4.41 μs |    1 |   6.8359 |   1.9531 |   30.84 KB |
| 'Ad-Hoc LINQ Lambda Where Execution'                        |   574.4 μs | 11.47 μs | 26.12 μs |    2 |   2.9297 |   0.9766 |   11.97 KB |
| 'CompiledQueryCache.Execute (Cached Delegate Steady-State)' |   801.8 μs |  8.65 μs |  7.67 μs |    3 |   1.9531 |        - |   13.07 KB |
| 'CompiledQueryCache Compilation (Cache Miss Cold Path)'     | 4,981.9 μs | 75.67 μs | 59.08 μs |    4 |        - |        - |    24.1 KB |
| 'Session.QueryAsync (Compiled Query Cached)'                | 5,137.2 μs | 28.68 μs | 25.43 μs |    4 | 468.7500 | 312.5000 | 2739.09 KB |


## Patch API

### 5.1 JSON Pointer Patch Operations (`PatchExpressionBenchmarks`)
Measures AST compilation and patch operation building for single operations, nested pointer paths, and compound batches.

| Method                                         | Mean       | Error    | StdDev   | Rank | Gen0   | Allocated |
|----------------------------------------------- |-----------:|---------:|---------:|-----:|-------:|----------:|
| 'Build Single Set Operation'                   |   477.0 ns |  8.86 ns |  8.29 ns |    1 | 0.2155 |     904 B |
| 'Build Single Remove Operation'                |   534.1 ns |  3.36 ns |  3.14 ns |    2 | 0.2155 |     904 B |
| 'Build Single Append Operation'                |   552.3 ns |  3.37 ns |  3.15 ns |    3 | 0.2155 |     904 B |
| 'Build Single Increment Operation'             |   585.5 ns | 11.52 ns | 13.27 ns |    4 | 0.2193 |     920 B |
| 'Build Nested Property Pointer (Address.City)' |   688.3 ns |  2.71 ns |  2.26 ns |    5 | 0.2842 |    1192 B |
| 'Session.Patch Fluent Registration'            | 1,355.2 ns | 23.81 ns | 21.11 ns |    6 | 0.7172 |    3000 B |
| 'Build Multi-Operation Compound Patch (4 Ops)' | 2,179.3 ns | 43.59 ns | 98.38 ns |    7 | 0.7935 |    3328 B |

---

## Maintenance & Automation

This baseline document can be regenerated or updated at any time using the automated runner script:

```bash
# Run all benchmarks and aggregate baseline results
python3 scripts/run-benchmarks.py

# Run a specific benchmark category and aggregate results
python3 scripts/run-benchmarks.py --filter *Cosmos*

# Fast dry-run validation
python3 scripts/run-benchmarks.py --dry-run

# Re-aggregate existing BenchmarkDotNet results without re-running benchmarks
python3 scripts/run-benchmarks.py --aggregate-only
```
