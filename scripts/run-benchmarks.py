#!/usr/bin/env python3
"""
Aquila Performance Benchmarks Runner & Aggregator
=================================================

Automates executing BenchmarkDotNet benchmarks for the Aquila Framework,
collecting generated GitHub markdown reports, comparing results against
the baseline (docs/benchmarks/BASELINE.md), and producing a timestamped
run report with comprehensive comparison metrics.

Baseline Policy:
    The baseline file (docs/benchmarks/BASELINE.md) is NEVER overwritten
    during regular runs. Future runs generate timestamped reports in
    docs/benchmarks/RUN_YYYYMMDD_HHMMSS.md with delta comparisons.
    To explicitly update the baseline, use the --set-baseline flag.

Usage:
    python3 scripts/run-benchmarks.py [options]

Examples:
    # Run all benchmarks and generate a timestamped comparison report
    python3 scripts/run-benchmarks.py

    # Run specific benchmark suite
    python3 scripts/run-benchmarks.py --filter *Cosmos*

    # Fast validation dry run
    python3 scripts/run-benchmarks.py --dry-run

    # Only aggregate & compare existing BDN results without re-running
    python3 scripts/run-benchmarks.py --aggregate-only

    # Explicitly update/overwrite the baseline file
    python3 scripts/run-benchmarks.py --set-baseline
"""

import argparse
import datetime
import glob
import os
import re
import subprocess
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_PROJECT = os.path.join(REPO_ROOT, "benchmarks", "Aquila.Benchmarks", "Aquila.Benchmarks.csproj")
DEFAULT_ARTIFACTS_DIR = os.path.join(REPO_ROOT, "BenchmarkDotNet.Artifacts", "results")
DEFAULT_BASELINE_FILE = os.path.join(REPO_ROOT, "docs", "benchmarks", "BASELINE.md")
DOCS_BENCHMARKS_DIR = os.path.join(REPO_ROOT, "docs", "benchmarks")

CATEGORY_MAPPINGS = {
    "Cosmos DB Provider": [
        ("CosmosPartitionKeyBenchmarks", "1.1 Partition Key Construction (`CosmosPartitionKeyBenchmarks`)",
         "Measures partition key generation for single-part and multi-part hierarchical partition keys."),
        ("CosmosExpressionRewriterBenchmarks", "1.2 LINQ Expression Rewriter (`CosmosExpressionRewriterBenchmarks`)",
         "Measures the AST visitor rewriting LINQ expressions from entity models to Cosmos document models."),
        ("CosmosSerializationBenchmarks", "1.3 JSON Serialization Roundtrips (`CosmosSerializationBenchmarks`)",
         "Measures `AquilaCosmosJsonSerializer.ToStream<T>` and `FromStream<T>` UTF-8 stream serialization."),
    ],
    "Event Sourcing Engine": [
        ("EventStoreAppendBenchmarks", "2.1 Event Store Appends (`EventStoreAppendBenchmarks`)",
         "Measures session-level `StartStream` + `SaveChangesAsync`, stream append throughput, and direct SPI storage calls."),
        ("AggregateRehydrationBenchmarks", "2.2 Aggregate Rehydration (`AggregateRehydrationBenchmarks`)",
         "Compares full event stream replay vs snapshot-accelerated rehydration."),
        ("EventUpcastingBenchmarks", "2.3 Event Upcasting (`EventUpcastingBenchmarks`)",
         "Evaluates schema migration pipeline overhead (identity no-op, single-step V1 -> V2, chained V1 -> V2 -> V3)."),
    ],
    "Session & Change Tracking": [
        ("SessionTrackingModeBenchmarks", "3.1 Session Tracking Modes (`SessionTrackingModeBenchmarks`)",
         "Evaluates throughput and allocation profiles of `Lightweight`, `IdentityMap`, and `DirtyTracking` modes."),
        ("DirtyCheckingBenchmarks", "3.2 Dirty Checking & JSON Snapshots (`DirtyCheckingBenchmarks`)",
         "Measures UTF-8 snapshotting cost and change detection diffing across entity mutation ratios."),
    ],
    "Projections & Queries": [
        ("ProjectionExecutionBenchmarks", "4.1 Projection Execution (`ProjectionExecutionBenchmarks`)",
         "Measures in-memory folding and read-model persistence for single-stream and multi-stream projections."),
        ("CompiledQueryBenchmarks", "4.2 Compiled Queries & Dynamic LINQ (`CompiledQueryBenchmarks`)",
         "Compares `CompiledQueryCache` compilation vs cached delegate execution and ad-hoc LINQ expression queries."),
    ],
    "Patch API": [
        ("PatchExpressionBenchmarks", "5.1 JSON Pointer Patch Operations (`PatchExpressionBenchmarks`)",
         "Measures AST compilation and patch operation building for single operations, nested pointer paths, and compound batches."),
    ],
}

KNOWN_PARAM_COLUMNS = {"BatchSize", "StreamLength", "Mode", "Size", "Count", "Length", "Ratio", "Iterations"}


def parse_latency_to_ns(val_str):
    """Parses a BDN latency string (e.g., '402.7 ns', '1.731 μs', '18.09 ms') to nanoseconds."""
    if not val_str or val_str.strip() in ("-", "NA", "N/A", "?"):
        return None
    val_str = val_str.strip().replace(",", "").replace("&mu;", "μ").replace("&#956;", "μ")
    match = re.match(r"^([0-9.]+)\s*([a-zA-Zμ]+)$", val_str)
    if not match:
        return None
    num = float(match.group(1))
    unit = match.group(2).lower()
    if unit in ("ns", "nanosecond", "nanoseconds"):
        return num
    elif unit in ("μs", "us", "microsecond", "microseconds"):
        return num * 1000.0
    elif unit in ("ms", "millisecond", "milliseconds"):
        return num * 1_000_000.0
    elif unit in ("s", "sec", "second", "seconds"):
        return num * 1_000_000_000.0
    return num


def format_latency_ns(ns_val):
    """Formats nanoseconds into an appropriate human-readable time unit."""
    if ns_val is None:
        return "-"
    if ns_val < 1000.0:
        return f"{ns_val:.2f} ns"
    elif ns_val < 1_000_000.0:
        return f"{ns_val / 1000.0:.2f} μs"
    elif ns_val < 1_000_000_000.0:
        return f"{ns_val / 1_000_000.0:.2f} ms"
    else:
        return f"{ns_val / 1_000_000_000.0:.2f} s"


def parse_memory_to_bytes(val_str):
    """Parses a BDN allocated memory string (e.g., '152 B', '6.36 KB', '1.2 MB', '-') to bytes."""
    if not val_str or val_str.strip() in ("-", "NA", "N/A", "0", "0 B"):
        return 0.0
    val_str = val_str.strip().replace(",", "")
    match = re.match(r"^([0-9.]+)\s*([a-zA-Z]+)$", val_str)
    if not match:
        return 0.0
    num = float(match.group(1))
    unit = match.group(2).upper()
    if unit in ("B", "BYTE", "BYTES"):
        return num
    elif unit in ("KB", "KILOBYTE", "KILOBYTES"):
        return num * 1024.0
    elif unit in ("MB", "MEGABYTE", "MEGABYTES"):
        return num * 1024.0 * 1024.0
    elif unit in ("GB", "GIGABYTE", "GIGABYTES"):
        return num * 1024.0 * 1024.0 * 1024.0
    return num


def format_memory_bytes(bytes_val):
    """Formats bytes into a readable unit string."""
    if bytes_val is None or bytes_val == 0.0:
        return "-"
    if bytes_val < 1024.0:
        return f"{int(round(bytes_val))} B"
    elif bytes_val < 1024.0 * 1024.0:
        return f"{bytes_val / 1024.0:.2f} KB"
    else:
        return f"{bytes_val / (1024.0 * 1024.0):.2f} MB"


def clean_markdown_cell(cell):
    """Cleans HTML entities and backticks from BDN markdown table cells."""
    return (
        cell.strip()
        .replace("&#39;", "'")
        .replace("&gt;", ">")
        .replace("&lt;", "<")
        .replace("&quot;", '"')
        .replace("`", "")
    )


def extract_tables_and_env(markdown_content):
    """
    Parses BDN report markdown or BASELINE.md and extracts structured tables.
    Returns: (list_of_parsed_tables, list_of_env_lines)
    """
    lines = markdown_content.strip().splitlines()
    tables = []
    current_table = []
    in_table = False
    env_lines = []
    in_env = False

    for line in lines:
        if line.startswith("```") and not in_env:
            in_env = True
            continue
        elif line.startswith("```") and in_env:
            in_env = False
            continue
        elif in_env:
            if line.strip():
                env_lines.append(line.strip())
            continue

        if line.strip().startswith("|"):
            in_table = True
            current_table.append(line.strip())
        elif in_table:
            if current_table:
                tables.append(parse_single_table(current_table))
                current_table = []
            in_table = False

    if current_table:
        tables.append(parse_single_table(current_table))

    return tables, env_lines


def parse_single_table(table_lines):
    """Parses a markdown table into headers and list of row dicts."""
    if len(table_lines) < 3:
        return {"headers": [], "rows": []}

    raw_headers = [clean_markdown_cell(c) for c in table_lines[0].split("|")[1:-1]]
    rows = []

    for line in table_lines[2:]:  # Skip header and separator
        if not line.strip().startswith("|"):
            continue
        cols = [clean_markdown_cell(c) for c in line.split("|")[1:-1]]
        if len(cols) != len(raw_headers):
            continue
        row_dict = dict(zip(raw_headers, cols))
        rows.append(row_dict)

    return {"headers": raw_headers, "rows": rows}


def build_benchmark_key(row_dict):
    """Creates a unique composite key for a benchmark method and its parameters."""
    method = row_dict.get("Method", "").strip("'\" ")
    params = []
    for k in sorted(row_dict.keys()):
        if k in KNOWN_PARAM_COLUMNS or k.startswith("Param"):
            params.append(f"{k}={row_dict[k]}")
    return f"{method} [{', '.join(params)}]" if params else method


def load_baseline_data(baseline_file_path):
    """
    Loads baseline benchmark metrics from BASELINE.md.
    Returns: dict mapping composite_key -> { 'mean_str', 'mean_ns', 'alloc_str', 'alloc_bytes', 'row' }
    """
    if not os.path.exists(baseline_file_path):
        return {}

    with open(baseline_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    tables, _ = extract_tables_and_env(content)
    baseline_map = {}

    for table in tables:
        headers = table["headers"]
        if "Method" not in headers or "Mean" not in headers:
            continue
        for row in table["rows"]:
            key = build_benchmark_key(row)
            mean_str = row.get("Mean", "")
            alloc_str = row.get("Allocated", "-")
            baseline_map[key] = {
                "mean_str": mean_str,
                "mean_ns": parse_latency_to_ns(mean_str),
                "alloc_str": alloc_str,
                "alloc_bytes": parse_memory_to_bytes(alloc_str),
                "row": row,
            }

    return baseline_map


def build_comparison_table(current_table_dict, baseline_map, threshold_pct=5.0):
    """
    Constructs a GitHub markdown table comparing current run with baseline metrics.
    Returns: (table_markdown, list_of_summary_items)
    """
    headers = current_table_dict["headers"]
    rows = current_table_dict["rows"]

    param_headers = [h for h in headers if h in KNOWN_PARAM_COLUMNS or h.startswith("Param")]
    other_headers = [h for h in headers if h not in ("Method", "Mean", "Allocated") and h not in param_headers]

    out_headers = ["Method"] + param_headers + [
        "Current Mean",
        "Baseline Mean",
        "Δ Latency",
        "Allocated",
        "Base Alloc",
        "Δ Alloc",
    ] + [h for h in other_headers if h in ("Rank", "Gen0", "Gen1")]

    table_lines = [
        "| " + " | ".join(out_headers) + " |",
        "| " + " | ".join([":---" if h in ("Method", "Mode") else "---:" for h in out_headers]) + " |",
    ]

    summary_items = []

    for row in rows:
        key = build_benchmark_key(row)
        curr_mean_str = row.get("Mean", "-")
        curr_mean_ns = parse_latency_to_ns(curr_mean_str)
        curr_alloc_str = row.get("Allocated", "-")
        curr_alloc_bytes = parse_memory_to_bytes(curr_alloc_str)

        base_info = baseline_map.get(key)
        if base_info and base_info["mean_ns"] is not None and curr_mean_ns is not None:
            base_mean_ns = base_info["mean_ns"]
            base_mean_str = base_info["mean_str"]
            base_alloc_str = base_info["alloc_str"]
            base_alloc_bytes = base_info["alloc_bytes"]

            # Calculate Latency Delta
            diff_ns = curr_mean_ns - base_mean_ns
            diff_pct = (diff_ns / base_mean_ns) * 100.0

            if abs(diff_pct) < 3.0:
                delta_str = f"`~ {diff_pct:+.1f}%`"
                status = "neutral"
            elif diff_pct <= -threshold_pct:
                delta_str = f"🟢 **`{diff_pct:+.1f}%`**"
                status = "improved"
            elif diff_pct >= threshold_pct:
                delta_str = f"🔴 **`{diff_pct:+.1f}%`**"
                status = "regressed"
            else:
                delta_str = f"`{diff_pct:+.1f}%`"
                status = "neutral"

            # Calculate Allocation Delta
            diff_bytes = curr_alloc_bytes - base_alloc_bytes
            if diff_bytes == 0:
                delta_alloc_str = "-"
            elif diff_bytes > 0:
                delta_alloc_str = f"🔺 `+{format_memory_bytes(diff_bytes)}`"
            else:
                delta_alloc_str = f"🔹 `-{format_memory_bytes(abs(diff_bytes))}`"

            summary_items.append({
                "key": key,
                "status": status,
                "diff_pct": diff_pct,
                "diff_ns": diff_ns,
                "curr_mean": curr_mean_str,
                "base_mean": base_mean_str,
                "curr_alloc": curr_alloc_str,
                "base_alloc": base_alloc_str,
                "diff_bytes": diff_bytes,
            })
        else:
            base_mean_str = "-"
            base_alloc_str = "-"
            delta_str = "`[NEW]`"
            delta_alloc_str = "-"
            summary_items.append({
                "key": key,
                "status": "new",
                "diff_pct": 0.0,
                "diff_ns": 0.0,
                "curr_mean": curr_mean_str,
                "base_mean": "-",
                "curr_alloc": curr_alloc_str,
                "base_alloc": "-",
                "diff_bytes": 0,
            })

        row_vals = [f"**{row.get('Method', '')}**"]
        for ph in param_headers:
            row_vals.append(row.get(ph, "-"))
        row_vals.extend([
            f"`{curr_mean_str}`",
            f"`{base_mean_str}`" if base_mean_str != "-" else "-",
            delta_str,
            f"`{curr_alloc_str}`" if curr_alloc_str != "-" else "-",
            f"`{base_alloc_str}`" if base_alloc_str != "-" else "-",
            delta_alloc_str,
        ])
        for oh in other_headers:
            if oh in ("Rank", "Gen0", "Gen1"):
                row_vals.append(row.get(oh, "-"))

        table_lines.append("| " + " | ".join(row_vals) + " |")

    return "\n".join(table_lines), summary_items


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Aquila benchmarks and generate comparison report against baseline."
    )
    parser.add_argument("--filter", "-f", help="BenchmarkDotNet glob filter pattern (e.g. *Cosmos*)")
    parser.add_argument("--job", help="BenchmarkDotNet job type (e.g. short, dry, medium, default)")
    parser.add_argument("--dry-run", action="store_true", help="Shortcut for --job dry")
    parser.add_argument("--short", action="store_true", help="Shortcut for --job short")
    parser.add_argument("--aggregate-only", action="store_true", help="Skip running dotnet; aggregate & compare existing reports")
    parser.add_argument("--set-baseline", action="store_true", help="Explicitly update docs/benchmarks/BASELINE.md with current results")
    parser.add_argument("--baseline", default=DEFAULT_BASELINE_FILE, help="Path to baseline markdown file for comparison")
    parser.add_argument("--output", "-o", help="Custom output file path for the generated report")
    parser.add_argument("--threshold", type=float, default=5.0, help="Regression alert threshold percentage (default: 5.0%%)")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="Path to benchmarks .csproj")
    parser.add_argument("--artifacts-dir", default=DEFAULT_ARTIFACTS_DIR, help="Path to BenchmarkDotNet results artifacts directory")
    parser.add_argument("--extra-args", nargs=argparse.REMAINDER, help="Additional arguments passed directly to BenchmarkDotNet")
    return parser.parse_args()


def run_benchmarks(args):
    cmd = ["dotnet", "run", "--project", args.project, "-c", "Release", "--"]

    if args.filter:
        cmd.extend(["--filter", args.filter])
    if args.dry_run:
        cmd.extend(["--job", "dry"])
    elif args.short:
        cmd.extend(["--job", "short"])
    elif args.job:
        cmd.extend(["--job", args.job])

    if args.extra_args:
        cmd.extend(args.extra_args)

    print(f"==> Executing benchmark command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        print(f"Warning: Benchmark process exited with code {result.returncode}", file=sys.stderr)
        return False
    return True


def generate_benchmark_document(artifacts_dir, baseline_file, output_file, is_baseline_mode=False, threshold_pct=5.0):
    report_files = glob.glob(os.path.join(artifacts_dir, "*-report-github.md"))
    if not report_files:
        print(f"Error: No benchmark reports found in {artifacts_dir}", file=sys.stderr)
        return False

    print(f"==> Found {len(report_files)} benchmark reports.")

    # Load existing baseline for comparisons (if not in set-baseline mode)
    baseline_map = {}
    if not is_baseline_mode and os.path.exists(baseline_file):
        print(f"==> Loading baseline from: {baseline_file}")
        baseline_map = load_baseline_data(baseline_file)
        print(f"    Loaded {len(baseline_map)} baseline benchmark entries.")

    reports_by_name = {}
    detected_env = []

    for file_path in report_files:
        basename = os.path.basename(file_path)
        match = re.search(r"([A-Za-z0-9_]+)-report-github\.md$", basename)
        suite_name = match.group(1) if match else basename

        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        tables, env = extract_tables_and_env(content)
        if tables and tables[0]["rows"]:
            reports_by_name[suite_name] = tables[0]
        if env and not detected_env:
            detected_env = env

    env_str = " | ".join(detected_env) if detected_env else "Linux | .NET 10.0 | BenchmarkDotNet v0.15.8"
    now_dt = datetime.datetime.now()
    now_str = now_dt.strftime("%B %d, %Y %H:%M:%S")
    timestamp_tag = now_dt.strftime("%Y%m%d_%H%M%S")

    all_summaries = []
    processed_suites = set()
    suite_sections = []

    for category_name, suites in CATEGORY_MAPPINGS.items():
        category_has_data = any(s[0] in reports_by_name for s in suites)
        if not category_has_data:
            continue

        suite_sections.append(f"\n## {category_name}\n")

        for suite_key, section_title, description in suites:
            if suite_key in reports_by_name:
                processed_suites.add(suite_key)
                suite_sections.append(f"### {section_title}")
                if description:
                    suite_sections.append(description + "\n")

                curr_table = reports_by_name[suite_key]
                if is_baseline_mode or not baseline_map:
                    # Render standard baseline table
                    table_md, _ = build_comparison_table(curr_table, {})
                else:
                    # Render comparison table
                    table_md, sum_items = build_comparison_table(curr_table, baseline_map, threshold_pct)
                    all_summaries.extend(sum_items)

                suite_sections.append(table_md)
                suite_sections.append("")

    # Handle remaining / ad-hoc suites
    remaining = set(reports_by_name.keys()) - processed_suites
    if remaining:
        suite_sections.append("\n## Additional Benchmark Suites\n")
        for suite_key in sorted(remaining):
            suite_sections.append(f"### `{suite_key}`\n")
            curr_table = reports_by_name[suite_key]
            if is_baseline_mode or not baseline_map:
                table_md, _ = build_comparison_table(curr_table, {})
            else:
                table_md, sum_items = build_comparison_table(curr_table, baseline_map, threshold_pct)
                all_summaries.extend(sum_items)
            suite_sections.append(table_md)
            suite_sections.append("")

    # Construct Document Header & Executive Summary
    if is_baseline_mode:
        doc_title = "# Aquila Framework — Performance Benchmarks Baseline"
        meta_sub = f"> **Generated**: {now_str}  \n> **Environment**: {env_str}  \n> **Type**: Baseline Master"
    else:
        doc_title = f"# Aquila Framework — Benchmark Run ({timestamp_tag})"
        base_rel = os.path.relpath(baseline_file, os.path.dirname(output_file)) if os.path.exists(baseline_file) else "None"
        meta_sub = (
            f"> **Run Date**: {now_str}  \n"
            f"> **Baseline Reference**: [`{os.path.basename(baseline_file)}`]({base_rel})  \n"
            f"> **Environment**: {env_str}"
        )

    doc_sections = [
        doc_title,
        "",
        meta_sub,
        "",
        "---",
        "",
        "## Executive Summary",
        "",
    ]

    if not is_baseline_mode and baseline_map and all_summaries:
        improved = [s for s in all_summaries if s["status"] == "improved"]
        regressed = [s for s in all_summaries if s["status"] == "regressed"]
        neutral = [s for s in all_summaries if s["status"] == "neutral"]
        new_items = [s for s in all_summaries if s["status"] == "new"]
        alloc_changes = [s for s in all_summaries if s["diff_bytes"] != 0]

        doc_sections.extend([
            f"**Comparison against Baseline** (`{len(all_summaries)}` benchmarks evaluated):",
            f"- 🟢 **Faster / Improved (>{threshold_pct}%)**: `{len(improved)}`",
            f"- ⚪ **Neutral / Within Jitter (±{threshold_pct}%)**: `{len(neutral)}`",
            f"- 🔴 **Regressions (>{threshold_pct}%)**: `{len(regressed)}`",
            f"- 📦 **New Benchmarks**: `{len(new_items)}`",
            f"- 💾 **Allocations Changed**: `{len(alloc_changes)}`",
            "",
        ])

        if regressed:
            doc_sections.extend([
                "### ⚠️ Regressions Detected",
                "| Benchmark | Current Mean | Baseline Mean | Δ Latency (%) | Allocated |",
                "| :--- | ---: | ---: | ---: | ---: |",
            ])
            for r in regressed:
                doc_sections.append(
                    f"| **{r['key']}** | `{r['curr_mean']}` | `{r['base_mean']}` | 🔴 **`{r['diff_pct']:+.1f}%`** | `{r['curr_alloc']}` |"
                )
            doc_sections.append("")

        if improved:
            doc_sections.extend([
                "### ⚡ Performance Improvements",
                "| Benchmark | Current Mean | Baseline Mean | Δ Latency (%) | Allocated |",
                "| :--- | ---: | ---: | ---: | ---: |",
            ])
            for imp in improved:
                doc_sections.append(
                    f"| **{imp['key']}** | `{imp['curr_mean']}` | `{imp['base_mean']}` | 🟢 **`{imp['diff_pct']:+.1f}%`** | `{imp['curr_alloc']}` |"
                )
            doc_sections.append("")
    else:
        doc_sections.extend([
            "The Aquila performance benchmarking suite evaluates latency, throughput, and memory allocations across all core subsystems:",
            "1. **Cosmos DB Provider**: Partition key construction, LINQ envelope AST rewriting, and JSON serialization.",
            "2. **Event Sourcing Engine**: Stream creation, event appending, stream fetching, upcasting evolutions, and snapshot-accelerated rehydration.",
            "3. **Session & Change Tracking**: Lightweight, IdentityMap, and DirtyTracking modes, plus UTF-8 JSON snapshotting and delta calculation.",
            "4. **Projections**: In-memory event application, single-stream aggregate folds, and multi-stream session batch processing.",
            "5. **Compiled Queries & LINQ**: Cold compilation vs steady-state cached delegate execution.",
            "6. **Patch API**: Single and compound JSON pointer patch operations.",
            "",
        ])

    doc_sections.append("---")
    doc_sections.extend(suite_sections)

    doc_sections.extend([
        "---",
        "",
        "## Maintenance & Automation",
        "",
        "Run benchmarks ad-hoc and generate timestamped comparison reports using:",
        "",
        "```bash",
        "# Run benchmarks and create timestamped report with baseline comparisons",
        "python3 scripts/run-benchmarks.py",
        "",
        "# Run a specific benchmark category",
        "python3 scripts/run-benchmarks.py --filter *Cosmos*",
        "",
        "# Fast dry-run validation",
        "python3 scripts/run-benchmarks.py --dry-run",
        "",
        "# Compare existing results without re-running",
        "python3 scripts/run-benchmarks.py --aggregate-only",
        "",
        "# Explicitly overwrite baseline with current results",
        "python3 scripts/run-benchmarks.py --set-baseline",
        "```",
        ""
    ])

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(doc_sections))

    print(f"==> Report generated successfully at: {output_file}")
    return True


def main():
    args = parse_args()

    # Determine target output file
    if args.set_baseline:
        output_file = args.baseline
        is_baseline_mode = True
    elif args.output:
        output_file = os.path.abspath(args.output)
        is_baseline_mode = False
    else:
        now_tag = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(DOCS_BENCHMARKS_DIR, f"RUN_{now_tag}.md")
        is_baseline_mode = False

    if not args.aggregate_only:
        success = run_benchmarks(args)
        if not success:
            print("Warning: Benchmarks execution failed or had errors.", file=sys.stderr)

    generate_benchmark_document(
        artifacts_dir=args.artifacts_dir,
        baseline_file=args.baseline,
        output_file=output_file,
        is_baseline_mode=is_baseline_mode,
        threshold_pct=args.threshold,
    )


if __name__ == "__main__":
    main()
