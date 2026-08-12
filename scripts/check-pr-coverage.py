#!/usr/bin/env python3
import os
import sys
import xml.etree.ElementTree as ET
import subprocess

def main():
    cobertura_path = sys.argv[1] if len(sys.argv) > 1 else 'coverage/Cobertura.xml'
    threshold = float(sys.argv[2]) if len(sys.argv) > 2 else 85.0

    if not os.path.exists(cobertura_path):
        print(f"::error::Cobertura report file not found at {cobertura_path}")
        sys.exit(1)

    # Determine PR base ref or compare commit
    base_ref = os.getenv('GITHUB_BASE_REF')
    changed_files = []

    if base_ref:
        print(f"Detecting changed files against origin/{base_ref}...")
        try:
            subprocess.run(['git', 'fetch', 'origin', base_ref, '--depth=1'], check=False)
            diff_cmd = subprocess.run(['git', 'diff', '--name-only', f'origin/{base_ref}...HEAD'], capture_output=True, text=True, check=True)
            changed_files = [f.strip() for f in diff_cmd.stdout.splitlines() if f.strip()]
        except Exception as e:
            print(f"Warning fetching git diff: {e}")
            changed_files = []
    else:
        # Fallback for non-PR runs (e.g. push or manual trigger): check diff against HEAD~1 if available
        try:
            diff_cmd = subprocess.run(['git', 'diff', '--name-only', 'HEAD~1', 'HEAD'], capture_output=True, text=True)
            if diff_cmd.returncode == 0:
                changed_files = [f.strip() for f in diff_cmd.stdout.splitlines() if f.strip()]
        except Exception:
            changed_files = []

    # Filter for C# source files (e.g. src/**/*.cs)
    pr_source_files = [f for f in changed_files if f.startswith('src/') and f.endswith('.cs')]

    print(f"PR Source files modified ({len(pr_source_files)}):")
    for f in pr_source_files:
        print(f" - {f}")

    tree = ET.parse(cobertura_path)
    root = tree.getroot()

    # Map normalized file paths to coverage stats
    file_stats = {}  # filename -> [covered, total]

    for cls in root.findall('.//class'):
        filename = cls.get('filename', '').replace('\\', '/')
        lines = cls.findall('.//line')
        covered = sum(1 for l in lines if int(l.get('hits', 0)) > 0)
        total = len(lines)
        if filename not in file_stats:
            file_stats[filename] = [0, 0]
        file_stats[filename][0] += covered
        file_stats[filename][1] += total

    # Match PR source files with Cobertura file stats
    matched_stats = {}
    for pr_file in pr_source_files:
        matched = False
        for cob_file, (cov, tot) in file_stats.items():
            if pr_file.endswith(cob_file) or cob_file.endswith(pr_file):
                matched_stats[pr_file] = (cov, tot)
                matched = True
                break
        if not matched:
            matched_stats[pr_file] = (0, 0)

    total_pr_covered = 0
    total_pr_coverable = 0

    if pr_source_files:
        for pr_file, (cov, tot) in matched_stats.items():
            total_pr_covered += cov
            total_pr_coverable += tot
    else:
        for cov, tot in file_stats.values():
            total_pr_covered += cov
            total_pr_coverable += tot

    overall_pct = (total_pr_covered / total_pr_coverable * 100.0) if total_pr_coverable > 0 else 100.0

    print("\n--- PR Coverage Breakdown ---")
    if pr_source_files:
        print(f"Targeting {len(pr_source_files)} modified file(s) in PR:")
        for pr_file, (cov, tot) in matched_stats.items():
            pct = (cov / tot * 100.0) if tot > 0 else 100.0
            print(f"  {pr_file}: {cov}/{tot} lines ({pct:.1f}%)")
    else:
        print("No C# source files modified in PR diff. Evaluated overall repository coverage.")

    print(f"\nFinal Coverage: {overall_pct:.2f}% (Required: {threshold:.1f}%)")

    # Generate Github Step Summary markdown if environment variable set
    github_summary = os.getenv('GITHUB_STEP_SUMMARY')
    if github_summary:
        with open(github_summary, 'a') as f:
            f.write(f"\n### 📊 PR Code Coverage Analysis\n\n")
            f.write(f"- **Coverage Target**: `{threshold:.1f}%`\n")
            f.write(f"- **Achieved Coverage**: `{overall_pct:.2f}%` ({total_pr_covered} / {total_pr_coverable} lines)\n\n")
            if pr_source_files:
                f.write("| Modified Source File | Covered / Total | Coverage % |\n")
                f.write("|:---|:---:|:---:|\n")
                for pr_file, (cov, tot) in matched_stats.items():
                    pct = (cov / tot * 100.0) if tot > 0 else 100.0
                    status = "✅" if pct >= threshold else "❌"
                    f.write(f"| `{pr_file}` | {cov} / {tot} | {pct:.1f}% {status} |\n")

    if overall_pct < threshold:
        print(f"::error::PR Code Coverage ({overall_pct:.2f}%) is below the required threshold of {threshold:.1f}%!")
        sys.exit(1)
    else:
        print("✅ Coverage check PASSED!")

if __name__ == '__main__':
    main()
