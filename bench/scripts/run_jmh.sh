#!/bin/bash
# JMH microbenchmarks (Gorilla encode/decode, posting-list intersection, WAL append), converted to
# results/jmh-gorilla.jsonl, jmh-postings.jsonl and jmh-wal.jsonl. Waits for an idle window first.
source "$(dirname "$0")/env.sh"
wait_idle
cpu=$(host_cpu)
cd $REPO
./gradlew --console=plain -q :core:jmh || exit 1
python3 - core/build/results/jmh/results.json "$RESULTS" "$GIT_REV" "$cpu" "$(host_cpu)" "$(cut -d' ' -f1 /proc/loadavg)" <<'PY'
import json, sys, time
src, results, git, c0, c1, load = sys.argv[1:]
groups = {"GorillaBench": "jmh-gorilla.jsonl", "PostingsBench": "jmh-postings.jsonl", "WalBench": "jmh-wal.jsonl"}
for r in json.load(open(src)):
    cls = r["benchmark"].split(".")[-2]
    row = {"exp": "jmh", "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "benchmark": cls + "." + r["benchmark"].split(".")[-1],
           "params": r.get("params", {}), "mode": r["mode"], "threads": r["threads"],
           "score": r["primaryMetric"]["score"], "error": r["primaryMetric"]["scoreError"], "unit": r["primaryMetric"]["scoreUnit"],
           "jvm": r.get("vmVersion"), "git": git, "host_cpu_pct_before": float(c0), "host_cpu_pct_after": float(c1),
           "wsl_load1_after": float(load), "machine": "AMD Ryzen 3 4300U, WSL2 2 vCPU"}
    open(results + "/" + groups[cls], "a").write(json.dumps(row) + "\n")
    print(row["benchmark"], row["params"], round(row["score"], 2), row["unit"])
PY
