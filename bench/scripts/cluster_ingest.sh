#!/bin/bash
# Ingest through the router into 3 storage nodes at RF=2, all on this machine (exp2, cluster row).
source "$(dirname "$0")/env.sh"
LINES=${LINES:-2000000}
C=$W/run/cluster; rm -rf $C; mkdir -p $C
pids=()
for i in 0 1 2; do
  nohup java -Xmx1g -jar $JAR --server.port=$((9211+i)) --metricsdb.role=storage --metricsdb.node-name=n$i \
    --metricsdb.data-dir=$C/n$i > $C/n$i.log 2>&1 & pids+=($!)
done
nohup java -Xmx768m -jar $JAR --server.port=9201 --metricsdb.role=router --metricsdb.node-name=router \
  --metricsdb.data-dir=$C/router --metricsdb.cluster.replication-factor=2 \
  --metricsdb.cluster.nodes=n0=http://127.0.0.1:9211,n1=http://127.0.0.1:9212,n2=http://127.0.0.1:9213 > $C/router.log 2>&1 & pids+=($!)
for p in 9211 9212 9213 9201; do for k in $(seq 1 240); do curl -sf localhost:$p/internal/health >/dev/null && break; sleep 0.5; done; done
cpu0=$(host_cpu)
HOST_CPU_PCT=$cpu0 $BENCH load --file $TSBS_FILE --url http://localhost:9201/write --workers ${WORKERS:-4} --batch-lines ${BATCH:-1000} \
  --limit-lines $LINES --label "metricsdb-3-nodes-rf2" --exp exp2 > $W/logs/exp2-cluster.json
python3 - $W/logs/exp2-cluster.json $RESULTS "$GIT_REV" "$cpu0" "$(host_cpu)" <<'PY'
import json, sys
src, results, git, c0, c1 = sys.argv[1:]
row = json.loads(open(src).read().strip().splitlines()[-1])
row.update({"db": "metricsdb-cluster", "nodes": 3, "rf": 2, "git": git,
            "host_cpu_pct_before": float(c0), "host_cpu_pct_after": float(c1),
            "note": "router and 3 storage nodes share the 2-vCPU WSL VM with the loader; every sample is stored twice"})
row["machine"]["git"] = git
open(results + "/exp2.jsonl", "a").write(json.dumps(row) + "\n")
print(json.dumps({"samples_per_s": row["samples_per_s"], "p99_ms": row["batch_latency_ms"]["p99"]}))
PY
for i in 0 1 2; do curl -s localhost:$((9211+i))/admin/stats | head -c 160; echo; done
kill ${pids[@]}; sleep 3; rm -rf $C
