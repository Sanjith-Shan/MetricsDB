#!/bin/bash
# exp1 (bytes per sample on disk) and exp2 (ingest throughput, batch latency) for one database:
#   exp_ingest.sh metricsdb|vm|influx [label]
# Same file, same loader, same batch size and concurrency for every database; one database runs
# at a time; its data directory is measured after its own flush and then deleted.
source "$(dirname "$0")/env.sh"
db=$1; label=${2:-$db}
workers=${WORKERS:-4}; batch=${BATCH:-1000}
cpu_before=$(host_cpu)
$REPO/bench/scripts/db.sh start $db || exit 1
case $db in
  metricsdb) url=http://localhost:9201/write ;;
  vm) url=http://localhost:8428/write ;;
  influx) curl -s -XPOST localhost:8086/query --data-urlencode "q=CREATE DATABASE benchmark" >/dev/null; url="http://localhost:8086/write?db=benchmark" ;;
esac
file=$TSBS_FILE; [ $db = influx ] && file=$INFLUX_FILE
HOST_CPU_PCT=$cpu_before $BENCH load --file $file --url "$url" --workers $workers --batch-lines $batch --label "$label" --exp exp2 > $W/logs/exp2-$db.json || exit 1
# flush to disk the way each database does it, then measure the directory
case $db in
  metricsdb) curl -s -XPOST localhost:9201/admin/flush >/dev/null; sleep 5
             bytes=$(du -sb $W/run/metricsdb/data | cut -f1)
             detail="$(curl -s localhost:9201/admin/stats | sed 's/,"blockList".*/}/') wal fsync: $(curl -s localhost:9201/metrics | grep -E '^metricsdb_wal_fsync_seconds_(count|sum|max)' | tr '
' ' ')" ;;
  vm) curl -s localhost:8428/internal/force_flush; sleep 5
      curl -s "localhost:8428/internal/force_merge?partition_prefix=2026_10"; sleep 60
      bytes=$(docker exec mdb-vm du -sb /victoria-metrics-data | cut -f1)
      detail=$(curl -s localhost:8428/metrics | grep -E '^vm_app_version|^vm_data_size_bytes' | tr '\n' ';' | sed 's/"/\\"/g') ;;
  influx) sleep 120
          bytes=$(docker exec mdb-influx du -sb /var/lib/influxdb | cut -f1)
          detail="data+wal+meta after 120 s idle, default compaction settings" ;;
esac
samples=$(grep -o '"samples":[0-9]*' $W/logs/exp2-$db.json | head -1 | cut -d: -f2)
python3 - "$W/logs/exp2-$db.json" "$RESULTS" "$db" "$label" "$bytes" "$samples" "$cpu_before" "$(host_cpu)" "$GIT_REV" "$detail" "$(stat -c %s $file)" <<'PY'
import json, sys
src, results, db, label, nbytes, samples, cpu0, cpu1, git, detail, fsize = sys.argv[1:]
row = json.loads(open(src).read().strip().splitlines()[-1])
row["db"] = db; row["exp"] = "exp2"; row["git"] = git
row["host_cpu_pct_before"] = float(cpu0 or -1); row["host_cpu_pct_after"] = float(cpu1 or -1)
row["machine"]["git"] = git
open(results + "/exp2.jsonl", "a").write(json.dumps(row) + "\n")
s = int(samples); b = int(nbytes)
exp1 = {"exp": "exp1", "ts": row["ts"], "db": db, "label": label, "samples": s, "bytes_on_disk": b,
        "bytes_per_sample": round(b / s, 3), "raw_bytes_per_sample": 16,
        "line_protocol_bytes_per_sample": round(int(fsize) / s, 2),
        "detail": detail, "git": git, "machine": row["machine"]}
open(results + "/exp1.jsonl", "a").write(json.dumps(exp1) + "\n")
print(json.dumps({"db": db, "samples_per_s": row["samples_per_s"], "p99_ms": row["batch_latency_ms"]["p99"], "bytes_per_sample": exp1["bytes_per_sample"]}))
PY
[ -n "$KEEP" ] || $REPO/bench/scripts/db.sh stop $db
