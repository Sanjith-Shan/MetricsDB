#!/bin/bash
# exp1 to exp4 for MetricsDB, VictoriaMetrics and InfluxDB on the same machine and data.
# Order: MetricsDB (load, size, queries), VictoriaMetrics (load, size, queries, answer diff
# against MetricsDB), then InfluxDB. One comparison database runs at a time.
source "$(dirname "$0")/env.sh"
set -x
QV=$DATA/q-victoriametrics; QI=$DATA/q-influx
db=$REPO/bench/scripts/db.sh
REPEAT=${REPEAT:-1}
for r in $(seq 1 $REPEAT); do
  wait_idle
  KEEP=1 $REPO/bench/scripts/exp_ingest.sh metricsdb metricsdb
  wait_idle
  HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:9201 --db metricsdb --queries $QV --out $RESULTS/exp3.jsonl
  $REPO/bench/scripts/exp_rollups.sh
  KEEP=1 $db stop metricsdb   # data kept for the diff
done
# MetricsDB with the WAL fsync off, which is how VictoriaMetrics acknowledges by default (it flushes
# buffered data every few seconds), as a second, like-for-like ingest row
wait_idle
MDB_ARGS="--metricsdb.storage.wal-sync=none" KEEP=1 $REPO/bench/scripts/exp_ingest.sh metricsdb metricsdb-fsync-off
KEEP=1 $REPO/bench/scripts/db.sh stop metricsdb
for r in $(seq 1 $REPEAT); do
  wait_idle
  KEEP=1 $REPO/bench/scripts/exp_ingest.sh vm victoriametrics
  wait_idle
  HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:8428 --db victoriametrics --queries $QV --out $RESULTS/exp3.jsonl
done
# exp4: restart MetricsDB on its loaded data and diff every benchmark query against VictoriaMetrics
nohup java -Xmx2g -jar $JAR --server.port=9201 --metricsdb.data-dir=$W/run/metricsdb/data > $W/run/metricsdb/node2.log 2>&1 &
echo $! > $W/run/metricsdb/pid
for i in $(seq 1 240); do curl -sf localhost:9201/internal/health >/dev/null && break; sleep 0.5; done
$BENCH diff --a http://localhost:9201 --b http://localhost:8428 --queries $QV --out $RESULTS/exp4.jsonl
$db stop metricsdb; $db stop vm
for r in $(seq 1 $REPEAT); do
  wait_idle
  KEEP=1 $REPO/bench/scripts/exp_ingest.sh influx influxdb
  wait_idle
  HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:8086 --db influxdb --queries $QI --suffix "&db=benchmark" --out $RESULTS/exp3.jsonl
  $db stop influx
done
echo ALL DONE
