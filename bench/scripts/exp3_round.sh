#!/bin/bash
# One more exp3 round with all three databases back to back, so their rows share host conditions.
source "$(dirname "$0")/env.sh"
cd $REPO
export IDLE_MAX_WAIT=600 IDLE_PCT=50
db=$REPO/bench/scripts/db.sh
QV=$DATA/q-victoriametrics
$db start metricsdb && $BENCH load --file $TSBS_FILE --url http://localhost:9201/write --workers 4 --batch-lines 1000 --label exp3-round > /dev/null
curl -s -XPOST localhost:9201/admin/flush > /dev/null
wait_idle; HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:9201 --db metricsdb --queries $QV --out $RESULTS/exp3.jsonl > /dev/null
$REPO/bench/scripts/exp_rollups.sh
wait_idle; HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:9201 --db metricsdb-rollups-off --label metricsdb-rollups-off --suffix "&rollup=off" --queries $QV --out $RESULTS/exp3.jsonl > /dev/null
$db stop metricsdb
$db start vm && $BENCH load --file $TSBS_FILE --url http://localhost:8428/write --workers 4 --batch-lines 1000 --label exp3-round > /dev/null
curl -s localhost:8428/internal/force_flush; sleep 10
wait_idle; HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:8428 --db victoriametrics --queries $QV --out $RESULTS/exp3.jsonl > /dev/null
$db stop vm
$db start influx && curl -s -XPOST localhost:8086/query --data-urlencode "q=CREATE DATABASE benchmark" > /dev/null
$BENCH load --file $INFLUX_FILE --url "http://localhost:8086/write?db=benchmark" --workers 4 --batch-lines 1000 --label exp3-round > /dev/null
sleep 60
wait_idle; HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:8086 --db influxdb --queries $DATA/q-influx --suffix "&db=benchmark" --out $RESULTS/exp3.jsonl > /dev/null
$db stop influx
echo ROUND DONE
