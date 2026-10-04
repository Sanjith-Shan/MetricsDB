#!/bin/bash
# After the rollup-plus-raw-edges query path: MetricsDB's exp3 again (rollups auto, and off for
# comparison), the rollup experiment, and exp4 against VictoriaMetrics.
source "$(dirname "$0")/env.sh"
cd $REPO
./gradlew -q --console=plain :server:bootJar :bench:installDist || exit 1
export IDLE_MAX_WAIT=600 IDLE_PCT=50
db=$REPO/bench/scripts/db.sh
$db start metricsdb
$BENCH load --file $TSBS_FILE --url http://localhost:9201/write --workers 4 --batch-lines 1000 --label requery > /dev/null
curl -s -XPOST localhost:9201/admin/flush > /dev/null
wait_idle
HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:9201 --db metricsdb --queries $DATA/q-victoriametrics --out $RESULTS/exp3.jsonl
wait_idle
HOST_CPU_PCT=$(host_cpu) $BENCH querybench --url http://localhost:9201 --db metricsdb-rollups-off --label metricsdb-rollups-off --suffix "&rollup=off" --queries $DATA/q-victoriametrics --out $RESULTS/exp3.jsonl
$REPO/bench/scripts/exp_rollups.sh
$db start vm
$BENCH load --file $TSBS_FILE --url http://localhost:8428/write --workers 4 --batch-lines 1000 --label requery > /dev/null
curl -s localhost:8428/internal/force_flush; sleep 10
$BENCH diff --a http://localhost:9201 --b http://localhost:8428 --queries $DATA/q-victoriametrics --out $RESULTS/exp4.jsonl | tail -1 | cut -c1-200
$db stop vm; $db stop metricsdb
echo REQUERY DONE
