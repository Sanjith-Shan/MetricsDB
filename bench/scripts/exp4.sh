#!/bin/bash
# exp4 on its own: load the benchmark day into MetricsDB and VictoriaMetrics, then diff every query.
source "$(dirname "$0")/env.sh"
db=$REPO/bench/scripts/db.sh
$db start metricsdb && $BENCH load --file $TSBS_FILE --url http://localhost:9201/write --workers 4 --batch-lines 1000 --label exp4-load > /dev/null
curl -s -XPOST localhost:9201/admin/flush > /dev/null
$db start vm && $BENCH load --file $TSBS_FILE --url http://localhost:8428/write --workers 4 --batch-lines 1000 --label exp4-load > /dev/null
curl -s localhost:8428/internal/force_flush; sleep 10
$BENCH diff --a http://localhost:9201 --b http://localhost:8428 --queries $DATA/q-victoriametrics --out $RESULTS/exp4.jsonl | tail -1 | cut -c1-300
$db stop metricsdb; $db stop vm
