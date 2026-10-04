#!/bin/bash
# Everything that produces a reported number: exp1-exp4 and rollups, exp5 and cluster ingest,
# JMH, then charts and NUMBERS.md. Late samples and fault injection were recorded earlier with
# run_cluster_experiments.sh (results/ooo.jsonl, results/faults.jsonl).
source "$(dirname "$0")/env.sh"
cd $REPO
./gradlew -q --console=plain :server:bootJar :bench:installDist || exit 1
export IDLE_MAX_WAIT=${IDLE_MAX_WAIT:-900} IDLE_PCT=${IDLE_PCT:-50}
$REPO/bench/scripts/run_benchmarks.sh
wait_idle
for rf in 1 2; do
  $BENCH exp5 --jar $JAR --rf $rf --series 1000 --ticks-per-s 10 --kill-at 20 --restart-at 40 --end-at 60 \
    --work $W/run/exp5 --out $RESULTS/exp5.jsonl --timeline $RESULTS/exp5_timeline.jsonl
done
$BENCH exp5 --jar $JAR --rf 2 --wipe --series 1000 --ticks-per-s 10 --kill-at 20 --restart-at 40 --end-at 60 \
  --work $W/run/exp5 --out $RESULTS/exp5.jsonl --timeline $RESULTS/exp5_timeline.jsonl --label rf2-wipe
wait_idle
$REPO/bench/scripts/cluster_ingest.sh
$REPO/bench/scripts/run_jmh.sh
$W/venv/bin/python $REPO/bench/charts.py $RESULTS /mnt/c/Mac/Documents/MetricsDB/docs
python3 $REPO/bench/ledger.py $RESULTS /mnt/c/Mac/Documents/MetricsDB/NUMBERS.md
echo FINAL DONE
