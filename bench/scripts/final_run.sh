#!/bin/bash
# Everything that produces a reported number, in order: exp1-exp4 (+ rollups), cluster experiments
# (exp5, late samples, fault injection, cluster ingest), JMH, then charts and NUMBERS.md.
source "$(dirname "$0")/env.sh"
cd $REPO
./gradlew -q --console=plain :server:bootJar :bench:installDist || exit 1
export IDLE_MAX_WAIT=${IDLE_MAX_WAIT:-5400}
$REPO/bench/scripts/run_benchmarks.sh
$REPO/bench/scripts/run_cluster_experiments.sh
$REPO/bench/scripts/run_jmh.sh
$W/venv/bin/python $REPO/bench/charts.py $RESULTS /mnt/c/Mac/Documents/MetricsDB/docs
python3 $REPO/bench/ledger.py $RESULTS /mnt/c/Mac/Documents/MetricsDB/NUMBERS.md
echo FINAL DONE
