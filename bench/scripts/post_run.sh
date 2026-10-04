#!/bin/bash
# Follow-up session: ingest repeats, exp4 on its own, exp5 again, a query-overhead breakdown,
# then charts and NUMBERS.md.
source "$(dirname "$0")/env.sh"
cd $REPO
./gradlew -q --console=plain :server:bootJar :bench:installDist || exit 1
$REPO/bench/scripts/exp4.sh
ROUNDS=${ROUNDS:-3} $REPO/bench/scripts/exp2_repeats.sh
export IDLE_MAX_WAIT=600 IDLE_PCT=50
wait_idle
for rf in 1 2; do
  $BENCH exp5 --jar $JAR --rf $rf --series 1000 --ticks-per-s 10 --kill-at 20 --restart-at 40 --end-at 60 \
    --work $W/run/exp5 --out $RESULTS/exp5.jsonl --timeline $RESULTS/exp5_timeline.jsonl
done
$W/venv/bin/python $REPO/bench/charts.py $RESULTS /mnt/c/Mac/Documents/MetricsDB/docs
python3 $REPO/bench/ledger.py $RESULTS /mnt/c/Mac/Documents/MetricsDB/NUMBERS.md
echo POST DONE
