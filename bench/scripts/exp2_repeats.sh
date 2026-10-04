#!/bin/bash
# exp1/exp2 repeated: each round loads every configuration back to back, so a round's rows share
# the same host conditions; NUMBERS.md reports the median and range across rounds.
source "$(dirname "$0")/env.sh"
ROUNDS=${ROUNDS:-3}
export IDLE_MAX_WAIT=${IDLE_MAX_WAIT:-600} IDLE_PCT=${IDLE_PCT:-50}
for r in $(seq 1 $ROUNDS); do
  wait_idle; $REPO/bench/scripts/exp_ingest.sh metricsdb metricsdb
  wait_idle; MDB_ARGS="--metricsdb.storage.wal-sync=none" $REPO/bench/scripts/exp_ingest.sh metricsdb metricsdb-fsync-off
  wait_idle; $REPO/bench/scripts/exp_ingest.sh vm victoriametrics
  wait_idle; $REPO/bench/scripts/exp_ingest.sh influx influxdb
done
echo REPEATS DONE
