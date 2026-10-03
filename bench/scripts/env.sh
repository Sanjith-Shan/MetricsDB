# Shared settings for the experiment scripts. Everything large lives under $W, outside the repo.
export W=${W:-$HOME/metricsdb-work}
[ -f $W/env.sh ] && source $W/env.sh
export REPO=${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
export DATA=$W/data/tsbs
export SCALE=${SCALE:-100}
export TSBS_FILE=$DATA/data-victoriametrics-s$SCALE.txt
export INFLUX_FILE=$DATA/data-influx-s$SCALE.txt
if [ -d /mnt/c/Mac/Documents/MetricsDB/results ]; then export RESULTS=${RESULTS:-/mnt/c/Mac/Documents/MetricsDB/results}; else export RESULTS=${RESULTS:-$REPO/results}; fi
export JAR=$REPO/server/build/libs/metricsdb.jar
export BENCH=$REPO/bench/build/install/bench/bin/bench
export VM_IMAGE=${VM_IMAGE:-victoriametrics/victoria-metrics:v1.153.0}
export INFLUX_IMAGE=${INFLUX_IMAGE:-influxdb:1.12.4}
export GIT_REV=$(cd /mnt/c/Mac/Documents/MetricsDB 2>/dev/null && git rev-parse --short HEAD 2>/dev/null || echo unknown)
# Windows host CPU, recorded next to every measurement (the box is shared)
PS_EXE=/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe
host_cpu() { local v; v=$($PS_EXE -NoProfile -Command "(Get-CimInstance Win32_Processor).LoadPercentage" 2>/dev/null | tr -dc '0-9'); echo ${v:--1}; }
