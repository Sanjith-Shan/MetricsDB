#!/bin/bash
# M0: TSBS's own loader against VictoriaMetrics in Docker, recorded in results/m0_tsbs_baseline.jsonl.
# Optional first argument: target (vm or metricsdb), default vm.
source "$(dirname "$0")/env.sh"
target=${1:-vm}
workers=${WORKERS:-2}
out=$RESULTS/m0_tsbs_baseline.jsonl
cpu_before=$(host_cpu)
if [ $target = vm ]; then
  $REPO/bench/scripts/db.sh start vm; url=http://localhost:8428/write
else
  $REPO/bench/scripts/db.sh start metricsdb; url=http://localhost:9201/write
fi
log=$W/logs/m0-$target.log
start=$(date +%s.%N)
tsbs_load_victoriametrics --file=$TSBS_FILE --urls=$url --workers=$workers --batch-size=${BATCH:-1000} > $log 2>&1
rc=$?
end=$(date +%s.%N)
summary=$(grep -E "^loaded|mean rate" $log | tr '\n' ' ' | sed 's/"/\\"/g')
version=$([ $target = vm ] && curl -s localhost:8428/metrics | grep -o 'vm_app_version{[^}]*}' | head -1 | sed 's/"/\\"/g')
printf '{"exp":"m0","ts":"%s","target":"%s","version":"%s","tool":"tsbs_load_victoriametrics","workers":%s,"batch_size":%s,"exit":%s,"seconds":%.1f,"tsbs_summary":"%s","scale":%s,"git":"%s","host_cpu_pct_before":%s,"host_cpu_pct_after":%s,"wsl_load1_after":%s}\n' \
  "$(date -Iseconds)" "$target" "$version" "$workers" "${BATCH:-1000}" "$rc" "$(awk "BEGIN{print $end - $start}")" "$summary" "$SCALE" "$GIT_REV" "${cpu_before:--1}" "$(host_cpu)" "$(cut -d' ' -f1 /proc/loadavg)" | tee -a $out
$REPO/bench/scripts/db.sh stop $target
