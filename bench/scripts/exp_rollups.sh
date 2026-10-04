#!/bin/bash
# Long-range queries on the loaded benchmark day, raw chunks versus 5-minute rollups.
# Expects MetricsDB running on :9201 with the data loaded and flushed.
source "$(dirname "$0")/env.sh"
python3 - "$RESULTS" "$GIT_REV" "$(host_cpu)" <<'PY'
import json, sys, time, urllib.parse, urllib.request, statistics
results, git, cpu = sys.argv[1:]
base = "http://localhost:9201"
queries = {
  "max per host per hour, 24 h": "max by (hostname) (max_over_time(cpu_usage_user[1h]))",
  "mean per region per hour, 24 h": "avg by (region) (avg_over_time(cpu_usage_user[1h]))",
  "all 10 cpu fields, max per hour, 24 h": "max by (__name__) (max_over_time({__name__=~'cpu_.*'}[1h]))",
}
start, end, step = 1790812800 + 3600, 1790812800 + 86400, 3600
stats = json.load(urllib.request.urlopen(base + "/admin/stats"))
for label, q in queries.items():
    out = {}
    for mode in ["off", "force"]:
        url = base + "/api/v1/query_range?" + urllib.parse.urlencode({"query": q, "start": start, "end": end, "step": step, "rollup": mode})
        bodies, lat = [], []
        for i in range(25):
            t0 = time.perf_counter(); body = json.load(urllib.request.urlopen(url)); lat.append((time.perf_counter() - t0) * 1000)
            bodies.append(body)
        lat = lat[5:]
        out[mode] = {"p50_ms": round(statistics.median(lat), 2), "max_ms": round(max(lat), 2),
                     "samples_loaded": bodies[-1]["stats"]["samplesLoaded"], "used_rollup": bodies[-1]["stats"]["rollup"],
                     "result": bodies[-1]["data"]["result"]}
    def flat(res): return {json.dumps(s["metric"], sort_keys=True): [(t, float(v)) for t, v in s["values"]] for s in res}
    a, b = flat(out["off"]["result"]), flat(out["force"]["result"])
    same = a.keys() == b.keys() and all(len(a[k]) == len(b[k]) and all(x[0] == y[0] for x, y in zip(a[k], b[k])) for k in a)
    maxrel = max((abs(x[1] - y[1]) / max(1e-300, abs(x[1])) for k in a if k in b for x, y in zip(a[k], b[k])), default=0.0)
    for m in out.values(): del m["result"]
    row = {"exp": "rollups", "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "query": label, "promql": q, "raw": out["off"],
           "rollup": out["force"], "same_series_and_points": same, "max_relative_difference": maxrel, "speedup": round(out["off"]["p50_ms"] / out["force"]["p50_ms"], 1),
           "rollup_bytes": stats["blockRollupBytes"], "raw_chunk_bytes": stats["blockChunkBytes"], "git": git, "host_cpu_pct": float(cpu)}
    open(results + "/rollups.jsonl", "a").write(json.dumps(row) + "\n")
    print(json.dumps(row))
PY
