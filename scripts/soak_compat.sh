#!/usr/bin/env bash
set -euo pipefail

BIN="${1:-./zig-out/bin/ziggy}"
OUT_DIR="${2:-./artifacts/soak}"
DURATION_SECS="${3:-120}"   # nightly CI: 21600
mkdir -p "$OUT_DIR"

# Hard acceptance thresholds (override via env in CI if needed)
RSS_MAX_KB="${RSS_MAX_KB:-262144}"                    # 256 MiB absolute peak (per command)
RSS_GROWTH_MAX_KB="${RSS_GROWTH_MAX_KB:-65536}"       # 64 MiB growth bound (harness process)
FD_GROWTH_MAX="${FD_GROWTH_MAX:-8}"                   # fd leak bound for harness process
REQUIRE_COMPAT_CHECK="${REQUIRE_COMPAT_CHECK:-1}"     # set 0 for local smoke runs
PUT_P95_SLO_US="${PUT_P95_SLO_US:-50000}"
PUT_P99_SLO_US="${PUT_P99_SLO_US:-150000}"
GET_P95_SLO_US="${GET_P95_SLO_US:-50000}"
GET_P99_SLO_US="${GET_P99_SLO_US:-150000}"
SCAN_P95_SLO_US="${SCAN_P95_SLO_US:-120000}"
SCAN_P99_SLO_US="${SCAN_P99_SLO_US:-300000}"

DB="$(mktemp -d)"
LAT_DIR="$(mktemp -d)"
trap 'rm -rf "$DB" "$LAT_DIR"' EXIT

PUT_LAT="$LAT_DIR/put_us.txt"
GET_LAT="$LAT_DIR/get_us.txt"
SCAN_LAT="$LAT_DIR/scan_us.txt"
: >"$PUT_LAT"
: >"$GET_LAT"
: >"$SCAN_LAT"

now_ns() {
  python3 - <<'PY'
import time
print(time.time_ns())
PY
}

fd_count() {
  if [[ -d "/proc/$$/fd" ]]; then
    ls -1 "/proc/$$/fd" | wc -l
  else
    # Best-effort fallback on non-Linux hosts
    ls -1 /dev/fd 2>/dev/null | wc -l || echo 0
  fi
}

rss_kb() {
  if [[ -r "/proc/$$/status" ]]; then
    awk '/VmRSS:/ {print $2; exit}' "/proc/$$/status"
  else
    ps -o rss= -p $$ | awk '{print $1+0}'
  fi
}

TIME_BIN=""
if command -v gtime >/dev/null 2>&1; then
  TIME_BIN="$(command -v gtime)"
elif /usr/bin/time -f '%M' -o /dev/null true >/dev/null 2>&1; then
  TIME_BIN="/usr/bin/time"
fi

run_with_rss() {
  local rss_file="$LAT_DIR/rss.tmp"
  local rss=0
  if [[ -n "$TIME_BIN" ]]; then
    if "$TIME_BIN" -f '%M' -o "$rss_file" "$@" >/dev/null; then
      :
    else
      local rc=$?
      return "$rc"
    fi
    if [[ -f "$rss_file" ]]; then
      rss=$(tr -d '[:space:]' <"$rss_file")
      [[ -z "$rss" ]] && rss=0
    fi
  else
    "$@" >/dev/null
  fi
  echo "$rss"
}

start_ns=$(now_ns)

ops=0
restarts=0
fault_rounds=0
compat_checked=false
compat_minor_policy_checked=false
deadline=$(( $(date +%s) + DURATION_SECS ))

fd_base=$(fd_count)
fd_peak=$fd_base
rss_script_base=$(rss_kb)
rss_script_peak=$rss_script_base
rss_cmd_peak=0

while (( $(date +%s) < deadline )); do
  key="k-$ops"

  t0=$(now_ns)
  cmd_rss=$(run_with_rss "$BIN" put --path "$DB" --key "$key" --value "v-$ops")
  t1=$(now_ns)
  (( cmd_rss > rss_cmd_peak )) && rss_cmd_peak=$cmd_rss
  echo $(( (t1-t0)/1000 )) >>"$PUT_LAT"

  if (( ops % 5 == 0 )); then
    "$BIN" delete --path "$DB" --key "k-$((ops/2))" >/dev/null 2>&1 || true
  fi

  if (( ops % 11 == 0 )); then
    g0=$(now_ns)
    if g_rss=$(run_with_rss "$BIN" get --path "$DB" --key "$key" 2>/dev/null); then
      (( g_rss > rss_cmd_peak )) && rss_cmd_peak=$g_rss
    fi
    g1=$(now_ns)
    echo $(( (g1-g0)/1000 )) >>"$GET_LAT"
  fi

  if (( ops % 37 == 0 )); then
    s0=$(now_ns)
    if s_rss=$(run_with_rss "$BIN" scan --path "$DB" --prefix "k-" 2>/dev/null); then
      (( s_rss > rss_cmd_peak )) && rss_cmd_peak=$s_rss
    fi
    s1=$(now_ns)
    echo $(( (s1-s0)/1000 )) >>"$SCAN_LAT"
  fi

  if (( ops % 17 == 0 )); then
    "$BIN" open --path "$DB" >/dev/null
    restarts=$((restarts+1))
  fi

  if (( ops % 89 == 0 )); then
    fault_rounds=$((fault_rounds+1))
    cp "$DB/wal.log" "$DB/wal.log.bak"
    python3 - "$DB/wal.log" <<'PY'
import os,sys
p=sys.argv[1]
if os.path.exists(p):
    b=bytearray(open(p,'rb').read())
    if len(b)>16:
        b[-1]^=0xAA
        open(p,'wb').write(b)
PY
    "$BIN" open --path "$DB" >/dev/null 2>&1 || true
    mv "$DB/wal.log.bak" "$DB/wal.log"
  fi

  if [[ "$compat_checked" == false && $ops -ge 40 ]]; then
    sst_file=$(ls "$DB"/sst-*.sst 2>/dev/null | head -n 1 || true)
    if [[ -n "$sst_file" ]]; then
      cp "$sst_file" "$sst_file.bak"
      tail -n +2 "$sst_file" >"$sst_file.tail"
      {
        echo 'ZIGGY-SST 999 0'
        cat "$sst_file.tail"
      } >"$sst_file"
      rm -f "$sst_file.tail"

      if "$BIN" open --path "$DB" >/dev/null 2>&1; then
        echo "compat fail-fast check failed" >&2
        exit 1
      fi

      mv "$sst_file.bak" "$sst_file"
      compat_checked=true
    fi
  fi

  # Resource stability sampling for leak detection
  if (( ops % 25 == 0 )); then
    fd_now=$(fd_count)
    (( fd_now > fd_peak )) && fd_peak=$fd_now
    rss_script_now=$(rss_kb)
    (( rss_script_now > rss_script_peak )) && rss_script_peak=$rss_script_now
  fi

  ops=$((ops+1))
done

# Fallback compatibility gate when workload did not naturally generate SST files.
if [[ "$compat_checked" == false && "$REQUIRE_COMPAT_CHECK" == "1" ]]; then
  if command -v zig >/dev/null 2>&1 && [[ -f "src/ziggy/engine.zig" ]]; then
    if zig test src/ziggy/engine.zig --test-filter "task 10.3 negative: unsupported major SST format fails fast on open" >/dev/null; then
      compat_checked=true
    fi
  fi
fi

# Minor compatibility policy gate (supported minor upgrade path + unsupported minor fail-fast).
if [[ "$REQUIRE_COMPAT_CHECK" == "1" ]]; then
  if command -v zig >/dev/null 2>&1 && [[ -f "src/ziggy/engine.zig" ]]; then
    if zig test src/ziggy/engine.zig --test-filter "task 10.3 positive: supported minor SST format opens successfully" >/dev/null \
      && zig test src/ziggy/engine.zig --test-filter "task 10.3 negative: unsupported minor SST format fails fast on open" >/dev/null; then
      compat_minor_policy_checked=true
    fi
  fi
fi

calc_pct() {
  python3 - "$1" "$2" <<'PY'
import sys
p=int(sys.argv[2])
vals=[]
with open(sys.argv[1]) as f:
    for ln in f:
        ln=ln.strip()
        if ln:
            vals.append(int(ln))
if not vals:
    print(0)
    raise SystemExit
vals.sort()
idx=max(0, (len(vals)*p + 99)//100 - 1)
print(vals[idx])
PY
}

put_p50=$(calc_pct "$PUT_LAT" 50)
put_p95=$(calc_pct "$PUT_LAT" 95)
put_p99=$(calc_pct "$PUT_LAT" 99)
get_p50=$(calc_pct "$GET_LAT" 50)
get_p95=$(calc_pct "$GET_LAT" 95)
get_p99=$(calc_pct "$GET_LAT" 99)
scan_p50=$(calc_pct "$SCAN_LAT" 50)
scan_p95=$(calc_pct "$SCAN_LAT" 95)
scan_p99=$(calc_pct "$SCAN_LAT" 99)

fd_growth=$((fd_peak - fd_base))
rss_script_growth_kb=$((rss_script_peak - rss_script_base))

declare -a violations=()

if [[ "$REQUIRE_COMPAT_CHECK" == "1" && "$compat_checked" != true ]]; then
  violations+=("compatibility fail-fast check did not execute")
fi
if [[ "$REQUIRE_COMPAT_CHECK" == "1" && "$compat_minor_policy_checked" != true ]]; then
  violations+=("minor compatibility policy checks did not execute")
fi
(( fd_growth <= FD_GROWTH_MAX )) || violations+=("fd growth ${fd_growth} exceeds ${FD_GROWTH_MAX}")
(( rss_cmd_peak <= RSS_MAX_KB )) || violations+=("rss peak ${rss_cmd_peak}KB exceeds ${RSS_MAX_KB}KB")
(( rss_script_growth_kb <= RSS_GROWTH_MAX_KB )) || violations+=("harness rss growth ${rss_script_growth_kb}KB exceeds ${RSS_GROWTH_MAX_KB}KB")
(( put_p95 <= PUT_P95_SLO_US )) || violations+=("put p95 ${put_p95}us exceeds ${PUT_P95_SLO_US}us")
(( put_p99 <= PUT_P99_SLO_US )) || violations+=("put p99 ${put_p99}us exceeds ${PUT_P99_SLO_US}us")
(( get_p95 <= GET_P95_SLO_US )) || violations+=("get p95 ${get_p95}us exceeds ${GET_P95_SLO_US}us")
(( get_p99 <= GET_P99_SLO_US )) || violations+=("get p99 ${get_p99}us exceeds ${GET_P99_SLO_US}us")
(( scan_p95 <= SCAN_P95_SLO_US )) || violations+=("scan p95 ${scan_p95}us exceeds ${SCAN_P95_SLO_US}us")
(( scan_p99 <= SCAN_P99_SLO_US )) || violations+=("scan p99 ${scan_p99}us exceeds ${SCAN_P99_SLO_US}us")

ok=true
if (( ${#violations[@]} > 0 )); then
  ok=false
fi

violations_json=$(python3 - "${violations[@]:-}" <<'PY'
import json,sys
arr=[v for v in sys.argv[1:] if v]
print(json.dumps(arr))
PY
)

end_ns=$(now_ns)

cat >"$OUT_DIR/result.json" <<JSON
{
  "ok": $ok,
  "duration_secs": $DURATION_SECS,
  "ops": $ops,
  "restarts": $restarts,
  "fault_rounds": $fault_rounds,
  "compat_fail_fast_checked": $compat_checked,
  "compat_minor_policy_checked": $compat_minor_policy_checked,
  "thresholds": {
    "rss_max_kb": $RSS_MAX_KB,
    "rss_growth_max_kb": $RSS_GROWTH_MAX_KB,
    "fd_growth_max": $FD_GROWTH_MAX,
    "require_compat_check": $REQUIRE_COMPAT_CHECK,
    "latency_slo_us": {
      "put": {"p95": $PUT_P95_SLO_US, "p99": $PUT_P99_SLO_US},
      "get": {"p95": $GET_P95_SLO_US, "p99": $GET_P99_SLO_US},
      "scan": {"p95": $SCAN_P95_SLO_US, "p99": $SCAN_P99_SLO_US}
    }
  },
  "resources": {
    "fd_base": $fd_base,
    "fd_peak": $fd_peak,
    "fd_growth": $fd_growth,
    "rss_cmd_peak_kb": $rss_cmd_peak,
    "rss_script_base_kb": $rss_script_base,
    "rss_script_peak_kb": $rss_script_peak,
    "rss_script_growth_kb": $rss_script_growth_kb
  },
  "latency_us": {
    "put": {"p50": $put_p50, "p95": $put_p95, "p99": $put_p99},
    "get": {"p50": $get_p50, "p95": $get_p95, "p99": $get_p99},
    "scan": {"p50": $scan_p50, "p95": $scan_p95, "p99": $scan_p99}
  },
  "violations": $violations_json,
  "duration_ms": $(( (end_ns-start_ns)/1000000 ))
}
JSON

echo "wrote $OUT_DIR/result.json"

if [[ "$ok" != true ]]; then
  echo "soak compatibility acceptance failed" >&2
  exit 1
fi