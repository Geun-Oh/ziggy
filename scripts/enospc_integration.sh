#!/usr/bin/env bash
set -euo pipefail

BIN="${1:-./zig-out/bin/ziggy}"
OUT_DIR="${2:-./artifacts/enospc}"
mkdir -p "$OUT_DIR"

if [[ "$(uname -s)" != "Linux" ]]; then
  cat >"$OUT_DIR/result.json" <<JSON
{"ok": true, "skipped": true, "reason": "linux-only loopback ENOSPC campaign"}
JSON
  echo "wrote $OUT_DIR/result.json"
  exit 0
fi

if ! command -v sudo >/dev/null 2>&1; then
  cat >"$OUT_DIR/result.json" <<JSON
{"ok": true, "skipped": true, "reason": "sudo not available"}
JSON
  echo "wrote $OUT_DIR/result.json"
  exit 0
fi

WORK="$(mktemp -d)"
IMG="$WORK/enospc.img"
MNT="$WORK/mnt"
DB="$MNT/db"
ERR_LOG="$WORK/put.err"
OPEN_ERR="$WORK/open.err"
REOPEN_ERR="$WORK/reopen.err"

cleanup() {
  set +e
  if mountpoint -q "$MNT" 2>/dev/null; then
    sudo umount "$MNT"
  fi
  sudo rm -rf "$MNT" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p "$MNT"

dd if=/dev/zero of="$IMG" bs=1M count=64 status=none
mkfs.ext4 -q "$IMG"
sudo mount -o loop "$IMG" "$MNT"
sudo chown "$(id -u):$(id -g)" "$MNT"
mkdir -p "$DB"

# Acknowledged baseline writes must remain safe across ENOSPC handling.
"$BIN" put --path "$DB" --key seed --value stable >/dev/null
"$BIN" put --path "$DB" --key baseline-a --value ok >/dev/null
"$BIN" put --path "$DB" --key baseline-b --value ok >/dev/null

value=$(python3 - <<'PY'
print('x' * 16384)
PY
)

ops=0
enospc_hit=false
failed_key=""
while true; do
  key="fill-$ops"
  if "$BIN" put --path "$DB" --key "$key" --value "$value" >/dev/null 2>"$ERR_LOG"; then
    ops=$((ops + 1))
    continue
  fi

  enospc_hit=true
  failed_key="$key"
  break
done

if [[ "$enospc_hit" != true ]]; then
  echo "ENOSPC was not triggered" >&2
  exit 1
fi

last_error=""
if [[ -f "$ERR_LOG" ]]; then
  last_error=$(tr '\n' ' ' <"$ERR_LOG" | sed 's/[[:space:]]\+/ /g' | sed 's/^ //; s/ $//')
fi

error_mentions_diskfull=false
if echo "$last_error" | grep -Eiq 'ENOSPC|No ?space ?left|DiskFull|NoSpaceLeft|FileTooBig|Quota'; then
  error_mentions_diskfull=true
fi

reopen_mode="fail-closed"
if "$BIN" open --path "$DB" >/dev/null 2>"$OPEN_ERR"; then
  reopen_mode="open-ok"
fi

# Determinism check for fail-closed policy: repeated open should keep failing.
second_open_same_outcome=true
if [[ "$reopen_mode" == "fail-closed" ]]; then
  if "$BIN" open --path "$DB" >/dev/null 2>"$REOPEN_ERR"; then
    second_open_same_outcome=false
  fi
fi

seed_visible=false
baseline_a_visible=false
baseline_b_visible=false
first_fill_visible=false
mid_fill_visible=false
last_fill_visible=false
failed_key_visible=false
post_failure_put_rejected=false

if "$BIN" get --path "$DB" --key seed >/dev/null 2>&1; then
  seed_visible=true
fi
if "$BIN" get --path "$DB" --key baseline-a >/dev/null 2>&1; then
  baseline_a_visible=true
fi
if "$BIN" get --path "$DB" --key baseline-b >/dev/null 2>&1; then
  baseline_b_visible=true
fi

if (( ops > 0 )); then
  if "$BIN" get --path "$DB" --key fill-0 >/dev/null 2>&1; then
    first_fill_visible=true
  fi
  mid=$((ops/2))
  if "$BIN" get --path "$DB" --key "fill-$mid" >/dev/null 2>&1; then
    mid_fill_visible=true
  fi
  if "$BIN" get --path "$DB" --key "fill-$((ops-1))" >/dev/null 2>&1; then
    last_fill_visible=true
  fi
fi

if [[ -n "$failed_key" ]]; then
  if "$BIN" get --path "$DB" --key "$failed_key" >/dev/null 2>&1; then
    failed_key_visible=true
  fi
fi

if ! "$BIN" put --path "$DB" --key post-enospc --value still-full >/dev/null 2>"$WORK/post.err"; then
  post_failure_put_rejected=true
fi

# Explicit pass/fail assertions.
declare -a violations=()

(( ops > 0 )) || violations+=("ENOSPC hit before any fill writes; campaign not meaningful")
[[ "$error_mentions_diskfull" == true ]] || violations+=("final put failure did not report ENOSPC/DiskFull semantics")
[[ "$post_failure_put_rejected" == true ]] || violations+=("post-ENOSPC write unexpectedly succeeded")

if [[ "$reopen_mode" == "fail-closed" ]]; then
  [[ "$second_open_same_outcome" == true ]] || violations+=("fail-closed mode was not deterministic across reopen attempts")
else
  [[ "$seed_visible" == true ]] || violations+=("acknowledged key 'seed' not visible after reopen")
  [[ "$baseline_a_visible" == true ]] || violations+=("acknowledged key 'baseline-a' not visible after reopen")
  [[ "$baseline_b_visible" == true ]] || violations+=("acknowledged key 'baseline-b' not visible after reopen")
  if (( ops > 0 )); then
    [[ "$first_fill_visible" == true ]] || violations+=("first successful fill write missing after reopen")
    [[ "$mid_fill_visible" == true ]] || violations+=("mid successful fill write missing after reopen")
    [[ "$last_fill_visible" == true ]] || violations+=("last successful fill write missing after reopen")
  fi
  [[ "$failed_key_visible" == false ]] || violations+=("failed ENOSPC write became visible")
fi

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

python3 - "$OUT_DIR/result.json" "$ops" "$reopen_mode" "$seed_visible" "$baseline_a_visible" "$baseline_b_visible" "$first_fill_visible" "$mid_fill_visible" "$last_fill_visible" "$failed_key_visible" "$post_failure_put_rejected" "$error_mentions_diskfull" "$second_open_same_outcome" "$failed_key" "$last_error" "$ok" "$violations_json" <<'PY'
import json,sys
(
  out, ops, reopen_mode, seed_visible, baseline_a_visible, baseline_b_visible,
  first_fill_visible, mid_fill_visible, last_fill_visible, failed_key_visible,
  post_failure_put_rejected, error_mentions_diskfull, second_open_same_outcome,
  failed_key, last_error, ok, violations_json
) = sys.argv[1:]
payload = {
  "ok": ok.lower() == "true",
  "skipped": False,
  "enospc_triggered": True,
  "writes_before_failure": int(ops),
  "reopen_mode": reopen_mode,
  "assertions": {
    "error_mentions_diskfull": error_mentions_diskfull.lower() == "true",
    "second_open_same_outcome": second_open_same_outcome.lower() == "true",
    "seed_visible_after_failure": seed_visible.lower() == "true",
    "baseline_visible_after_failure": {
      "baseline-a": baseline_a_visible.lower() == "true",
      "baseline-b": baseline_b_visible.lower() == "true",
    },
    "fill_visibility_samples": {
      "first": first_fill_visible.lower() == "true",
      "mid": mid_fill_visible.lower() == "true",
      "last": last_fill_visible.lower() == "true",
    },
    "failed_key_not_visible": failed_key_visible.lower() != "true",
    "post_failure_put_rejected": post_failure_put_rejected.lower() == "true",
  },
  "failed_key": failed_key,
  "last_error": last_error,
  "violations": json.loads(violations_json),
}
with open(out,'w') as f:
  json.dump(payload,f,indent=2)
PY

echo "wrote $OUT_DIR/result.json"

if [[ "$ok" != true ]]; then
  echo "enospc acceptance assertions failed" >&2
  exit 1
fi