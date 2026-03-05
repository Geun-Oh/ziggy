#!/usr/bin/env bash
set -euo pipefail

BIN="${1:-./zig-out/bin/ziggy}"
OUT_DIR="${2:-./artifacts/fault-matrix}"
mkdir -p "$OUT_DIR"

DB_ROOT="$(mktemp -d)"
TMP_PASS="$DB_ROOT/pass.txt"
TMP_FAIL="$DB_ROOT/fail.txt"
TMP_INV="$DB_ROOT/invariants.txt"
: >"$TMP_PASS"
: >"$TMP_FAIL"
: >"$TMP_INV"
trap 'rm -rf "$DB_ROOT"' EXIT

pass() {
  echo "$1" >>"$TMP_PASS"
  echo "PASS: $1"
}

fail() {
  echo "$1" >>"$TMP_FAIL"
  echo "FAIL: $1"
}

invariant() {
  echo "$1" >>"$TMP_INV"
}

# A) Disk-full style fault (write resource exhaustion simulation)
DB_A="$DB_ROOT/db-diskfull"
mkdir -p "$DB_A"
"$BIN" put --path "$DB_A" --key seed --value stable >/dev/null
chmod 0444 "$DB_A/wal.log"
if "$BIN" put --path "$DB_A" --key huge --value large >/dev/null 2>&1; then
  chmod 0644 "$DB_A/wal.log"
  fail "disk_full_simulated_write_failure"
else
  chmod 0644 "$DB_A/wal.log"
  if "$BIN" get --path "$DB_A" --key seed >/dev/null 2>&1; then
    pass "disk_full_simulated_write_failure"
  elif ! "$BIN" open --path "$DB_A" >/dev/null 2>&1; then
    pass "disk_full_simulated_write_failure_fail_closed"
  else
    fail "disk_full_simulated_write_failure_recovery"
  fi
fi
invariant "disk_full_simulated_write_failure: when write-path cannot allocate/append, prior committed state remains readable or reopen fails closed"

# B) CURRENT partial write corruption
DB_B="$DB_ROOT/db-current"
mkdir -p "$DB_B"
"$BIN" open --path "$DB_B" >/dev/null
printf 'MANI' >"$DB_B/CURRENT"
if "$BIN" open --path "$DB_B" >/dev/null 2>&1; then
  fail "current_partial_write_fail_closed"
else
  pass "current_partial_write_fail_closed"
fi
invariant "current_partial_write_fail_closed: startup fails closed on truncated CURRENT"

# C) MANIFEST partial write corruption
DB_C="$DB_ROOT/db-manifest"
mkdir -p "$DB_C"
"$BIN" put --path "$DB_C" --key a --value 1 >/dev/null
CUR_MANIFEST="$(tr -d '\n' <"$DB_C/CURRENT")"
python3 - "$DB_C/$CUR_MANIFEST" <<'PY'
import sys
p=sys.argv[1]
b=open(p,'rb').read()
open(p,'wb').write(b[:max(0, len(b)//2)])
PY
if "$BIN" open --path "$DB_C" >/dev/null 2>&1; then
  fail "manifest_partial_write_fail_closed"
else
  pass "manifest_partial_write_fail_closed"
fi
invariant "manifest_partial_write_fail_closed: startup rejects malformed MANIFEST"

# D) CURRENT.tmp leftover (rename boundary)
DB_D="$DB_ROOT/db-current-tmp"
mkdir -p "$DB_D"
"$BIN" put --path "$DB_D" --key x --value 1 >/dev/null
cp "$DB_D/CURRENT" "$DB_D/CURRENT.tmp"
printf 'MANIFEST-DOES-NOT-EXIST\n' >"$DB_D/CURRENT.tmp"
if "$BIN" open --path "$DB_D" >/dev/null 2>&1; then
  pass "current_tmp_leftover_ignored"
else
  fail "current_tmp_leftover_ignored"
fi
invariant "current_tmp_leftover_ignored: CURRENT.tmp is ignored unless atomically renamed to CURRENT"

# E) SST temp/orphan visibility (write->fsync->rename boundary)
DB_E="$DB_ROOT/db-sst-rename"
mkdir -p "$DB_E"
"$BIN" put --path "$DB_E" --key k1 --value v1 >/dev/null
printf 'ZIGGY-SST 1 0\n1\t0\torphan\tvalue\n' >"$DB_E/sst-999.tmp"
if "$BIN" open --path "$DB_E" >/dev/null 2>&1 && ! "$BIN" get --path "$DB_E" --key orphan >/dev/null 2>&1; then
  pass "sst_tmp_orphan_not_visible"
else
  fail "sst_tmp_orphan_not_visible"
fi
invariant "sst_tmp_orphan_not_visible: un-renamed SST temp files are not query-visible"

# F) WAL corruption baseline
DB_F="$DB_ROOT/db-wal"
mkdir -p "$DB_F"
"$BIN" put --path "$DB_F" --key a --value 1 >/dev/null
python3 - "$DB_F/wal.log" <<'PY'
import sys
p=sys.argv[1]
b=bytearray(open(p,'rb').read())
if len(b)>8:
    b[8]^=0xFF
open(p,'wb').write(b)
PY
if "$BIN" open --path "$DB_F" >/dev/null 2>&1; then
  fail "wal_corruption_fail_open"
else
  pass "wal_corruption_fail_open"
fi
invariant "wal_corruption_fail_open: WAL header corruption is detected deterministically"

python3 - "$TMP_PASS" "$TMP_FAIL" "$TMP_INV" "$OUT_DIR/result.json" <<'PY'
import json,sys

def lines(p):
    with open(p) as f:
        return [ln.strip() for ln in f if ln.strip()]

passes=lines(sys.argv[1])
fails=lines(sys.argv[2])
invariants=lines(sys.argv[3])
out=sys.argv[4]
payload={
    "ok": len(fails)==0,
    "cases": {"pass": passes, "fail": fails},
    "invariants": invariants,
}
with open(out,'w') as f:
    json.dump(payload, f, indent=2)
PY

if [ -s "$TMP_FAIL" ]; then
  echo "fault matrix failed"
  exit 1
fi

echo "wrote $OUT_DIR/result.json"
