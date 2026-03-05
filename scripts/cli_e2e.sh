#!/usr/bin/env bash
set -euo pipefail

BIN="${1:-zig-out/bin/ziggy}"
if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN" >&2
  exit 1
fi

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/ziggy-e2e.XXXXXX")"
trap 'rm -rf "$WORKDIR"' EXIT
DB="$WORKDIR/db"

run_ok() {
  "$@" >/dev/null
}

run_fail() {
  set +e
  "$@" >/tmp/ziggy-e2e.out 2>&1
  local code=$?
  set -e
  if [[ $code -eq 0 ]]; then
    echo "expected failure but command succeeded: $*" >&2
    cat /tmp/ziggy-e2e.out >&2
    exit 1
  fi
}

# success path: put/get/delete/get-miss
run_ok "$BIN" put --path "$DB" --key "k1" --value "v1"
out="$($BIN get --path "$DB" --key "k1")"
[[ "$out" == "v1" ]] || { echo "unexpected get output: $out" >&2; exit 1; }
run_ok "$BIN" delete --path "$DB" --key "k1"
run_fail "$BIN" get --path "$DB" --key "k1"

# invalid args path (non-zero + usage)
run_fail "$BIN" put --path "$DB" --key "missing-value"

# invalid filesystem path (non-zero)
: >"$WORKDIR/not-a-dir"
run_fail "$BIN" open --path "$WORKDIR/not-a-dir/child"

# JSON escaping regression
special=$'line\n"quoted"\\tab\x01'
run_ok "$BIN" put --path "$DB" --key "esc" --value "$special"
json_out="$($BIN get --path "$DB" --key "esc" --json)"
[[ "$json_out" == *'"value":"line\n\"quoted\"\\tab\u0001"'* ]] || {
  echo "unexpected json output: $json_out" >&2
  exit 1
}

# JSON InvalidArguments error shape
run_fail "$BIN" put --path "$DB" --json
json_err="$($BIN put --path "$DB" --json 2>&1 >/dev/null || true)"
[[ "$json_err" == *'{"ok":false,"error":"invalid arguments"}'* ]] || {
  echo "unexpected json error: $json_err" >&2
  exit 1
}

echo "cli_e2e: ok"
