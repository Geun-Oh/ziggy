#!/usr/bin/env bash
set -euo pipefail

ZIG_VERSION="${1:-0.15.2}"
CACHE_DIR="${RUNNER_TEMP:-/tmp}/zig-cache"
INSTALL_DIR="${CACHE_DIR}/zig-${ZIG_VERSION}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"

case "$os" in
  linux) zig_os="linux" ;;
  darwin) zig_os="macos" ;;
  *) echo "Unsupported runner OS: $os" >&2; exit 1 ;;
esac

case "$arch" in
  x86_64|amd64) zig_arch="x86_64" ;;
  arm64|aarch64) zig_arch="aarch64" ;;
  *) echo "Unsupported runner architecture: $arch" >&2; exit 1 ;;
esac

if [ ! -x "${INSTALL_DIR}/zig" ]; then
  mkdir -p "$CACHE_DIR"
  archive="zig-${zig_arch}-${zig_os}-${ZIG_VERSION}.tar.xz"
  url="https://ziglang.org/download/${ZIG_VERSION}/${archive}"
  tmp_archive="${CACHE_DIR}/${archive}"
  rm -f "$tmp_archive"
  echo "Downloading ${url}"
  curl --fail --location --retry 5 --retry-delay 5 --retry-all-errors \
    --connect-timeout 20 --max-time 300 \
    -o "$tmp_archive" "$url"
  rm -rf "$INSTALL_DIR"
  mkdir -p "$INSTALL_DIR"
  tar -xJf "$tmp_archive" -C "$INSTALL_DIR" --strip-components=1
fi

echo "${INSTALL_DIR}" >> "$GITHUB_PATH"
"${INSTALL_DIR}/zig" version
