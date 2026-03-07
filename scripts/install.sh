#!/usr/bin/env sh
set -eu

REPO="Geun-Oh/ziggy"
BINARY_NAME="ziggy"
INSTALL_DIR="${INSTALL_DIR:-}"
VERSION="${VERSION:-latest}"
TMP_DIR="${TMPDIR:-/tmp}/ziggy-install-$$"

print_help() {
  cat <<'EOF'
ziggy installer

Usage:
  sh install.sh [--version <tag|latest>] [--install-dir <dir>]
  VERSION=v0.1.1 INSTALL_DIR=/usr/local/bin sh install.sh

Options:
  --version <tag|latest>   Release tag to install. Default: latest
  --install-dir <dir>      Destination directory for the ziggy binary
  -h, --help               Show this help text
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      [ "$#" -ge 2 ] || { echo "error: --version requires a value" >&2; exit 1; }
      VERSION="$2"
      shift 2
      ;;
    --install-dir)
      [ "$#" -ge 2 ] || { echo "error: --install-dir requires a value" >&2; exit 1; }
      INSTALL_DIR="$2"
      shift 2
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      print_help >&2
      exit 1
      ;;
  esac
done

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT INT TERM

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

need_cmd uname
need_cmd tar

if command -v curl >/dev/null 2>&1; then
  FETCH='curl -fsSL --retry 5 --retry-delay 2 --retry-all-errors'
elif command -v wget >/dev/null 2>&1; then
  FETCH='wget -qO-'
else
  echo "error: need curl or wget to download ziggy" >&2
  exit 1
fi

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux) platform_os="linux" ;;
  Darwin) platform_os="macos" ;;
  *)
    echo "error: unsupported OS: $os" >&2
    echo "supported release targets: Linux, Darwin" >&2
    exit 1
    ;;
esac

case "$arch" in
  x86_64|amd64) platform_arch="x86_64" ;;
  arm64|aarch64) platform_arch="aarch64" ;;
  *)
    echo "error: unsupported architecture: $arch" >&2
    echo "supported release targets: x86_64, aarch64" >&2
    exit 1
    ;;
esac

target="${platform_arch}-${platform_os}"
archive_name="${BINARY_NAME}-${target}.tar.gz"
checksum_name="${archive_name}.sha256"

if [ -z "$INSTALL_DIR" ]; then
  for candidate in /usr/local/bin /opt/homebrew/bin "${HOME}/.local/bin"; do
    parent_dir=$(dirname "$candidate")
    if [ -d "$candidate" ] && [ -w "$candidate" ]; then
      INSTALL_DIR="$candidate"
      break
    fi
    if [ ! -d "$candidate" ] && [ -d "$parent_dir" ] && [ -w "$parent_dir" ]; then
      INSTALL_DIR="$candidate"
      break
    fi
  done
fi

if [ -z "$INSTALL_DIR" ]; then
  INSTALL_DIR="${HOME}/.local/bin"
fi

if [ "$VERSION" = "latest" ]; then
  base_url="https://github.com/${REPO}/releases/latest/download"
else
  base_url="https://github.com/${REPO}/releases/download/${VERSION}"
fi

archive_url="${base_url}/${archive_name}"
checksum_url="${base_url}/${checksum_name}"

mkdir -p "$TMP_DIR"
mkdir -p "$INSTALL_DIR"

fetch_to_file() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 5 --retry-delay 2 --retry-all-errors -o "$out" "$url"
  else
    wget -qO "$out" "$url"
  fi
}

echo "Downloading ${archive_name} for ${target}..."
fetch_to_file "$archive_url" "$TMP_DIR/$archive_name"

if fetch_to_file "$checksum_url" "$TMP_DIR/$checksum_name" 2>/dev/null; then
  echo "Verifying checksum..."
  checksum_tmp="$TMP_DIR/${checksum_name}.normalized"
  sed "s#  .*${archive_name}#  ${archive_name}#" "$TMP_DIR/$checksum_name" > "$checksum_tmp"
  (
    cd "$TMP_DIR"
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum -c "$(basename "$checksum_tmp")"
    elif command -v shasum >/dev/null 2>&1; then
      shasum -a 256 -c "$(basename "$checksum_tmp")"
    else
      echo "warning: sha256sum/shasum not found, skipped checksum verification" >&2
    fi
  )
else
  echo "warning: checksum file not found, continuing without verification" >&2
fi

rm -rf "$TMP_DIR/extract"
mkdir -p "$TMP_DIR/extract"
tar -xzf "$TMP_DIR/$archive_name" -C "$TMP_DIR/extract"

if [ ! -f "$TMP_DIR/extract/${BINARY_NAME}-${target}/${BINARY_NAME}" ]; then
  echo "error: downloaded archive did not contain expected binary path" >&2
  exit 1
fi

install_path="$INSTALL_DIR/$BINARY_NAME"
cp "$TMP_DIR/extract/${BINARY_NAME}-${target}/${BINARY_NAME}" "$install_path"
chmod +x "$install_path"

echo "Installed ${BINARY_NAME} to ${install_path}"
case ":$PATH:" in
  *":$INSTALL_DIR:"*)
    ;;
  *)
    echo "warning: $INSTALL_DIR is not in PATH" >&2
    echo "add this to your shell profile:" >&2
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\"" >&2
    ;;
esac

echo "Run: ${BINARY_NAME} open --path ./data"
