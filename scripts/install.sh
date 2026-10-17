#!/usr/bin/env bash
set -euo pipefail

# Build and install opencode from the current checkout.
#
# Usage:
#   ./scripts/install.sh                # build + install to ~/.local/bin
#   ./scripts/install.sh --no-install   # build only, skip symlink
#   ./scripts/install.sh --no-deps      # skip bun install (assume deps ready)
#   ./scripts/install.sh --name marcelo  # override the version label

INSTALL=true
INSTALL_DEPS=true
NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-install) INSTALL=false; shift ;;
    --no-deps) INSTALL_DEPS=false; shift ;;
    --name) NAME="$2"; shift 2 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_DIR="$REPO/packages/cli"
LOCAL_BIN="$HOME/.local/bin"

# Match the build script's target naming: opencode-{os}-{arch}[-baseline][-musl]
OS_NAME="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH="x64" ;;
  aarch64) ARCH="arm64" ;;
esac
# The build script names output dirs cli-{os}-{arch} (it replaces "opencode"
# with "cli" in the target name); the binary inside is still "opencode".
BINARY_PATH="$CLI_DIR/dist/cli-${OS_NAME}-${ARCH}/bin/opencode"

cd "$REPO"

COMMIT_SHA="$(git rev-parse --short=7 HEAD)"
# Read the base version from the root package.json and append the commit hash,
# so the built binary reports e.g. 2.0.32+marcelo-714e929
BASE_VERSION="$(grep -m1 '"version"' package.json | sed 's/.*"version": *"\([^"]*\)".*/\1/')"
LABEL="${NAME:-$(git rev-parse --abbrev-ref HEAD)}"
VERSION="${BASE_VERSION}+${LABEL}-${COMMIT_SHA}"

if [[ "$INSTALL_DEPS" == true ]]; then
  echo "Installing dependencies..."
  bun install --frozen-lockfile
fi

echo "Building opencode $VERSION..."
cd "$CLI_DIR"
OPENCODE_VERSION="$VERSION" bun run build --single --skip-install

if [[ ! -f "$BINARY_PATH" ]]; then
  echo "ERROR: Build output not found at $BINARY_PATH" >&2
  exit 1
fi

echo "Built: $BINARY_PATH"

if [[ "$INSTALL" == true ]]; then
  mkdir -p "$LOCAL_BIN"
  ln -sf "$BINARY_PATH" "$LOCAL_BIN/opencode"
  echo "Symlinked to $LOCAL_BIN/opencode"
  echo ""
  echo "Done. opencode $VERSION is installed."
  opencode --version
else
  echo ""
  echo "Done. Binary at $BINARY_PATH"
fi
