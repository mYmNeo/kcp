#!/usr/bin/env bash
set -e

BUILD_DIR=$(dirname "$0")/build
mkdir -p $BUILD_DIR
cd $BUILD_DIR

# --- Tool Check ---
# Determine the SHA checksum utility (sha1sum or shasum)
if command -v sha1sum &> /dev/null; then
    SUM_TOOL="sha1sum"
elif command -v shasum &> /dev/null; then
    SUM_TOOL="shasum -a 1"
else
    echo "Error: Neither sha1sum nor shasum found."
    exit 1
fi

export GO111MODULE=on
echo "Setting GO111MODULE to" $GO111MODULE

SALT=${SALT:-$(dd bs=18 count=1 if=/dev/urandom | base64 | tr +/ _.)}
VERSION=`date -u +%Y%m%d`
LDFLAGS="-X main.VERSION=$VERSION -s -w -X main.SALT=${SALT}"
GCFLAGS=""

# --- Functions ---

# Packages two binaries into a tar.gz archive per platform.
package_target() {
    local os=$1
    local arch=$2

    local ext=""
    if [ "${os}" == "windows" ]; then
        ext=".exe"
    fi

    local client_bin="client_${os}_${arch}${ext}"
    local server_bin="server_${os}_${arch}${ext}"
    local archive_file="kcptun-${os}-${arch}-${VERSION}.tar.gz"

    echo "--- Packaging ${os}/${arch} ---"
    tar -czf "${archive_file}" "${client_bin}" "${server_bin}"
}

# --- Build ---

# AMD64
OSES=(linux)
for os in ${OSES[@]}; do
  suffix=""
  if [[ "$os" == "windows" ]]; then
    suffix=".exe"
  fi
  env CGO_ENABLED=0 GOOS=$os GOARCH=amd64 go build -pgo=auto -ldflags "$LDFLAGS" -gcflags "$GCFLAGS" -o client_${os}_amd64${suffix} github.com/xtaci/kcptun/client
  env CGO_ENABLED=0 GOOS=$os GOARCH=amd64 go build -pgo=auto -ldflags "$LDFLAGS" -gcflags "$GCFLAGS" -o server_${os}_amd64${suffix} github.com/xtaci/kcptun/server
  package_target $os amd64
done

# ARM64
OSES=(linux darwin)
for os in ${OSES[@]}; do
  suffix=""
  if [[ "$os" == "windows" ]]; then
    suffix=".exe"
  fi
  env CGO_ENABLED=0 GOOS=$os GOARCH=arm64 go build -pgo=auto -ldflags "$LDFLAGS" -gcflags "$GCFLAGS" -o client_${os}_arm64${suffix} github.com/xtaci/kcptun/client
  env CGO_ENABLED=0 GOOS=$os GOARCH=arm64 go build -pgo=auto -ldflags "$LDFLAGS" -gcflags "$GCFLAGS" -o server_${os}_arm64${suffix} github.com/xtaci/kcptun/server
  package_target $os arm64
done

# --- Cleanup & Checksums ---

echo "--- Cleaning intermediate binaries ---"
find . -type f -regex "\./\(client\|server\)_.*" -delete

echo "--- Generating SHA1 Checksums ---"
$SUM_TOOL *.tar.gz > SHA1SUMS

echo "--- SHA1SUMS Output ---"
cat SHA1SUMS
echo "---"

echo "--- Build Complete ---"
echo "All release packages are located in ${BUILD_DIR}/"
