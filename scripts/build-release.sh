#!/usr/bin/env bash
# Build locally only; never tag, upload or publish.
set -euo pipefail
export LC_ALL=C TZ=UTC GOTOOLCHAIN=local GOPROXY=off
export CGO_ENABLED=0 GOOS=linux GOAMD64=v1 GOARM64=v8.0
export GOENV=off GOWORK=off GOFIPS140=off GOFLAGS= GOEXPERIMENT=
unset GOARM GOMIPS GOMIPS64 GOPPC64 GORISCV64

cd "$(dirname "$0")/.."
version=v0.1.0
toolchain=go1.27.3
if [[ $(go env GOVERSION) != "$toolchain" ]]; then
  echo "Release builds require $toolchain; install it and download dependencies first." >&2
  exit 1
fi
if [[ -n $(git status --porcelain --untracked-files=normal) ]]; then
  echo 'Release builds require a clean checkout, including untracked inputs.' >&2
  exit 1
fi
revision=$(git rev-parse HEAD)
epoch=$(git show -s --format=%ct HEAD)
output=${1:-.private/release}
mkdir -p "$output"
output=$(realpath "$output")
mkdir -p .private
work=$(mktemp -d "$PWD/.private/release-build.XXXXXX")
trap 'rm -rf "$work"' EXIT

for arch in amd64 arm64; do
  name="marc_${version}_linux_${arch}"
  stage="$work/$name"
  mkdir -p "$stage/docs"
  GOARCH=$arch go build -mod=readonly -trimpath -buildvcs=false \
    -ldflags='-buildid=' -o "$stage/marc" ./src/cmd/marc
  # Copy committed distribution inputs only, including all third-party notices.
  git archive HEAD LICENSE COPYRIGHT THIRD_PARTY_NOTICES.md SECURITY.md \
    docs/usage.md docs/mcp.md third_party 'schemas/*.schema.json' | tar -x -C "$stage"
  printf 'version=%s\nrevision=%s\ngo=%s\nos=linux\narch=%s\nCGO_ENABLED=0\nSOURCE_DATE_EPOCH=%s\n' \
    "$version" "$revision" "$toolchain" "$arch" "$epoch" > "$stage/BUILD-INFO"
  chmod 0755 "$stage/marc"
  find "$stage" -type d -exec chmod 0755 {} +
  find "$stage" -type f ! -name marc -exec chmod 0644 {} +
  tar --sort=name --format=ustar --mtime="@$epoch" --owner=0 --group=0 \
    --numeric-owner -C "$work" -cf - "$name" | gzip -n > "$output/$name.tar.gz"
done
(cd "$output" && sha256sum "marc_${version}_linux_amd64.tar.gz" \
  "marc_${version}_linux_arm64.tar.gz" > SHA256SUMS)
echo "Prepared $version Linux archives and SHA256SUMS. Nothing was published."
