# v0.1.0 Technical Preview — draft release notes

MARC is a local Linux control plane for coding-agent workflows. This preview
provides sequential Lead → Developer → Lead → QA → Lead → Reviewer → Lead work,
bounded remediation and explicit Finding resolution. Readiness ends at
`READY_FOR_HUMAN_MERGE`; merge and push remain human actions.

## Features covered by deterministic tests

- SQLite persistence, concurrent writers, crash recovery and restart.
- Exact replay, stale-result rejection, role isolation and Orchestrator-only
  authoritative mutations.
- Local stdio MCP workflow tools, worktree capture and proof-bound readiness.
- A deterministic fake workflow, explicit budget approval and truthful incomplete
  or unknown usage. External client usage is reported as `excluded`.

The [release validation process](release.md) runs the complete race suite and
existing CLI/MCP integration tests without model access. These checks do not
verify a live external client's coding or model execution.

## Installation

Once the release is published, download `SHA256SUMS` and the Linux archive for
your CPU from the repository's v0.1.0 release page. For x86-64:

```sh
sha256sum --ignore-missing -c SHA256SUMS
tar -xzf marc_v0.1.0_linux_amd64.tar.gz
mkdir -p "$HOME/.local/bin"
install -m 0755 marc_v0.1.0_linux_amd64/marc "$HOME/.local/bin/marc"
```

For AArch64, use `marc_v0.1.0_linux_arm64.tar.gz` instead. Ensure the matching
archive reports `OK` before extraction. Checksums detect corruption; they are
not signatures. Git and durable local Linux storage are required at runtime;
Go and CGO are unnecessary for the packaged binary. `BUILD-INFO` records the
source revision, toolchain and target. The CLI has no `--version` flag.

For source builds, configuration, a fake workflow example and MCP registration,
see [usage](usage.md) and the [MCP interface](mcp.md).

## Known limitations

- Technical Preview: no stable API, database upgrade/import path or long-term
  support commitment. Earlier pre-release schema checksums may be rejected.
- Trusted local Linux host only; no hostile same-user sandbox, remote MCP
  authentication, multi-user isolation or application-level encryption.
- Managed-provider execution is unavailable. External model requests, identity,
  fresh role contexts and claimed test execution are outside MARC's attestation
  and token admission/accounting boundary.
- No automatic merge, push, process supervision or destructive cleanup.
- Network/FUSE/9p, tmpfs/ramfs and WSL Windows storage are rejected. Use durable
  Linux storage, including under WSL2.
- arm64 archives are cross-compiled; this validation workflow executes amd64
  binaries only. Native arm64 runtime validation remains outstanding.

Read [Security](../SECURITY.md) before connecting an external client.
