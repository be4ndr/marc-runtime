# Release preparation and validation

The [v0.1.0 notes](release-v0.1.0.md) are a draft. Release preparation does not
create a tag, GitHub Release or uploaded artifact. Publication is a separate
manual decision after review and validation of the exact intended commit.

## Manual release gate

Run **Release validation** from GitHub Actions with the branch containing
[the workflow](../.github/workflows/release-validation.yml). A newly added
dispatch workflow must exist on the default branch before GitHub exposes its
manual trigger. After merge, run it again against the intended release commit
and retain the run URL and commit identity in the release review.

The workflow uses Go 1.27.3 on Ubuntu 24.04 and read-only repository permission.
It downloads pinned dependencies before running tests and builds offline:

1. Run `go test -race -p 1 ./... -count=1`. Package serialization limits resource
   contention without reducing race instrumentation. Leave `MARC_TEST_BINARY`
   unset so MCP subprocesses use the race-instrumented test helper. CLI binary
   smoke tests run against the packaged executable in step 4; their short
   subprocess deadlines are intended for production binaries.
2. Run `go vet ./...` and repeat existing SQLite multiprocess crash recovery,
   concurrent journal, POSIX lock and MCP shared-database tests three times.
3. Build both Linux architectures twice; compare archives and `SHA256SUMS`
   byte-for-byte and verify checksums.
4. Extract the amd64 archive and run existing CLI and MCP tests against its
   executable, including startup, shutdown, workflow execution and resume.
5. Check documentation links, Go formatting, shell syntax and unchanged inputs.

The independent **Native Linux ARM64** job uses `ubuntu-24.04-arm` and the same
Go 1.27.3 pin. It checks the host and Go architecture, runs the full native test
suite and vet, then uses the unchanged packaging script and verifies checksums.
It extracts the arm64 archive and runs the existing CLI/MCP tests with
`MARC_TEST_BINARY` pointing at that packaged executable. The native suite covers
`TestFreshInitializationAndRestart`, `TestSQLiteMultiprocessDurabilityAndRecovery`,
`TestWorkflowRestartCutPoints` and the external restart/replay tests. Both jobs
initialize durable test storage before `setup-go`. ARM64 native checks complement
the existing AMD64 race, repeated recovery/concurrency and reproducibility gates.

The full suite includes `TestWorkflowRestartCutPoints`,
`TestB7RemediationResolutionRestartAndReplay`,
`TestMutationActorRejectsEveryDomainMutationAtomically`,
`TestB4WorkflowCannotCompleteOrReadyWithoutAuthority` and
`TestMCPWorktreeEndToEndAndResume`. No new substitute tests or live model calls
are used. Test data and generated files stay under ignored `.private/` storage.
Nothing is uploaded by this workflow; failed checks block its success.

## Reproducible local packaging

Use a clean source checkout, Linux, Git, Go **1.27.3**, GNU tar, gzip and GNU
coreutils. Race validation additionally requires a C compiler. Dependencies must
already be cached. `.private/` must be on durable local Linux storage for tests.

```sh
go mod download
go mod verify
bash scripts/build-release.sh .private/release/first
bash scripts/build-release.sh .private/release/second
for file in marc_v0.1.0_linux_amd64.tar.gz marc_v0.1.0_linux_arm64.tar.gz SHA256SUMS; do
  cmp ".private/release/first/$file" ".private/release/second/$file"
done
(cd .private/release/first && sha256sum -c SHA256SUMS)
```

[The packaging script](../scripts/build-release.sh) rejects dirty inputs and
unexpected Go versions. It pins target CPU baselines, disables CGO, VCS stamping
and build IDs, trims source paths, and records the commit in `BUILD-INFO`.
Archives have sorted entries, normalized modes/ownership and commit timestamps;
gzip omits its timestamp and filename. Each contains `marc`, source/license
attributions, security guidance and product documentation. Reproduction requires
the same commit, toolchain and dependencies; the workflow compares independent
build directories on one host, not independent machines or toolchain bootstrap.

## Before publication

Require both hosted release-validation jobs to pass for the final commit, review
the checksums and archive contents, and review the draft notes against that run.
Native ARM64 coverage requires a successful ARM64 job for that commit; an AMD64
pass or cross-compilation alone is insufficient. Daily CI and release validation
use Go 1.27.3; the minimum supported version is Go 1.27.0.
Private vulnerability reporting and branch protection are repository settings;
the release workflow does not reconfigure or attest them. No signing or
provenance attestation is prepared by this process.

Do not publish local databases, logs, client traces, credentials or `.private/`
contents wholesale. Only the two named archives and `SHA256SUMS` are intended
as eventual release assets, after separate publication authorization.
