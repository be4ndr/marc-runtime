# Contributing

Changes should preserve Linux-only scope, Orchestrator mutation authority, role
isolation, truthful unknown usage and the human merge boundary. Keep changes
bounded and update product documentation when behavior changes. Use deterministic
regression tests and fake adapters; core tests require no model credentials.

Use this checkout as the active workspace. Put internal plans, research, logs,
diagnostics, local databases and temporary development artifacts under the ignored
`.private/` directory. Never stage that directory or force-add ignored files.
Keep credentials out of source and public examples.

Contributions intentionally submitted for inclusion follow Section 5 of
[Apache-2.0](LICENSE) unless explicitly stated otherwise. Submit only authorized
material and preserve required third-party notices.

```sh
go mod download
test -z "$(git ls-files -z '*.go' | xargs -0 gofmt -l)"
CGO_ENABLED=0 GOPROXY=off GOTOOLCHAIN=local go build -o bin/marc ./src/cmd/marc
CGO_ENABLED=0 GOPROXY=off GOTOOLCHAIN=local MARC_TEST_BINARY="$PWD/bin/marc" go test ./... -count=1
GOPROXY=off GOTOOLCHAIN=local go vet ./...
python3 scripts/check-docs.py
git diff --check
```

Go 1.27.0 is the minimum supported version; use Go 1.27.3 for CI and release
validation. Python 3 is needed for documentation checks. CI tests Linux with
Go 1.27.3 and runs focused race checks, requiring CGO and a C compiler.
CI acquires dependencies before running core checks offline. It has read-only
repository permission and cancels outdated runs for the same pull request.
The required check is `Linux Go 1.27.3`. When adopting this upgrade, replace
`Linux Go 1.25.0` and `Linux Go 1.26.8` in the GitHub Ruleset's required status
checks with `Linux Go 1.27.3` from GitHub Actions.

The suite uses `MARC_TEST_BINARY` to smoke-test the built CLI's readiness,
SIGINT/SIGTERM shutdown, fake workflow and resume. Existing MCP subprocess tests
use the same executable for protocol startup, workflow execution, EOF shutdown,
restart/replay and authority boundaries, without a second copy of those tests.
These binary tests require durable local storage for Go's temporary directories;
set `TMPDIR` accordingly if the default is tmpfs. Without `MARC_TEST_BINARY`, CLI
binary smoke tests skip and MCP tests use their test helper. Focused race checks
leave it unset to retain race instrumentation in MCP subprocesses. The full suite
continues to cover StateStore and Orchestrator restart/replay and authority
regressions. No external AI provider or model credentials are needed.

CI has no publication step. Full release testing and label-based CI remain
deferred. Hosted execution requires separate verification. Report security issues
through [Security](SECURITY.md).
