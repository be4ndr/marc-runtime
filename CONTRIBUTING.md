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
CGO_ENABLED=0 GOPROXY=off GOTOOLCHAIN=local go test ./... -count=1
GOPROXY=off GOTOOLCHAIN=local go vet ./...
CGO_ENABLED=0 GOPROXY=off GOTOOLCHAIN=local go build -o bin/marc ./src/cmd/marc
python3 scripts/check-docs.py
git diff --check
```

Python 3 is needed for documentation checks. CI tests Linux with Go 1.25.0 and
1.26.8; the latter also runs focused race checks, requiring CGO and a C compiler.
CI acquires dependencies before running core checks offline. It has read-only
repository permission and no publication step. Hosted execution requires separate
verification. Report security issues through [Security](SECURITY.md).
