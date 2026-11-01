# MARC (Multi-Agent Runtime Control)

MARC (Multi-Agent Runtime Control) is an open-source control plane for orchestrating AI agents. It coordinates multi-agent workflows, enforces execution boundaries, maintains durable state, and supports auditable execution.

Linux-only local control plane for coding-agent workflows. Go owns workflow
transitions and SQLite persistence; an external Codex MCP client performs coding
and validation.

Repository name: `marc-runtime`. The Go module is `marc-runtime` and the executable
is `marc`; build from a source checkout.

The v0.1.0 Technical Preview supports sequential Lead → Developer → Lead → QA →
Lead → Reviewer → Lead work, bounded remediation, explicit Finding resolution,
restart/replay and readiness for human merge. Merge and push remain human actions.

The external client's model requests and usage are outside control-plane token
admission and accounting. Managed-provider execution is unavailable. A deterministic
fake workflow exercises the managed contracts without model access.

Use a trusted local Linux host. There is no sandbox against hostile processes
running as the controller user, remote MCP authentication or multi-user isolation.
Read [Security](SECURITY.md) before connecting a client.

- [Build, configuration and usage](docs/usage.md)
- [MCP workflow interface](docs/mcp.md)
- [Contributing and validation](CONTRIBUTING.md)

Linux, Git and Go 1.27.0 or later are required. Go 1.27.3 is the validated
toolchain used by CI. Build from this checkout:

```sh
go mod download
CGO_ENABLED=0 go build -o bin/marc ./src/cmd/marc
```

The source is licensed under [Apache-2.0](LICENSE). See [copyright attribution](COPYRIGHT)
and [third-party notices](THIRD_PARTY_NOTICES.md). This source tree contains no
runtime installations, client traces or development-history archives.
