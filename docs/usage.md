# Build, configuration and usage

## Requirements

Use Linux, Git on PATH and Go 1.27.0 or later. Go 1.27.3 is the validated
toolchain used by CI. WSL2 is supported with source/data
on the Linux filesystem. SQLite is pure Go; CGO and a SQLite CLI are unnecessary.
Python 3 is needed only for the documentation checker.

```sh
go mod download
CGO_ENABLED=0 go build -o bin/marc ./src/cmd/marc
```

No model credentials are required by the fake workflow or core tests. Module and
toolchain acquisition can require network access. Core tests use local fixtures.

## Storage

Use a new durable local Linux data directory, owned by the controller user with
mode 0700. Startup can create it. Network/FUSE/9p filesystems, volatile tmpfs/ramfs,
unsafe ancestors, symlinks and WSL Windows mounts are rejected by normal startup.
The default is `$XDG_DATA_HOME/ai-control-plane` or `~/.local/share/ai-control-plane`.
One `control-plane.db` serves all Runs in the installation.

Startup initializes schema version 1 and checks exact checksums/definitions,
foreign keys and integrity before readiness. Earlier pre-release schema checksums
are rejected; no database upgrade/import path is available. Preserve old data
and choose a new directory rather than deleting evidence to bypass an error.

## Deterministic example

Use a fresh Run ID for a new task. The data path below must be on durable Linux
storage, including the home directory's underlying filesystem.

```sh
MARC_DATA="$HOME/.local/share/marc-preview"
./bin/marc --data-dir "$MARC_DATA" \
  --fake-run example-run --goal 'Verify the deterministic task artifact.' \
  --normal-tokens 1000 --approve-budget 'I approve 1000 normal WORK tokens.'
./bin/marc --data-dir "$MARC_DATA" --resume example-run
```

The fake produces an immutable goal artifact and fixture usage, then validates
its role/proof chain. A successful final JSON reports `READY_FOR_HUMAN_MERGE`, a
`Terminal` reference and `AccountingStatus: "complete"`. Resume preserves the
terminal report and starts no new work after readiness. This is a control-plane
example, not validation of a real coding task or model provider.

The fake requires explicit budget approval, including for its 500,000-token
default. Its Run Hard window defaults to one hour and never extends on restart.
Optional human decisions use `--increase-normal-tokens`, `--approve-increase`
and `--interaction-id` on a running resumed fake Run. Presented Hard requests
can be decided with `--hard-request`, `--hard-decision APPROVE|DENY|TIMEOUT`,
and actual `--human-response`/`--interaction-id` for explicit responses.
A timeout cannot increase the Run bank. Only use real human authorization.

Without workflow flags, the CLI opens persistence and waits:

```sh
./bin/marc --data-dir "$MARC_DATA"
```

SIGINT/SIGTERM closes the store. MCP mode reserves stdout for JSON-RPC.

## External workflow

Register the server with absolute paths:

```sh
codex mcp add marc -- /absolute/path/to/bin/marc --mcp \
  --data-dir /absolute/path/to/private/control-data
```

Registration changes local client configuration; it does not start a workflow.
Use a separate private data directory outside coding worktrees for simpler
operation. A contained data root is also excluded from worktree capture.
See [MCP usage](mcp.md) for the four tools and submission rules.

The external workflow reports accounting as `excluded`. It does not constrain
client model requests or attest model identity, fresh contexts or test execution.
There is no external process supervisor or automatic merge/push/cleanup.

## Compatibility and failures

The CLI has no `--version` flag; record the source identity, Go version and
`go version -m bin/marc`. MCP initialization reports version `0.1.0`.
No stable API, migration compatibility or long-term support window is promised.

Inspect current workflow state after an uncertain acknowledgement before retrying.
Identical replay is supported; altered accepted payloads or stale fences reject.
Original deadlines remain immutable. New roles after expiry reject even if an
already-issued role submitted its result late. Missing authority blocks readiness.
No automatic retry renews a budget, deadline or semantic role.

## Rename compatibility

MARC was previously named AI Control Plane. Build the renamed CLI from
`./src/cmd/marc`; the previous Go module/import path and command source directory
have changed. The module remains a checkout-local path, `marc-runtime`; no GitHub
owner or hosted module installation path is assumed.

Existing CLI flags and MCP tool names/submission fields are unchanged. MCP
initialization now identifies the server as `marc`. Existing MCP registration
names remain usable; persisted workflow routing names are not rewritten. To
retain an existing executable path while rebuilding, use:

```sh
CGO_ENABLED=0 go build -o bin/ai-control-plane ./src/cmd/marc
```

Alternatively, update the client command to `bin/marc`. Existing live processes
continue using their loaded binary until restarted. No user-level client
configuration is automatically changed by the source rename.

The existing default `ai-control-plane` data directory and `control-plane.db`
filename are retained to avoid creating a second installation or stranding Runs.
Use `--data-dir` to select a different location explicitly. Schema resolver URIs,
database migration bytes, JSON wire contracts, stored identifiers and old test
helper environment names remain unchanged. `MARC_BENCH_ROOT` is the preferred
benchmark storage variable; `ACP_B6_BENCH_ROOT` remains a fallback. The rename
does not migrate or rewrite stored evidence.
