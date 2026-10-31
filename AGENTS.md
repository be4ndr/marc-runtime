# Development instructions

AI Control Plane is a Linux-only Go project. Keep changes narrowly scoped.
Preserve Orchestrator-only authoritative mutations, SQLite authority, role
isolation, exact replay/currentness checks, truthful unknown usage and manual merge.
Do not synthesize external client calls or accounting.

Use source, schemas, migrations and product documentation as implementation
context. Put internal plans, research, diagnostics, logs, databases and temporary
artifacts under the ignored `.private/` directory. Never stage private material.
Do not publish credentials, private paths or real client identities.

Run relevant deterministic tests and `go vet ./...`, check documentation links,
`git diff --check` and `git status --short`. Core tests require no model access.
Do not commit, push, publish or alter Git history unless explicitly requested.
