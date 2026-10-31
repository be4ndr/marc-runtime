# External MCP workflow

The boundary is external MCP client → MCP server → Orchestrator → StateStore.
Only Orchestrator initiates authoritative mutations. Lead supplies semantic intent;
Workers return structured claims. SQLite validates persisted ownership, lifecycle,
content and replay fences. Local actor identity is not cryptographic authentication.

Use [installation instructions](usage.md) to register a trusted local stdio server.
The client discovers tool schemas through MCP `tools/list`.

| Tool | Inputs and behavior |
| --- | --- |
| `workflow_start` | `run_id`, `goal`, canonical existing Git `worktree_path`, actual `approval_response`, and `mcp_client_name`. Optional `hard_window_seconds` defaults to 3600, maximum 86400. Creates an approved external task and initial Lead role. |
| `workflow_inspect` | `run_id`. Returns current role, task, invocation, ownership/semantic fences, candidate identity, original deadline and role-scoped facts. Lead-only `history: {limit: 1..4, before_step?: N}` retrieves bounded prior decisions. |
| `workflow_advance` | `run_id` and optional `submission`. Without a submission, reconciles captured work or issues the next accepted role. A submission returns the current role's structured result/decision with exact returned fences. |
| `workflow_complete` | `run_id`. Verifies final Lead approval, current candidate and positive independent role/proof chain before readiness for human merge. |

`mcp_client_name` is the registration name (`control-plane` in the setup example),
not initialize's client identity. It is immutable routing information, not authority.
It must be nonempty UTF-8, at most 256 code points, with no whitespace/control characters.

## Role submissions

Use a fresh external context for each Worker role. Never pass transcripts between
roles. The server cannot erase or attest client context. Follow returned task
instructions, requested model/effort and `next_decision`.

Every submission includes `invocation_id`, `ownership_epoch`, `actor_revision`,
`semantic_revision` and `summary` from the current view. Lead sends `decision`:
`DEVELOPER`, `QA`, `REVIEWER`, `REMEDIATE`, `APPROVE` or `BLOCK`.
Workers send `outcome` and actual `evidence` text. Developer can include
`remaining_work` for incomplete work or `blocker` for blocked work.

QA includes checks with `check_id`, `required`, `outcome` and `summary`. Passed
QA requires an explicitly passed required check. Overall QA `failed` or Reviewer
`changes_required` requires an occurrence of a confirmed blocking Finding; an
individual failed check alone does not establish that overall verdict.

Finding definitions/occurrences follow the schemas returned by `tools/list` and
[role-result definitions](../schemas/role-result-common.schema.json). Use returned
`result_id`, cycle and `evidence_id` for origin/evidence bindings. Keep stable
Finding definitions immutable. Never invent findings or relabel a confirmed
blocker as inconclusive. Resolution requires explicit Lead
`resolved_findings: [{finding_id, evidence_id}]` with accepted same-Run evidence.
A later occurrence reopens the same Finding; disappearance is not resolution.

## Candidates and limits

Developer submission freezes a controller-captured candidate. Stop editing before
submission. QA/Reviewer preserve its content. Completion rechecks paths, modes,
file lengths/hashes and HEAD. Changes invalidate dependent authority. Only an
admitted remediation Developer may open a new candidate edit window.

`REMEDIATE` selects current open Finding IDs and needs concrete `escalation_reason`.
There are at most three candidates and three invocations per Worker role, counted
across the Run. Requested profile levels cannot be skipped. Exhaustion returns
to Lead for controlled BLOCK. There is no automatic unblock or attestation of the
actual external model used.

Descriptive text allows 1024 Unicode code points per field/item; structural strings
normally allow 256. Ingress is bounded to 1 MiB, 32 nested containers and 65,536
JSON tokens. Malformed Unicode, duplicate keys and stale/conflicting submissions
reject before authoritative mutation. On an unambiguous descriptive-length error,
follow the field-specific advice; never rewrite actual approval or accepted text.

Worktree capture allows a 512 KiB manifest, 64 MiB per regular file, 256 MiB aggregate
content and 1 MiB Git output. It rejects unsupported symlinks, submodules and
nonregular files. Protected data-root contents/aliases are excluded. The host
controller user and mount namespace remain trusted; hostile concurrent edits
and privileged filesystem changes are not contained.

## Readiness and recovery

The workflow is Lead → Developer → Lead → QA → Lead → Reviewer → Lead.
Only the final controller transition establishes `READY_FOR_HUMAN_MERGE`. It
requires current positive Developer/QA/Reviewer results, Lead approval, no blocking
Findings and a complete proof chain. A claim that tests passed is still semantic
evidence, not independent controller attestation of arbitrary external commands.

Inspection/restart/identical replay never extends the original deadline or launches
the client. Lost acknowledgements reconcile captured state instead of starting
new external work. Unknown outcomes do not fabricate completion. External model
calls, receipts, token grants and usage totals are never synthesized.

The optional metrics projector is a controller API and is disabled unless explicitly
bound; metrics are not recovery input. External context-health bytes/hashes measure
returned projections. Unobserved prompt, token, context-window and reset metrics
remain unknown. No automatic data or worktree deletion is performed.
