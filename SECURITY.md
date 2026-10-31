# Security

The Technical Preview targets a trusted local Linux host. It has no stable-release
support window, hostile same-UID sandbox, remote MCP authentication or multi-user
isolation. External clients retain their own filesystem, credential and process
permissions. Managed-provider execution is unavailable.

Only Orchestrator requests authoritative mutations. StateStore validates schema,
ownership, lifecycle and replay fences. Worktree hashes bind candidate content;
semantic results do not prove arbitrary claimed tests actually executed. Fresh
external role contexts and appropriate client permissions are operator obligations.

Use private durable Linux storage. Database contents, approvals, diagnostics and
evidence may be sensitive. No application-level encryption or automatic retention/
destructive cleanup is provided. Never upload databases, full diagnostics, client
transcripts or credentials to a public issue.

## Confidential vulnerability reports

Use this GitHub repository's **Security → Advisories → Report a vulnerability**.
The administrator must enable private vulnerability reporting and verify reporter
availability and maintainer notifications before public release. Activation is
not verified by these local source files.

If the private button is absent, withhold sensitive details until a confidential
GitHub channel is available. Public contacts use GitHub only; email addresses in
license attribution are not reporting contacts. Do not use a public issue,
discussion or pull request for confidential exploit details.

Include the source identity, Linux/Go/client versions, affected boundary, expected
and observed behavior, and a minimal sanitized reproduction using synthetic data.
Coordinate remediation and disclosure privately. No response-time guarantee is
currently defined. A report does not authorize destructive testing or access to
other users' systems.
