package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"marc-runtime/internal/identity"
	"marc-runtime/schemas"
	"marc-runtime/src/internal/roleresult"
	"marc-runtime/src/internal/statestore"
)

// ExternalStart descriptions (Goal, ApprovalResponse) allow 1024 Unicode code
// points. Identifiers, MCP client names and paths retain their existing limits.
type ExternalStart struct {
	RunID             string `json:"run_id"`
	Goal              string `json:"goal"`
	WorktreePath      string `json:"worktree_path"`
	ApprovalResponse  string `json:"approval_response"`
	MCPClientName     string `json:"mcp_client_name,omitempty"`
	HardWindowSeconds int64  `json:"hard_window_seconds,omitempty"`
}
type ExternalCheck struct {
	CheckID  string `json:"check_id"`
	Required bool   `json:"required"`
	Outcome  string `json:"outcome"`
	Summary  string `json:"summary"`
}
type ExternalResolution struct {
	FindingID  string `json:"finding_id"`
	EvidenceID string `json:"evidence_id"`
}

// ExternalSubmission descriptive text allows 1024 Unicode code points per field
// or array item; identifiers, decisions and outcomes retain existing limits.
type ExternalSubmission struct {
	EscalationReason   string                  `json:"escalation_reason,omitempty"`
	FindingIDs         []string                `json:"finding_ids,omitempty"`
	ResolvedFindings   []ExternalResolution    `json:"resolved_findings,omitempty"`
	InvocationID       string                  `json:"invocation_id"`
	OwnershipEpoch     int64                   `json:"ownership_epoch"`
	ActorRevision      int64                   `json:"actor_revision"`
	SemanticRevision   int64                   `json:"semantic_revision"`
	Decision           string                  `json:"decision,omitempty"`
	Outcome            string                  `json:"outcome,omitempty"`
	Summary            string                  `json:"summary"`
	Evidence           string                  `json:"evidence,omitempty"`
	Checks             []ExternalCheck         `json:"checks,omitempty"`
	RemainingWork      []string                `json:"remaining_work,omitempty"`
	Blocker            string                  `json:"blocker,omitempty"`
	FindingDefinitions []roleresult.Finding    `json:"finding_definitions,omitempty"`
	FindingOccurrences []roleresult.Occurrence `json:"finding_occurrences,omitempty"`
}
type worktreeFile struct {
	Path   string
	Mode   uint32
	Chunks [][]byte
	Size   int64  `json:",omitempty"`
	SHA256 string `json:",omitempty"`
}
type worktreeSnapshot struct {
	Version int `json:",omitempty"`
	HEAD    string
	Files   []worktreeFile
}
type externalOutput struct {
	Submission ExternalSubmission
	Worktree   worktreeSnapshot
	At         string
}
type ExternalResult struct {
	ResultID           string                   `json:"result_id"`
	Role               string                   `json:"role"`
	Outcome            string                   `json:"outcome"`
	ContentIdentity    string                   `json:"content_identity"`
	Summary            string                   `json:"summary"`
	EvidenceRefs       []roleresult.EvidenceRef `json:"evidence_refs"`
	Checks             []roleresult.Check       `json:"checks,omitempty"`
	FindingDefinitions []roleresult.Finding     `json:"finding_definitions,omitempty"`
	FindingOccurrences []roleresult.Occurrence  `json:"finding_occurrences,omitempty"`
}

type ExternalFinding struct {
	Definition           roleresult.Finding `json:"definition"`
	SourceRole           string             `json:"source_role"`
	SourceResultID       string             `json:"source_result_id"`
	ContentIdentity      string             `json:"content_identity"`
	ResolutionEvidenceID string             `json:"resolution_evidence_id,omitempty"`
}
type ExternalView struct {
	ContextHealth        *ExternalContextHealth        `json:"context_health,omitempty"`
	History              *ExternalHistoryPage          `json:"history,omitempty"`
	MCPClientName        string                        `json:"mcp_client_name,omitempty"`
	Instructions         string                        `json:"instructions,omitempty"`
	TaskInstructions     string                        `json:"task_instructions,omitempty"`
	LeadIntent           string                        `json:"lead_intent,omitempty"`
	FindingIDs           []string                      `json:"finding_ids,omitempty"`
	Findings             []ExternalFinding             `json:"findings,omitempty"`
	NextDecision         string                        `json:"next_decision,omitempty"`
	EscalationLevel      int                           `json:"escalation_level"`
	ModelScope           string                        `json:"model_scope"`
	RunID                string                        `json:"run_id"`
	Status               string                        `json:"status"`
	HardDeadline         string                        `json:"hard_deadline"`
	Goal                 string                        `json:"goal"`
	WorktreePath         string                        `json:"worktree_path"`
	ContentIdentity      string                        `json:"content_identity"`
	ContentDigest        string                        `json:"content_digest,omitempty"`
	Step                 *statestore.CurrentInvocation `json:"step,omitempty"`
	OwnershipEpoch       int64                         `json:"ownership_epoch"`
	ActorRevision        int64                         `json:"actor_revision"`
	SemanticRevision     int64                         `json:"semantic_revision"`
	StepCompleted        bool                          `json:"step_completed"`
	AcceptedResults      []ExternalResult              `json:"accepted_results,omitempty"`
	Terminal             statestore.ProofReference     `json:"terminal"`
	AccountingScope      string                        `json:"accounting_scope"`
	AccountingStatus     string                        `json:"accounting_status"`
	AccountingIncomplete bool                          `json:"accounting_incomplete"`
	ResultID             string                        `json:"result_id,omitempty"`
	EvidenceID           string                        `json:"evidence_id,omitempty"`
}

func gitRead(ctx context.Context, root *os.File, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	argv := append([]string{"-c", "core.fsmonitor=false", "-C", "/proc/self/fd/3"}, args...)
	cmd := exec.CommandContext(ctx, "git", argv...)
	cmd.ExtraFiles = []*os.File{root}
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_OPTIONAL_LOCKS=0"}
	var out, stderr boundedBuffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("read Git worktree: %w", err)
	}
	return out.Bytes(), nil
}

const (
	maxWorktreeManifestBytes       = identity.MaxJSONBytes / 2
	maxWorktreeFileBytes     int64 = 64 << 20
	maxWorktreeBytes         int64 = 256 << 20
)

func (r *Runtime) snapshotWorktree(ctx context.Context, path string) (worktreeSnapshot, error) {
	return r.captureWorktree(ctx, path, 1)
}

// Retained version-zero evidence must keep its original encoding and digest.
func (r *Runtime) captureWorktree(ctx context.Context, path string, version int) (worktreeSnapshot, error) {
	var out worktreeSnapshot
	if version != 0 && version != 1 {
		return out, fmt.Errorf("unsupported worktree snapshot version %d", version)
	}
	out.Version = version
	if !identity.ValidText(path) || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return out, fmt.Errorf("absolute canonical worktree required")
	}
	protection, err := r.worktreeProtection()
	if err != nil {
		return out, err
	}
	protected, err := protection.contains(path)
	if err != nil {
		return out, err
	}
	if protected {
		return out, fmt.Errorf("worktree is inside protected data directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return out, fmt.Errorf("worktree path must not traverse symlinks")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return out, fmt.Errorf("cannot open worktree safely")
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return out, fmt.Errorf("cannot open worktree safely")
	}
	defer directory.Close()
	actual, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()))
	if err != nil {
		return out, fmt.Errorf("cannot establish worktree location")
	}
	protected, err = protection.contains(actual)
	if err != nil || protected || actual != path {
		return out, fmt.Errorf("worktree location changed or is protected")
	}
	if err := protection.checkGitMetadata(directory, actual); err != nil {
		return out, err
	}
	top, err := gitRead(ctx, directory, "rev-parse", "--show-toplevel")
	if err != nil || strings.TrimSpace(string(top)) != path {
		return out, fmt.Errorf("use the Git worktree root")
	}
	head, err := gitRead(ctx, directory, "rev-parse", "HEAD")
	if err != nil {
		return out, err
	}
	out.HEAD = strings.TrimSpace(string(head))
	args := []string{"ls-files", "--cached", "--others", "--exclude-standard", "-z"}
	args = append(args, protection.gitExcludes(actual)...)
	names, err := gitRead(ctx, directory, args...)
	if err != nil {
		return out, err
	}
	// Exact JSON size of the array form, plus each encoded file and comma.
	// Re-encoding the entire growing snapshot made bounded ingestion quadratic.
	size := len(marshal(worktreeSnapshot{Version: version, HEAD: out.HEAD, Files: []worktreeFile{}}))
	var total int64
	seen := map[string]bool{}
	for _, part := range bytes.Split(names, []byte{0}) {
		name := string(part)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if !identity.ValidText(name) || filepath.IsAbs(name) || filepath.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") {
			return out, fmt.Errorf("unsafe worktree path")
		}
		protected, e := protection.contains(filepath.Join(actual, name))
		if e != nil {
			return out, e
		}
		if protected {
			continue
		}
		if err := r.cut("worktree_before_open"); err != nil {
			return out, err
		}
		current, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()))
		if err != nil || current != actual {
			return out, fmt.Errorf("worktree location changed during capture")
		}
		f, e := protection.openFile(directory, name)
		if e != nil {
			return out, e
		}
		if f == nil {
			continue
		}
		st, e := f.Stat()
		if e != nil || !st.Mode().IsRegular() {
			f.Close()
			return out, fmt.Errorf("worktree contains nonregular content")
		}
		file := worktreeFile{Path: name, Mode: uint32(st.Mode().Perm())}
		if version == 0 {
			raw, err := io.ReadAll(io.LimitReader(f, identity.MaxJSONBytes+1))
			f.Close()
			if err != nil {
				return out, err
			}
			for offset := 0; offset < len(raw); offset += 192 {
				file.Chunks = append(file.Chunks, raw[offset:min(offset+192, len(raw))])
			}
		} else {
			limit := min(maxWorktreeFileBytes, maxWorktreeBytes-total)
			if st.Size() > limit {
				f.Close()
				return out, fmt.Errorf("worktree content exceeds byte bound at %q (file limit %d, remaining total %d)", name, maxWorktreeFileBytes, maxWorktreeBytes-total)
			}
			h := sha256.New()
			n, err := io.Copy(h, io.LimitReader(f, limit+1))
			f.Close()
			if err != nil {
				return out, err
			}
			if n > limit || n != st.Size() {
				return out, fmt.Errorf("worktree content grew or changed size at %q", name)
			}
			if err := ctx.Err(); err != nil {
				return out, err
			}
			total += n
			file.Size, file.SHA256 = n, fmt.Sprintf("%x", h.Sum(nil))
		}
		size += len(marshal(file))
		if len(out.Files) > 0 {
			size++
		}
		if size > maxWorktreeManifestBytes {
			return out, fmt.Errorf("worktree snapshot JSON exceeds %d-byte bound", maxWorktreeManifestBytes)
		}
		out.Files = append(out.Files, file)
	}
	current, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()))
	if err != nil || current != actual {
		return out, fmt.Errorf("worktree location changed during capture")
	}
	return out, nil
}
func snapshotDigest(s worktreeSnapshot) string {
	// Git groups untracked and indexed files differently. Compare canonical
	// content order without rewriting retained snapshots or evidence.
	s.Files = slices.Clone(s.Files)
	slices.SortFunc(s.Files, func(a, b worktreeFile) int { return strings.Compare(a.Path, b.Path) })
	return fmt.Sprintf("%x", sha256.Sum256(marshal(s)))
}
func externalMode(w statestore.WorkflowSnapshot) error {
	if w.Spec.Mode != "EXTERNAL" {
		return fmt.Errorf("MCP requires an external workflow")
	}
	return nil
}
func (r *Runtime) frozen(ctx context.Context, w statestore.WorkflowSnapshot) (string, error) {
	for _, s := range w.Steps {
		if s.Role == "DEVELOPER" && s.ContentIdentity != nil && *s.ContentIdentity == w.ContentIdentity && len(s.Output) > 0 {
			var old externalOutput
			if err := json.Unmarshal(s.Output, &old); err != nil {
				return "", err
			}
			now, err := r.captureWorktree(ctx, w.Spec.WorktreePath, old.Worktree.Version)
			if err != nil {
				return "", err
			}
			if snapshotDigest(now) != snapshotDigest(old.Worktree) {
				return "", fmt.Errorf("worktree changed after Developer submission; QA/review authority is stale")
			}
			return snapshotDigest(now), nil
		}
	}
	return "", nil
}
func (r *Runtime) ExternalInspect(ctx context.Context, run string) (ExternalView, error) {
	return r.externalInspect(ctx, run, nil)
}

func (r *Runtime) externalInspect(ctx context.Context, run string, history *ExternalHistoryRequest) (ExternalView, error) {
	w, err := r.store.GetWorkflow(ctx, run)
	if err != nil {
		return ExternalView{}, err
	}
	if err = externalMode(w); err != nil {
		return ExternalView{}, err
	}
	digest, err := r.frozen(ctx, w)
	if err != nil {
		return ExternalView{}, err
	}
	v := ExternalView{RunID: run, Status: w.Status, HardDeadline: w.Spec.HardDeadline, Goal: w.Spec.Goal, WorktreePath: w.Spec.WorktreePath, ContentIdentity: w.ContentIdentity, ContentDigest: digest, OwnershipEpoch: w.Owner.OwnershipEpoch, ActorRevision: w.Owner.ActorRevision, SemanticRevision: w.SemanticRevision, Terminal: w.Terminal, AccountingScope: "Codex internal requests excluded; no managed provider calls", AccountingStatus: w.AccountingStatus, AccountingIncomplete: w.AccountingIncomplete}
	if w.Spec.MCPClientName != "" {
		v.MCPClientName = w.Spec.MCPClientName
		v.Instructions = fmt.Sprintf("MANDATORY — EXCLUSIVE MCP WORKFLOW: All task-related reasoning, execution, delegation, and decisions MUST be coordinated through MCP %s. DO NOT independently initiate agents, workflows, or additional tasks; return all requests and results through %s and AWAIT authorization.", v.MCPClientName, v.MCPClientName)
	}
	if w.Current != nil {
		x := w.Current.CurrentInvocation
		v.LeadIntent = w.Current.Intent
		v.FindingIDs = w.Current.FindingIDs
		v.Step = &x
		v.StepCompleted = w.Current.CompletedAt != ""
		if w.Status == "RUNNING" && !v.StepCompleted {
			v.TaskInstructions = externalTaskInstructions(x.Role)
		}
		if x.Role != "LEAD" {
			v.ResultID = roleresult.ResultID(x.Cycle, w.Current.StepNo)
			v.EvidenceID = x.InvocationID + "-evidence"
		}
		v.ModelScope = "requested role profile; external client model selection is not attested"
		if x.Role == "LEAD" {
			v.NextDecision = externalNext(w)
			v.EscalationLevel = x.Cycle - 1
		}
		if x.Role != "LEAD" {
			for _, prior := range w.Steps {
				if prior.Role == x.Role && prior.StepNo < w.Current.StepNo {
					v.EscalationLevel++
				}
			}
		}
		if err := externalContextFacts(&v, w); err != nil {
			return v, err
		}
	}
	if history != nil {
		v.History, err = externalHistory(w, *history)
		if err != nil {
			return ExternalView{}, err
		}
	}
	return observeExternalContext(v)
}
func (r *Runtime) ExternalStart(ctx context.Context, p ExternalStart) (ExternalView, error) {
	if p.HardWindowSeconds == 0 {
		p.HardWindowSeconds = 3600
	}
	if p.HardWindowSeconds < 1 || p.HardWindowSeconds > 86400 {
		return ExternalView{}, fmt.Errorf("Hard window must be 1..86400 seconds")
	}
	if _, err := r.snapshotWorktree(ctx, p.WorktreePath); err != nil {
		return ExternalView{}, err
	}
	at := r.now().UTC()
	started, deadline := at.Format(time.RFC3339Nano), at.Add(time.Duration(p.HardWindowSeconds)*time.Second).Format(time.RFC3339Nano)
	replay := false
	var confirmation *statestore.HumanBudgetApproval
	if old, err := r.store.GetWorkflow(ctx, p.RunID); err == nil {
		replay = old.Current != nil
		retained := old.Spec.Confirmation
		retained.Response = p.ApprovalResponse
		confirmation = &retained
		if err = externalMode(old); err != nil {
			return ExternalView{}, err
		}
		a, _ := time.Parse(time.RFC3339Nano, old.Spec.StartedAt)
		d, _ := time.Parse(time.RFC3339Nano, old.Spec.HardDeadline)
		if d.Sub(a) != time.Duration(p.HardWindowSeconds)*time.Second {
			return ExternalView{}, fmt.Errorf("conflicting original Hard window")
		}
		started, deadline = old.Spec.StartedAt, old.Spec.HardDeadline
		// An omitted routing name on replay retains the immutable original,
		// including the absence of a name in legacy workflow input.
		if p.MCPClientName == "" {
			p.MCPClientName = old.Spec.MCPClientName
		}
	} else if p.MCPClientName == "" {
		return ExternalView{}, fmt.Errorf("mcp_client_name is required for a new external workflow")
	}
	spec := statestore.WorkflowSpec{RunID: p.RunID, Mode: "EXTERNAL", Goal: p.Goal, WorktreePath: p.WorktreePath, ContentIdentity: fmt.Sprintf("external-content-%x", sha256.Sum256([]byte(p.RunID))), StartedAt: started, HardDeadline: deadline, Confirmation: statestore.HumanBudgetApproval{ApprovalID: fmt.Sprintf("start-%x", sha256.Sum256([]byte(p.RunID))), InteractionID: fmt.Sprintf("start-interaction-%x", sha256.Sum256([]byte(p.RunID))), Prompt: "Start this external coding workflow for the supplied goal and worktree?", Response: p.ApprovalResponse, Decision: "APPROVE"}}
	spec.MCPClientName = p.MCPClientName
	if confirmation != nil {
		spec.Confirmation = *confirmation
	}
	if err := r.store.CreateWorkflow(ctx, spec); err != nil {
		return ExternalView{}, err
	}
	if replay {
		return r.ExternalInspect(ctx, p.RunID)
	}
	return r.ExternalAdvance(ctx, p.RunID, nil)
}
func (r *Runtime) ExternalAdvance(ctx context.Context, run string, input *ExternalSubmission) (ExternalView, error) {
	w, err := r.store.GetWorkflow(ctx, run)
	if err != nil {
		return ExternalView{}, err
	}
	if err = externalMode(w); err != nil {
		return ExternalView{}, err
	}
	if _, err = r.frozen(ctx, w); err != nil {
		return ExternalView{}, err
	}
	if input != nil {
		for _, s := range w.Steps {
			if s.InvocationID == input.InvocationID && len(s.Output) > 0 {
				var old externalOutput
				if err := json.Unmarshal(s.Output, &old); err != nil {
					return ExternalView{}, err
				}
				if string(marshal(old.Submission)) != string(marshal(input)) {
					return ExternalView{}, fmt.Errorf("conflicting external result replay")
				}
				if s.CompletedAt != "" {
					return r.ExternalInspect(ctx, run)
				}
			}
		}
	}
	if w.Status != "RUNNING" {
		if input != nil {
			return ExternalView{}, fmt.Errorf("workflow is not running")
		}
		return r.ExternalInspect(ctx, run)
	}
	if w.Current != nil && w.Current.CompletedAt == "" {
		s := *w.Current
		if len(s.Output) == 0 {
			if input == nil {
				return r.ExternalInspect(ctx, run)
			}
			if input.InvocationID != s.InvocationID || input.OwnershipEpoch != w.Owner.OwnershipEpoch || input.ActorRevision != w.Owner.ActorRevision || input.SemanticRevision != w.SemanticRevision {
				return ExternalView{}, fmt.Errorf("stale external submission fence")
			}
			at := r.at()
			semantic, err := r.externalSemantic(ctx, w, *input, at)
			if err != nil {
				return ExternalView{}, err
			}
			snap, err := r.snapshotWorktree(ctx, w.Spec.WorktreePath)
			if err != nil {
				return ExternalView{}, err
			}
			out := externalOutput{*input, snap, at}
			if s.Role == "LEAD" {
				var lead struct {
					State  json.RawMessage
					Action json.RawMessage
				}
				if err = json.Unmarshal(semantic, &lead); err != nil {
					return ExternalView{}, err
				}
				if err = r.store.ValidateWorkflowLeadState(ctx, lead.State, statestore.MutationOptions{Fence: statestore.ResultFence{OwnershipEpoch: w.Owner.OwnershipEpoch, ActorRevision: w.Owner.ActorRevision}, ExpectedSemanticRevision: w.SemanticRevision}); err != nil {
					return ExternalView{}, err
				}
				err = r.store.CaptureExternalWorkflowOutput(ctx, run, s.InvocationID, marshal(out), out.At, w.Owner, w.SemanticRevision)
			} else {
				err = r.store.CaptureExternalWorkflowResult(ctx, run, s.InvocationID, marshal(out), semantic, input.FindingDefinitions, []byte(input.Evidence), out.At, w.Owner, w.SemanticRevision)
			}
			if err != nil {
				return ExternalView{}, err
			}
			if err = r.cut("external_after_capture"); err != nil {
				return ExternalView{}, err
			}
			w, err = r.store.GetWorkflow(ctx, run)
			if err != nil {
				return ExternalView{}, err
			}
		}
		if err = r.reconcileExternal(ctx, w); err != nil {
			return ExternalView{}, err
		}
		return r.ExternalInspect(ctx, run)
	}
	if input != nil {
		return ExternalView{}, fmt.Errorf("submission does not match current work")
	}
	action := ""
	if w.Current != nil && w.Current.Role == "LEAD" {
		var out externalOutput
		if err = json.Unmarshal(w.Current.Output, &out); err != nil {
			return ExternalView{}, err
		}
		if out.Submission.Decision == "APPROVE" {
			return r.ExternalInspect(ctx, run)
		}
		if out.Submission.Decision == "BLOCK" {
			if err = r.store.BlockWorkflow(ctx, run, "Lead blocked external workflow", r.at()); err != nil {
				return ExternalView{}, err
			}
			return r.ExternalInspect(ctx, run)
		}
		action = w.Current.InvocationID + "-action"
	}
	if _, err = r.store.BeginWorkflowStep(ctx, run, action, w.Owner, r.at()); err != nil {
		return ExternalView{}, err
	}
	if err = r.cut("external_after_step"); err != nil {
		return ExternalView{}, err
	}
	w, err = r.store.GetWorkflow(ctx, run)
	if err != nil {
		return ExternalView{}, err
	}
	if w.Current.Role == "REVIEWER" {
		if err = r.reviewAdmission(ctx, w); err != nil {
			return ExternalView{}, err
		}
	}
	return r.ExternalInspect(ctx, run)
}

func (r *Runtime) externalSemantic(ctx context.Context, w statestore.WorkflowSnapshot, in ExternalSubmission, at string) (json.RawMessage, error) {
	s := *w.Current
	if !identity.ValidDescription(in.EscalationReason) {
		return nil, fmt.Errorf("invalid escalation reason")
	}
	for _, work := range in.RemainingWork {
		if !identity.ValidDescription(work) {
			return nil, fmt.Errorf("invalid remaining work")
		}
	}
	if !identity.ValidDescription(in.Evidence) || !identity.ValidDescription(in.Blocker) {
		return nil, fmt.Errorf("invalid evidence text")
	}
	for _, c := range in.Checks {
		if !identity.ValidText(c.CheckID) || !identity.ValidDescription(c.Summary) || !identity.ValidText(c.Outcome) {
			return nil, fmt.Errorf("invalid QA text")
		}
	}
	if strings.TrimSpace(in.Summary) == "" || !identity.ValidDescription(in.Summary) {
		return nil, fmt.Errorf("structured summary required")
	}
	if s.Role == "LEAD" {
		if in.Outcome != "" || in.Evidence != "" || len(in.Checks) > 0 || len(in.RemainingWork) > 0 || in.Blocker != "" || len(in.FindingDefinitions) > 0 || len(in.FindingOccurrences) > 0 {
			return nil, fmt.Errorf("Lead supplies a decision, not Worker results")
		}
		qa, review := map[string]any{"status": "not_run"}, map[string]any{"status": "not_run"}
		for _, x := range w.Steps {
			if x.ResultID == "" || x.ContentIdentity == nil || *x.ContentIdentity != w.ContentIdentity {
				continue
			}
			v, e := r.store.WorkflowResult(ctx, w.Spec.RunID, x.InvocationID)
			if e != nil {
				return nil, e
			}
			if x.Role == "QA" {
				qa = map[string]any{"status": v.Outcome, "result_ref": map[string]string{"result_id": v.ResultID}}
			}
			if x.Role == "REVIEWER" {
				review = map[string]any{"status": v.Outcome, "result_ref": map[string]string{"result_id": v.ResultID}}
			}
		}
		next := externalNext(w)
		if in.Decision != next && in.Decision != "BLOCK" {
			return nil, fmt.Errorf("expected %s or BLOCK", next)
		}
		target := in.Decision
		if target == "REMEDIATE" {
			target = "DEVELOPER"
		}
		for _, x := range w.Steps {
			if x.Role == target && in.Decision != "BLOCK" && (strings.TrimSpace(in.EscalationReason) == "" || !identity.ValidDescription(in.EscalationReason)) {
				return nil, fmt.Errorf("semantic escalation requires concrete Lead justification")
			}
		}
		findings, err := externalFindings(w, in.ResolvedFindings)
		if err != nil {
			return nil, err
		}
		open, resolved := []any{}, []any{}
		openIDs := map[string]bool{}
		for _, f := range findings {
			entry := map[string]any{"finding_id": f.Definition.FindingID, "source_role": strings.ToLower(f.SourceRole), "source_result_ref": map[string]string{"result_id": f.SourceResultID}, "content_identity": f.ContentIdentity, "blocking": f.Definition.Blocking, "summary": f.Definition.Summary}
			if f.ResolutionEvidenceID != "" {
				entry["resolution_ref"] = map[string]string{"evidence_id": f.ResolutionEvidenceID}
				resolved = append(resolved, entry)
			} else {
				if in.Decision == "APPROVE" && f.Definition.Blocking {
					return nil, fmt.Errorf("approval has an unresolved blocking finding")
				}
				open = append(open, entry)
				openIDs[f.Definition.FindingID] = true
			}
		}
		ids := append([]string{}, in.FindingIDs...)
		if in.Decision == "REMEDIATE" {
			if len(ids) == 0 {
				for _, f := range findings {
					if openIDs[f.Definition.FindingID] {
						ids = append(ids, f.Definition.FindingID)
					}
				}
			}
			seen := map[string]bool{}
			for _, id := range ids {
				if !openIDs[id] || seen[id] {
					return nil, fmt.Errorf("remediation targets must be unique current open findings")
				}
				seen[id] = true
			}
			if len(ids) == 0 {
				return nil, fmt.Errorf("remediation requires open findings")
			}
		} else if len(ids) > 0 {
			return nil, fmt.Errorf("finding targets require remediation")
		}
		status := "active"
		if in.Decision == "APPROVE" {
			status = "approved"
		}
		if in.Decision == "BLOCK" {
			status = "blocked"
		}
		state := map[string]any{"schema_version": 1, "lead_state_id": s.InvocationID + "-state", "run_id": w.Spec.RunID, "cycle": s.Cycle, "producer": map[string]any{"execution_id": s.ExecutionID, "attempt": 1, "invocation_id": s.InvocationID, "invocation": s.Ordinal}, "content_identity": w.ContentIdentity, "goal": w.Spec.Goal, "acceptance_criteria": []string{w.Spec.Goal}, "open_findings": open, "resolved_findings": resolved, "qa": qa, "review": review, "semantic_status": status, "created_at": at}
		var action any
		if status == "active" {
			purpose := map[string]string{"DEVELOPER": "IMPLEMENT", "QA": "QA", "REVIEWER": "REVIEW", "REMEDIATE": "REMEDIATE"}[in.Decision]
			action = map[string]any{"schema_version": 1, "action_id": s.InvocationID + "-action", "run_id": w.Spec.RunID, "cycle": s.Cycle, "based_on_lead_state": map[string]string{"lead_state_id": s.InvocationID + "-state"}, "producer": state["producer"], "content_identity": w.ContentIdentity, "target_role": target, "purpose": purpose, "finding_ids": ids, "intent": externalIntent(in), "created_at": at}
		}
		if action != nil {
			if err := schemas.Validate("next-action.schema.json", marshal(action)); err != nil {
				return nil, err
			}
		}
		return marshal(struct{ State, Action any }{state, action}), nil
	}
	if in.Decision != "" || strings.TrimSpace(in.Evidence) == "" || len(in.ResolvedFindings) > 0 || len(in.FindingIDs) > 0 || in.EscalationReason != "" {
		return nil, fmt.Errorf("Worker supplies outcome and evidence, not Lead authority")
	}
	if s.Role != "QA" && len(in.Checks) > 0 {
		return nil, fmt.Errorf("checks require QA")
	}
	if s.Role != "DEVELOPER" && (len(in.RemainingWork) > 0 || in.Blocker != "") {
		return nil, fmt.Errorf("remaining work/blocker requires Developer")
	}
	if s.Role == "DEVELOPER" && (len(in.FindingDefinitions) > 0 || len(in.FindingOccurrences) > 0) {
		return nil, fmt.Errorf("findings require QA or Reviewer")
	}
	known, err := externalFindings(w, nil)
	if err != nil {
		return nil, err
	}
	knownIDs := map[string]bool{}
	for _, f := range known {
		knownIDs[f.Definition.FindingID] = true
	}
	for _, f := range in.FindingDefinitions {
		knownIDs[f.FindingID] = true
	}
	if len(knownIDs) > 128 {
		return nil, fmt.Errorf("MVP workflow supports at most 128 stable findings")
	}
	for _, f := range in.FindingDefinitions {
		if err := roleresult.ValidateFinding(f); err != nil {
			return nil, err
		}
	}
	for _, o := range in.FindingOccurrences {
		if err := identity.CheckID("finding_id", o.FindingID); err != nil {
			return nil, err
		}
		locations := append([]roleresult.Location{}, o.Locations...)
		for _, ref := range o.EvidenceRefs {
			if err := identity.CheckID("evidence_id", ref.EvidenceID); err != nil {
				return nil, err
			}
			if ref.Location != nil {
				locations = append(locations, *ref.Location)
			}
		}
		for _, l := range locations {
			if !identity.ValidText(l.Path) {
				return nil, fmt.Errorf("invalid finding location text")
			}
		}
	}
	refs := []any{map[string]string{"evidence_id": s.InvocationID + "-evidence", "kind": "test_result"}}
	result := map[string]any{"schema_version": 1, "result_id": roleresult.ResultID(s.Cycle, s.StepNo), "result_ordinal": s.StepNo, "run_id": w.Spec.RunID, "cycle": s.Cycle, "execution_id": s.ExecutionID, "attempt_id": s.AttemptID, "invocation_id": s.InvocationID, "role": s.Role, "content_identity": w.ContentIdentity, "summary": in.Summary, "outcome": in.Outcome, "evidence_refs": refs, "produced_at": at}
	switch s.Role {
	case "DEVELOPER":
		remaining := in.RemainingWork
		if remaining == nil {
			remaining = []string{}
		}
		result["remaining_work"] = remaining
		if in.Blocker != "" {
			result["blocker"] = map[string]any{"summary": in.Blocker, "evidence_refs": refs}
		}
	case "QA":
		if in.Outcome == "passed" {
			required := false
			for _, c := range in.Checks {
				if c.Required {
					required = true
					if c.Outcome != "passed" {
						return nil, fmt.Errorf("required QA check did not pass")
					}
				}
			}
			if !required {
				return nil, fmt.Errorf("passed QA requires an explicit required check")
			}
		}
		var checks []any
		for _, c := range in.Checks {
			checks = append(checks, map[string]any{"check_id": c.CheckID, "required": c.Required, "outcome": c.Outcome, "summary": c.Summary, "evidence_refs": refs})
		}
		if checks == nil {
			checks = []any{}
		}
		result["checks"] = checks
		result["finding_occurrences"] = in.FindingOccurrences
		if in.FindingOccurrences == nil {
			result["finding_occurrences"] = []any{}
		}
	case "REVIEWER":
		result["finding_occurrences"] = in.FindingOccurrences
		if in.FindingOccurrences == nil {
			result["finding_occurrences"] = []any{}
		}
	}
	raw := marshal(result)
	if _, err := roleresult.Decode(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// Fold accepted facts, preserving stable origins and explicit Lead resolutions.
// A later occurrence reopens its existing identity; absence is never resolution.
func externalFindings(w statestore.WorkflowSnapshot, resolutions []ExternalResolution) ([]ExternalFinding, error) {
	definitions := map[string]roleresult.Finding{}
	facts := map[string]ExternalFinding{}
	for _, step := range w.Steps {
		if step.ResultID == "" && (step.Role != "LEAD" || step.CompletedAt == "") {
			continue
		}
		var out externalSemanticOutput
		if err := json.Unmarshal(step.Output, &out); err != nil {
			return nil, err
		}
		for _, f := range out.Submission.FindingDefinitions {
			definitions[f.FindingID] = f
		}
		for _, o := range out.Submission.FindingOccurrences {
			f, ok := definitions[o.FindingID]
			if !ok {
				return nil, fmt.Errorf("missing accepted finding definition")
			}
			facts[o.FindingID] = ExternalFinding{Definition: f, SourceRole: step.Role, SourceResultID: roleresult.ResultID(step.Cycle, step.StepNo), ContentIdentity: *step.ContentIdentity}
		}
		for _, r := range out.Submission.ResolvedFindings {
			f := facts[r.FindingID]
			f.ResolutionEvidenceID = r.EvidenceID
			facts[r.FindingID] = f
		}
	}
	seen := map[string]bool{}
	for _, r := range resolutions {
		f, ok := facts[r.FindingID]
		if !ok || f.ResolutionEvidenceID != "" || seen[r.FindingID] || !identity.ValidID(r.EvidenceID) {
			return nil, fmt.Errorf("resolution needs a current open finding and accepted evidence")
		}
		seen[r.FindingID] = true
		f.ResolutionEvidenceID = r.EvidenceID
		facts[r.FindingID] = f
	}
	ids := make([]string, 0, len(facts))
	for id := range facts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]ExternalFinding, 0, len(ids))
	for _, id := range ids {
		out = append(out, facts[id])
	}
	return out, nil
}

func externalIntent(in ExternalSubmission) string {
	if in.EscalationReason != "" {
		return in.EscalationReason
	}
	return in.Summary
}

func externalNext(w statestore.WorkflowSnapshot) string {
	next := "DEVELOPER"
	for _, s := range w.Steps {
		if s.ResultID == "" || s.ContentIdentity == nil || *s.ContentIdentity != w.ContentIdentity {
			continue
		}
		var out externalSemanticOutput
		if json.Unmarshal(s.Output, &out) != nil {
			return "BLOCK"
		}
		switch s.Role {
		case "DEVELOPER":
			if out.Submission.Outcome == "completed" {
				next = "QA"
			} else {
				next = "BLOCK"
			}
		case "QA":
			switch out.Submission.Outcome {
			case "passed":
				next = "REVIEWER"
			case "failed":
				next = "REMEDIATE"
			default:
				next = "QA"
			}
		case "REVIEWER":
			switch out.Submission.Outcome {
			case "approved":
				next = "APPROVE"
			case "changes_required":
				next = "REMEDIATE"
			default:
				next = "REVIEWER"
			}
		}
	}
	target := next
	if target == "REMEDIATE" {
		target = "DEVELOPER"
		if w.Current != nil && w.Current.Cycle >= 3 {
			return "BLOCK"
		}
	}
	count := 0
	for _, s := range w.Steps {
		if s.Role == target {
			count++
		}
	}
	if count >= 3 {
		return "BLOCK"
	}
	return next
}

func (r *Runtime) reconcileExternal(ctx context.Context, w statestore.WorkflowSnapshot) error {
	s := *w.Current
	var out externalOutput
	if err := json.Unmarshal(s.Output, &out); err != nil {
		return err
	}
	if _, err := r.frozen(ctx, w); err != nil {
		return err
	}
	semantic, err := r.externalSemantic(ctx, w, out.Submission, out.At)
	if s.Role != "LEAD" && s.ResultID != "" {
		err = nil
	}
	if err != nil {
		return err
	}
	if s.Role == "LEAD" {
		if err = r.acceptLead(ctx, w, captured{Response: Response{Semantic: semantic}}); err != nil {
			return err
		}
	} else if s.ResultID == "" {
		if s.Role == "REVIEWER" {
			if err = r.reviewAdmission(ctx, w); err != nil {
				return err
			}
		}
		if _, err = r.store.RegisterWorkflowArtifact(ctx, w.Spec.RunID, s.InvocationID+"-evidence", "test_result", []byte(out.Submission.Evidence), s.StartedAt); err != nil {
			return err
		}
		if err = r.store.AcceptRoleResultAndReturnToLead(ctx, semantic, out.Submission.FindingDefinitions, statestore.ResultReturnOptions{Fence: statestore.ResultFence{OwnershipEpoch: w.Owner.OwnershipEpoch, ActorRevision: w.Owner.ActorRevision}, ReturnedAt: out.At}); err != nil {
			return err
		}
	}
	if err = r.cut("external_after_result"); err != nil {
		return err
	}
	return r.store.FinishWorkflowStep(ctx, w.Spec.RunID, s.InvocationID, r.at())
}
func (r *Runtime) ExternalComplete(ctx context.Context, run string) (ExternalView, error) {
	w, err := r.store.GetWorkflow(ctx, run)
	if err != nil {
		return ExternalView{}, err
	}
	if err = externalMode(w); err != nil {
		return ExternalView{}, err
	}
	if _, err = r.frozen(ctx, w); err != nil {
		return ExternalView{}, err
	}
	if w.Status == "READY_FOR_HUMAN_MERGE" {
		return r.ExternalInspect(ctx, run)
	}
	if w.Status != "RUNNING" || w.Current == nil || w.Current.Role != "LEAD" || w.Current.CompletedAt == "" {
		return ExternalView{}, fmt.Errorf("completed final Lead approval required")
	}
	var out externalOutput
	if err = json.Unmarshal(w.Current.Output, &out); err != nil {
		return ExternalView{}, err
	}
	if out.Submission.Decision != "APPROVE" {
		return ExternalView{}, fmt.Errorf("Lead has not approved readiness")
	}
	if err = r.ready(ctx, w); err != nil {
		return ExternalView{}, err
	}
	return r.ExternalInspect(ctx, run)
}

// Worktree observations are controller evidence, separate from Worker claims.
func (r *Runtime) externalValidationEvidence(ctx context.Context, w statestore.WorkflowSnapshot, s statestore.WorkflowStep, id string) (statestore.ProofArtifact, error) {
	var out externalOutput
	if err := json.Unmarshal(s.Output, &out); err != nil {
		return statestore.ProofArtifact{}, err
	}
	now, err := r.captureWorktree(ctx, w.Spec.WorktreePath, out.Worktree.Version)
	if err != nil {
		return statestore.ProofArtifact{}, err
	}
	if snapshotDigest(now) != snapshotDigest(out.Worktree) {
		return statestore.ProofArtifact{}, fmt.Errorf("result worktree content changed")
	}
	return r.store.RegisterWorkflowArtifact(ctx, w.Spec.RunID, id, "controller_validation", marshal(out.Worktree), s.StartedAt)
}
