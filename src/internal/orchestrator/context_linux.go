package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"ai-control-plane/src/internal/roleresult"
	"ai-control-plane/src/internal/statestore"
)

// Semantic projections never decode stored file chunks. The complete output is
// still retained and decoded by frozen-content and authoritative replay checks.
type externalSemanticOutput struct {
	Submission ExternalSubmission
	At         string
}

const MaxExternalHistoryEntries = 4

type ExternalHistoryRequest struct {
	BeforeStep int `json:"before_step,omitempty"`
	Limit      int `json:"limit"`
}

// History contains selected semantic facts, not prompts, transcripts, source
// snapshots, or approval input. References retain their original Run provenance.
type ExternalHistoryEntry struct {
	StepNo           int                      `json:"step_no"`
	Cycle            int                      `json:"cycle"`
	InvocationID     string                   `json:"invocation_id"`
	Role             string                   `json:"role"`
	ContentIdentity  string                   `json:"content_identity"`
	Decision         string                   `json:"decision,omitempty"`
	Outcome          string                   `json:"outcome,omitempty"`
	Summary          string                   `json:"summary"`
	EscalationReason string                   `json:"escalation_reason,omitempty"`
	Evidence         string                   `json:"evidence,omitempty"`
	EvidenceRefs     []roleresult.EvidenceRef `json:"evidence_refs,omitempty"`
}

type ExternalHistoryPage struct {
	Entries        []ExternalHistoryEntry `json:"entries"`
	NextBeforeStep int                    `json:"next_before_step,omitempty"`
}

// Bytes and digest describe the serialized ExternalView excluding this health
// object. No process-local counters or inferred external-client metrics are used.
type ExternalContextHealth struct {
	ProjectionBytes       int    `json:"projection_bytes"`
	ProjectionSHA256      string `json:"projection_sha256"`
	AcceptedResults       int    `json:"accepted_results"`
	Findings              int    `json:"findings"`
	HistoryEntries        int    `json:"history_entries"`
	FreshContextRequired  bool   `json:"fresh_context_required"`
	InputTokens           *int64 `json:"input_tokens"`
	ContextWindowTokens   *int64 `json:"context_window_tokens"`
	ContextGrowthBytes    *int64 `json:"context_growth_bytes"`
	RepeatedContextBytes  *int64 `json:"repeated_context_bytes"`
	ToolResultVolumeBytes *int64 `json:"tool_result_volume_bytes"`
	MaxPromptBytes        *int64 `json:"max_prompt_bytes"`
	ContextResetCount     *int64 `json:"context_reset_count"`
}

func observeExternalContext(v ExternalView) (ExternalView, error) {
	v.ContextHealth = nil
	raw, err := json.Marshal(v)
	if err != nil {
		return ExternalView{}, err
	}
	h := &ExternalContextHealth{
		ProjectionBytes: len(raw), ProjectionSHA256: fmt.Sprintf("%x", sha256.Sum256(raw)),
		AcceptedResults: len(v.AcceptedResults), Findings: len(v.Findings),
		FreshContextRequired: v.Status == "RUNNING" && v.Step != nil && !v.StepCompleted,
	}
	if v.History != nil {
		h.HistoryEntries = len(v.History.Entries)
	}
	v.ContextHealth = h
	return v, nil
}

func externalContextFacts(v *ExternalView, w statestore.WorkflowSnapshot) error {
	if w.Current == nil {
		return nil
	}
	findings, err := externalFindings(w, nil)
	if err != nil {
		return err
	}
	v.Findings = findings
	if w.Current.Role != "LEAD" && w.Current.Role != "REVIEWER" {
		return nil
	}
	for _, s := range w.Steps {
		if s.ResultID == "" || s.ContentIdentity == nil || *s.ContentIdentity != w.ContentIdentity {
			continue
		}
		var out externalSemanticOutput
		if err := json.Unmarshal(s.Output, &out); err != nil {
			return err
		}
		// GetWorkflow joins these identities to persisted role_results. No extra
		// result query or old candidate narrative is needed for this projection.
		in := out.Submission
		// s.ResultID is the Run-scoped SQLite key, not the public wire ID.
		facts := ExternalResult{ResultID: roleresult.ResultID(s.Cycle, s.StepNo), Role: s.Role, Outcome: in.Outcome, ContentIdentity: *s.ContentIdentity, Summary: in.Summary,
			EvidenceRefs:       []roleresult.EvidenceRef{{EvidenceID: s.InvocationID + "-evidence", Kind: "test_result"}},
			FindingDefinitions: in.FindingDefinitions, FindingOccurrences: in.FindingOccurrences}
		for _, c := range in.Checks {
			facts.Checks = append(facts.Checks, roleresult.Check{CheckID: c.CheckID, Required: c.Required, Outcome: c.Outcome, Summary: c.Summary, EvidenceRefs: facts.EvidenceRefs})
		}
		v.AcceptedResults = append(v.AcceptedResults, facts)
	}
	return nil
}

// ExternalInspectHistory is read-only and only exposes history to the current
// Lead. It neither changes role authority nor authorizes a new dispatch.
func (r *Runtime) ExternalInspectHistory(ctx context.Context, run string, request ExternalHistoryRequest) (ExternalView, error) {
	if request.Limit < 1 || request.Limit > MaxExternalHistoryEntries || request.BeforeStep < 0 {
		return ExternalView{}, fmt.Errorf("history limit must be 1..%d and before_step nonnegative", MaxExternalHistoryEntries)
	}
	return r.externalInspect(ctx, run, &request)
}

func externalHistory(w statestore.WorkflowSnapshot, request ExternalHistoryRequest) (*ExternalHistoryPage, error) {
	if w.Current == nil || w.Current.Role != "LEAD" {
		return nil, fmt.Errorf("historical context requires the current Lead role")
	}
	page := &ExternalHistoryPage{Entries: []ExternalHistoryEntry{}}
	for i := len(w.Steps) - 1; i >= 0; i-- {
		s := w.Steps[i]
		if s.StepNo >= w.Current.StepNo || (request.BeforeStep != 0 && s.StepNo >= request.BeforeStep) || s.CompletedAt == "" || (s.Role != "LEAD" && s.ResultID == "") {
			continue
		}
		if len(page.Entries) == request.Limit {
			page.NextBeforeStep = page.Entries[len(page.Entries)-1].StepNo
			break
		}
		var out externalSemanticOutput
		if err := json.Unmarshal(s.Output, &out); err != nil {
			return nil, err
		}
		in := out.Submission
		e := ExternalHistoryEntry{StepNo: s.StepNo, Cycle: s.Cycle, InvocationID: s.InvocationID, Role: s.Role,
			Decision: in.Decision, Outcome: in.Outcome, Summary: in.Summary, EscalationReason: in.EscalationReason, Evidence: in.Evidence}
		if s.ContentIdentity != nil {
			e.ContentIdentity = *s.ContentIdentity
		}
		if s.ResultID != "" {
			e.EvidenceRefs = []roleresult.EvidenceRef{{EvidenceID: s.InvocationID + "-evidence", Kind: "test_result"}}
		}
		page.Entries = append(page.Entries, e)
	}
	return page, nil
}
