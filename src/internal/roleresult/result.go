// Package roleresult validates immutable semantic wire results. It does not
// supervise processes, allocate resources or update workflow state.
package roleresult

import (
	"encoding/json"
	"fmt"
	"marc-runtime/internal/identity"

	"marc-runtime/schemas"
)

type Location struct {
	Path      string `json:"path"`
	StartLine *int   `json:"start_line,omitempty"`
	EndLine   *int   `json:"end_line,omitempty"`
}
type EvidenceRef struct {
	EvidenceID string    `json:"evidence_id"`
	Kind       string    `json:"kind"`
	Location   *Location `json:"location,omitempty"`
}
type Finding struct {
	FindingID      string `json:"finding_id"`
	FindingOrdinal int    `json:"finding_ordinal"`
	OriginCycle    int    `json:"origin_cycle"`
	OriginResultID string `json:"origin_result_id"`
	Blocking       bool   `json:"blocking"`
	Summary        string `json:"summary"`
}
type Occurrence struct {
	FindingID    string        `json:"finding_id"`
	EvidenceRefs []EvidenceRef `json:"evidence_refs"`
	Locations    []Location    `json:"locations,omitempty"`
}
type Check struct {
	CheckID      string        `json:"check_id"`
	Required     bool          `json:"required"`
	Outcome      string        `json:"outcome"`
	Summary      string        `json:"summary"`
	EvidenceRefs []EvidenceRef `json:"evidence_refs"`
}
type Blocker struct {
	Summary      string        `json:"summary"`
	EvidenceRefs []EvidenceRef `json:"evidence_refs"`
}
type Result struct {
	SchemaVersion      int           `json:"schema_version"`
	ResultID           string        `json:"result_id"`
	RunID              string        `json:"run_id"`
	Cycle              int           `json:"cycle"`
	ResultOrdinal      int           `json:"result_ordinal"`
	ExecutionID        string        `json:"execution_id"`
	AttemptID          string        `json:"attempt_id"`
	InvocationID       string        `json:"invocation_id"`
	Role               string        `json:"role"`
	ContentIdentity    string        `json:"content_identity"`
	Outcome            string        `json:"outcome"`
	Summary            string        `json:"summary"`
	EvidenceRefs       []EvidenceRef `json:"evidence_refs"`
	ProducedAt         string        `json:"produced_at"`
	RemainingWork      []string      `json:"remaining_work,omitempty"`
	Blocker            *Blocker      `json:"blocker,omitempty"`
	Checks             []Check       `json:"checks,omitempty"`
	FindingOccurrences []Occurrence  `json:"finding_occurrences,omitempty"`
}

// ResultID and FindingID derive display/logical identity only from fields.
func ResultID(cycle, ordinal int) string { return fmt.Sprintf("res-c%03d-r%03d", cycle, ordinal) }
func FindingID(ordinal int) string       { return fmt.Sprintf("fnd-%06d", ordinal) }

func Decode(raw []byte) (Result, error) {
	var r Result
	if err := identity.CheckJSONEnvelope(raw); err != nil {
		return r, err
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	name := map[string]string{"DEVELOPER": "developer-result", "QA": "qa-result", "REVIEWER": "reviewer-result"}[r.Role]
	if name == "" {
		return r, fmt.Errorf("invalid result role %q", r.Role)
	}
	if err := schemas.Validate(name+".schema.json", raw); err != nil {
		return r, err
	}
	if r.ResultID != ResultID(r.Cycle, r.ResultOrdinal) {
		return r, fmt.Errorf("result_id disagrees with structured cycle/result_ordinal")
	}
	if err := checkRefs(r.EvidenceRefs); err != nil {
		return r, err
	}
	if r.Blocker != nil {
		if err := checkRefs(r.Blocker.EvidenceRefs); err != nil {
			return r, err
		}
	}
	seenChecks := map[string]bool{}
	for _, c := range r.Checks {
		if seenChecks[c.CheckID] {
			return r, fmt.Errorf("duplicate check_id %s", c.CheckID)
		}
		seenChecks[c.CheckID] = true
		if err := checkRefs(c.EvidenceRefs); err != nil {
			return r, err
		}
	}
	seenFindings := map[string]bool{}
	for _, o := range r.FindingOccurrences {
		if seenFindings[o.FindingID] {
			return r, fmt.Errorf("duplicate finding occurrence %s", o.FindingID)
		}
		seenFindings[o.FindingID] = true
		if err := checkRefs(o.EvidenceRefs); err != nil {
			return r, err
		}
		for _, l := range o.Locations {
			if err := checkLocation(&l); err != nil {
				return r, err
			}
		}
	}
	return r, nil
}

func checkLocation(l *Location) error {
	if l != nil && l.EndLine != nil && (l.StartLine == nil || *l.EndLine < *l.StartLine) {
		return fmt.Errorf("invalid source line range")
	}
	return nil
}
func checkRefs(refs []EvidenceRef) error {
	seen := map[string]bool{}
	for _, e := range refs {
		if seen[e.EvidenceID] {
			return fmt.Errorf("duplicate evidence_id %s in one link collection", e.EvidenceID)
		}
		seen[e.EvidenceID] = true
		if err := checkLocation(e.Location); err != nil {
			return err
		}
	}
	return nil
}

// ValidateFinding validates standalone origin/identity fields, without inferring
// issue identity or consulting prior state. The caller supplies the run scope.
func ValidateFinding(f Finding) error {
	for _, text := range []string{f.FindingID, f.OriginResultID} {
		if err := identity.CheckText("finding text", text); err != nil {
			return err
		}
	}
	if err := identity.CheckDescription("finding summary", f.Summary); err != nil {
		return err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	// Validate the reusable definition directly.
	if err := schemas.Validate("role-result-common.schema.json#/$defs/finding", raw); err != nil {
		return err
	}
	if f.FindingID != FindingID(f.FindingOrdinal) {
		return fmt.Errorf("finding_id disagrees with finding_ordinal")
	}
	return nil
}

// ValidateReferences receives authoritative run-scoped findings and registered
// evidence kinds from the caller's consistent SQLite transaction. Never infer a
// finding identity from text, paths, locations or fingerprints.
func ValidateReferences(r Result, findings map[string]Finding, evidence map[string]string) error {
	check := func(refs []EvidenceRef) error {
		for _, e := range refs {
			kind, ok := evidence[e.EvidenceID]
			if !ok || kind != e.Kind {
				return fmt.Errorf("unresolved or conflicting evidence %s", e.EvidenceID)
			}
		}
		return nil
	}
	if err := check(r.EvidenceRefs); err != nil {
		return err
	}
	if r.Blocker != nil {
		if err := check(r.Blocker.EvidenceRefs); err != nil {
			return err
		}
	}
	requiredPassed := true
	for _, c := range r.Checks {
		if err := check(c.EvidenceRefs); err != nil {
			return err
		}
		if c.Required && c.Outcome != "passed" {
			requiredPassed = false
		}
	}
	blocking := false
	for _, o := range r.FindingOccurrences {
		f, ok := findings[o.FindingID]
		if !ok {
			return fmt.Errorf("unresolved stable finding %s", o.FindingID)
		}
		if err := check(o.EvidenceRefs); err != nil {
			return err
		}
		blocking = blocking || f.Blocking
	}
	if r.Role == "QA" {
		if (r.Outcome == "failed") != blocking {
			return fmt.Errorf("QA failed requires and follows an observed blocking finding")
		}
		if r.Outcome == "passed" && !requiredPassed {
			return fmt.Errorf("QA passed requires every required check passed")
		}
		// Inconclusive can also reflect insufficient evidence beyond the declared
		// checks. Schema/validator cannot prove the sufficiency of semantic evidence.
	}
	if r.Role == "REVIEWER" {
		if r.Outcome == "approved" && blocking {
			return fmt.Errorf("Reviewer approved with blocking finding")
		}
		if r.Outcome == "changes_required" && !blocking {
			return fmt.Errorf("changes_required without blocking finding")
		}
	}
	return nil
}

// SameFinding compares immutable semantic fields, including origin.
func SameFinding(a, b Finding) bool { return a == b }
