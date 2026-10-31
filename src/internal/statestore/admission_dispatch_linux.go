package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

var ErrDispatchClaimConflict = errors.New("conflicting dispatch claim")

// DispatchClaimConflictError reports an immutable claim ID reused with a
// different contract. DiagnosticError is intentionally separate: failure to
// persist the report never changes the rejection.
type DispatchClaimConflictError struct {
	ClaimID, StoredDigest, IncomingDigest, ReportReference string
	DiagnosticError                                        error
}

func (e *DispatchClaimConflictError) Error() string {
	message := fmt.Sprintf("conflicting dispatch claim %q (stored digest %s, incoming digest %s)", e.ClaimID, e.StoredDigest, e.IncomingDigest)
	if e.ReportReference != "" {
		return message + "; diagnostic report " + e.ReportReference
	}
	return message + "; diagnostic report unavailable"
}
func (e *DispatchClaimConflictError) Unwrap() error { return ErrDispatchClaimConflict }

type dispatchClaimConflictReport struct {
	Kind                       string                        `json:"kind"`
	Reason                     string                        `json:"reason"`
	ClaimID                    string                        `json:"claim_id"`
	RunID                      string                        `json:"run_id"`
	AdmissionID                string                        `json:"admission_id"`
	StoredDigest               string                        `json:"stored_digest"`
	IncomingDigest             string                        `json:"incoming_digest"`
	Differences                []dispatchClaimParameterDiff  `json:"parameter_differences"`
	StoredCapabilitySnapshot   string                        `json:"stored_capabilities"`
	IncomingCapabilitySnapshot *dispatchCapabilityDiagnostic `json:"incoming_capabilities,omitempty"`
	StoredParametersAvailable  bool                          `json:"stored_parameters_available"`
}

type dispatchCapabilityDiagnostic struct {
	Provider            string `json:"provider"`
	Model               string `json:"model"`
	ContextWindowTokens int64  `json:"context_window_tokens"`
	MaxOutputTokens     int64  `json:"max_output_tokens"`
	MaxInputTokens      *int64 `json:"max_input_tokens,omitempty"`
}

type dispatchClaimParameterDiff struct {
	Parameter string `json:"parameter"`
	Stored    any    `json:"stored"`
	Incoming  any    `json:"incoming"`
}

type dispatchClaimConflict struct {
	report         dispatchClaimConflictReport
	storedCall     *ProviderCall
	storedBounds   []DispatchBound
	completionOnly bool
}

type safeDispatchConflict struct {
	Kind                      string   `json:"kind"`
	ClaimID                   string   `json:"claim_id"`
	RunID                     string   `json:"run_id"`
	AdmissionID               string   `json:"admission_id"`
	StoredDigest              string   `json:"stored_digest"`
	IncomingDigest            string   `json:"incoming_digest"`
	DifferentParameters       []string `json:"different_parameters"`
	StoredParametersAvailable bool     `json:"stored_parameters_available"`
}

func readAdmission(ctx context.Context, q querier, run, id string) (InvocationAdmission, error) {
	var a InvocationAdmission
	var role string
	var content *string
	f := &a.Fence
	b := &a.Binding
	err := q.QueryRowContext(ctx, `SELECT a.admission_id,a.run_id,a.invocation_id,a.execution_id,a.attempt_id,a.cycle_no,a.role,a.purpose,a.kind,a.content_identity,a.ownership_epoch,a.actor_revision,a.actor_role,a.actor_invocation_id,a.actor_content_identity,a.semantic_revision,a.run_revision,a.execution_revision,a.attempt_revision,a.invocation_revision,d.hard_deadline
 FROM invocation_admissions a JOIN execution_admission_deadlines d USING(execution_id) WHERE a.admission_id=? AND a.run_id=?`, id, run).Scan(&a.AdmissionID, &a.RunID, &b.InvocationID, &b.ExecutionID, &b.AttemptID, &b.Cycle, &role, &b.Purpose, &a.Kind, &content, &f.Owner.OwnershipEpoch, &f.Owner.ActorRevision, &f.Owner.Role, &f.Owner.InvocationID, &f.Owner.ContentIdentity, &f.SemanticRevision, &f.RunRevision, &f.ExecutionRevision, &f.AttemptRevision, &f.InvocationRevision, &a.HardDeadline)
	if content != nil {
		b.ContentIdentity = *content
	}
	if err != nil {
		return a, err
	}
	var reason string
	err = q.QueryRowContext(ctx, `SELECT reason FROM admission_contingency_authorizations WHERE admission_id=?`, id).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	} else {
		a.ContingencyReason = reason
	}
	return a, err
}

// ClaimAdmissionDispatch records immutable request intent and call provenance.
// Provider registration, sequential-call validation and the claim commit together.
// Physical release requires AuthorizeAdmissionRelease at the release boundary.
// Replay is history and cannot authorize another request.
func (s *StateStore) ClaimAdmissionDispatch(ctx context.Context, d AdmissionDispatchClaim, opts AccountingOptions) (newRecord bool, resultErr error) {
	return s.claimAdmissionDispatch(ctx, d, opts, writeDispatchConflictDumps)
}

func (s *StateStore) claimAdmissionDispatch(ctx context.Context, d AdmissionDispatchClaim, opts AccountingOptions, writeDumps func(string, *dispatchClaimConflict, AdmissionDispatchClaim) (string, string, error)) (newRecord bool, resultErr error) {
	defer func() {
		inv, call := "", ""
		if d.ProviderCall != nil {
			inv = d.ProviderCall.InvocationID
			call = d.ProviderCall.ProviderCallID
		}
		resultErr = numericFailure("ClaimAdmissionDispatch", d.RunID, inv, call, resultErr)
	}()
	var err error
	if d.ClaimID == "" || d.AdmissionID == "" || d.RunID == "" {
		return false, fmt.Errorf("dispatch claim requires identities")
	}
	if opts.At, err = canonicalTime(opts.At); err != nil {
		return false, numericFailure("ClaimAdmissionDispatch", d.RunID, "", "", err)
	}
	d.Bounds = append([]DispatchBound(nil), d.Bounds...)
	sort.Slice(d.Bounds, func(i, j int) bool {
		if d.Bounds[i].Dimension != d.Bounds[j].Dimension {
			return d.Bounds[i].Dimension < d.Bounds[j].Dimension
		}
		return d.Bounds[i].Amount < d.Bounds[j].Amount
	})
	seen := map[string]int64{}
	for _, b := range d.Bounds {
		if b.Amount < 0 || !boundDimension(b.Dimension) {
			return false, fmt.Errorf("invalid dispatch bound")
		}
		if _, ok := seen[b.Dimension]; ok {
			return false, fmt.Errorf("duplicate dispatch bound dimension")
		}
		seen[b.Dimension] = b.Amount
	}
	for _, r := range budgetResources {
		if _, ok := seen[r]; !ok {
			return false, fmt.Errorf("dispatch requires an explicit bound for each consumable dimension")
		}
	}
	if d.ProviderCall == nil {
		if d.Capabilities != nil {
			return false, fmt.Errorf("non-provider dispatch cannot carry provider/model capabilities")
		}
		for _, r := range []string{"WORK", "INPUT_TOKENS", "CACHED_INPUT_TOKENS", "OUTPUT_TOKENS", "REASONING_TOKENS"} {
			if seen[r] != 0 {
				return false, fmt.Errorf("non-provider dispatch cannot claim provider token capacity")
			}
		}
	}
	if err = admissionUTF8(d.ClaimID, d.AdmissionID, d.RunID); err != nil {
		return false, err
	}
	if err = requireIDs(struct{ name, value string }{"claim_id", d.ClaimID}, struct{ name, value string }{"admission_id", d.AdmissionID}, struct{ name, value string }{"run_id", d.RunID}); err != nil {
		return false, err
	}
	if p := d.ProviderCall; p != nil {
		if err = admissionUTF8(p.ProviderCallID, p.RunID, p.InvocationID, p.Provider, p.Model, p.ReasoningEffort); err != nil {
			return false, err
		}
		if err = requireIDs(struct{ name, value string }{"provider_call_id", p.ProviderCallID}, struct{ name, value string }{"run_id", p.RunID}, struct{ name, value string }{"invocation_id", p.InvocationID}); err != nil {
			return false, err
		}
	}
	if d.Capabilities != nil {
		// Keep digest identity canonical before looking up a possible replay. The
		// capability limits themselves are enforced only for a new claim below.
		if err = admissionUTF8(d.Capabilities.Provider, d.Capabilities.Model); err != nil {
			return false, err
		}
	}
	digest := admissionDigest(d)
	created := false
	var denied error
	var conflict *dispatchClaimConflict
	err = s.numericMutation(ctx, "ClaimAdmissionDispatch", d.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM admission_dispatch_claims WHERE claim_id=?`, d.ClaimID).Scan(&old)
		if err == nil {
			if old != digest {
				conflict = readDispatchClaimConflict(ctx, c, d, old, digest)
				return errAdmissionReplay
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := validateCompletionCapabilities(d); err != nil {
			return err
		}
		if p := d.ProviderCall; p != nil {
			if err := validateProviderRequestCapabilities(*p, seen, d.Capabilities); err != nil {
				return err
			}
		}
		a, err := readAdmission(ctx, c, d.RunID, d.AdmissionID)
		if err != nil {
			return err
		}
		var closed int
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_non_dispatch_proofs WHERE admission_id=?) OR (?='NORMAL' AND EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE 1=1 AND run_id=?))`, d.AdmissionID, a.Kind, d.RunID).Scan(&closed); err != nil {
			return err
		}
		if closed != 0 {
			denied = &AdmissionDeniedError{d.RunID, "sealed non-dispatch proof or durable hard stop; budget overruns require human decision"}
			return nil
		}
		if !beforeDeadline(opts.At, a.HardDeadline) {
			if err = observeHardStop(ctx, c, d.RunID, "", "UNPROVEN_CONTINUATION", "deadline:"+a.Binding.ExecutionID, nil, nil, opts.At); err != nil {
				return err
			}
			denied = &AdmissionDeniedError{d.RunID, "original hard deadline expired"}
			return nil
		}
		if err = validateAdmissionFence(ctx, c, d.RunID, a.Binding, a.Fence); err != nil {
			return err
		}
		if d.ProviderCall != nil {
			if a.ContingencyReason != "" {
				if err := validateContingencyWork(ctx, c, a); err != nil {
					return err
				}
			} else if err := validateActiveTokenWork(ctx, c, a); err != nil {
				return err
			}
			grant, err := admissionTokenLimit(ctx, c, a)
			if err != nil {
				return err
			}
			if grant == 0 {
				return &AdmissionDeniedError{d.RunID, "provider dispatch requires a durable active token spending limit"}
			}
		}

		if err := admissionClosed(ctx, c, a.AdmissionID); err != nil {
			return err
		}
		if d.ProviderCall != nil {
			var active int
			if err = c.QueryRowContext(ctx, `SELECT count(*) FROM provider_calls WHERE invocation_id=? AND duration_ns IS NULL`, a.Binding.InvocationID).Scan(&active); err != nil {
				return err
			}
			if active != 0 {
				return fmt.Errorf("previous ProviderCall completion remains unresolved")
			}
		}
		if d.ProviderCall != nil {
			grant, err := admissionTokenLimit(ctx, c, a)
			if err != nil {
				return err
			}
			used, _, err := invocationActual(ctx, c, a.Binding.InvocationID, "WORK")
			if err != nil {
				return err
			}
			remaining, err := checkedSubtract(grant, used)
			if err != nil {
				return err
			}
			bank, err := runTokenBalance(ctx, c, d.RunID)
			if err != nil {
				return err
			}
			available := bank.NormalAvailable
			if a.ContingencyReason != "" {
				available = bank.ContingencyAvailable
			}
			if remaining <= 0 || seen["WORK"] > remaining || available <= 0 || seen["WORK"] > available || bank.contingencyExhausted() {
				return &AdmissionDeniedError{d.RunID, "dispatch exceeds current spending limit or available bank"}
			}
			if a.ContingencyReason == "" {
				limits, err := agentTokenLimits(ctx, c, a)
				if err != nil {
					return err
				}
				if d.CompletionOnly && !limits.CompletionOnly {
					return &AdmissionDeniedError{d.RunID, "completion-only dispatch requires an explicit current Orchestrator top-up denial"}
				}
				if !d.CompletionOnly && (limits.CompletionOnly || used >= limits.SoftLimit) {
					return &AdmissionDeniedError{d.RunID, "agent Soft limit reached or top-up denied; pause and request Orchestrator decision"}
				}
			}
		}

		if d.ProviderCall != nil {
			if d.ProviderCall.RunID != d.RunID || d.ProviderCall.InvocationID != a.Binding.InvocationID {
				return fmt.Errorf("dispatch ProviderCall ancestry mismatch")
			}
			if err = insertProviderCall(ctx, c, *d.ProviderCall, opts.At); err != nil {
				return err
			}
		}
		var ordinal int64
		if err = c.QueryRowContext(ctx, `SELECT COALESCE(MAX(claim_ordinal),0) FROM admission_dispatch_claims WHERE admission_id=?`, d.AdmissionID).Scan(&ordinal); err != nil {
			return err
		}
		ordinal, err = checkedNext(ordinal, "dispatch_claim_ordinal")
		if err != nil {
			return err
		}

		_, err = c.ExecContext(ctx, `INSERT INTO admission_dispatch_claims(claim_id,admission_id,provider_call_id,claim_ordinal,claimed_at,contract_digest,completion_only,request_json) VALUES(?,?,?,?,?,?,?,?)`, d.ClaimID, d.AdmissionID, providerCallID(d.ProviderCall), ordinal, opts.At, digest, d.CompletionOnly, string(mustClaimJSON(d)))

		if err != nil {
			return err
		}
		for _, b := range d.Bounds {
			if _, err = c.ExecContext(ctx, `INSERT INTO admission_dispatch_bounds VALUES(?,?,?)`, d.ClaimID, b.Dimension, b.Amount); err != nil {
				return err
			}
		}
		created = true
		return nil
	})
	if conflict != nil {
		// Immutable conflict facts were copied under the transaction. Diagnostic
		// filesystem work must not hold the SQLite connection or writer lock.
		full, safe, diagnosticErr := writeDumps(s.dumpDir, conflict, d)
		var reference string
		err = s.numericMutation(ctx, "ReportDispatchClaimConflict", d.RunID, opts.Audit, func(c *sql.Conn) error {
			var reportErr error
			reference, reportErr = appendDispatchClaimConflictReport(ctx, c, conflict, full, safe, diagnosticErr == nil, opts.At)
			return reportErr
		})
		var projectionErr *MetricsProjectionError
		if err != nil && !errors.As(err, &projectionErr) {
			reference = ""
		}
		diagnosticErr = errors.Join(diagnosticErr, err)
		return false, &DispatchClaimConflictError{
			ClaimID: conflict.report.ClaimID, StoredDigest: conflict.report.StoredDigest,
			IncomingDigest: conflict.report.IncomingDigest, ReportReference: reference,
			DiagnosticError: diagnosticErr,
		}
	}
	if errors.Is(err, errAdmissionReplay) {
		return false, nil
	}
	var projection *MetricsProjectionError
	if errors.As(err, &projection) {
		if denied != nil {
			err = errors.Join(denied, err)
		}
		return created, err
	}
	if err != nil {
		return false, err
	}
	return created, denied
}

func appendDispatchClaimConflictReport(ctx context.Context, c *sql.Conn, conflict *dispatchClaimConflict, full, safe string, available bool, at string) (string, error) {
	report := conflict.report
	metadata := struct {
		Kind                string `json:"kind"`
		FullReport          string `json:"full_report,omitempty"`
		SafeReport          string `json:"safe_report,omitempty"`
		DiagnosticAvailable bool   `json:"diagnostic_available"`
	}{Kind: "dispatch_claim_conflict", FullReport: full, SafeReport: safe, DiagnosticAvailable: available}
	details, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	if err := appendAudit(ctx, c, report.RunID, []AuditRow{{
		EventType: "dispatch_claim_conflict", ActorRole: "ORCHESTRATOR", OccurredAt: at,
		Details: details,
	}}); err != nil {
		return "", err
	}
	var sequence int64
	if err := c.QueryRowContext(ctx, "SELECT last_insert_rowid()").Scan(&sequence); err != nil {
		return "", err
	}
	return fmt.Sprintf("events:%d", sequence), nil
}

func writeDispatchConflictDumps(dir string, conflict *dispatchClaimConflict, incoming AdmissionDispatchClaim) (string, string, error) {
	report := conflict.report
	root, err := openLocalDirectory(dir, true, true)
	if err != nil {
		return "", "", err
	}
	defer root.Close()
	dir = descriptorPath(root)
	var names []string
	for _, diff := range report.Differences {
		names = append(names, diff.Parameter)
	}
	safe := safeDispatchConflict{Kind: report.Kind, ClaimID: report.ClaimID, RunID: report.RunID, AdmissionID: report.AdmissionID, StoredDigest: report.StoredDigest, IncomingDigest: report.IncomingDigest, DifferentParameters: names, StoredParametersAvailable: report.StoredParametersAvailable}
	fullBytes, err := json.MarshalIndent(struct {
		Report                            dispatchClaimConflictReport `json:"report"`
		Incoming                          AdmissionDispatchClaim      `json:"incoming_request"`
		StoredProviderCall                *ProviderCall               `json:"stored_provider_call,omitempty"`
		StoredBounds                      []DispatchBound             `json:"stored_bounds"`
		StoredCompletionOnly              bool                        `json:"stored_completion_only"`
		StoredCapabilitySnapshotAvailable bool                        `json:"stored_capability_snapshot_available"`
	}{report, incoming, conflict.storedCall, conflict.storedBounds, conflict.completionOnly, false}, "", "  ")
	if err != nil {
		return "", "", err
	}
	safeBytes, err := json.MarshalIndent(safe, "", "  ")
	if err != nil {
		return "", "", err
	}
	prefix := "dispatch-conflict-" + report.IncomingDigest[:min(16, len(report.IncomingDigest))]
	write := func(suffix string, data []byte) (string, error) {
		f, err := os.CreateTemp(dir, prefix+"-*"+suffix)
		if err != nil {
			return "", err
		}
		name := filepath.Base(f.Name())
		ok := false
		defer func() {
			if !ok {
				_ = os.Remove(f.Name())
			}
		}()
		if err = f.Chmod(0600); err == nil {
			_, err = f.Write(data)
		}
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return "", err
		}
		ok = true
		return name, nil
	}
	fullName, err := write(".full.json", fullBytes)
	if err != nil {
		return "", "", err
	}
	safeName, err := write(".safe.json", safeBytes)
	if err != nil {
		_ = os.Remove(filepath.Join(dir, fullName))
		return "", "", err
	}
	if err := root.Sync(); err != nil {
		_ = os.Remove(filepath.Join(dir, fullName))
		_ = os.Remove(filepath.Join(dir, safeName))
		return "", "", err
	}
	return filepath.Join("dump", fullName), filepath.Join("dump", safeName), nil
}

func readDispatchClaimConflict(ctx context.Context, c *sql.Conn, incoming AdmissionDispatchClaim, storedDigest, incomingDigest string) *dispatchClaimConflict {
	report := dispatchClaimConflictReport{
		Kind:    "dispatch_claim_conflict",
		Reason:  "ClaimID already exists with a different immutable dispatch contract digest",
		ClaimID: incoming.ClaimID, RunID: incoming.RunID, AdmissionID: incoming.AdmissionID,
		StoredDigest: storedDigest, IncomingDigest: incomingDigest,
		StoredCapabilitySnapshot:   "unavailable",
		IncomingCapabilitySnapshot: capabilityDiagnostic(incoming.Capabilities),
	}
	conflict := &dispatchClaimConflict{report: report}
	var storedCallID sql.NullString
	var storedCompletionOnly bool
	var requestJSON string
	if err := c.QueryRowContext(ctx, `SELECT d.admission_id,a.run_id,d.provider_call_id,d.completion_only,d.request_json
FROM admission_dispatch_claims d JOIN invocation_admissions a USING(admission_id) WHERE d.claim_id=?`, incoming.ClaimID).Scan(
		&conflict.report.AdmissionID, &conflict.report.RunID, &storedCallID, &storedCompletionOnly, &requestJSON); err != nil {
		return conflict
	}
	conflict.completionOnly = storedCompletionOnly
	var storedRequest AdmissionDispatchClaim
	if err := json.Unmarshal([]byte(requestJSON), &storedRequest); err == nil {
		raw, _ := json.Marshal(capabilityDiagnostic(storedRequest.Capabilities))
		conflict.report.StoredCapabilitySnapshot = string(raw)
		addDispatchClaimDiff(&conflict.report, "capabilities", capabilityDiagnostic(storedRequest.Capabilities), capabilityDiagnostic(incoming.Capabilities))
		addDispatchClaimDiff(&conflict.report, "completion_capabilities", storedRequest.CompletionCapabilities, incoming.CompletionCapabilities)
	}

	conflict.report.StoredParametersAvailable = true
	var storedCall *ProviderCall
	if storedCallID.Valid {
		var call ProviderCall
		if err := c.QueryRowContext(ctx, `SELECT p.provider_call_id,i.run_id,p.invocation_id,p.provider,p.model,p.reasoning_effort
FROM provider_calls p JOIN invocations i USING(invocation_id) WHERE p.provider_call_id=?`, storedCallID.String).Scan(
			&call.ProviderCallID, &call.RunID, &call.InvocationID, &call.Provider, &call.Model, &call.ReasoningEffort); err != nil {
			conflict.report.StoredParametersAvailable = false
			return conflict
		}
		storedCall = &call
	}
	conflict.storedCall = storedCall
	var storedBounds []DispatchBound
	rows, err := c.QueryContext(ctx, `SELECT dimension,amount FROM admission_dispatch_bounds WHERE claim_id=? ORDER BY dimension`, incoming.ClaimID)
	if err != nil {
		conflict.report.StoredParametersAvailable = false
		return conflict
	}
	for rows.Next() {
		var bound DispatchBound
		if err = rows.Scan(&bound.Dimension, &bound.Amount); err != nil {
			rows.Close()
			conflict.report.StoredParametersAvailable = false
			return conflict
		}
		storedBounds = append(storedBounds, bound)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		conflict.report.StoredParametersAvailable = false
		return conflict
	}
	rows.Close()
	conflict.storedBounds = append([]DispatchBound(nil), storedBounds...)
	addDispatchClaimDiff(&conflict.report, "admission_id", conflict.report.AdmissionID, incoming.AdmissionID)
	addDispatchClaimDiff(&conflict.report, "run_id", conflict.report.RunID, incoming.RunID)
	addDispatchClaimDiff(&conflict.report, "provider_call_id", optionalCallID(storedCall), optionalCallID(incoming.ProviderCall))
	if storedCall != nil && incoming.ProviderCall != nil {
		for _, field := range []struct{ name, stored, incoming string }{
			{"provider_call.run_id", storedCall.RunID, incoming.ProviderCall.RunID},
			{"provider_call.invocation_id", storedCall.InvocationID, incoming.ProviderCall.InvocationID},
			{"provider_call.provider", storedCall.Provider, incoming.ProviderCall.Provider},
			{"provider_call.model", storedCall.Model, incoming.ProviderCall.Model},
			{"provider_call.reasoning_effort", storedCall.ReasoningEffort, incoming.ProviderCall.ReasoningEffort},
		} {
			addDispatchClaimDiff(&conflict.report, field.name, field.stored, field.incoming)
		}
	}
	storedByDimension, incomingByDimension := map[string]int64{}, map[string]int64{}
	for _, bound := range storedBounds {
		storedByDimension[bound.Dimension] = bound.Amount
	}
	for _, bound := range incoming.Bounds {
		incomingByDimension[bound.Dimension] = bound.Amount
	}
	dimensions := map[string]bool{}
	for dimension := range storedByDimension {
		dimensions[dimension] = true
	}
	for dimension := range incomingByDimension {
		dimensions[dimension] = true
	}
	ordered := make([]string, 0, len(dimensions))
	for dimension := range dimensions {
		ordered = append(ordered, dimension)
	}
	sort.Strings(ordered)
	for _, dimension := range ordered {
		stored, storedOK := storedByDimension[dimension]
		incomingAmount, incomingOK := incomingByDimension[dimension]
		var storedValue, incomingValue any
		if storedOK {
			storedValue = stored
		}
		if incomingOK {
			incomingValue = incomingAmount
		}
		addDispatchClaimDiff(&conflict.report, "bounds."+dimension, storedValue, incomingValue)
	}
	addDispatchClaimDiff(&conflict.report, "completion_only", storedCompletionOnly, incoming.CompletionOnly)

	return conflict
}

func capabilityDiagnostic(capabilities *ProviderModelCapabilities) *dispatchCapabilityDiagnostic {
	if capabilities == nil {
		return nil
	}
	return &dispatchCapabilityDiagnostic{
		Provider: capabilities.Provider, Model: capabilities.Model,
		ContextWindowTokens: capabilities.ContextWindowTokens, MaxOutputTokens: capabilities.MaxOutputTokens,
		MaxInputTokens: capabilities.MaxInputTokens,
	}
}

func optionalCallID(call *ProviderCall) any {
	if call == nil {
		return nil
	}
	return call.ProviderCallID
}

func addDispatchClaimDiff(report *dispatchClaimConflictReport, parameter string, stored, incoming any) {
	if !reflect.DeepEqual(stored, incoming) {
		report.Differences = append(report.Differences, dispatchClaimParameterDiff{Parameter: parameter, Stored: stored, Incoming: incoming})
	}
}
func providerCallID(p *ProviderCall) *string {
	if p == nil {
		return nil
	}
	return &p.ProviderCallID
}
func boundDimension(d string) bool {
	return d == "WORK" || d == "INPUT_TOKENS" || d == "CACHED_INPUT_TOKENS" || d == "OUTPUT_TOKENS" || d == "REASONING_TOKENS"
}

func validateProviderRequestCapabilities(call ProviderCall, bounds map[string]int64, capabilities *ProviderModelCapabilities) error {
	if capabilities == nil {
		return fmt.Errorf("provider dispatch requires model capabilities")
	}
	if err := admissionUTF8(capabilities.Provider, capabilities.Model); err != nil {
		return err
	}
	if capabilities.Provider != call.Provider || capabilities.Model != call.Model {
		return fmt.Errorf("provider/model capabilities do not match ProviderCall")
	}
	if capabilities.ContextWindowTokens <= 0 || capabilities.MaxOutputTokens <= 0 || capabilities.MaxOutputTokens > capabilities.ContextWindowTokens {
		return fmt.Errorf("invalid provider/model context or output capability")
	}
	if capabilities.MaxInputTokens != nil && (*capabilities.MaxInputTokens < 0 || *capabilities.MaxInputTokens > capabilities.ContextWindowTokens) {
		return fmt.Errorf("invalid provider/model input capability")
	}
	inputBound, hasInput := bounds["INPUT_TOKENS"]
	outputBound, hasOutput := bounds["OUTPUT_TOKENS"]
	if !hasInput || !hasOutput || bounds["WORK"] <= 0 || outputBound <= 0 {
		return fmt.Errorf("provider dispatch requires input and output token bounds")
	}
	combined, err := checkedAdd(inputBound, outputBound)
	if err != nil {
		return numericFailure("ClaimAdmissionDispatch", call.RunID, call.InvocationID, call.ProviderCallID, err)
	}
	if combined > capabilities.ContextWindowTokens {
		return fmt.Errorf("provider request bounds exceed model context window")
	}
	if combined > bounds["WORK"] {
		return fmt.Errorf("provider request bounds exceed per-call WORK admission bound")
	}
	if outputBound > capabilities.MaxOutputTokens {
		return fmt.Errorf("provider request output bound exceeds model output limit")
	}
	if capabilities.MaxInputTokens != nil && inputBound > *capabilities.MaxInputTokens {
		return fmt.Errorf("provider request input bound exceeds model input limit")
	}
	return nil
}

func invocationActual(ctx context.Context, q querier, invocation, resource string) (int64, bool, error) {
	var total int64
	if resource == "WORK" || resource == "SPEND" {
		column := "work"
		if resource == "SPEND" {
			column = "spend"
		}
		rows, err := q.QueryContext(ctx, `SELECT c.`+column+`_amount,c.`+column+`_state FROM provider_calls p JOIN provider_call_usage c USING(provider_call_id) WHERE p.invocation_id=? ORDER BY p.call_ordinal`, invocation)
		if err != nil {
			return 0, false, err
		}
		defer rows.Close()
		complete := true
		for rows.Next() {
			var amount *int64
			var state string
			if err = rows.Scan(&amount, &state); err != nil {
				return 0, false, err
			}
			if amount == nil || state != "final" {
				complete = false
			}
			if amount != nil {
				total, err = checkedAdd(total, *amount)
				if err != nil {
					return 0, false, err
				}
			}
		}
		return total, complete, rows.Err()
	}
	return 0, false, fmt.Errorf("only tokens have resource actuals")
}

// SealAdmissionNonDispatch accepts a positive controller assertion and proves it
// has not been contradicted by any durable release claim, physical call, usage or
// actual observation. The proof closes this admission and releases all its holds
// atomically. It cannot be used after an ambiguous or already-claimed dispatch.
func (s *StateStore) SealAdmissionNonDispatch(ctx context.Context, run, admission, proof, assertion string, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"admission_id", admission}, struct{ name, value string }{"proof_id", proof}); err != nil {
		return false, err
	}
	if proof == "" || assertion != "NEVER_RELEASED" {
		return false, fmt.Errorf("explicit NEVER_RELEASED controller assertion required")
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, numericFailure("SealAdmissionNonDispatch", run, "", "", err)
	}
	created := false
	err = s.numericMutation(ctx, "SealAdmissionNonDispatch", run, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT admission_id FROM admission_non_dispatch_proofs WHERE proof_id=?`, proof).Scan(&old)
		if err == nil {
			var belongs string
			if err = c.QueryRowContext(ctx, `SELECT run_id FROM invocation_admissions WHERE admission_id=?`, old).Scan(&belongs); err != nil {
				return err
			}
			if old != admission || belongs != run {
				return fmt.Errorf("conflicting non-dispatch proof replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		a, err := readAdmission(ctx, c, run, admission)
		if err != nil {
			return err
		}
		var contradiction int
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_dispatch_claims WHERE admission_id=?)
   OR EXISTS(SELECT 1 FROM provider_calls WHERE invocation_id=?)`, admission, a.Binding.InvocationID).Scan(&contradiction); err != nil {
			return err
		}
		if contradiction != 0 {
			return fmt.Errorf("non-dispatch assertion conflicts with durable release/completion/usage evidence")
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO admission_non_dispatch_proofs VALUES(?,?,?,?)`, proof, admission, assertion, at); err != nil {
			return err
		}
		created = true
		return nil
	})
	if errors.Is(err, errAdmissionReplay) {
		return false, nil
	}
	var projection *MetricsProjectionError
	if errors.As(err, &projection) {
		return created, err
	}
	if err != nil {
		return false, err
	}
	return created, nil
}

// ControllerReleaseEvidence records observed controller release, not provider
// actuals. SubmittedBounds describe the same normalized constraints as the claim.
// They are optional factual evidence, never inferred from receipt counters.
type ControllerReleaseEvidence struct {
	RunID, ClaimID, EvidenceID, ReleasedAt string
	SubmittedBounds                        []DispatchBound
}

// RecordControllerReleaseEvidence is retrospective trusted controller evidence,
// not dispatch permission. Proved release past the original deadline or outside a
// recorded bound creates a controller violation; valid late usage remains accepted.
func (s *StateStore) RecordControllerReleaseEvidence(ctx context.Context, e ControllerReleaseEvidence, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", e.RunID}, struct{ name, value string }{"claim_id", e.ClaimID}, struct{ name, value string }{"evidence_id", e.EvidenceID}); err != nil {
		return false, err
	}
	if e.EvidenceID == "" {
		return false, fmt.Errorf("controller release evidence identity required")
	}
	var err error
	if e.ReleasedAt, err = canonicalTime(e.ReleasedAt); err != nil {
		return false, numericFailure("RecordControllerReleaseEvidence", e.RunID, "", "", err)
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, numericFailure("RecordControllerReleaseEvidence", e.RunID, "", "", err)
	}
	e.SubmittedBounds = append([]DispatchBound(nil), e.SubmittedBounds...)
	sort.Slice(e.SubmittedBounds, func(i, j int) bool { return e.SubmittedBounds[i].Dimension < e.SubmittedBounds[j].Dimension })
	seen := map[string]bool{}
	for _, b := range e.SubmittedBounds {
		if b.Amount < 0 || !boundDimension(b.Dimension) || seen[b.Dimension] {
			return false, fmt.Errorf("invalid controller submitted bound")
		}
		seen[b.Dimension] = true
	}
	if err = admissionUTF8(e.RunID, e.ClaimID, e.EvidenceID); err != nil {
		return false, err
	}
	digest := admissionDigest(e)
	created := false
	err = s.numericMutation(ctx, "RecordControllerReleaseEvidence", e.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM admission_controller_release_evidence WHERE evidence_id=?`, e.EvidenceID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting controller release replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var deadline string
		if err = c.QueryRowContext(ctx, `SELECT x.hard_deadline FROM admission_dispatch_claims d JOIN invocation_admissions a USING(admission_id) JOIN execution_admission_deadlines x USING(execution_id) WHERE d.claim_id=? AND a.run_id=?`, e.ClaimID, e.RunID).Scan(&deadline); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO admission_controller_release_evidence VALUES(?,?,?,?,?)`, e.EvidenceID, e.ClaimID, e.ReleasedAt, at, digest); err != nil {
			return err
		}
		violation := !beforeDeadline(e.ReleasedAt, deadline)
		for _, b := range e.SubmittedBounds {
			if _, err = c.ExecContext(ctx, `INSERT INTO admission_controller_release_bounds VALUES(?,?,?)`, e.EvidenceID, b.Dimension, b.Amount); err != nil {
				return err
			}
			var limit int64
			err = c.QueryRowContext(ctx, `SELECT amount FROM admission_dispatch_bounds WHERE claim_id=? AND dimension=?`, e.ClaimID, b.Dimension).Scan(&limit)
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil {
				return err
			}
			if b.Amount > limit {
				violation = true
			}
		}
		if violation {
			if err = observeHardStop(ctx, c, e.RunID, "", "CONTROLLER_RELEASE_VIOLATION", e.EvidenceID, nil, nil, at); err != nil {
				return err
			}
			if err = resolveObservedHardStop(ctx, c, stopIdentity(e.RunID, "CONTROLLER_RELEASE_VIOLATION", e.EvidenceID), "CONTROLLER_ADMISSION_BUG", at); err != nil {
				return err
			}
		}
		created = true
		return nil
	})
	if errors.Is(err, errAdmissionReplay) {
		return false, nil
	}
	var projection *MetricsProjectionError
	if errors.As(err, &projection) {
		return created, err
	}
	if err != nil {
		return false, err
	}
	return created, nil
}

// Admission closure is a separate no-further-call fact. It never supplies FINAL
// counters or semantic completion, and late authorized receipts remain acceptable.
func admissionClosed(ctx context.Context, q querier, id string) error {
	var closed bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_closures WHERE admission_id=?1) OR EXISTS(SELECT 1 FROM admission_non_dispatch_proofs WHERE admission_id=?1)`, id).Scan(&closed); err != nil {
		return err
	}
	if closed {
		return fmt.Errorf("admission has a durable no-further-call fact")
	}
	return nil
}
func (s *StateStore) CloseAdmission(ctx context.Context, run, id string, ref ProofReference, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"admission_id", id}, struct{ name, value string }{"proof_id", ref.ProofID}); err != nil {
		return false, err
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, err
	}
	payload := struct {
		Run, Admission, At string
		Containment        ProofReference
	}{run, id, at, ref}
	digest := admissionDigest(payload)
	created := false
	err = s.numericMutation(ctx, "CloseAdmission", run, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM admission_closures WHERE admission_id=?`, id).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting no-further-call closure")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		a, err := readAdmission(ctx, c, run, id)
		if err != nil {
			return err
		}
		if err := admissionClosed(ctx, c, id); err != nil {
			return err
		}
		p := AuthorityProof{RunID: run, ContentIdentity: a.Binding.ContentIdentity}
		body, err := boundParent(ctx, c, p, ref, "RECOVERY", true)
		if err != nil {
			return err
		}
		if body.Request.Boundary.Binding.InvocationID != a.Binding.InvocationID || body.Request.Verdict != "RECOVERABLE" || body.Request.Recovery.Containment != "CONFIRMED" {
			return fmt.Errorf("closure requires current exact-admission confirmed containment")
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO admission_closures VALUES(?,?,'NO_FURTHER_CALLS',?,?)`, id, ref.ProofID, at, digest); err != nil {
			return err
		}
		created = true
		return nil
	})
	return committedCreation(created, err)
}

// AuthorizeAdmissionRelease is the final durable gate immediately before the
// controller releases a physical request. Only a new, error-free authorization
// may be used once. Replays, lost acknowledgements and projection errors confer
// no repeat-release authority. Actual adapter atomic release is still ACP-001/B4.
func (s *StateStore) AuthorizeAdmissionRelease(ctx context.Context, run, id string, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"claim_id", id}); err != nil {
		return false, err
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, err
	}
	digest := admissionDigest(struct{ Run, Claim, At string }{run, id, at})
	created := false
	err = s.numericMutation(ctx, "AuthorizeAdmissionRelease", run, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM admission_release_authorizations WHERE claim_id=?`, id).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting release-authorization replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var raw, contract string
		if err := c.QueryRowContext(ctx, `SELECT d.request_json,d.contract_digest FROM admission_dispatch_claims d JOIN invocation_admissions a USING(admission_id) WHERE d.claim_id=? AND a.run_id=?`, id, run).Scan(&raw, &contract); err != nil {
			return err
		}
		var d AdmissionDispatchClaim
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			return err
		}
		if d.RunID != run || d.ClaimID != id || admissionDigest(d) != contract {
			return fmt.Errorf("stored release contract identity changed")
		}
		a, err := readAdmission(ctx, c, run, d.AdmissionID)
		if err != nil {
			return err
		}
		if err := admissionClosed(ctx, c, a.AdmissionID); err != nil {
			return err
		}
		if !beforeDeadline(at, a.HardDeadline) {
			return &AdmissionDeniedError{run, "original Hard window expired before release"}
		}
		if err := validateAdmissionFence(ctx, c, run, a.Binding, a.Fence); err != nil {
			return err
		}
		var stops bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE run_id=?)`, run).Scan(&stops); err != nil {
			return err
		}
		if stops && a.Kind == "NORMAL" {
			return &AdmissionDeniedError{run, "durable hard stop before release"}
		}
		if d.ProviderCall != nil {
			var active bool
			if err := c.QueryRowContext(ctx, `SELECT duration_ns IS NULL FROM provider_calls WHERE provider_call_id=? AND invocation_id=?`, d.ProviderCall.ProviderCallID, a.Binding.InvocationID).Scan(&active); err != nil {
				return err
			}
			if !active {
				return fmt.Errorf("completed ProviderCall cannot be released again")
			}
			bounds := map[string]int64{}
			for _, b := range d.Bounds {
				bounds[b.Dimension] = b.Amount
			}
			if err := validateProviderRequestCapabilities(*d.ProviderCall, bounds, d.Capabilities); err != nil {
				return err
			}
			if err := validateCompletionCapabilities(d); err != nil {
				return err
			}
			grant, err := admissionTokenLimit(ctx, c, a)
			if err != nil {
				return err
			}
			used, _, err := invocationActual(ctx, c, a.Binding.InvocationID, "WORK")
			if err != nil {
				return err
			}
			bank, err := runTokenBalance(ctx, c, run)
			if err != nil {
				return err
			}
			available := bank.NormalAvailable
			if a.ContingencyReason != "" {
				available = bank.ContingencyAvailable
			}
			remaining, err := checkedSubtract(grant, used)
			if err != nil {
				return err
			}
			if bounds["WORK"] <= 0 || bounds["WORK"] > remaining || bounds["WORK"] > available || bank.contingencyExhausted() {
				return &AdmissionDeniedError{run, "capacity changed before release"}
			}
			if a.ContingencyReason == "" {
				limits, err := agentTokenLimits(ctx, c, a)
				if err != nil {
					return err
				}
				if d.CompletionOnly && !limits.CompletionOnly || !d.CompletionOnly && (limits.CompletionOnly || used >= limits.SoftLimit) {
					return &AdmissionDeniedError{run, "Soft/denial state changed before release"}
				}
			}
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO admission_release_authorizations VALUES(?,?,?)`, id, at, digest); err != nil {
			return err
		}
		created = true
		return nil
	})
	return committedCreation(created, err)
}

func mustClaimJSON(d AdmissionDispatchClaim) []byte { raw, _ := json.Marshal(d); return raw }
func validateCompletionCapabilities(d AdmissionDispatchClaim) error {
	if !d.CompletionOnly {
		if d.CompletionCapabilities != nil {
			return fmt.Errorf("completion capabilities require completion-only mode")
		}
		return nil
	}
	p := d.CompletionCapabilities
	if p == nil || p.ToolsEnabled || p.WorkspaceWriteEnabled || !p.FinalResponseOnly {
		return fmt.Errorf("completion-only requires final-response capability with tools and writes disabled")
	}
	return nil
}
