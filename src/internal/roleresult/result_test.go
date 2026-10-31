package roleresult_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"marc-runtime/schemas"
	"marc-runtime/src/internal/roleresult"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestFindingRejectsInvalidTypedUTF8BeforeMarshal(t *testing.T) {
	f := roleresult.Finding{FindingID: "fnd-000001", FindingOrdinal: 1, OriginCycle: 1, OriginResultID: "res-c001-r001", Summary: "valid"}
	f.Summary = string([]byte{255})
	if err := roleresult.ValidateFinding(f); err == nil {
		t.Fatal("typed invalid UTF-8 normalized and accepted")
	}
	f.Summary = "�"
	if err := roleresult.ValidateFinding(f); err != nil {
		t.Fatal("literal replacement character rejected", err)
	}
}

func TestDraft202012Metaschemas(t *testing.T) {
	c := jsonschema.NewCompiler()
	meta, err := c.Compile("https://json-schema.org/draft/2020-12/schema")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := schemas.Files.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Run(e.Name(), func(t *testing.T) {
			raw, err := schemas.Files.ReadFile(e.Name())
			if err != nil {
				t.Fatal(err)
			}
			v, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			if err := meta.Validate(v); err != nil {
				t.Fatal(err)
			}
			if err := schemas.Validate(e.Name(), []byte(`{}`)); err != nil && strings.Contains(err.Error(), "schema resource is not embedded") {
				t.Fatal(err)
			}
		})
	}
}
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../schemas/testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestPermanentFixtures(t *testing.T) {
	raw := fixture(t, "manifest")
	var cases []struct {
		File, Schema string
		Valid        bool
		RuntimeValid bool `json:"runtime_valid"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	var f roleresult.Finding
	if err := json.Unmarshal(fixture(t, "finding"), &f); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.File, func(t *testing.T) {
			raw, err := os.ReadFile("../../../schemas/testdata/" + c.File)
			if err != nil {
				t.Fatal(err)
			}
			err = schemas.Validate(c.Schema, raw)
			if (err == nil) != c.Valid {
				t.Fatalf("schema valid=%t: %v", c.Valid, err)
			}
			if !c.Valid {
				return
			}
			switch {
			case strings.Contains(c.Schema, "-result.schema"):
				r, err := roleresult.Decode(raw)
				if err == nil {
					err = roleresult.ValidateReferences(r, map[string]roleresult.Finding{f.FindingID: f}, map[string]string{"proof": "test_result"})
				}
				if (err == nil) != c.RuntimeValid {
					t.Fatalf("runtime valid=%t: %v", c.RuntimeValid, err)
				}
			case strings.HasSuffix(c.Schema, "/$defs/finding"):
				var f roleresult.Finding
				if err := json.Unmarshal(raw, &f); err != nil {
					t.Fatal(err)
				}
				err := roleresult.ValidateFinding(f)
				if (err == nil) != c.RuntimeValid {
					t.Fatalf("finding valid=%t: %v", c.RuntimeValid, err)
				}
			case c.File == "invalid-line-range.json":
				// End/start comparisons are deterministic checks outside JSON Schema.
				var e roleresult.EvidenceRef
				json.Unmarshal(raw, &e)
				var r map[string]any
				json.Unmarshal(fixture(t, "developer-completed"), &r)
				r["evidence_refs"] = []roleresult.EvidenceRef{e}
				doc, _ := json.Marshal(r)
				if _, err := roleresult.Decode(doc); err == nil {
					t.Fatal("bad range accepted")
				}
			}
		})
	}
}
func TestLeadStatusEnumsAndApprovalGate(t *testing.T) {
	var l map[string]any
	json.Unmarshal(fixture(t, "lead-qa-inconclusive"), &l)
	qa := l["qa"].(map[string]any)
	review := l["review"].(map[string]any)
	for _, q := range []string{"not_run", "passed", "failed", "inconclusive"} {
		for _, r := range []string{"not_run", "approved", "changes_required", "inconclusive"} {
			qa["status"] = q
			review["status"] = r
			l["semantic_status"] = "active"
			raw, _ := json.Marshal(l)
			if err := schemas.Validate("lead-state.schema.json", raw); err != nil {
				t.Fatal(q, r, err)
			}
			l["semantic_status"] = "approved"
			raw, _ = json.Marshal(l)
			err := schemas.Validate("lead-state.schema.json", raw)
			if (err != nil) != (q == "inconclusive" || r == "inconclusive") {
				t.Fatal(q, r, err)
			}
		}
	}
}
func TestResultShapeFailures(t *testing.T) {
	for _, name := range []string{"developer-completed", "qa-passed", "reviewer-approved"} {
		for _, change := range []string{"unknown", "outcome", "role", "id", "invocation", "timestamp", "evidence", "missing"} {
			t.Run(name+"/"+change, func(t *testing.T) {
				var d map[string]any
				json.Unmarshal(fixture(t, name), &d)
				switch change {
				case "unknown":
					d["provider"] = "must-not-be-duplicated"
				case "outcome":
					d["outcome"] = "timeout"
				case "role":
					d["role"] = "LEAD"
				case "id":
					d["result_id"] = "bad-result"
				case "invocation":
					d["invocation_id"] = "../unsafe"
				case "timestamp":
					d["produced_at"] = "yesterday"
				case "evidence":
					d["evidence_refs"] = []any{map[string]any{"evidence_id": "proof", "kind": "severity", "artifact_ref": map[string]any{"file": "log.txt"}}}
				case "missing":
					delete(d, "attempt_id")
				}
				raw, _ := json.Marshal(d)
				if _, err := roleresult.Decode(raw); err == nil {
					t.Fatal("invalid result accepted")
				}
			})
		}
	}
}

func TestNestedValidationAndAggregates(t *testing.T) {
	for _, name := range []string{"duplicate-check", "duplicate-finding", "duplicate-evidence", "check-no-evidence", "check-unknown", "check-enum", "line-zero", "line-end-without-start", "location-unknown", "finding-status", "result-transcript", "review-approved-blocking", "review-changes-no-blocking", "qa-inconclusive-blocking", "missing-finding", "missing-evidence", "evidence-kind"} {
		t.Run(name, func(t *testing.T) {
			var d map[string]any
			fixtureName := "qa-failed"
			if name == "review-approved-blocking" {
				fixtureName = "reviewer-changes-required"
			}
			if name == "review-changes-no-blocking" {
				fixtureName = "reviewer-approved"
			}
			json.Unmarshal(fixture(t, fixtureName), &d)
			checks, _ := d["checks"].([]any)
			occurrences := d["finding_occurrences"].([]any)
			refs := d["evidence_refs"].([]any)
			evidence := map[string]string{"proof": "test_result"}
			var f roleresult.Finding
			json.Unmarshal(fixture(t, "finding"), &f)
			findings := map[string]roleresult.Finding{f.FindingID: f}
			switch name {
			case "duplicate-check":
				d["checks"] = append(checks, checks[0])
			case "duplicate-finding":
				d["finding_occurrences"] = append(occurrences, occurrences[0])
			case "duplicate-evidence":
				d["evidence_refs"] = append(refs, refs[0])
			case "check-no-evidence":
				checks[0].(map[string]any)["evidence_refs"] = []any{}
			case "check-unknown":
				checks[0].(map[string]any)["provider"] = "fake"
			case "check-enum":
				checks[0].(map[string]any)["outcome"] = "timeout"
			case "line-zero":
				refs[0].(map[string]any)["location"].(map[string]any)["start_line"] = 0
			case "line-end-without-start":
				delete(refs[0].(map[string]any)["location"].(map[string]any), "start_line")
			case "location-unknown":
				refs[0].(map[string]any)["location"].(map[string]any)["file"] = "unexpected"
			case "finding-status":
				occurrences[0].(map[string]any)["status"] = "open"
			case "result-transcript":
				d["transcript"] = "must remain outside state"
			case "review-approved-blocking":
				d["outcome"] = "approved"
			case "review-changes-no-blocking":
				d["outcome"] = "changes_required"
			case "qa-inconclusive-blocking":
				d["outcome"] = "inconclusive"
			case "missing-finding":
				delete(findings, f.FindingID)
			case "missing-evidence":
				delete(evidence, "proof")
			case "evidence-kind":
				evidence["proof"] = "source"
			}
			raw, _ := json.Marshal(d)
			r, err := roleresult.Decode(raw)
			if err == nil {
				err = roleresult.ValidateReferences(r, findings, evidence)
			}
			if err == nil {
				t.Fatal("invalid nested/aggregate contract accepted")
			}
		})
	}
}

func TestEarlierContractRelationalReferences(t *testing.T) {
	// Lead source roles remain separate from overall verdicts; no filename can
	// substitute for an accepted relational reference.
	var lead map[string]any
	json.Unmarshal(fixture(t, "lead-qa-inconclusive"), &lead)
	for _, test := range []string{"unknown", "source-role", "old-file-reference", "missing-resolution"} {
		t.Run(test, func(t *testing.T) {
			raw, _ := json.Marshal(lead)
			var d map[string]any
			json.Unmarshal(raw, &d)
			finding := d["open_findings"].([]any)[0].(map[string]any)
			switch test {
			case "unknown":
				d["resources"] = map[string]any{}
			case "source-role":
				finding["source_role"] = "developer"
			case "old-file-reference":
				finding["source_result_ref"] = map[string]any{"cycle": 1, "file": "qa.json", "digest": "sha256:" + strings.Repeat("0", 64)}
			case "missing-resolution":
				d["resolved_findings"] = []any{finding}
				d["open_findings"] = []any{}
			}
			raw, _ = json.Marshal(d)
			if err := schemas.Validate("lead-state.schema.json", raw); err == nil {
				t.Fatal("invalid Lead reference accepted")
			}
		})
	}
	action := map[string]any{"schema_version": 1, "action_id": "action2", "run_id": "run1", "cycle": 2, "producer": lead["producer"], "based_on_lead_state": map[string]any{"lead_state_id": "lead-snapshot2"}, "content_identity": "content2", "target_role": "DEVELOPER", "purpose": "REMEDIATE", "finding_ids": []string{"fnd-000005"}, "intent": "Resolve focused finding.", "created_at": lead["created_at"]}
	raw, _ := json.Marshal(action)
	if err := schemas.Validate("next-action.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	action["finding_ids"] = []string{}
	raw, _ = json.Marshal(action)
	if err := schemas.Validate("next-action.schema.json", raw); err == nil {
		t.Fatal("remediation without findings")
	}
	request := map[string]any{"schema_version": 1, "run_id": "run1", "cycle": 2, "request_ordinal": 1, "request_id": "rr-c002-r001", "requester": map[string]any{"role": "lead", "invocation_id": "lead-invocation2"}, "target": map[string]any{"execution_id": "execution2", "attempt_id": "attempt2", "role": "QA", "purpose": "QA", "content_identity": "content2"}, "source_invocation_id": "invocation2", "requested": map[string]any{"work_tokens": 1}, "justification": "New evidence can support a bounded conclusion.", "based_on": map[string]any{"lead_state_ref": map[string]any{"lead_state_id": "lead-snapshot2"}, "source_result_ref": map[string]any{"result_id": "res-c002-r003"}}, "created_at": lead["created_at"]}
	raw, _ = json.Marshal(request)
	if err := schemas.Validate("resource-request.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	request["target"].(map[string]any)["invocation_id"] = "future-invocation"
	raw, _ = json.Marshal(request)
	if err := schemas.Validate("resource-request.schema.json", raw); err == nil {
		t.Fatal("request preassigned invocation")
	}
}

func TestAmbiguousJSONFailsClosed(t *testing.T) {
	raw := fixture(t, "developer-completed")
	raw = []byte(strings.Replace(string(raw), `"cycle": 2`, `"cycle": 2, "cycle": 2`, 1))
	if _, err := roleresult.Decode(raw); err == nil {
		t.Fatal("duplicate authoritative JSON field accepted")
	}
	if err := schemas.Validate("developer-result.schema.json", append(fixture(t, "developer-completed"), []byte(`{}`)...)); err == nil {
		t.Fatal("trailing JSON document accepted")
	}
}

func TestNonblockingFindingsPermitPositiveVerdicts(t *testing.T) {
	var f roleresult.Finding
	json.Unmarshal(fixture(t, "finding"), &f)
	f.Blocking = false
	for _, name := range []string{"qa-failed", "reviewer-changes-required"} {
		var d map[string]any
		json.Unmarshal(fixture(t, name), &d)
		if name == "qa-failed" {
			d["outcome"] = "passed"
			d["checks"].([]any)[0].(map[string]any)["outcome"] = "passed"
		} else {
			d["outcome"] = "approved"
		}
		raw, _ := json.Marshal(d)
		r, err := roleresult.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := roleresult.ValidateReferences(r, map[string]roleresult.Finding{f.FindingID: f}, map[string]string{"proof": "test_result"}); err != nil {
			t.Fatal(err)
		}
	}
}
func TestDeveloperCompletionAndBlockerExclusivity(t *testing.T) {
	for _, name := range []string{"completed-with-work", "completed-with-blocker", "incomplete-with-blocker"} {
		var d map[string]any
		json.Unmarshal(fixture(t, "developer-completed"), &d)
		if name == "completed-with-work" {
			d["remaining_work"] = []string{"unfinished"}
		} else {
			d["blocker"] = map[string]any{"summary": "prerequisite", "evidence_refs": []any{}}
			if name == "incomplete-with-blocker" {
				d["outcome"] = "incomplete"
				d["remaining_work"] = []string{"unfinished"}
			}
		}
		raw, _ := json.Marshal(d)
		if _, err := roleresult.Decode(raw); err == nil {
			t.Fatal("invalid Developer contract", name)
		}
	}
}

func TestFindingDescriptionCodePointBoundaries(t *testing.T) {
	for _, n := range []int{256, 257, 1024, 1025} {
		for _, char := range []string{"x", "😀"} {
			f := roleresult.Finding{FindingID: "fnd-000001", FindingOrdinal: 1, OriginCycle: 1, OriginResultID: "res-c001-r001", Summary: strings.Repeat(char, n)}
			if (roleresult.ValidateFinding(f) == nil) != (n <= 1024) {
				t.Fatalf("finding summary boundary %d", n)
			}
		}
	}
}
