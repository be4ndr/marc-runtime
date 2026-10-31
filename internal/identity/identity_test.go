package identity

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestJSONStringLimitLocationAndStructuralPriority(t *testing.T) {
	for _, tc := range []struct {
		raw, pointer string
		limit        int
	}{
		{`{"submission":{"checks":[{}, {"summary":"` + strings.Repeat("😀", 1025) + `"}]}}`, "/submission/checks/1/summary", 1024},
		{`{"a~/b":{"remaining_work":["` + strings.Repeat("x", 1025) + `"]}}`, "/a~0~1b/remaining_work/0", 1024},
		{`{"invocation_id":"` + strings.Repeat("x", 257) + `"}`, "/invocation_id", 256},
	} {
		err := CheckJSONEnvelope([]byte(tc.raw))
		var limit *JSONStringLimitError
		if !errors.As(err, &limit) || limit.Pointer != tc.pointer || limit.Limit != tc.limit {
			t.Fatalf("incorrect length location: %+v (%v)", limit, err)
		}
	}
	long := `{"summary":"` + strings.Repeat("x", 1025) + `"`
	for _, raw := range []string{long + `,"\u0073ummary":"again"}`, long + `} {}`, long + `,"next":`, long + `,"items":[` + strings.Repeat("[", MaxJSONDepth) + `0` + strings.Repeat("]", MaxJSONDepth) + `]}`} {
		err := CheckJSONEnvelope([]byte(raw))
		var limit *JSONStringLimitError
		if err == nil || errors.As(err, &limit) {
			t.Fatal("structurally invalid envelope misclassified as a length error", err)
		}
	}
}

func TestIdentifierPolicyAndTextBoundaries(t *testing.T) {
	for _, value := range []string{"a", "0", "a-0", strings.Repeat("a", 256)} {
		if !ValidID(value) {
			t.Fatalf("valid ID rejected: %q", value)
		}
	}
	for _, value := range []string{"", "-a", "a-", "a--b", "A", "a_b", "a.b", "a:b", "a b", strings.Repeat("a", 257)} {
		if ValidID(value) {
			t.Fatalf("invalid ID accepted: %q", value)
		}
	}
	if !ValidText(strings.Repeat("界", 256)) || ValidText(strings.Repeat("界", 257)) || ValidText(string([]byte{0xff})) {
		t.Fatal("text UTF-8/code-point boundary mismatch")
	}
}

func TestStrictJSONUnicodeDuplicatesAndEnvelope(t *testing.T) {
	for _, raw := range []string{`{"x":"\ud83d\ude00"}`, `{"x":"�"}`, `{"x":"\\ud800"}`, `{"n":9007199254740993,"huge":1e1000000}`} {
		if err := CheckJSONEnvelope([]byte(raw)); err != nil {
			t.Fatalf("valid exact JSON rejected: %s: %v", raw, err)
		}
	}
	for name, raw := range map[string]string{
		"high": `{"x":"\ud800"}`, "low": `{"x":"\udfff"}`, "bad pair": `{"x":"\ud800\u0041"}`,
		"duplicate": `{"x":1,"x":2}`, "escaped duplicate": `{"x":1,"\u0078":2}`,
		"hidden oversize": `{"x":"` + strings.Repeat("a", 257) + `","x":"ok"}`,
		"invalid utf8":    string([]byte{'"', 255, '"'}), "trailing": `{} {}`,
		"bytes": strings.Repeat(" ", MaxJSONBytes) + `{}`,
		"depth": strings.Repeat("[", MaxJSONDepth+1) + `0` + strings.Repeat("]", MaxJSONDepth+1),
		"count": `[` + strings.Repeat(`0,`, MaxJSONTokens) + `0]`,
	} {
		t.Run(name, func(t *testing.T) {
			if CheckJSONEnvelope([]byte(raw)) == nil {
				t.Fatal("unsafe JSON accepted")
			}
		})
	}
	if err := CheckJSONEnvelope([]byte(strings.Repeat("[", MaxJSONDepth) + `0` + strings.Repeat("]", MaxJSONDepth))); err != nil {
		t.Fatal("exact depth boundary rejected", err)
	}
	if err := CheckJSONEnvelope([]byte(`{}` + strings.Repeat(" ", MaxJSONBytes-2))); err != nil {
		t.Fatal("exact byte boundary rejected", err)
	}
	if err := CheckJSONEnvelope([]byte(`[` + strings.Repeat(`0,`, MaxJSONTokens-3) + `0]`)); err != nil {
		t.Fatal("exact token boundary rejected", err)
	}
	// Large trusted observational exports keep their approved complete width.
	if err := CheckJSONStrings([]byte(`[` + strings.Repeat(`0,`, MaxJSONTokens) + `0]`)); err != nil {
		t.Fatal("trusted export width restricted", err)
	}
}

func TestDescriptionLimitsAndStructuralIsolation(t *testing.T) {
	for _, char := range []string{"x", "界", "😀"} {
		for _, n := range []int{256, 257, 1024, 1025} {
			t.Run(fmt.Sprintf("%s-%d", char, n), func(t *testing.T) {
				text := strings.Repeat(char, n)
				if ValidDescription(text) != (n <= 1024) || (CheckDescription("summary", text) == nil) != (n <= 1024) {
					t.Fatal("description bound")
				}
				for _, field := range []string{"goal", "Goal", "summary", "blocker", "escalation_reason", "evidence", "intent", "justification", "prompt", "response", "approval_response", "reason", "contingency_reason"} {
					raw, _ := json.Marshal(map[string]any{field: text})
					if (CheckJSONEnvelope(raw) == nil) != (n <= 1024) {
						t.Fatalf("%s limit mismatch", field)
					}
				}
				for _, field := range []string{"remaining_work", "acceptance_criteria"} {
					raw, _ := json.Marshal(map[string]any{field: []string{text}})
					if (CheckJSONEnvelope(raw) == nil) != (n <= 1024) {
						t.Fatalf("%s item limit mismatch", field)
					}
				}
				for _, field := range []string{"run_id", "mcp_client_name", "outcome", "decision", "role", "status", "path", "model", "unknown"} {
					raw, _ := json.Marshal(map[string]any{field: text})
					if (CheckJSONEnvelope(raw) == nil) != (n <= 256) {
						t.Fatalf("structural %s limit changed", field)
					}
				}
			})
		}
	}
	for _, raw := range []string{
		`{"summary":{"run_id":"` + strings.Repeat("x", 257) + `"}}`,
		`{"remaining_work":[{"path":"` + strings.Repeat("x", 257) + `"}]}`,
		`{"` + strings.Repeat("x", 257) + `":"ok"}`,
		`{"summary":"\ud800"}`, `{"goal":"\udfff"}`, `{"summary":"ok","\u0073ummary":"again"}`,
	} {
		if CheckJSONEnvelope([]byte(raw)) == nil {
			t.Fatal("unsafe descriptive JSON accepted")
		}
	}
	if ValidDescription(string([]byte{255})) {
		t.Fatal("invalid UTF-8 accepted")
	}
	// Escaped supplementary-plane characters count once after decoding.
	for _, n := range []int{1024, 1025} {
		raw := []byte(`{"summary":"` + strings.Repeat(`\ud83d\ude00`, n) + `"}`)
		if (CheckJSONEnvelope(raw) == nil) != (n == 1024) {
			t.Fatal("surrogate pair count mismatch")
		}
	}
}
