package schemas

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"ai-control-plane/internal/numeric"
)

func usageWireFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/usage-receipts/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestUsageReceiptWireFixtures(t *testing.T) {
	var cases []struct {
		File  string `json:"file"`
		Valid bool   `json:"wire_valid"`
	}
	if err := json.Unmarshal(usageWireFixture(t, "manifest.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.File, func(t *testing.T) {
			err := Validate("usage-receipt.schema.json", usageWireFixture(t, tc.File))
			if (err == nil) != tc.Valid {
				t.Fatalf("valid=%v: %v", tc.Valid, err)
			}
		})
	}
}

func TestUsageReceiptWireSafety(t *testing.T) {
	base := string(usageWireFixture(t, "unknown.json"))
	for _, n := range []string{"1e1000000000", "1e999999", "0e999999", "1e2", "1.0", `"100"`, "9223372036854775808", "-1"} {
		t.Run(n, func(t *testing.T) {
			raw := strings.Replace(base, `"input_tokens": null`, `"input_tokens": `+n, 1)
			err := Validate("usage-receipt.schema.json", []byte(raw))
			var invariant *numeric.Error
			if !errors.As(err, &invariant) {
				t.Fatal("numeric wire syntax not rejected safely", err)
			}
		})
	}
	for _, tc := range []struct{ name, old, value string }{
		{"label-too-long", `"source": "fake"`, `"source": "` + strings.Repeat("a", 257) + `"`},
		{"model-blank", `"model": "fake-model"`, `"model": " "`},
		{"id-too-long", `"receipt_id": "wire-receipt"`, `"receipt_id": "` + strings.Repeat("a", 257) + `"`},
		{"id-uppercase", `"receipt_id": "wire-receipt"`, `"receipt_id": "Wire-receipt"`},
		{"id-underscore", `"receipt_id": "wire-receipt"`, `"receipt_id": "wire_receipt"`},
		{"missing-input", `"input_tokens": null,`, ""},
		{"nested-input", `"input_tokens": null`, `"input_tokens": {"value": 1}`},
		{"array-input", `"input_tokens": null`, `"input_tokens": [1]`},
		{"null-version", `"schema_version": 1`, `"schema_version": null`},
		{"wrong-version", `"schema_version": 1`, `"schema_version": 2`},
		{"null-model", `"model": "fake-model"`, `"model": null`},
		{"raw-telemetry", `"state": "unknown"`, `"state": "unknown", "provider_raw": {"input_tokens": 1}`},
		{"wrong-case", `"input_tokens": null`, `"Input_tokens": null`},
		{"sub-nanosecond-time", `"reported_at": "2026-10-01T12:00:00Z"`, `"reported_at": "2026-10-01T12:00:00.1234567891Z"`},
		{"bad-time", `"reported_at": "2026-10-01T12:00:00Z"`, `"reported_at": "2026-02-30T12:00:00Z"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := Validate("usage-receipt.schema.json", []byte(strings.Replace(base, tc.old, tc.value, 1))); err == nil {
				t.Fatal("invalid contract accepted")
			}
		})
	}
	for _, raw := range [][]byte{[]byte(base + "{}"), append([]byte(base), 0xff)} {
		if err := Validate("usage-receipt.schema.json", raw); err == nil {
			t.Fatal("trailing content or invalid UTF-8 accepted")
		}
	}
	// Whitespace counts toward the byte bound. Test both sides of the boundary.
	full := []byte(base + strings.Repeat(" ", MaxUsageReceiptBytes-len(base)))
	if err := Validate("usage-receipt.schema.json", full); err != nil {
		t.Fatal(err)
	}
	if err := Validate("usage-receipt.schema.json", append(full, ' ')); err == nil {
		t.Fatal("oversized receipt accepted")
	}
	for _, replacement := range []string{`"input_tokens": 9223372036854775807`, `"source": "` + strings.Repeat("a", 256) + `"`} {
		old := `"input_tokens": null`
		if strings.Contains(replacement, "source") {
			old = `"source": "fake"`
		}
		if err := Validate("usage-receipt.schema.json", []byte(strings.Replace(base, old, replacement, 1))); err != nil {
			t.Fatal("representable boundary rejected", err)
		}
	}
}

func TestSharedIdentifierAndUnicodeStringPolicy(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"a", true}, {"0-a", true}, {strings.Repeat("a", 256), true},
		{"A", false}, {"a_b", false}, {"a--b", false}, {"a.", false}, {strings.Repeat("a", 257), false},
	} {
		raw, _ := json.Marshal(tc.value)
		err := Validate("role-result-common.schema.json#/$defs/identifier", raw)
		if (err == nil) != tc.valid {
			t.Errorf("identifier %q valid=%v err=%v", tc.value, tc.valid, err)
		}
	}
	base := string(usageWireFixture(t, "unknown.json"))
	valid := strings.Replace(base, `"source": "fake"`, `"source": "`+strings.Repeat("界", 256)+`"`, 1)
	valid = strings.Replace(valid, `"model": "fake-model"`, `"model": "`+strings.Repeat("m", 256)+`"`, 1)
	valid = strings.Replace(valid, `"provider_call_id": "i1-call"`, `"provider_call_id": "`+strings.Repeat("a", 256)+`"`, 1)
	if err := Validate("usage-receipt.schema.json", []byte(valid)); err != nil {
		t.Fatalf("valid boundary receipt rejected: %v", err)
	}
	invalidUTF8 := append([]byte(base), 0xff)
	if err := Validate("usage-receipt.schema.json", invalidUTF8); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}
