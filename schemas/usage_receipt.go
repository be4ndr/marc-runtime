package schemas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"marc-runtime/internal/numeric"
)

// MaxUsageReceiptBytes bounds one flat normalized record, including whitespace
// and escaping. It is a wire-safety bound, not an actual-usage or admission limit.
const MaxUsageReceiptBytes = 16 * 1024

// Check the fixed shape and exact numeric syntax before the schema validator's
// rational parser. No raw telemetry, nested objects or arrays belong to this wire.
func validateUsageReceiptWire(raw []byte) error {
	if len(raw) > MaxUsageReceiptBytes {
		return fmt.Errorf("usage receipt exceeds %d-byte wire limit", MaxUsageReceiptBytes)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("usage receipt is not UTF-8")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for name, value := range fields {
		switch name {
		case "schema_version", "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_tokens":
			if name != "schema_version" && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				continue
			}
			var n int64
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &n) != nil || n < 0 {
				return numeric.Invalid(name, "invalid_input", "usage wire number must be a non-negative base-10 int64 integer literal")
			}
		case "receipt_id", "run_id", "invocation_id", "provider_call_id", "source", "model", "provider_receipt_id", "reported_at", "state":
			var s *string
			if err := json.Unmarshal(value, &s); err != nil {
				return fmt.Errorf("usage wire field %s must be a string or permitted null", name)
			}
		default:
			return fmt.Errorf("unknown usage receipt field")
		}
	}
	return nil
}
