package schemas

import (
	"errors"
	"testing"

	"ai-control-plane/internal/numeric"
)

func TestExtremeNumericSyntaxReturnsError(t *testing.T) {
	for _, value := range []string{"1e1000000000", "1e-1000000000", "1e9223372036854775808", "1e-9223372036854775809", "0e1000000000", "1.0e-1000000"} {
		t.Run(value, func(t *testing.T) {
			// This is the public validation path. A panic fails the test directly.
			err := Validate("resource-request.schema.json", []byte(`{"requested":{"work_tokens":`+value+`}}`))
			var invariant *numeric.Error
			if !errors.As(err, &invariant) {
				t.Fatal("extreme exponent not rejected before validation", err)
			}
		})
	}
	for _, value := range []string{"1e2", "1e-6", "9223372036854775807", "0.000001"} {
		if _, err := decodeUnique([]byte(value)); err != nil {
			t.Fatal(value, err)
		}
	}
}
