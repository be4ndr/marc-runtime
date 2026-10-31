package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"ai-control-plane/internal/numeric"
)

func TestNumericFailureLoggingDoesNotLeakPayload(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	err := numeric.WithContext(numeric.Invalid("WORK", "overflow", "addition is unrepresentable"), numeric.Context{Operation: "AcceptUsageReceipt", RunID: "run", ExecutionID: "execution", AttemptID: "attempt", InvocationID: "invocation", ProviderCallID: "call"})
	if !LogNumericFailure(context.Background(), logger, fmt.Errorf("secret provider payload: %w", err)) {
		t.Fatal("numeric error not classified")
	}
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"operation": "AcceptUsageReceipt", "numeric_domain": "WORK", "error_category": "overflow", "run_id": "run", "execution_id": "execution", "attempt_id": "attempt", "invocation_id": "invocation", "provider_call_id": "call"} {
		if record[key] != want {
			t.Fatal(key, record)
		}
	}
	if strings.Contains(out.String(), "secret") {
		t.Fatal("payload leaked")
	}
	out.Reset()
	if LogNumericFailure(context.Background(), logger, fmt.Errorf("other failure")) || out.Len() != 0 {
		t.Fatal("non-numeric error misclassified")
	}
}
