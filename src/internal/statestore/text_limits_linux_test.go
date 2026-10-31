package statestore

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"marc-runtime/internal/identity"
)

func TestWorkflowDescriptiveLimitsAndAtomicRejection(t *testing.T) {
	for _, n := range []int{256, 257, 1024, 1025} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := testStore(t)
			spec := workflowSpec()
			text := strings.Repeat("😀", n)
			spec.Goal = text
			spec.Confirmation.Prompt = text
			spec.Confirmation.Response = text
			before := mutationDatabaseSnapshot(t, s)
			err := s.CreateWorkflow(accountingContext, spec)
			if n == 1025 {
				if err == nil || !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("oversized input changed state", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before = mutationDatabaseSnapshot(t, s)
			if err = s.CreateWorkflow(accountingContext, spec); err != nil || !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("replay changed authority", err)
			}
			got, err := s.GetWorkflow(accountingContext, spec.RunID)
			if err != nil || got.Spec.Goal != text || got.Spec.Confirmation.Response != text {
				t.Fatal("text changed", err)
			}
			for _, field := range []string{"goal", "prompt", "response"} {
				bad := spec
				switch field {
				case "goal":
					bad.Goal = text + "x"
				case "prompt":
					bad.Confirmation.Prompt = text + "x"
				case "response":
					bad.Confirmation.Response = text + "x"
				}
				if err = s.CreateWorkflow(accountingContext, bad); err == nil {
					t.Fatal("conflicting replay accepted")
				}
			}
		})
	}
	if descriptionUTF8(strings.Repeat("界", 1024)) != nil || descriptionUTF8(strings.Repeat("界", 1025)) == nil || descriptionUTF8(string([]byte{255})) == nil {
		t.Fatal("typed description boundary")
	}
	if admissionUTF8(strings.Repeat("界", 256)) != nil || admissionUTF8(strings.Repeat("界", 257)) == nil || identity.ValidID(strings.Repeat("x", 257)) {
		t.Fatal("structural limit widened")
	}
}
