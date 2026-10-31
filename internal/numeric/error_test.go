package numeric

import (
	"errors"
	"fmt"
	"testing"
)

type testWrapper struct{ cause error }

func (e *testWrapper) Error() string { return "wrapper: " + e.cause.Error() }
func (e *testWrapper) Unwrap() error { return e.cause }

func TestWithContextPreservesErrorTree(t *testing.T) {
	original := Invalid("WORK", "overflow", "addition is unrepresentable")
	sibling := errors.New("joined sibling")
	wrapper := &testWrapper{cause: fmt.Errorf("original operation: %w", original)}
	tree := errors.Join(wrapper, sibling)
	enriched := WithContext(tree, Context{Operation: "AcceptUsageReceipt", RunID: "run"})
	enriched = WithContext(enriched, Context{ProviderCallID: "call"})
	var failure *Error
	if !errors.As(enriched, &failure) || failure.Operation != "AcceptUsageReceipt" || failure.RunID != "run" || failure.ProviderCallID != "call" || failure.Domain != "WORK" {
		t.Fatalf("missing enriched numeric context: %#v", failure)
	}
	for _, cause := range []error{original, sibling, wrapper, tree} {
		if !errors.Is(enriched, cause) {
			t.Fatalf("lost cause %T", cause)
		}
	}
	var retained *testWrapper
	if !errors.As(enriched, &retained) || retained != wrapper {
		t.Fatal("lost wrapper classification")
	}
	if enriched.Error() != tree.Error() {
		t.Fatal("lost original diagnostic text")
	}
	if original.(*Error).Operation != "" || original.(*Error).ProviderCallID != "" {
		t.Fatal("enrichment mutated original metadata")
	}
}

func TestWithContextLeavesUnrelatedErrorsUnchanged(t *testing.T) {
	for _, err := range []error{nil, errors.New("integer overflow"), errors.New("numeric invariant violation:")} {
		if got := WithContext(err, Context{Operation: "operation"}); got != err {
			t.Fatalf("unrelated error changed: %v", got)
		}
	}
}
