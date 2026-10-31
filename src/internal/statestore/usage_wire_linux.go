package statestore

import (
	"context"
	"encoding/json"

	"marc-runtime/schemas"
)

type usageReceiptWire struct {
	SchemaVersion     int     `json:"schema_version"`
	ReceiptID         string  `json:"receipt_id"`
	RunID             string  `json:"run_id"`
	InvocationID      string  `json:"invocation_id"`
	ProviderCallID    string  `json:"provider_call_id"`
	Source            string  `json:"source"`
	Model             string  `json:"model"`
	ProviderReceiptID *string `json:"provider_receipt_id"`
	ReportedAt        string  `json:"reported_at"`
	State             string  `json:"state"`
	InputTokens       *int64  `json:"input_tokens"`
	CachedInputTokens *int64  `json:"cached_input_tokens"`
	OutputTokens      *int64  `json:"output_tokens"`
	ReasoningTokens   *int64  `json:"reasoning_tokens"`
}

type usageProvenance struct{ source, model string }

// AcceptUsageReceiptJSON is Orchestrator-only persistence of validated adapter
// observations. Adapters report to Orchestrator and never receive StateStore.
// Wire, original call authority and FINAL closure checks share the typed API's
// receipt/current-usage/audit transaction. Source/model must match the call.
func (s *StateStore) AcceptUsageReceiptJSON(ctx context.Context, raw []byte, opts AccountingOptions) error {
	if err := schemas.Validate("usage-receipt.schema.json", raw); err != nil {
		return numericFailure("AcceptUsageReceiptJSON", "", "", "", err)
	}
	var w usageReceiptWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	r := UsageReceipt{
		ReceiptID: w.ReceiptID, RunID: w.RunID, InvocationID: w.InvocationID,
		ProviderCallID: w.ProviderCallID, ProviderReceiptID: w.ProviderReceiptID,
		Completeness: w.State, ReportedAt: w.ReportedAt,
		InputTokens: w.InputTokens, CachedInputTokens: w.CachedInputTokens,
		OutputTokens: w.OutputTokens, ReasoningTokens: w.ReasoningTokens,
	}
	return s.acceptUsageReceipt(ctx, r, &usageProvenance{w.Source, w.Model}, opts)
}
