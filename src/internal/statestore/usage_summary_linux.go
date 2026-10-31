package statestore

import "marc-runtime/internal/numeric"

// Amount and knowledge are independent: a partial amount may be NULL. These
// summaries serve authoritative accounting and its observational projection.
type usageValue struct {
	State string `json:"state"`
	Value *int64 `json:"value"`
}

type usageSummary struct {
	ReceiptState string     `json:"receipt_state"`
	Input        usageValue `json:"input_tokens"`
	Cache        usageValue `json:"cached_input_tokens"`
	Fresh        usageValue `json:"fresh_input_tokens"`
	Output       usageValue `json:"output_tokens"`
	Reasoning    usageValue `json:"reasoning_tokens"`
	Work         usageValue `json:"work_tokens"`
	Spend        usageValue `json:"spend_tokens"`
}

func unknownMetric() usageValue { return usageValue{State: "unknown"} }
func emptyUsage() usageSummary {
	u := usageSummary{ReceiptState: "unknown"}
	for _, v := range usageValues(&u) {
		*v = unknownMetric()
	}
	return u
}

func usageValues(u *usageSummary) []*usageValue {
	return []*usageValue{&u.Input, &u.Cache, &u.Fresh, &u.Output, &u.Reasoning, &u.Work, &u.Spend}
}

func receiptMetric(state string, value *int64) usageValue {
	if value == nil && state != "partial" {
		return unknownMetric()
	}
	if value != nil && state == "unknown" {
		state = "partial"
	}
	return usageValue{State: state, Value: value}
}

func derivedMetric(left, right usageValue, subtract bool) (usageValue, error) {
	if left.Value == nil || right.Value == nil {
		if left.State == "partial" || right.State == "partial" {
			return usageValue{State: "partial"}, nil
		}
		return unknownMetric(), nil
	}
	var n int64
	var err error
	if subtract {
		n, err = checkedSubtract(*left.Value, *right.Value)
	} else {
		n, err = checkedAdd(*left.Value, *right.Value)
	}
	if err != nil {
		return usageValue{}, err
	}
	if n < 0 {
		return usageValue{}, numeric.Invalid("usage_subset", "invalid_input", "invalid usage subset")
	}
	state := "partial"
	if left.State == "final" && right.State == "final" {
		state = "final"
	}
	return usageValue{State: state, Value: &n}, nil
}

func usageFromReceipt(r UsageReceipt) (usageSummary, error) {
	for n, value := range []*int64{r.InputTokens, r.CachedInputTokens, r.OutputTokens, r.ReasoningTokens} {
		if value != nil && *value < 0 {
			return usageSummary{}, numeric.Invalid([]string{"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_tokens"}[n], "invalid_input", "negative usage counter")
		}
	}
	if r.InputTokens != nil && r.CachedInputTokens != nil && *r.CachedInputTokens > *r.InputTokens {
		return usageSummary{}, numeric.Invalid("cached_input_tokens", "invalid_input", "cached input exceeds input")
	}
	if r.OutputTokens != nil && r.ReasoningTokens != nil && *r.ReasoningTokens > *r.OutputTokens {
		return usageSummary{}, numeric.Invalid("reasoning_tokens", "invalid_input", "reasoning exceeds output")
	}
	summary := emptyUsage()
	u := &summary
	u.ReceiptState = r.Completeness
	u.Input = receiptMetric(r.Completeness, r.InputTokens)
	u.Cache = receiptMetric(r.Completeness, r.CachedInputTokens)
	u.Output = receiptMetric(r.Completeness, r.OutputTokens)
	u.Reasoning = receiptMetric(r.Completeness, r.ReasoningTokens)
	var err error
	if u.Fresh, err = derivedMetric(u.Input, u.Cache, true); err != nil {
		return *u, numeric.WithContext(err, numeric.Context{Domain: "fresh_input_tokens"})
	}
	if u.Work, err = derivedMetric(u.Input, u.Output, false); err != nil {
		return *u, numeric.WithContext(err, numeric.Context{Domain: "WORK"})
	}
	if u.Spend, err = derivedMetric(u.Fresh, u.Output, false); err != nil {
		return *u, numeric.WithContext(err, numeric.Context{Domain: "SPEND"})
	}
	return *u, nil
}

func aggregateUsage(children []usageSummary) (usageSummary, error) {
	parent := emptyUsage()
	allFinal := len(children) > 0
	anyReceipt := false
	for _, child := range children {
		allFinal = allFinal && child.ReceiptState == "final"
		anyReceipt = anyReceipt || child.ReceiptState != "unknown"
	}
	if allFinal {
		parent.ReceiptState = "final"
	} else if anyReceipt {
		parent.ReceiptState = "partial"
	}
	for n, m := range usageValues(&parent) {
		values := make([]usageValue, 0, len(children))
		for _, child := range children {
			values = append(values, *usageValues(&child)[n])
		}
		var err error
		*m, err = aggregateDimension(values)
		if err != nil {
			return parent, numeric.WithContext(err, numeric.Context{Domain: usageDomains[n]})
		}
	}
	return parent, nil
}

// Knowledge and amounts aggregate independently for valid state. Authoritative
// mutations validate every affected dimension before commit.
func aggregateDimension(children []usageValue) (usageValue, error) {
	result := unknownMetric()
	var sum int64
	known, partial := false, false
	final := len(children) > 0
	for _, value := range children {
		if value.State != "unknown" && value.State != "partial" && value.State != "final" ||
			value.State == "unknown" && value.Value != nil || value.State == "final" && value.Value == nil ||
			value.Value != nil && *value.Value < 0 {
			return result, numeric.Invalid("current_usage", "corrupt_state", "invalid current usage knowledge/amount")
		}
		final = final && value.State == "final"
		partial = partial || value.State == "partial"
		if value.Value != nil {
			var err error
			sum, err = checkedAdd(sum, *value.Value)
			if err != nil {
				return result, err
			}
			known = true
		}
	}
	if partial {
		result.State = "partial"
	}
	if known {
		result.Value = &sum
		result.State = "partial"
		if final {
			result.State = "final"
		}
	}
	return result, nil
}
