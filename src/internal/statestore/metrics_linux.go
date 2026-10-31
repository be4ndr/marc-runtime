package statestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"ai-control-plane/schemas"
)

type metricsValue = usageValue
type metricsUsage = usageSummary

type metricsAggregate struct {
	Usage     metricsUsage            `json:"usage"`
	Telemetry map[string]metricsValue `json:"telemetry"`
	Events    map[string]metricsValue `json:"resource_events"`
}

type metricsActor struct {
	Role       string  `json:"role"`
	Invocation *string `json:"invocation_id"`
	Epoch      int64   `json:"ownership_epoch"`
	Revision   int64   `json:"revision"`
}

type metricsRunState struct {
	Status       string        `json:"status"`
	CurrentCycle int           `json:"current_cycle"`
	Revision     int64         `json:"revision"`
	Actor        *metricsActor `json:"active_actor"`
}

type metricsInvocationState struct {
	InvocationStatus string  `json:"invocation_status"`
	AttemptStatus    string  `json:"attempt_status"`
	ExecutionStatus  string  `json:"execution_status"`
	Purpose          string  `json:"purpose"`
	Current          bool    `json:"current"`
	StartedAt        string  `json:"started_at"`
	CompletedAt      *string `json:"completed_at"`
}

type metricsInvocation struct {
	Ordinal         int                    `json:"invocation"`
	ID              string                 `json:"invocation_id"`
	ExecutionID     string                 `json:"execution_id"`
	Attempt         int                    `json:"attempt"`
	AttemptID       string                 `json:"attempt_id"`
	Source          string                 `json:"source"`
	Model           string                 `json:"model"`
	ReasoningEffort string                 `json:"reasoning_effort"`
	ReceiptID       *string                `json:"receipt_id"`
	ReportedAt      *string                `json:"receipt_reported_at"`
	State           metricsInvocationState `json:"execution_state"`
	Metrics         metricsAggregate       `json:"metrics"`
	ProviderCalls   []metricsProviderCall  `json:"provider_calls"`
}

type metricsRole struct {
	Role        string              `json:"role"`
	Metrics     metricsAggregate    `json:"metrics"`
	Invocations []metricsInvocation `json:"invocations"`
}

type metricsCycle struct {
	Cycle   int              `json:"cycle"`
	Metrics metricsAggregate `json:"metrics"`
	Roles   []metricsRole    `json:"roles"`
}

type metricsDocument struct {
	SchemaVersion        int              `json:"schema_version"`
	RunID                string           `json:"run_id"`
	UpdatedAt            string           `json:"updated_at"`
	GeneratedAt          string           `json:"generated_at"`
	SourceSnapshot       string           `json:"source_snapshot"`
	State                metricsRunState  `json:"execution_state"`
	Totals               metricsAggregate `json:"totals"`
	Cycles               []metricsCycle   `json:"cycles"`
	AccountingStatus     string           `json:"accounting_status"`
	AccountingIncomplete bool             `json:"accounting_incomplete"`
}

type metricsSourceInvocation struct {
	Cycle      int
	Role       string
	Invocation metricsInvocation
}

type metricsSourceProviderCall struct {
	ID              string             `json:"provider_call_id"`
	InvocationID    string             `json:"invocation_id"`
	Ordinal         int                `json:"ordinal"`
	Source          string             `json:"source"`
	Model           string             `json:"model"`
	ReasoningEffort string             `json:"reasoning_effort"`
	StartedAt       string             `json:"started_at"`
	DurationNS      *int64             `json:"duration_ns"`
	ReceiptID       *string            `json:"receipt_id"`
	ReportedAt      *string            `json:"receipt_reported_at"`
	Revision        int64              `json:"revision"`
	Usage           metricsUsage       `json:"usage"`
	UsageEvidence   map[string]*string `json:"usage_evidence"`
}

// Only the projection contains finished_at. It is absent from authoritative
// snapshot data and therefore cannot participate in source identity.
type metricsProviderCall struct {
	metricsSourceProviderCall
	FinishedAt *string `json:"finished_at"`
}

type metricsSource struct {
	RunID, UpdatedAt string
	State            metricsRunState
	Cycles           []int
	Invocations      []metricsSourceInvocation
	ProviderCalls    []metricsSourceProviderCall
	External         bool
}

// readMetricsSource must run inside one explicit read transaction. Its first
// SELECT pins the WAL snapshot; all subsequent reads use that same connection.
func readMetricsSource(ctx context.Context, c *sql.Conn, runID string) (metricsSource, error) {
	source := metricsSource{RunID: runID, Cycles: []int{}, Invocations: []metricsSourceInvocation{}, ProviderCalls: []metricsSourceProviderCall{}}
	var actor metricsActor
	var role sql.NullString
	var epoch, revision sql.NullInt64
	err := c.QueryRowContext(ctx, `SELECT r.status,r.current_cycle,r.revision,r.updated_at,a.active_role,a.active_invocation_id,a.ownership_epoch,a.revision,COALESCE(json_extract(w.spec_json,'$.Mode')='EXTERNAL',0)
 FROM runs r LEFT JOIN run_active_actor a ON a.run_id=r.run_id LEFT JOIN workflow_runs w ON w.run_id=r.run_id WHERE r.run_id=?`, runID).Scan(&source.State.Status, &source.State.CurrentCycle, &source.State.Revision, &source.UpdatedAt, &role, &actor.Invocation, &epoch, &revision, &source.External)
	if err != nil {
		return source, err
	}
	if role.Valid {
		actor.Role = role.String
		actor.Epoch = epoch.Int64
		actor.Revision = revision.Int64
		source.State.Actor = &actor
	}
	rows, err := c.QueryContext(ctx, `SELECT cycle_no FROM cycles WHERE run_id=? ORDER BY cycle_no`, runID)
	if err != nil {
		return source, err
	}
	for rows.Next() {
		var cycle int
		if err := rows.Scan(&cycle); err != nil {
			rows.Close()
			return source, err
		}
		source.Cycles = append(source.Cycles, cycle)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return source, err
	}
	rows, err = c.QueryContext(ctx, `SELECT i.cycle_no,i.role,i.invocation_id,i.execution_id,i.attempt_id,i.invocation_ordinal,a.attempt_ordinal,i.provider,i.model,i.reasoning_effort,i.status,a.status,e.status,i.purpose,i.started_at,i.completed_at
 FROM invocations i JOIN attempts a ON a.attempt_id=i.attempt_id AND a.execution_id=i.execution_id AND a.run_id=i.run_id
 JOIN executions e ON e.execution_id=i.execution_id AND e.run_id=i.run_id
 WHERE i.run_id=? ORDER BY i.cycle_no,i.role,i.execution_id,a.attempt_ordinal,i.invocation_ordinal,i.invocation_id`, runID)
	if err != nil {
		return source, err
	}
	for rows.Next() {
		var entry metricsSourceInvocation
		i := &entry.Invocation
		if err := rows.Scan(&entry.Cycle, &entry.Role, &i.ID, &i.ExecutionID, &i.AttemptID, &i.Ordinal, &i.Attempt, &i.Source, &i.Model, &i.ReasoningEffort, &i.State.InvocationStatus, &i.State.AttemptStatus, &i.State.ExecutionStatus, &i.State.Purpose, &i.State.StartedAt, &i.State.CompletedAt); err != nil {
			rows.Close()
			return source, err
		}
		i.State.Current = source.State.Actor != nil && source.State.Actor.Invocation != nil && *source.State.Actor.Invocation == i.ID
		source.Invocations = append(source.Invocations, entry)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return source, err
	}
	rows, err = c.QueryContext(ctx, `SELECT p.provider_call_id,p.invocation_id,p.call_ordinal,p.provider,p.model,p.reasoning_effort,p.started_at,p.duration_ns,c.receipt_id,c.reported_at,c.revision,`+currentUsageEvidenceColumns+`,`+currentUsageColumns+`
 FROM provider_calls p LEFT JOIN provider_call_usage c USING(provider_call_id) JOIN invocations i USING(invocation_id)
 WHERE i.run_id=? ORDER BY p.invocation_id,p.call_ordinal`, runID)
	if err != nil {
		return source, err
	}
	for rows.Next() {
		var call metricsSourceProviderCall
		var evidence [7]*string
		prefix := []any{&call.ID, &call.InvocationID, &call.Ordinal, &call.Source, &call.Model, &call.ReasoningEffort, &call.StartedAt, &call.DurationNS, &call.ReceiptID, &call.ReportedAt, &call.Revision}
		for n := range evidence {
			prefix = append(prefix, &evidence[n])
		}
		if err := scanCurrentUsage(rows.Scan, &call.Usage, prefix...); err != nil {
			rows.Close()
			return source, err
		}
		call.UsageEvidence = map[string]*string{}
		for n, name := range []string{"input_tokens", "cached_input_tokens", "fresh_input_tokens", "output_tokens", "reasoning_tokens", "work_tokens", "spend_tokens"} {
			call.UsageEvidence[name] = evidence[n]
		}

		source.ProviderCalls = append(source.ProviderCalls, call)
	}
	err = rows.Err()
	rows.Close()
	return source, err
}

func (s *StateStore) metricsSnapshot(ctx context.Context, runID string) (metricsSource, error) {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return metricsSource{}, err
	}
	defer c.Close()
	if _, err := c.ExecContext(ctx, "BEGIN"); err != nil {
		return metricsSource{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := c.ExecContext(cleanup, "ROLLBACK"); err != nil {
			_ = c.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	return readMetricsSource(ctx, c, runID)
}

func emptyMetrics() metricsAggregate {
	a := metricsAggregate{Usage: emptyUsage(), Telemetry: map[string]metricsValue{}, Events: map[string]metricsValue{}}
	for _, name := range []string{"model_calls", "max_input_tokens", "elapsed_time_sec", "projection_bytes", "context_growth_bytes", "tool_time_sec", "waiting_time_sec"} {
		a.Telemetry[name] = unknownMetric()
	}
	for _, name := range []string{"soft_limit", "resource_request", "grant", "partial_grant", "deny", "degrade", "context_reset", "finalization", "recovery"} {
		a.Events[name] = unknownMetric()
	}
	return a
}

func rollupMetrics(children []metricsAggregate) (metricsAggregate, error) {
	parent := emptyMetrics()
	summaries := make([]usageSummary, 0, len(children))
	for _, child := range children {
		summaries = append(summaries, child.Usage)
	}
	var err error
	parent.Usage, err = aggregateUsage(summaries)
	return parent, err
}

func renderMetrics(source metricsSource, generatedAt time.Time) ([]byte, error) {
	canonical, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	doc := metricsDocument{SchemaVersion: 1, RunID: source.RunID, UpdatedAt: source.UpdatedAt, GeneratedAt: generatedAt.UTC().Format(time.RFC3339Nano), SourceSnapshot: fmt.Sprintf("sha256:%x", sha256.Sum256(canonical)), State: source.State, Cycles: []metricsCycle{}}
	incomplete := 0
	for _, call := range source.ProviderCalls {
		if call.Usage.ReceiptState != "final" || call.Usage.Work.State != "final" || call.Usage.Spend.State != "final" || call.Usage.Work.Value == nil || call.Usage.Spend.Value == nil {
			incomplete++
		}
	}
	doc.AccountingStatus = accountingStatus(source.External, len(source.ProviderCalls), incomplete)
	doc.AccountingIncomplete = doc.AccountingStatus == "unknown" || doc.AccountingStatus == "incomplete"
	var cycleChildren []metricsAggregate
	for _, cycle := range source.Cycles {
		c := metricsCycle{Cycle: cycle, Roles: []metricsRole{}}
		var roleChildren []metricsAggregate
		for _, entry := range source.Invocations {
			if entry.Cycle != cycle {
				continue
			}
			i := entry.Invocation
			i.Metrics = emptyMetrics()
			i.ProviderCalls = []metricsProviderCall{}
			var calls []metricsAggregate
			for _, call := range source.ProviderCalls {
				if call.InvocationID == i.ID {
					projected := metricsProviderCall{metricsSourceProviderCall: call}
					if call.DurationNS != nil {
						start, err := time.Parse(time.RFC3339Nano, call.StartedAt)
						if err != nil {
							return nil, fmt.Errorf("ProviderCall start timestamp: %w", err)
						}
						finish := start.Add(time.Duration(*call.DurationNS))
						if finish.Year() > 9999 {
							return nil, fmt.Errorf("derived ProviderCall finish timestamp is unrepresentable")
						}
						at := finish.Format(time.RFC3339Nano)
						projected.FinishedAt = &at
					}
					i.ProviderCalls = append(i.ProviderCalls, projected)
					a := emptyMetrics()
					a.Usage = call.Usage
					calls = append(calls, a)
				}
			}
			i.Metrics, err = rollupMetrics(calls)
			if err != nil {
				return nil, err
			}
			// Legacy invocation receipt fields are meaningful only for a single call.
			if len(i.ProviderCalls) == 1 {
				i.ReceiptID = i.ProviderCalls[0].ReceiptID
				i.ReportedAt = i.ProviderCalls[0].ReportedAt
			}
			if len(c.Roles) == 0 || c.Roles[len(c.Roles)-1].Role != entry.Role {
				c.Roles = append(c.Roles, metricsRole{Role: entry.Role, Invocations: []metricsInvocation{}})
			}
			r := &c.Roles[len(c.Roles)-1]
			r.Invocations = append(r.Invocations, i)
		}
		for n := range c.Roles {
			var invocationChildren []metricsAggregate
			for _, i := range c.Roles[n].Invocations {
				invocationChildren = append(invocationChildren, i.Metrics)
			}
			c.Roles[n].Metrics, err = rollupMetrics(invocationChildren)
			if err != nil {
				return nil, err
			}
			roleChildren = append(roleChildren, c.Roles[n].Metrics)
		}
		c.Metrics, err = rollupMetrics(roleChildren)
		if err != nil {
			return nil, err
		}
		cycleChildren = append(cycleChildren, c.Metrics)
		doc.Cycles = append(doc.Cycles, c)
	}
	doc.Totals, err = rollupMetrics(cycleChildren)
	if err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := schemas.Validate("metrics.schema.json", raw); err != nil {
		return nil, fmt.Errorf("metrics schema: %w", err)
	}
	return append(raw, '\n'), nil
}
