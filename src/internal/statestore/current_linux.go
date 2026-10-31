package statestore

import "context"

// Nil relationships stay nil. A missing run/actor returns sql.ErrNoRows; no
// current entity is inferred from row order, ordinals, timestamps or events.
type ActiveActor struct {
	RunID          string
	Role           string
	InvocationID   *string
	OwnershipEpoch int64
	Revision       int64
	ActivatedAt    string
}
type CurrentSemanticState struct {
	RunID       string
	LeadStateID *string
	Revision    int64
	UpdatedAt   string
}
type CurrentInvocation struct {
	RunID           string
	Cycle           int
	ExecutionID     string
	AttemptID       string
	InvocationID    string
	Ordinal         int
	Role            string
	Purpose         string
	ContentIdentity *string
	Provider        string
	Model           string
	ReasoningEffort string
}
type CurrentAttempt struct {
	RunID       string
	Cycle       int
	ExecutionID string
	AttemptID   string
	Ordinal     int
}
type CurrentExecution struct {
	RunID       string
	Cycle       int
	ExecutionID string
	Role        string
	Purpose     string
}

func (s *StateStore) GetActiveActor(ctx context.Context, runID string) (ActiveActor, error) {
	if err := requireIDs(struct{name, value string}{"run_id", runID}); err != nil { return ActiveActor{}, err }
	var a ActiveActor
	err := s.db.QueryRowContext(ctx, "SELECT run_id,active_role,active_invocation_id,ownership_epoch,revision,activated_at FROM run_active_actor WHERE run_id=?", runID).Scan(&a.RunID, &a.Role, &a.InvocationID, &a.OwnershipEpoch, &a.Revision, &a.ActivatedAt)
	return a, err
}
func (s *StateStore) GetCurrentSemanticState(ctx context.Context, runID string) (CurrentSemanticState, error) {
	if err := requireIDs(struct{name, value string}{"run_id", runID}); err != nil { return CurrentSemanticState{}, err }
	var state CurrentSemanticState
	err := s.db.QueryRowContext(ctx, "SELECT run_id,lead_state_id,revision,updated_at FROM run_current_semantic_state WHERE run_id=?", runID).Scan(&state.RunID, &state.LeadStateID, &state.Revision, &state.UpdatedAt)
	return state, err
}
func (s *StateStore) GetCurrentInvocation(ctx context.Context, runID string) (CurrentInvocation, error) {
	if err := requireIDs(struct{name, value string}{"run_id", runID}); err != nil { return CurrentInvocation{}, err }
	var i CurrentInvocation
	err := s.db.QueryRowContext(ctx, `SELECT i.run_id,i.cycle_no,i.execution_id,i.attempt_id,i.invocation_id,i.invocation_ordinal,i.role,i.purpose,i.content_identity,i.provider,i.model,i.reasoning_effort
 FROM run_active_actor a JOIN invocations i ON i.invocation_id=a.active_invocation_id AND i.run_id=a.run_id WHERE a.run_id=?`, runID).Scan(&i.RunID, &i.Cycle, &i.ExecutionID, &i.AttemptID, &i.InvocationID, &i.Ordinal, &i.Role, &i.Purpose, &i.ContentIdentity, &i.Provider, &i.Model, &i.ReasoningEffort)
	return i, err
}
func (s *StateStore) GetCurrentAttempt(ctx context.Context, runID string) (CurrentAttempt, error) {
	if err := requireIDs(struct{name, value string}{"run_id", runID}); err != nil { return CurrentAttempt{}, err }
	var a CurrentAttempt
	err := s.db.QueryRowContext(ctx, `SELECT t.run_id,t.cycle_no,t.execution_id,t.attempt_id,t.attempt_ordinal
 FROM run_active_actor a JOIN invocations i ON i.invocation_id=a.active_invocation_id AND i.run_id=a.run_id
 JOIN attempts t ON t.attempt_id=i.attempt_id AND t.execution_id=i.execution_id AND t.run_id=i.run_id WHERE a.run_id=?`, runID).Scan(&a.RunID, &a.Cycle, &a.ExecutionID, &a.AttemptID, &a.Ordinal)
	return a, err
}
func (s *StateStore) GetCurrentExecution(ctx context.Context, runID string) (CurrentExecution, error) {
	if err := requireIDs(struct{name, value string}{"run_id", runID}); err != nil { return CurrentExecution{}, err }
	var e CurrentExecution
	err := s.db.QueryRowContext(ctx, `SELECT e.run_id,e.cycle_no,e.execution_id,e.role,e.purpose
 FROM run_active_actor a JOIN invocations i ON i.invocation_id=a.active_invocation_id AND i.run_id=a.run_id
 JOIN attempts t ON t.attempt_id=i.attempt_id AND t.execution_id=i.execution_id AND t.run_id=i.run_id
 JOIN executions e ON e.execution_id=t.execution_id AND e.run_id=t.run_id WHERE a.run_id=?`, runID).Scan(&e.RunID, &e.Cycle, &e.ExecutionID, &e.Role, &e.Purpose)
	return e, err
}
