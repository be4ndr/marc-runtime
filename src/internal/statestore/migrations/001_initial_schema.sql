-- Canonical v0.1 schema. Pre-release databases are disposable; no upgrade path.

CREATE TABLE admission_agent_token_limits (
 admission_id TEXT PRIMARY KEY REFERENCES invocation_admissions(admission_id),
 initial_soft_limit INTEGER NOT NULL CHECK(initial_soft_limit>=0),
 initial_hard_limit INTEGER NOT NULL CHECK(initial_hard_limit>=initial_soft_limit)
) STRICT;

CREATE TABLE admission_contingency_authorizations (
 admission_id TEXT PRIMARY KEY REFERENCES invocation_admissions(admission_id),
 reason TEXT NOT NULL CHECK(length(trim(reason))>0)
) STRICT;

CREATE TABLE admission_controller_release_bounds (
    evidence_id TEXT NOT NULL REFERENCES admission_controller_release_evidence(evidence_id),
    dimension TEXT NOT NULL CHECK(dimension IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')),
    amount INTEGER NOT NULL CHECK(amount>=0),
    PRIMARY KEY(evidence_id,dimension)
) STRICT;

CREATE TABLE admission_controller_release_evidence (
    evidence_id TEXT PRIMARY KEY CHECK(length(evidence_id)>0),
    claim_id TEXT NOT NULL REFERENCES admission_dispatch_claims(claim_id),
    released_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64)
) STRICT;

CREATE TABLE admission_dispatch_bounds (
    claim_id TEXT NOT NULL REFERENCES admission_dispatch_claims(claim_id),
    dimension TEXT NOT NULL CHECK(dimension IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')),
    amount INTEGER NOT NULL CHECK(amount>=0),
    PRIMARY KEY(claim_id,dimension)
) STRICT;

CREATE TABLE admission_dispatch_claims (
    claim_id TEXT PRIMARY KEY CHECK(length(claim_id)>0),
    admission_id TEXT NOT NULL REFERENCES invocation_admissions(admission_id),
    provider_call_id TEXT UNIQUE REFERENCES provider_calls(provider_call_id),
    claim_ordinal INTEGER NOT NULL CHECK(claim_ordinal>0),
    claimed_at TEXT NOT NULL,
    contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64), completion_only INTEGER NOT NULL DEFAULT 0 CHECK(completion_only IN (0,1)),
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    UNIQUE(admission_id,claim_ordinal)
) STRICT;

CREATE TABLE admission_grants (
    admission_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    allocation_id TEXT NOT NULL REFERENCES resource_allocations(allocation_id),
    PRIMARY KEY(admission_id,resource_type,allocation_id),
    FOREIGN KEY(admission_id,resource_type) REFERENCES admission_resources(admission_id,resource_type)
) STRICT;

CREATE TABLE admission_non_dispatch_proofs (
    proof_id TEXT PRIMARY KEY CHECK(length(proof_id)>0),
    admission_id TEXT NOT NULL UNIQUE REFERENCES invocation_admissions(admission_id),
    assertion TEXT NOT NULL CHECK(assertion='NEVER_RELEASED'),
    sealed_at TEXT NOT NULL
) STRICT;

CREATE TABLE admission_resources (
 admission_id TEXT PRIMARY KEY REFERENCES invocation_admissions(admission_id),
 resource_type TEXT NOT NULL CHECK(resource_type='WORK'),
 pool_id TEXT NOT NULL REFERENCES resource_pools(pool_id),
 initial_limit INTEGER NOT NULL CHECK(initial_limit>=0),
 UNIQUE(admission_id,resource_type)
) STRICT;

CREATE TABLE admission_token_topup_denials (
 decision_id TEXT PRIMARY KEY CHECK(length(trim(decision_id))>0),
 admission_id TEXT NOT NULL REFERENCES invocation_admissions(admission_id),
 hard_limit INTEGER NOT NULL CHECK(hard_limit>=0),
 reason TEXT NOT NULL CHECK(length(trim(reason))>0),
 decided_at TEXT NOT NULL
) STRICT;

CREATE TABLE attempts (
    attempt_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal >= 1),
    status TEXT NOT NULL CHECK (length(status) > 0),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    UNIQUE (execution_id, attempt_ordinal),
    UNIQUE (attempt_id, execution_id, run_id, cycle_no),
    UNIQUE (attempt_id, execution_id, run_id),
    UNIQUE (attempt_id, run_id),
    FOREIGN KEY (execution_id, run_id, cycle_no) REFERENCES executions(execution_id, run_id, cycle_no)
) STRICT;

CREATE TABLE authority_proof_dependencies (
 proof_id TEXT NOT NULL REFERENCES authority_proofs(proof_id),
 invocation_id TEXT REFERENCES invocations(invocation_id),
 result_id TEXT REFERENCES role_results(result_id),
 lead_state_id TEXT REFERENCES lead_states(lead_state_id),
 provider_call_id TEXT REFERENCES provider_calls(provider_call_id),
 receipt_id TEXT REFERENCES usage_receipts(receipt_id),
 admission_id TEXT REFERENCES invocation_admissions(admission_id),
 pool_id TEXT REFERENCES resource_pools(pool_id),
 allocation_id TEXT REFERENCES resource_allocations(allocation_id),
 hard_stop_id TEXT REFERENCES run_hard_stops(stop_id),
 artifact_id TEXT REFERENCES evidence_artifacts(evidence_id),
 parent_proof_id TEXT REFERENCES authority_proofs(proof_id),
 human_disposition_id TEXT REFERENCES human_readiness_dispositions(disposition_id),
 CHECK(((invocation_id IS NOT NULL)+(result_id IS NOT NULL)+(lead_state_id IS NOT NULL)+(provider_call_id IS NOT NULL)+(receipt_id IS NOT NULL)+(admission_id IS NOT NULL)+(pool_id IS NOT NULL)+(allocation_id IS NOT NULL)+(hard_stop_id IS NOT NULL)+(artifact_id IS NOT NULL)+(parent_proof_id IS NOT NULL)+(human_disposition_id IS NOT NULL))=1)
) STRICT;

CREATE TABLE authority_proofs (
 proof_id TEXT PRIMARY KEY CHECK(length(proof_id)>0),
 run_id TEXT NOT NULL REFERENCES runs(run_id),
 kind TEXT NOT NULL CHECK(kind IN ('RECOVERY','CONTROLLER_VALIDATION','REVIEW_ADMISSION','TERMINAL')),
 invocation_id TEXT NOT NULL REFERENCES invocations(invocation_id),
 content_identity TEXT NOT NULL CHECK(length(content_identity)>0),
 verdict TEXT NOT NULL,
 evidence_id TEXT NOT NULL UNIQUE REFERENCES evidence_artifacts(evidence_id),
 request_digest TEXT NOT NULL CHECK(length(request_digest)=64),
 source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
 sealed_at TEXT NOT NULL,
 FOREIGN KEY(invocation_id,run_id) REFERENCES invocations(invocation_id,run_id),
 CHECK((kind='RECOVERY' AND verdict IN ('RECOVERABLE','UNCONFIRMED'))
 OR (kind='CONTROLLER_VALIDATION' AND verdict IN ('VALIDATED','REJECTED'))
 OR (kind='REVIEW_ADMISSION' AND verdict IN ('ELIGIBLE','BLOCKED'))
 OR (kind='TERMINAL' AND verdict IN ('SUPPORTS_READINESS','BLOCKED')))
) STRICT;

CREATE TABLE cycles (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    cycle_no INTEGER NOT NULL CHECK (cycle_no >= 1),
    status TEXT NOT NULL CHECK (length(status) > 0),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    PRIMARY KEY (run_id, cycle_no)
) STRICT;

CREATE TABLE developer_blocker_evidence (
    result_id TEXT NOT NULL REFERENCES developer_blockers(result_id),
    evidence_id TEXT NOT NULL REFERENCES evidence_artifacts(evidence_id),
    location_path TEXT,
    start_line INTEGER CHECK (start_line >= 1),
    end_line INTEGER,
    PRIMARY KEY (result_id, evidence_id),
    CHECK (end_line IS NULL OR (start_line IS NOT NULL AND end_line >= start_line)),
    CHECK (start_line IS NULL OR location_path IS NOT NULL)
) STRICT;

CREATE TABLE developer_blockers (
    result_id TEXT PRIMARY KEY REFERENCES role_results(result_id),
    summary TEXT NOT NULL
) STRICT;

CREATE TABLE developer_remaining_work (
    result_id TEXT NOT NULL REFERENCES role_results(result_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    summary TEXT NOT NULL,
    PRIMARY KEY (result_id, ordinal)
) STRICT;

CREATE TABLE events (
    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    event_type TEXT NOT NULL CHECK (length(event_type) > 0),
    cycle_no INTEGER,
    execution_id TEXT REFERENCES executions(execution_id),
    attempt_id TEXT REFERENCES attempts(attempt_id),
    invocation_id TEXT REFERENCES invocations(invocation_id),
    actor_role TEXT NOT NULL CHECK (actor_role IN ('ORCHESTRATOR','LEAD','DEVELOPER','QA','REVIEWER')),
    occurred_at TEXT NOT NULL,
    details_json TEXT CHECK (details_json IS NULL OR json_valid(details_json)),
    FOREIGN KEY (run_id, cycle_no) REFERENCES cycles(run_id, cycle_no),
    FOREIGN KEY (execution_id, run_id) REFERENCES executions(execution_id, run_id),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempts(attempt_id, run_id),
    FOREIGN KEY (invocation_id, run_id) REFERENCES invocations(invocation_id, run_id),
    FOREIGN KEY (execution_id, run_id, cycle_no) REFERENCES executions(execution_id, run_id, cycle_no),
    FOREIGN KEY (attempt_id, execution_id, run_id, cycle_no) REFERENCES attempts(attempt_id, execution_id, run_id, cycle_no),
    FOREIGN KEY (invocation_id, attempt_id, execution_id, run_id, cycle_no)
        REFERENCES invocations(invocation_id, attempt_id, execution_id, run_id, cycle_no)
) STRICT;

CREATE TABLE evidence_artifacts (
    evidence_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    storage_kind TEXT NOT NULL CHECK (storage_kind IN ('sqlite','file')),
    sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    inline_blob BLOB,
    external_path TEXT,
    created_at TEXT NOT NULL,
    CHECK ((evidence_id GLOB 'sealed-v1-authority-proof-*') = (kind='authority_proof')),
    CHECK ((evidence_id GLOB 'sealed-v1-human-readiness-*') = (kind='human_readiness_disposition')),
    CHECK ((storage_kind = 'sqlite' AND inline_blob IS NOT NULL AND external_path IS NULL AND length(inline_blob) = size_bytes)
        OR (storage_kind = 'file' AND inline_blob IS NULL AND external_path IS NOT NULL AND length(external_path) > 0))
) STRICT;

CREATE TABLE execution_admission_deadlines (
    execution_id TEXT PRIMARY KEY REFERENCES executions(execution_id),
    run_id TEXT NOT NULL REFERENCES run_admission_policy(run_id),
    hard_deadline TEXT NOT NULL,
    FOREIGN KEY(execution_id,run_id) REFERENCES executions(execution_id,run_id)
) STRICT;

CREATE TABLE executions (
    execution_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('ORCHESTRATOR','LEAD','DEVELOPER','QA','REVIEWER')),
    purpose TEXT NOT NULL CHECK (purpose IN ('LEAD','IMPLEMENT','REMEDIATE','FINALIZE_IMPLEMENTATION','QA','REVIEW','FINALIZE_REVIEW','RECOVER')),
    status TEXT NOT NULL CHECK (length(status) > 0),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    UNIQUE (execution_id, run_id, cycle_no),
    UNIQUE (execution_id, run_id),
    FOREIGN KEY (run_id, cycle_no) REFERENCES cycles(run_id, cycle_no)
) STRICT;

CREATE TABLE finding_occurrence_evidence (
    occurrence_id INTEGER NOT NULL REFERENCES finding_occurrences(occurrence_id),
    evidence_id TEXT NOT NULL REFERENCES evidence_artifacts(evidence_id),
    location_path TEXT,
    start_line INTEGER CHECK (start_line >= 1),
    end_line INTEGER,
    PRIMARY KEY (occurrence_id, evidence_id),
    CHECK (end_line IS NULL OR (start_line IS NOT NULL AND end_line >= start_line)),
    CHECK (start_line IS NULL OR location_path IS NOT NULL)
) STRICT;

CREATE TABLE finding_occurrence_locations (
    occurrence_id INTEGER NOT NULL REFERENCES finding_occurrences(occurrence_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    path TEXT NOT NULL CHECK (length(path) > 0),
    start_line INTEGER CHECK (start_line >= 1),
    end_line INTEGER,
    PRIMARY KEY (occurrence_id, ordinal),
    CHECK (end_line IS NULL OR (start_line IS NOT NULL AND end_line >= start_line))
) STRICT;

CREATE TABLE finding_occurrences (
    occurrence_id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    finding_id TEXT NOT NULL,
    result_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE (run_id, finding_id, result_id),
    FOREIGN KEY (run_id, finding_id) REFERENCES findings(run_id, finding_id),
    FOREIGN KEY (result_id, run_id) REFERENCES role_results(result_id, run_id)
) STRICT;

CREATE TABLE findings (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    finding_id TEXT NOT NULL,
    finding_ordinal INTEGER NOT NULL CHECK (finding_ordinal >= 1),
    origin_cycle INTEGER NOT NULL,
    origin_result_id TEXT NOT NULL,
    blocking INTEGER NOT NULL CHECK (blocking IN (0,1)),
    summary TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, finding_id),
    UNIQUE (run_id, finding_ordinal),
    FOREIGN KEY (origin_result_id, run_id, origin_cycle) REFERENCES role_results(result_id, run_id, cycle_no)
) STRICT;

CREATE TABLE hard_stop_decisions (
    stop_id TEXT NOT NULL REFERENCES run_hard_stops(stop_id),
    ordinal INTEGER NOT NULL CHECK(ordinal IN (0,1)),
    classification TEXT NOT NULL CHECK(classification IN ('PENDING','BOUNDED_TERMINAL_OVERRUN','CONTROLLER_ADMISSION_BUG','OBSERVED_DISPATCH_BOUND_EXCEEDED')),
    decided_at TEXT NOT NULL,
    CHECK((ordinal=0 AND classification='PENDING') OR (ordinal=1 AND classification<>'PENDING')),
    PRIMARY KEY(stop_id,ordinal)
) STRICT;

CREATE TABLE hard_stop_usage_evidence (
    stop_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    provider_call_id TEXT NOT NULL REFERENCES provider_calls(provider_call_id),
    resource_type TEXT NOT NULL CHECK(resource_type IN ('WORK','SPEND','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')),
    usage_revision INTEGER NOT NULL CHECK(usage_revision>=0),
    receipt_id TEXT REFERENCES usage_receipts(receipt_id),
    amount INTEGER CHECK(amount>=0),
    knowledge TEXT NOT NULL CHECK(knowledge IN ('unknown','partial','final')),
    claim_id TEXT REFERENCES admission_dispatch_claims(claim_id),
    dispatch_bound INTEGER CHECK(dispatch_bound>=0),
    PRIMARY KEY(stop_id,ordinal,provider_call_id,resource_type),
    FOREIGN KEY(stop_id,ordinal) REFERENCES hard_stop_decisions(stop_id,ordinal)
) STRICT;

CREATE TABLE human_disposition_dependencies (
 disposition_id TEXT NOT NULL REFERENCES human_readiness_dispositions(disposition_id),
 evidence_id TEXT NOT NULL REFERENCES evidence_artifacts(evidence_id),
 PRIMARY KEY(disposition_id,evidence_id)
) STRICT;

CREATE TABLE human_readiness_dispositions (
 disposition_id TEXT PRIMARY KEY CHECK(length(disposition_id)>0),
 run_id TEXT NOT NULL REFERENCES runs(run_id),
 stop_id TEXT NOT NULL REFERENCES run_hard_stops(stop_id),
 classification TEXT NOT NULL CHECK(classification IN ('CONTROLLER_ADMISSION_BUG','OBSERVED_DISPATCH_BOUND_EXCEEDED')),
 content_identity TEXT NOT NULL CHECK(length(content_identity)>0),
 interaction_id TEXT NOT NULL UNIQUE CHECK(length(interaction_id)>0),
 decision TEXT NOT NULL CHECK(decision IN ('ALLOW_TERMINAL_EVALUATION','DENY_TERMINAL_EVALUATION')),
 evidence_id TEXT NOT NULL UNIQUE REFERENCES evidence_artifacts(evidence_id),
 request_digest TEXT NOT NULL CHECK(length(request_digest)=64),
 recorded_at TEXT NOT NULL,
 invocation_id TEXT NOT NULL REFERENCES invocations(invocation_id),
 owner_invocation_id TEXT REFERENCES invocations(invocation_id),
 FOREIGN KEY(invocation_id,run_id) REFERENCES invocations(invocation_id,run_id),
 FOREIGN KEY(owner_invocation_id,run_id) REFERENCES invocations(invocation_id,run_id),
 UNIQUE(stop_id,content_identity)
) STRICT;

CREATE TABLE invocation_admissions (
    admission_id TEXT PRIMARY KEY CHECK(length(admission_id)>0),
    run_id TEXT NOT NULL REFERENCES run_admission_policy(run_id),
    invocation_id TEXT NOT NULL UNIQUE REFERENCES invocations(invocation_id),
    execution_id TEXT NOT NULL REFERENCES execution_admission_deadlines(execution_id),
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    cycle_no INTEGER NOT NULL CHECK(cycle_no>0),
    role TEXT NOT NULL,
    purpose TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('NORMAL','CONTAINMENT')),
    content_identity TEXT,
    ownership_epoch INTEGER NOT NULL CHECK(ownership_epoch>0),
    actor_revision INTEGER NOT NULL CHECK(actor_revision>=0),
    actor_role TEXT NOT NULL,
    actor_invocation_id TEXT REFERENCES invocations(invocation_id),
    actor_content_identity TEXT,
    semantic_revision INTEGER NOT NULL CHECK(semantic_revision>=0),
    run_revision INTEGER NOT NULL CHECK(run_revision>=0),
    execution_revision INTEGER NOT NULL CHECK(execution_revision>=0),
    attempt_revision INTEGER NOT NULL CHECK(attempt_revision>=0),
    invocation_revision INTEGER NOT NULL CHECK(invocation_revision>=0),
    admitted_at TEXT NOT NULL,
    contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64),
    FOREIGN KEY(invocation_id,attempt_id,execution_id,run_id,cycle_no)
        REFERENCES invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no)
) STRICT;

CREATE TABLE invocations (
    invocation_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    invocation_ordinal INTEGER NOT NULL CHECK (invocation_ordinal >= 1),
    role TEXT NOT NULL CHECK (role IN ('ORCHESTRATOR','LEAD','DEVELOPER','QA','REVIEWER')),
    purpose TEXT NOT NULL CHECK (purpose IN ('LEAD','IMPLEMENT','REMEDIATE','FINALIZE_IMPLEMENTATION','QA','REVIEW','FINALIZE_REVIEW','RECOVER')),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    status TEXT NOT NULL CHECK (length(status) > 0),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    content_identity TEXT,
    ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 1),
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    UNIQUE (attempt_id, invocation_ordinal),
    UNIQUE (invocation_id, attempt_id, execution_id, run_id, cycle_no, role),
    UNIQUE (invocation_id, attempt_id, execution_id, run_id, cycle_no),
    UNIQUE (invocation_id, run_id, cycle_no),
    UNIQUE (invocation_id, run_id, role),
    UNIQUE (invocation_id, run_id),
    UNIQUE (invocation_id, run_id, role, ownership_epoch),
    FOREIGN KEY (attempt_id, execution_id, run_id, cycle_no) REFERENCES attempts(attempt_id, execution_id, run_id, cycle_no)
) STRICT;

CREATE TABLE lead_acceptance_criteria (
    lead_state_id TEXT NOT NULL REFERENCES lead_states(lead_state_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    criterion TEXT NOT NULL CHECK (length(criterion) > 0),
    PRIMARY KEY (lead_state_id, ordinal)
) STRICT;

CREATE TABLE lead_finding_dispositions (
    lead_state_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    finding_id TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('open','resolved')),
    resolution_ref TEXT REFERENCES evidence_artifacts(evidence_id),
    PRIMARY KEY (lead_state_id, finding_id),
    FOREIGN KEY (lead_state_id, run_id) REFERENCES lead_states(lead_state_id, run_id),
    FOREIGN KEY (run_id, finding_id) REFERENCES findings(run_id, finding_id),
    CHECK ((disposition = 'open' AND resolution_ref IS NULL) OR (disposition = 'resolved' AND resolution_ref IS NOT NULL))
) STRICT;

CREATE TABLE lead_finding_sources (
    lead_state_id TEXT NOT NULL,
    finding_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_result_id TEXT NOT NULL,
    PRIMARY KEY (lead_state_id, finding_id),
    FOREIGN KEY (lead_state_id, finding_id) REFERENCES lead_finding_dispositions(lead_state_id, finding_id),
    FOREIGN KEY (lead_state_id, run_id) REFERENCES lead_states(lead_state_id, run_id),
    FOREIGN KEY (source_result_id, run_id) REFERENCES role_results(result_id, run_id)
) STRICT;

CREATE TABLE lead_state_bindings (
    lead_state_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    content_identity TEXT NOT NULL CHECK (length(content_identity) > 0),
    qa_result_id TEXT,
    review_result_id TEXT,
    FOREIGN KEY (lead_state_id, run_id) REFERENCES lead_states(lead_state_id, run_id),
    FOREIGN KEY (qa_result_id, run_id) REFERENCES role_results(result_id, run_id),
    FOREIGN KEY (review_result_id, run_id) REFERENCES role_results(result_id, run_id)
) STRICT;

CREATE TABLE lead_states (
    lead_state_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    producer_invocation_id TEXT NOT NULL,
    semantic_status TEXT NOT NULL CHECK (semantic_status IN ('active','blocked','approved')),
    qa_status TEXT NOT NULL CHECK (qa_status IN ('not_run','passed','failed','inconclusive')),
    review_status TEXT NOT NULL CHECK (review_status IN ('not_run','approved','changes_required','inconclusive')),
    goal TEXT NOT NULL CHECK (length(goal) > 0),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    created_at TEXT NOT NULL,
    CHECK (semantic_status <> 'approved' OR (qa_status <> 'inconclusive' AND review_status <> 'inconclusive')),
    UNIQUE (lead_state_id, run_id, cycle_no),
    UNIQUE (lead_state_id, run_id),
    FOREIGN KEY (run_id, cycle_no) REFERENCES cycles(run_id, cycle_no),
    FOREIGN KEY (producer_invocation_id, run_id, cycle_no) REFERENCES invocations(invocation_id, run_id, cycle_no)
) STRICT;

CREATE TABLE next_action_findings (
    action_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    finding_id TEXT NOT NULL,
    PRIMARY KEY (action_id, finding_id),
    FOREIGN KEY (action_id, run_id) REFERENCES next_actions(action_id, run_id),
    FOREIGN KEY (run_id, finding_id) REFERENCES findings(run_id, finding_id)
) STRICT;

CREATE TABLE next_action_producers (
    action_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    producer_invocation_id TEXT NOT NULL,
    FOREIGN KEY (action_id, run_id) REFERENCES next_actions(action_id, run_id),
    FOREIGN KEY (producer_invocation_id, run_id) REFERENCES invocations(invocation_id, run_id)
) STRICT;

CREATE TABLE next_actions (
    action_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    lead_state_id TEXT NOT NULL,
    target_role TEXT NOT NULL CHECK (target_role IN ('DEVELOPER','QA','REVIEWER')),
    purpose TEXT NOT NULL,
    intent TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (action_id, run_id),
    FOREIGN KEY (lead_state_id, run_id, cycle_no) REFERENCES lead_states(lead_state_id, run_id, cycle_no),
    CHECK ((target_role = 'DEVELOPER' AND purpose IN ('IMPLEMENT','REMEDIATE','FINALIZE_IMPLEMENTATION'))
        OR (target_role = 'QA' AND purpose = 'QA')
        OR (target_role = 'REVIEWER' AND purpose IN ('REVIEW','FINALIZE_REVIEW')))
) STRICT;

CREATE TABLE provider_call_usage (
    provider_call_id TEXT PRIMARY KEY REFERENCES provider_calls(provider_call_id) ON DELETE RESTRICT,
    receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    receipt_state TEXT NOT NULL DEFAULT 'unknown' CHECK(receipt_state IN ('unknown','partial','final')),
    reported_at TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    input_amount INTEGER CHECK(input_amount>=0),
    input_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    input_state TEXT NOT NULL DEFAULT 'unknown' CHECK(input_state IN ('unknown','partial','final')),
    cache_amount INTEGER CHECK(cache_amount>=0),
    cache_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    cache_state TEXT NOT NULL DEFAULT 'unknown' CHECK(cache_state IN ('unknown','partial','final')),
    fresh_amount INTEGER CHECK(fresh_amount>=0),
    fresh_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    fresh_state TEXT NOT NULL DEFAULT 'unknown' CHECK(fresh_state IN ('unknown','partial','final')),
    output_amount INTEGER CHECK(output_amount>=0),
    output_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    output_state TEXT NOT NULL DEFAULT 'unknown' CHECK(output_state IN ('unknown','partial','final')),
    reasoning_amount INTEGER CHECK(reasoning_amount>=0),
    reasoning_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    reasoning_state TEXT NOT NULL DEFAULT 'unknown' CHECK(reasoning_state IN ('unknown','partial','final')),
    work_amount INTEGER CHECK(work_amount>=0),
    work_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    work_state TEXT NOT NULL DEFAULT 'unknown' CHECK(work_state IN ('unknown','partial','final')),
    spend_amount INTEGER CHECK(spend_amount>=0),
    spend_receipt_id TEXT REFERENCES usage_receipts(receipt_id) ON DELETE RESTRICT,
    spend_state TEXT NOT NULL DEFAULT 'unknown' CHECK(spend_state IN ('unknown','partial','final')),
    CHECK((input_state<>'unknown' OR input_amount IS NULL) AND (input_state<>'final' OR input_amount IS NOT NULL)),
    CHECK((input_amount IS NULL AND input_receipt_id IS NULL) OR (input_amount IS NOT NULL AND input_receipt_id IS NOT NULL)),
    CHECK((cache_state<>'unknown' OR cache_amount IS NULL) AND (cache_state<>'final' OR cache_amount IS NOT NULL)),
    CHECK((cache_amount IS NULL AND cache_receipt_id IS NULL) OR (cache_amount IS NOT NULL AND cache_receipt_id IS NOT NULL)),
    CHECK((fresh_state<>'unknown' OR fresh_amount IS NULL) AND (fresh_state<>'final' OR fresh_amount IS NOT NULL)),
    CHECK((fresh_amount IS NULL AND fresh_receipt_id IS NULL) OR (fresh_amount IS NOT NULL AND fresh_receipt_id IS NOT NULL)),
    CHECK((output_state<>'unknown' OR output_amount IS NULL) AND (output_state<>'final' OR output_amount IS NOT NULL)),
    CHECK((output_amount IS NULL AND output_receipt_id IS NULL) OR (output_amount IS NOT NULL AND output_receipt_id IS NOT NULL)),
    CHECK((reasoning_state<>'unknown' OR reasoning_amount IS NULL) AND (reasoning_state<>'final' OR reasoning_amount IS NOT NULL)),
    CHECK((reasoning_amount IS NULL AND reasoning_receipt_id IS NULL) OR (reasoning_amount IS NOT NULL AND reasoning_receipt_id IS NOT NULL)),
    CHECK((work_state<>'unknown' OR work_amount IS NULL) AND (work_state<>'final' OR work_amount IS NOT NULL)),
    CHECK((work_amount IS NULL AND work_receipt_id IS NULL) OR (work_amount IS NOT NULL AND work_receipt_id IS NOT NULL)),
    CHECK((spend_state<>'unknown' OR spend_amount IS NULL) AND (spend_state<>'final' OR spend_amount IS NOT NULL)),
    CHECK((spend_amount IS NULL AND spend_receipt_id IS NULL) OR (spend_amount IS NOT NULL AND spend_receipt_id IS NOT NULL)),
    CHECK((receipt_id IS NULL AND reported_at IS NULL AND revision=0 AND receipt_state='unknown')
        OR (receipt_id IS NOT NULL AND reported_at IS NOT NULL AND revision>0))
) STRICT;

CREATE TABLE provider_calls (
    provider_call_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL REFERENCES invocations(invocation_id) ON DELETE RESTRICT,
    call_ordinal INTEGER NOT NULL CHECK(call_ordinal>0),
    provider TEXT NOT NULL CHECK(length(trim(provider))>0),
    model TEXT NOT NULL CHECK(length(trim(model))>0),
    reasoning_effort TEXT NOT NULL,
    started_at TEXT NOT NULL, duration_ns INTEGER
    CHECK(duration_ns IS NULL OR (duration_ns>=0 AND duration_ns<9223372036854775807)),
    UNIQUE(invocation_id,call_ordinal)
) STRICT;

CREATE TABLE qa_check_evidence (
    result_id TEXT NOT NULL,
    check_id TEXT NOT NULL,
    evidence_id TEXT NOT NULL REFERENCES evidence_artifacts(evidence_id),
    location_path TEXT,
    start_line INTEGER CHECK (start_line >= 1),
    end_line INTEGER,
    PRIMARY KEY (result_id, check_id, evidence_id),
    FOREIGN KEY (result_id, check_id) REFERENCES qa_checks(result_id, check_id),
    CHECK (end_line IS NULL OR (start_line IS NOT NULL AND end_line >= start_line)),
    CHECK (start_line IS NULL OR location_path IS NOT NULL)
) STRICT;

CREATE TABLE qa_checks (
    result_id TEXT NOT NULL REFERENCES role_results(result_id),
    check_id TEXT NOT NULL,
    required INTEGER NOT NULL CHECK (required IN (0,1)),
    outcome TEXT NOT NULL CHECK (length(outcome) > 0),
    summary TEXT NOT NULL,
    PRIMARY KEY (result_id, check_id)
) STRICT;

CREATE TABLE resource_allocations (
    allocation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    execution_id TEXT REFERENCES executions(execution_id),
    attempt_id TEXT REFERENCES attempts(attempt_id),
    resource_type TEXT NOT NULL CHECK (resource_type='WORK'),
    amount INTEGER NOT NULL CHECK (amount >= 0),
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    pool_id TEXT NOT NULL REFERENCES resource_pools(pool_id),
    request_id TEXT REFERENCES resource_requests(request_id),
    FOREIGN KEY (execution_id, run_id) REFERENCES executions(execution_id, run_id),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempts(attempt_id, run_id),
    FOREIGN KEY (attempt_id, execution_id, run_id) REFERENCES attempts(attempt_id, execution_id, run_id)
) STRICT;

CREATE TABLE resource_pool_eligibility (
    pool_id TEXT NOT NULL REFERENCES resource_pools(pool_id),
    role TEXT NOT NULL,
    purpose TEXT NOT NULL,
    PRIMARY KEY (pool_id,role,purpose),
    CHECK ((role='ORCHESTRATOR' AND purpose='RECOVER')
        OR (role='LEAD' AND purpose='LEAD')
        OR (role='DEVELOPER' AND purpose IN ('IMPLEMENT','REMEDIATE','FINALIZE_IMPLEMENTATION'))
        OR (role='QA' AND purpose='QA')
        OR (role='REVIEWER' AND purpose IN ('REVIEW','FINALIZE_REVIEW')))
) STRICT;

CREATE TABLE resource_pools (
    pool_id TEXT PRIMARY KEY CHECK (length(pool_id) > 0),
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    scope TEXT NOT NULL CHECK (scope='RUN'),
    execution_id TEXT,
    attempt_id TEXT,
    resource_type TEXT NOT NULL CHECK (resource_type='WORK'),
    created_at TEXT NOT NULL,
    CHECK (execution_id IS NULL AND attempt_id IS NULL),
    UNIQUE(run_id),
    FOREIGN KEY (execution_id,run_id) REFERENCES executions(execution_id,run_id),
    FOREIGN KEY (attempt_id,execution_id,run_id) REFERENCES attempts(attempt_id,execution_id,run_id)
) STRICT;

CREATE TABLE resource_request_amounts (
    request_id TEXT NOT NULL REFERENCES resource_requests(request_id),
    resource_type TEXT NOT NULL CHECK (resource_type='WORK'),
    requested_amount INTEGER NOT NULL CHECK (requested_amount > 0),
    PRIMARY KEY (request_id, resource_type)
) STRICT;

CREATE TABLE resource_request_sources (
    request_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    lead_state_id TEXT NOT NULL,
    source_result_id TEXT NOT NULL,
    FOREIGN KEY (request_id, run_id) REFERENCES resource_requests(request_id, run_id),
    FOREIGN KEY (lead_state_id, run_id) REFERENCES lead_states(lead_state_id, run_id),
    FOREIGN KEY (source_result_id, run_id) REFERENCES role_results(result_id, run_id)
) STRICT;

CREATE TABLE resource_requests (
    request_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    request_ordinal INTEGER NOT NULL CHECK (request_ordinal >= 1),
    requester_invocation_id TEXT NOT NULL,
    source_invocation_id TEXT NOT NULL,
    target_execution_id TEXT NOT NULL,
    target_attempt_id TEXT NOT NULL,
    target_role TEXT NOT NULL CHECK (target_role IN ('DEVELOPER','QA','REVIEWER')),
    target_purpose TEXT NOT NULL,
    semantic_justification TEXT NOT NULL CHECK (length(semantic_justification) > 0),
    status TEXT NOT NULL CHECK (status IN ('PENDING','GRANTED','DENIED')),
    created_at TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    UNIQUE (run_id, cycle_no, request_ordinal),
    FOREIGN KEY (requester_invocation_id, run_id, cycle_no) REFERENCES invocations(invocation_id, run_id, cycle_no),
    FOREIGN KEY (source_invocation_id, target_attempt_id, target_execution_id, run_id, cycle_no, target_role)
        REFERENCES invocations(invocation_id, attempt_id, execution_id, run_id, cycle_no, role),
    CHECK ((target_role = 'DEVELOPER' AND target_purpose IN ('IMPLEMENT','REMEDIATE','FINALIZE_IMPLEMENTATION'))
        OR (target_role = 'QA' AND target_purpose = 'QA')
        OR (target_role = 'REVIEWER' AND target_purpose IN ('REVIEW','FINALIZE_REVIEW')))
) STRICT;

CREATE TABLE role_result_evidence (
    result_id TEXT NOT NULL REFERENCES role_results(result_id),
    evidence_id TEXT NOT NULL REFERENCES evidence_artifacts(evidence_id),
    location_path TEXT,
    start_line INTEGER CHECK (start_line >= 1),
    end_line INTEGER,
    PRIMARY KEY (result_id, evidence_id),
    CHECK (end_line IS NULL OR (start_line IS NOT NULL AND end_line >= start_line)),
    CHECK (start_line IS NULL OR location_path IS NOT NULL)
) STRICT;

CREATE TABLE role_results (
    result_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    cycle_no INTEGER NOT NULL,
    result_ordinal INTEGER NOT NULL CHECK (result_ordinal >= 1),
    execution_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    invocation_id TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('DEVELOPER','QA','REVIEWER')),
    outcome TEXT NOT NULL CHECK (length(outcome) > 0),
    summary TEXT NOT NULL,
    content_identity TEXT NOT NULL,
    produced_at TEXT NOT NULL,
    UNIQUE (run_id, cycle_no, result_ordinal),
    UNIQUE (result_id, run_id),
    UNIQUE (result_id, run_id, cycle_no),
    FOREIGN KEY (run_id, cycle_no) REFERENCES cycles(run_id, cycle_no),
    FOREIGN KEY (invocation_id, attempt_id, execution_id, run_id, cycle_no, role)
        REFERENCES invocations(invocation_id, attempt_id, execution_id, run_id, cycle_no, role)
) STRICT;

CREATE TABLE run_active_actor (
    run_id TEXT PRIMARY KEY REFERENCES runs(run_id),
    active_role TEXT NOT NULL CHECK (active_role IN ('LEAD','DEVELOPER','QA','REVIEWER')),
    active_invocation_id TEXT,
    ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 1),
    activated_at TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    FOREIGN KEY (active_invocation_id, run_id, active_role, ownership_epoch) REFERENCES invocations(invocation_id, run_id, role, ownership_epoch)
) STRICT;

CREATE TABLE run_admission_policy (
    run_id TEXT PRIMARY KEY REFERENCES runs(run_id),
    policy_id TEXT NOT NULL CHECK(length(policy_id)>0),
    hard_deadline TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE run_agent_token_profiles (
 run_id TEXT NOT NULL REFERENCES run_admission_policy(run_id),
 role TEXT NOT NULL CHECK(role IN ('LEAD','DEVELOPER','QA','REVIEWER')),
 soft_limit INTEGER NOT NULL CHECK(soft_limit>=0),
 hard_limit INTEGER NOT NULL CHECK(hard_limit>=soft_limit),
 PRIMARY KEY(run_id,role)
) STRICT;

CREATE TABLE run_current_semantic_state (
    run_id TEXT PRIMARY KEY REFERENCES runs(run_id),
    lead_state_id TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (lead_state_id, run_id) REFERENCES lead_states(lead_state_id, run_id)
) STRICT;

CREATE TABLE run_hard_stops (
    stop_id TEXT PRIMARY KEY CHECK(length(stop_id)>0),
    run_id TEXT NOT NULL REFERENCES run_admission_policy(run_id),
    resource_type TEXT,
    cause TEXT NOT NULL CHECK(cause IN ('BUDGET_EXCEEDED','DISPATCH_BOUND_EXCEEDED','UNPROVEN_CONTINUATION','CONTROLLER_RELEASE_VIOLATION')),
    source_id TEXT NOT NULL,
    observed_amount INTEGER CHECK(observed_amount>=0),
    applicable_limit INTEGER CHECK(applicable_limit>=0),
    created_at TEXT NOT NULL,
    UNIQUE(run_id,cause,source_id)
) STRICT;

CREATE TABLE run_resource_budgets (
    run_id TEXT NOT NULL REFERENCES run_admission_policy(run_id),
    resource_type TEXT NOT NULL CHECK(resource_type='WORK'),
    hard_limit INTEGER NOT NULL CHECK(hard_limit>=0),
    soft_limit INTEGER CHECK(soft_limit>=0 AND soft_limit<=hard_limit),
    PRIMARY KEY(run_id,resource_type)
) STRICT;

CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('CREATED','RUNNING','BLOCKED','READY_FOR_HUMAN_MERGE','FAILED','CANCELLED')),
    current_cycle INTEGER NOT NULL CHECK (current_cycle >= 1),
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    terminal_at TEXT
) STRICT;

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version >= 1),
    applied_at TEXT NOT NULL,
    description TEXT NOT NULL,
    checksum TEXT NOT NULL CHECK (length(checksum) = 64)
) STRICT;

CREATE TABLE usage_receipts (
    receipt_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL REFERENCES invocations(invocation_id),
    provider_receipt_id TEXT,
    completeness TEXT NOT NULL CHECK (completeness IN ('unknown','partial','final')),
    input_tokens INTEGER CHECK (input_tokens >= 0),
    cached_input_tokens INTEGER CHECK (cached_input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens >= 0),
    reasoning_tokens INTEGER CHECK (reasoning_tokens >= 0),
    work_tokens INTEGER CHECK (work_tokens >= 0),
    spend_tokens INTEGER CHECK (spend_tokens >= 0),
    reported_at TEXT NOT NULL,
    accepted_at TEXT,
    provider_raw_json TEXT CHECK (provider_raw_json IS NULL OR json_valid(provider_raw_json)), provider_call_id TEXT REFERENCES provider_calls(provider_call_id) ON DELETE RESTRICT,
    CHECK (cached_input_tokens IS NULL OR input_tokens IS NULL OR cached_input_tokens <= input_tokens),
    CHECK (reasoning_tokens IS NULL OR output_tokens IS NULL OR reasoning_tokens <= output_tokens),
    CHECK (work_tokens IS NULL OR input_tokens IS NULL OR output_tokens IS NULL OR work_tokens = input_tokens + output_tokens),
    CHECK (spend_tokens IS NULL OR input_tokens IS NULL OR cached_input_tokens IS NULL OR output_tokens IS NULL
        OR spend_tokens = input_tokens - cached_input_tokens + output_tokens)
) STRICT;

CREATE INDEX admission_claims_admission ON admission_dispatch_claims(admission_id,claim_ordinal);

CREATE INDEX events_run_sequence ON events(run_id, event_seq);

CREATE INDEX executions_run_cycle_status ON executions(run_id, cycle_no, status);

CREATE INDEX finding_occurrences_result ON finding_occurrences(result_id);

CREATE INDEX hard_stops_run ON run_hard_stops(run_id);

CREATE INDEX invocation_admissions_run ON invocation_admissions(run_id);

CREATE INDEX invocations_run_cycle ON invocations(run_id, cycle_no);

CREATE INDEX invocations_status ON invocations(status);

CREATE UNIQUE INDEX provider_calls_one_active ON provider_calls(invocation_id)
    WHERE duration_ns IS NULL;

CREATE INDEX resource_allocations_pool ON resource_allocations(pool_id);

CREATE UNIQUE INDEX resource_requests_id_run ON resource_requests(request_id, run_id);

CREATE INDEX resource_requests_pending ON resource_requests(run_id, cycle_no) WHERE status = 'PENDING';

CREATE INDEX role_results_run_cycle ON role_results(run_id, cycle_no);

CREATE INDEX usage_receipts_call ON usage_receipts(provider_call_id);

CREATE INDEX usage_receipts_invocation_completeness ON usage_receipts(invocation_id, completeness);

CREATE UNIQUE INDEX usage_receipts_one_final ON usage_receipts(provider_call_id)
    WHERE completeness='final' AND accepted_at IS NOT NULL;

CREATE VIEW v_active_actor AS
    SELECT a.*, r.status AS run_status FROM run_active_actor a JOIN runs r USING (run_id);

CREATE VIEW v_cycle_summary AS
    SELECT c.*,
        (SELECT count(*) FROM executions e WHERE e.run_id = c.run_id AND e.cycle_no = c.cycle_no) AS execution_count,
        (SELECT count(*) FROM invocations i WHERE i.run_id = c.run_id AND i.cycle_no = c.cycle_no) AS invocation_count,
        (SELECT count(*) FROM role_results r WHERE r.run_id = c.run_id AND r.cycle_no = c.cycle_no) AS result_count
    FROM cycles c;

CREATE VIEW v_finding_history AS
    SELECT f.*, o.occurrence_id, o.result_id AS observed_result_id, o.observed_at
    FROM findings f LEFT JOIN finding_occurrences o USING (run_id, finding_id);

CREATE VIEW v_invocation_usage AS
    SELECT i.invocation_id, i.run_id, i.cycle_no, i.execution_id, i.attempt_id,
        i.role, i.provider, i.model, i.reasoning_effort, u.receipt_id, u.completeness,
        u.input_tokens, u.cached_input_tokens, u.output_tokens, u.reasoning_tokens,
        u.work_tokens, u.spend_tokens, u.reported_at, u.accepted_at
    FROM invocations i LEFT JOIN usage_receipts u ON u.invocation_id = i.invocation_id AND u.accepted_at IS NOT NULL;

CREATE VIEW v_pending_resource_requests AS SELECT * FROM resource_requests WHERE status = 'PENDING';

CREATE VIEW v_resource_balance AS
SELECT p.pool_id,p.run_id,p.scope,p.execution_id,p.attempt_id,p.resource_type,
 b.normal_budget AS granted,b.normal_used AS consumed,b.normal_available AS available
FROM resource_pools p JOIN v_run_token_balance b USING(run_id);

CREATE VIEW v_run_status AS SELECT * FROM runs;

CREATE VIEW v_run_token_balance AS
WITH usage AS (
 SELECT b.run_id,e.normal_budget,e.normal_budget/10 AS contingency_budget,
 COALESCE((SELECT SUM(u.work_amount) FROM provider_calls p JOIN invocations i USING(invocation_id) JOIN provider_call_usage u USING(provider_call_id)
 WHERE i.run_id=b.run_id AND NOT EXISTS(SELECT 1 FROM invocation_admissions a JOIN admission_contingency_authorizations t USING(admission_id) WHERE a.invocation_id=p.invocation_id)),0) AS normal_used,
 COALESCE((SELECT SUM(u.work_amount) FROM provider_calls p JOIN invocations i USING(invocation_id) JOIN provider_call_usage u USING(provider_call_id)
 WHERE i.run_id=b.run_id AND EXISTS(SELECT 1 FROM invocation_admissions a JOIN admission_contingency_authorizations t USING(admission_id) WHERE a.invocation_id=p.invocation_id)),0) AS terminal_used
 FROM run_resource_budgets b JOIN (
 SELECT b.run_id,(SELECT SUM(value) FROM (SELECT b.hard_limit AS value UNION ALL SELECT x.amount FROM run_budget_increases x WHERE x.run_id=b.run_id)) AS normal_budget
 FROM run_resource_budgets b WHERE b.resource_type='WORK'
 ) e USING(run_id) WHERE b.resource_type='WORK'
), balance AS (
 SELECT *,normal_budget-normal_used AS normal_available,
 terminal_used+CASE WHEN normal_used>normal_budget THEN normal_used-normal_budget ELSE 0 END AS contingency_used
 FROM usage
)
SELECT run_id,normal_budget,normal_used,normal_available,contingency_budget,contingency_used,
 contingency_budget-contingency_used AS contingency_available,
 (SELECT SUM(value) FROM (SELECT normal_budget AS value UNION ALL SELECT contingency_budget)) AS absolute_limit
FROM balance;

CREATE VIEW v_run_token_hard_stops AS
SELECT s.* FROM run_hard_stops s
WHERE (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS'))
AND NOT (s.cause='UNPROVEN_CONTINUATION' AND s.source_id LIKE 'headroom:%')
AND (s.cause<>'BUDGET_EXCEEDED' OR (
 s.observed_amount>s.applicable_limit
 AND COALESCE((SELECT SUM(e.amount) FROM hard_stop_usage_evidence e WHERE e.stop_id=s.stop_id AND e.ordinal=0 AND e.resource_type='WORK'),0)>s.applicable_limit
));

CREATE TRIGGER actor_epoch_no_decrease BEFORE UPDATE ON run_active_actor
WHEN NEW.ownership_epoch < OLD.ownership_epoch BEGIN
    SELECT RAISE(ABORT, 'ownership_epoch cannot decrease');
END;

CREATE TRIGGER actor_mutation_requires_revision BEFORE UPDATE ON run_active_actor BEGIN
    SELECT CASE WHEN OLD.revision=9223372036854775807
        THEN RAISE(ABORT,'numeric invariant violation: actor revision exhausted') END;
    SELECT CASE WHEN NEW.revision<>OLD.revision+1
        THEN RAISE(ABORT,'actor mutation requires exactly one revision increment') END;
END;

CREATE TRIGGER actor_same_owner_preserves_epoch BEFORE UPDATE ON run_active_actor
WHEN NEW.active_role IS OLD.active_role
    AND NEW.ownership_epoch != OLD.ownership_epoch BEGIN
    SELECT RAISE(ABORT, 'same owner must preserve ownership_epoch');
END;

CREATE TRIGGER actor_transfer_requires_new_epoch BEFORE UPDATE ON run_active_actor
WHEN NEW.active_role IS NOT OLD.active_role BEGIN
    SELECT CASE WHEN OLD.ownership_epoch=9223372036854775807
        THEN RAISE(ABORT,'numeric invariant violation: ownership epoch exhausted') END;
    SELECT CASE WHEN NEW.ownership_epoch<>OLD.ownership_epoch+1
        THEN RAISE(ABORT,'owner change requires exactly one epoch increment') END;
END;

CREATE TRIGGER admission_agent_token_limits_binding BEFORE INSERT ON admission_agent_token_limits BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocation_admissions a WHERE a.admission_id=NEW.admission_id AND a.kind='NORMAL'
 AND a.role IN ('LEAD','DEVELOPER','QA','REVIEWER') AND a.actor_invocation_id=a.invocation_id AND a.actor_role=a.role)
 THEN RAISE(ABORT,'agent limits require active primary admission provenance') END;
END;

CREATE TRIGGER admission_agent_token_limits_no_delete BEFORE DELETE ON admission_agent_token_limits BEGIN SELECT RAISE(ABORT,'initial agent limits are immutable'); END;

CREATE TRIGGER admission_agent_token_limits_no_update BEFORE UPDATE ON admission_agent_token_limits BEGIN SELECT RAISE(ABORT,'initial agent limits are immutable'); END;

CREATE TRIGGER admission_claim_binding BEFORE INSERT ON admission_dispatch_claims BEGIN
    SELECT CASE WHEN EXISTS(SELECT 1 FROM admission_non_dispatch_proofs WHERE admission_id=NEW.admission_id)
        OR (NEW.provider_call_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM invocation_admissions a
            JOIN provider_calls p ON p.invocation_id=a.invocation_id WHERE a.admission_id=NEW.admission_id
            AND p.provider_call_id=NEW.provider_call_id))
        THEN RAISE(ABORT,'dispatch claim conflicts with admission or sealed proof') END;
END;

CREATE TRIGGER admission_contingency_binding BEFORE INSERT ON admission_contingency_authorizations BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocation_admissions a WHERE a.admission_id=NEW.admission_id AND a.kind='CONTAINMENT' AND a.role='ORCHESTRATOR' AND a.purpose='RECOVER')
 THEN RAISE(ABORT,'contingency is Orchestrator minimal-terminal authority') END;
END;

CREATE TRIGGER admission_contingency_no_delete BEFORE DELETE ON admission_contingency_authorizations BEGIN
 SELECT RAISE(ABORT,'contingency authorization is immutable');
END;

CREATE TRIGGER admission_contingency_no_update BEFORE UPDATE ON admission_contingency_authorizations BEGIN
 SELECT RAISE(ABORT,'contingency authorization is immutable');
END;

CREATE TRIGGER admission_controller_release_bounds_no_delete BEFORE DELETE ON admission_controller_release_bounds BEGIN
    SELECT RAISE(ABORT,'controller release evidence is immutable');
END;

CREATE TRIGGER admission_controller_release_bounds_no_update BEFORE UPDATE ON admission_controller_release_bounds BEGIN
    SELECT RAISE(ABORT,'controller release evidence is immutable');
END;

CREATE TRIGGER admission_controller_release_bounds_spend_retired BEFORE INSERT ON admission_controller_release_bounds
WHEN NEW.dimension='SPEND' BEGIN SELECT RAISE(ABORT,'SPEND is informational'); END;

CREATE TRIGGER admission_controller_release_bounds_tokens_only_insert BEFORE INSERT ON admission_controller_release_bounds
WHEN NEW.dimension IS NOT NULL AND NEW.dimension NOT IN ('WORK','SPEND','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS') BEGIN
    SELECT RAISE(ABORT,'only tokens have resource bounds or stops');
END;

CREATE TRIGGER admission_controller_release_evidence_no_delete BEFORE DELETE ON admission_controller_release_evidence BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_controller_release_evidence_no_update BEFORE UPDATE ON admission_controller_release_evidence BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_dispatch_bounds_no_delete BEFORE DELETE ON admission_dispatch_bounds BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_dispatch_bounds_no_update BEFORE UPDATE ON admission_dispatch_bounds BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_dispatch_bounds_spend_retired BEFORE INSERT ON admission_dispatch_bounds
WHEN NEW.dimension='SPEND' BEGIN SELECT RAISE(ABORT,'SPEND is informational'); END;

CREATE TRIGGER admission_dispatch_bounds_tokens_only_insert BEFORE INSERT ON admission_dispatch_bounds
WHEN NEW.dimension IS NOT NULL AND NEW.dimension NOT IN ('WORK','SPEND','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS') BEGIN
    SELECT RAISE(ABORT,'only tokens have resource bounds or stops');
END;

CREATE TRIGGER admission_dispatch_claims_no_delete BEFORE DELETE ON admission_dispatch_claims BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_dispatch_claims_no_update BEFORE UPDATE ON admission_dispatch_claims BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_grants_no_contingency_insert BEFORE INSERT ON admission_grants
WHEN EXISTS(SELECT 1 FROM admission_contingency_authorizations t WHERE t.admission_id=NEW.admission_id) BEGIN
 SELECT RAISE(ABORT,'contingency authorizes only Orchestrator emergency operations');
END;

CREATE TRIGGER admission_grants_no_delete BEFORE DELETE ON admission_grants BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_grants_no_update BEFORE UPDATE ON admission_grants BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_grants_provenance BEFORE INSERT ON admission_grants BEGIN
 SELECT CASE WHEN NOT EXISTS(
  SELECT 1 FROM invocation_admissions a JOIN admission_resources r USING(admission_id)
  JOIN resource_allocations g ON g.allocation_id=NEW.allocation_id
  JOIN resource_pools p ON p.pool_id=r.pool_id
  WHERE a.admission_id=NEW.admission_id AND r.resource_type=NEW.resource_type
  AND g.resource_type=r.resource_type AND g.pool_id=r.pool_id AND g.run_id=a.run_id AND p.run_id=a.run_id
  AND EXISTS(SELECT 1 FROM resource_pool_eligibility e WHERE e.pool_id=p.pool_id AND e.role=a.role AND e.purpose=a.purpose)
  AND (g.execution_id IS NULL OR g.execution_id=a.execution_id)
  AND (g.attempt_id IS NULL OR g.attempt_id=a.attempt_id)
  AND (g.request_id IS NULL OR EXISTS(SELECT 1 FROM resource_requests q WHERE q.request_id=g.request_id
   AND q.status='PENDING' AND q.run_id=a.run_id AND q.target_execution_id=a.execution_id AND q.target_attempt_id=a.attempt_id
   AND q.target_role=a.role AND q.target_purpose=a.purpose)))
 THEN RAISE(ABORT,'grant provenance mismatch') END;
END;

CREATE TRIGGER admission_non_dispatch_proofs_no_delete BEFORE DELETE ON admission_non_dispatch_proofs BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_non_dispatch_proofs_no_update BEFORE UPDATE ON admission_non_dispatch_proofs BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_resources_no_delete BEFORE DELETE ON admission_resources BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_resources_no_update BEFORE UPDATE ON admission_resources BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER admission_token_topup_denials_no_delete BEFORE DELETE ON admission_token_topup_denials BEGIN SELECT RAISE(ABORT,'top-up denial is immutable'); END;

CREATE TRIGGER admission_token_topup_denials_no_update BEFORE UPDATE ON admission_token_topup_denials BEGIN SELECT RAISE(ABORT,'top-up denial is immutable'); END;

CREATE TRIGGER authority_dependency_sealed_inventory BEFORE INSERT ON authority_proof_dependencies BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM authority_proofs p JOIN evidence_artifacts a USING(evidence_id),json_each(a.inline_blob,'$.Dependencies') e
 WHERE p.proof_id=NEW.proof_id AND json_extract(e.value,'$.Kind')=CASE WHEN NEW.invocation_id IS NOT NULL THEN 'invocation' WHEN NEW.result_id IS NOT NULL THEN 'result' WHEN NEW.lead_state_id IS NOT NULL THEN 'lead_state' WHEN NEW.provider_call_id IS NOT NULL THEN 'provider_call' WHEN NEW.receipt_id IS NOT NULL THEN 'receipt' WHEN NEW.admission_id IS NOT NULL THEN 'admission' WHEN NEW.pool_id IS NOT NULL THEN 'pool' WHEN NEW.allocation_id IS NOT NULL THEN 'allocation' WHEN NEW.hard_stop_id IS NOT NULL THEN 'hard_stop' WHEN NEW.artifact_id IS NOT NULL THEN 'artifact' WHEN NEW.parent_proof_id IS NOT NULL THEN 'proof' WHEN NEW.human_disposition_id IS NOT NULL THEN 'human_disposition' END AND json_extract(e.value,'$.ID')=COALESCE(NEW.invocation_id,NEW.result_id,NEW.lead_state_id,NEW.provider_call_id,NEW.receipt_id,NEW.admission_id,NEW.pool_id,NEW.allocation_id,NEW.hard_stop_id,NEW.artifact_id,NEW.parent_proof_id,NEW.human_disposition_id))
 OR EXISTS(SELECT 1 FROM authority_proof_dependencies d WHERE d.proof_id=NEW.proof_id AND ((NEW.invocation_id IS NOT NULL AND d.invocation_id=NEW.invocation_id) OR (NEW.result_id IS NOT NULL AND d.result_id=NEW.result_id) OR (NEW.lead_state_id IS NOT NULL AND d.lead_state_id=NEW.lead_state_id) OR (NEW.provider_call_id IS NOT NULL AND d.provider_call_id=NEW.provider_call_id) OR (NEW.receipt_id IS NOT NULL AND d.receipt_id=NEW.receipt_id) OR (NEW.admission_id IS NOT NULL AND d.admission_id=NEW.admission_id) OR (NEW.pool_id IS NOT NULL AND d.pool_id=NEW.pool_id) OR (NEW.allocation_id IS NOT NULL AND d.allocation_id=NEW.allocation_id) OR (NEW.hard_stop_id IS NOT NULL AND d.hard_stop_id=NEW.hard_stop_id) OR (NEW.artifact_id IS NOT NULL AND d.artifact_id=NEW.artifact_id) OR (NEW.parent_proof_id IS NOT NULL AND d.parent_proof_id=NEW.parent_proof_id) OR (NEW.human_disposition_id IS NOT NULL AND d.human_disposition_id=NEW.human_disposition_id)))
 THEN RAISE(ABORT,'dependency must match sealed inventory exactly once') END;
END;

CREATE TRIGGER authority_proof_dependencies_no_delete BEFORE DELETE ON authority_proof_dependencies BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER authority_proof_dependencies_no_update BEFORE UPDATE ON authority_proof_dependencies BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER authority_proofs_no_delete BEFORE DELETE ON authority_proofs BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER authority_proofs_no_update BEFORE UPDATE ON authority_proofs BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER developer_blocker_evidence_sealed_no_delete BEFORE DELETE ON developer_blocker_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_blocker_evidence_sealed_no_insert BEFORE INSERT ON developer_blocker_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_blocker_evidence_sealed_no_update BEFORE UPDATE ON developer_blocker_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_blockers_sealed_no_delete BEFORE DELETE ON developer_blockers WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_blockers_sealed_no_insert BEFORE INSERT ON developer_blockers WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_blockers_sealed_no_update BEFORE UPDATE ON developer_blockers WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_remaining_work_sealed_no_delete BEFORE DELETE ON developer_remaining_work WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_remaining_work_sealed_no_insert BEFORE INSERT ON developer_remaining_work WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER developer_remaining_work_sealed_no_update BEFORE UPDATE ON developer_remaining_work WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER events_no_delete BEFORE DELETE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TRIGGER events_no_update BEFORE UPDATE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TRIGGER events_validate_provenance BEFORE INSERT ON events BEGIN
    SELECT CASE WHEN NEW.execution_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM executions e WHERE e.execution_id=NEW.execution_id AND e.run_id=NEW.run_id
        AND (NEW.cycle_no IS NULL OR e.cycle_no=NEW.cycle_no)
    ) THEN RAISE(ABORT,'event execution provenance mismatch') END;
    SELECT CASE WHEN NEW.attempt_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM attempts a WHERE a.attempt_id=NEW.attempt_id AND a.run_id=NEW.run_id
        AND (NEW.cycle_no IS NULL OR a.cycle_no=NEW.cycle_no)
        AND (NEW.execution_id IS NULL OR a.execution_id=NEW.execution_id)
    ) THEN RAISE(ABORT,'event attempt provenance mismatch') END;
    SELECT CASE WHEN NEW.invocation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM invocations i WHERE i.invocation_id=NEW.invocation_id AND i.run_id=NEW.run_id
        AND (NEW.cycle_no IS NULL OR i.cycle_no=NEW.cycle_no)
        AND (NEW.execution_id IS NULL OR i.execution_id=NEW.execution_id)
        AND (NEW.attempt_id IS NULL OR i.attempt_id=NEW.attempt_id)
    ) THEN RAISE(ABORT,'event invocation provenance mismatch') END;
END;

CREATE TRIGGER evidence_artifacts_sealed_no_update BEFORE UPDATE ON evidence_artifacts WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE artifact_id=OLD.evidence_id) OR EXISTS(SELECT 1 FROM authority_proofs WHERE evidence_id=OLD.evidence_id) OR EXISTS(SELECT 1 FROM human_readiness_dispositions WHERE evidence_id=OLD.evidence_id) OR EXISTS(SELECT 1 FROM human_disposition_dependencies WHERE evidence_id=OLD.evidence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency is immutable'); END;

CREATE TRIGGER execution_admission_deadlines_no_delete BEFORE DELETE ON execution_admission_deadlines BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER execution_admission_deadlines_no_update BEFORE UPDATE ON execution_admission_deadlines BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER finding_occurrence_evidence_sealed_no_delete BEFORE DELETE ON finding_occurrence_evidence WHEN EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=OLD.occurrence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrence_evidence_sealed_no_insert BEFORE INSERT ON finding_occurrence_evidence WHEN EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=NEW.occurrence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrence_evidence_sealed_no_update BEFORE UPDATE ON finding_occurrence_evidence WHEN EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=OLD.occurrence_id) OR EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=NEW.occurrence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrence_locations_sealed_no_delete BEFORE DELETE ON finding_occurrence_locations WHEN EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=OLD.occurrence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrence_locations_sealed_no_insert BEFORE INSERT ON finding_occurrence_locations WHEN EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=NEW.occurrence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrence_locations_sealed_no_update BEFORE UPDATE ON finding_occurrence_locations WHEN EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=OLD.occurrence_id) OR EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.occurrence_id=NEW.occurrence_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrences_sealed_no_delete BEFORE DELETE ON finding_occurrences WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrences_sealed_no_insert BEFORE INSERT ON finding_occurrences WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER finding_occurrences_sealed_no_update BEFORE UPDATE ON finding_occurrences WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER findings_origin_immutable BEFORE UPDATE ON findings
WHEN NEW.run_id IS NOT OLD.run_id OR NEW.finding_id IS NOT OLD.finding_id
    OR NEW.finding_ordinal IS NOT OLD.finding_ordinal OR NEW.origin_cycle IS NOT OLD.origin_cycle
    OR NEW.origin_result_id IS NOT OLD.origin_result_id BEGIN
    SELECT RAISE(ABORT, 'finding origin and identity are immutable');
END;

CREATE TRIGGER findings_sealed_no_update BEFORE UPDATE ON findings WHEN
 EXISTS(SELECT 1 FROM finding_occurrences o JOIN authority_proof_dependencies d USING(result_id) WHERE o.run_id=OLD.run_id AND o.finding_id=OLD.finding_id)
 OR EXISTS(SELECT 1 FROM lead_finding_dispositions f JOIN authority_proof_dependencies d USING(lead_state_id) WHERE f.run_id=OLD.run_id AND f.finding_id=OLD.finding_id)
 BEGIN SELECT RAISE(ABORT,'sealed finding definition is immutable'); END;

CREATE TRIGGER hard_stop_decision_sequence BEFORE INSERT ON hard_stop_decisions
WHEN NEW.ordinal=1 BEGIN
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM hard_stop_decisions WHERE stop_id=NEW.stop_id AND ordinal=0)
        THEN RAISE(ABORT,'hard stop requires pending decision before resolution') END;
END;

CREATE TRIGGER hard_stop_decisions_no_delete BEFORE DELETE ON hard_stop_decisions BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER hard_stop_decisions_no_update BEFORE UPDATE ON hard_stop_decisions BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER hard_stop_usage_evidence_no_delete BEFORE DELETE ON hard_stop_usage_evidence BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER hard_stop_usage_evidence_no_update BEFORE UPDATE ON hard_stop_usage_evidence BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER human_dependency_sealed_inventory BEFORE INSERT ON human_disposition_dependencies BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM human_readiness_dispositions h JOIN evidence_artifacts a USING(evidence_id),json_each(a.inline_blob,'$.Evidence') e
 WHERE h.disposition_id=NEW.disposition_id AND json_extract(e.value,'$.EvidenceID')=NEW.evidence_id)
 THEN RAISE(ABORT,'human dependency must match sealed evidence') END;
END;

CREATE TRIGGER human_disposition_binding BEFORE INSERT ON human_readiness_dispositions BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM run_hard_stops s JOIN hard_stop_decisions d USING(stop_id)
 WHERE s.stop_id=NEW.stop_id AND s.run_id=NEW.run_id AND d.ordinal=1 AND d.classification=NEW.classification)
 THEN RAISE(ABORT,'human disposition requires exact resolved hard stop') END;
END;

CREATE TRIGGER human_disposition_dependencies_no_delete BEFORE DELETE ON human_disposition_dependencies BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER human_disposition_dependencies_no_update BEFORE UPDATE ON human_disposition_dependencies BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER human_readiness_dispositions_no_delete BEFORE DELETE ON human_readiness_dispositions BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER human_readiness_dispositions_no_update BEFORE UPDATE ON human_readiness_dispositions BEGIN
 SELECT RAISE(ABORT,'sealed authority evidence is immutable');
END;

CREATE TRIGGER invocation_admissions_no_delete BEFORE DELETE ON invocation_admissions BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER invocation_admissions_no_update BEFORE UPDATE ON invocation_admissions BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER lead_acceptance_criteria_sealed_no_delete BEFORE DELETE ON lead_acceptance_criteria WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_acceptance_criteria_sealed_no_insert BEFORE INSERT ON lead_acceptance_criteria WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_acceptance_criteria_sealed_no_update BEFORE UPDATE ON lead_acceptance_criteria WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_finding_dispositions_sealed_no_delete BEFORE DELETE ON lead_finding_dispositions WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_finding_dispositions_sealed_no_insert BEFORE INSERT ON lead_finding_dispositions WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_finding_dispositions_sealed_no_update BEFORE UPDATE ON lead_finding_dispositions WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_finding_sources_sealed_no_delete BEFORE DELETE ON lead_finding_sources WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_finding_sources_sealed_no_insert BEFORE INSERT ON lead_finding_sources WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_finding_sources_sealed_no_update BEFORE UPDATE ON lead_finding_sources WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_state_bindings_sealed_no_delete BEFORE DELETE ON lead_state_bindings WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_state_bindings_sealed_no_insert BEFORE INSERT ON lead_state_bindings WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_state_bindings_sealed_no_update BEFORE UPDATE ON lead_state_bindings WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=NEW.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER lead_states_sealed_no_update BEFORE UPDATE ON lead_states WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE lead_state_id=OLD.lead_state_id) BEGIN SELECT RAISE(ABORT,'sealed dependency is immutable'); END;

CREATE TRIGGER provider_call_initial_usage AFTER INSERT ON provider_calls BEGIN
    INSERT INTO provider_call_usage(provider_call_id) VALUES (NEW.provider_call_id);
END;

CREATE TRIGGER provider_call_usage_initial BEFORE INSERT ON provider_call_usage BEGIN
    SELECT CASE WHEN NEW.receipt_id IS NOT NULL OR NEW.receipt_state<>'unknown' OR NEW.revision<>0
        OR NEW.input_amount IS NOT NULL OR NEW.input_receipt_id IS NOT NULL OR NEW.input_state<>'unknown'
        OR NEW.cache_amount IS NOT NULL OR NEW.cache_receipt_id IS NOT NULL OR NEW.cache_state<>'unknown'
        OR NEW.fresh_amount IS NOT NULL OR NEW.fresh_receipt_id IS NOT NULL OR NEW.fresh_state<>'unknown'
        OR NEW.output_amount IS NOT NULL OR NEW.output_receipt_id IS NOT NULL OR NEW.output_state<>'unknown'
        OR NEW.reasoning_amount IS NOT NULL OR NEW.reasoning_receipt_id IS NOT NULL OR NEW.reasoning_state<>'unknown'
        OR NEW.work_amount IS NOT NULL OR NEW.work_receipt_id IS NOT NULL OR NEW.work_state<>'unknown'
        OR NEW.spend_amount IS NOT NULL OR NEW.spend_receipt_id IS NOT NULL OR NEW.spend_state<>'unknown'
        THEN RAISE(ABORT,'ProviderCall usage must start unknown') END;
END;

CREATE TRIGGER provider_call_usage_no_delete BEFORE DELETE ON provider_call_usage BEGIN
    SELECT RAISE(ABORT,'current ProviderCall usage cannot be deleted');
END;

CREATE TRIGGER provider_call_usage_update BEFORE UPDATE ON provider_call_usage BEGIN
    SELECT CASE WHEN OLD.revision=9223372036854775807
        THEN RAISE(ABORT,'numeric invariant violation: current usage revision exhausted') END;
    SELECT CASE WHEN EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND u.input_tokens IS NOT NULL AND u.output_tokens IS NOT NULL
        AND u.input_tokens>9223372036854775807-u.output_tokens)
        THEN RAISE(ABORT,'numeric invariant violation: WORK unrepresentable') END;
    SELECT CASE WHEN NEW.provider_call_id IS NOT OLD.provider_call_id
        OR NEW.revision<>OLD.revision+1 OR OLD.receipt_state='final'
        OR NOT EXISTS (SELECT 1 FROM usage_receipts u
            WHERE u.receipt_id=NEW.receipt_id AND u.provider_call_id=NEW.provider_call_id
                AND u.accepted_at IS NOT NULL AND u.completeness=NEW.receipt_state
                AND u.reported_at=NEW.reported_at)
        THEN RAISE(ABORT,'invalid current ProviderCall usage update') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.input_tokens IS NULL AND NEW.input_amount IS OLD.input_amount
                AND NEW.input_receipt_id IS OLD.input_receipt_id)
            OR (u.input_tokens IS NOT NULL AND NEW.input_amount IS (u.input_tokens)
                AND NEW.input_receipt_id IS u.receipt_id)))
        OR NEW.input_state IS NOT CASE
            WHEN NEW.input_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.input_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.input_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current input usage evidence/knowledge') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.cached_input_tokens IS NULL AND NEW.cache_amount IS OLD.cache_amount
                AND NEW.cache_receipt_id IS OLD.cache_receipt_id)
            OR (u.cached_input_tokens IS NOT NULL AND NEW.cache_amount IS (u.cached_input_tokens)
                AND NEW.cache_receipt_id IS u.receipt_id)))
        OR NEW.cache_state IS NOT CASE
            WHEN NEW.cache_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.cache_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.cache_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current cache usage evidence/knowledge') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.input_tokens-u.cached_input_tokens IS NULL AND NEW.fresh_amount IS OLD.fresh_amount
                AND NEW.fresh_receipt_id IS OLD.fresh_receipt_id)
            OR (u.input_tokens-u.cached_input_tokens IS NOT NULL AND NEW.fresh_amount IS (u.input_tokens-u.cached_input_tokens)
                AND NEW.fresh_receipt_id IS u.receipt_id)))
        OR NEW.fresh_state IS NOT CASE
            WHEN NEW.fresh_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.fresh_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.fresh_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current fresh usage evidence/knowledge') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.output_tokens IS NULL AND NEW.output_amount IS OLD.output_amount
                AND NEW.output_receipt_id IS OLD.output_receipt_id)
            OR (u.output_tokens IS NOT NULL AND NEW.output_amount IS (u.output_tokens)
                AND NEW.output_receipt_id IS u.receipt_id)))
        OR NEW.output_state IS NOT CASE
            WHEN NEW.output_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.output_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.output_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current output usage evidence/knowledge') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.reasoning_tokens IS NULL AND NEW.reasoning_amount IS OLD.reasoning_amount
                AND NEW.reasoning_receipt_id IS OLD.reasoning_receipt_id)
            OR (u.reasoning_tokens IS NOT NULL AND NEW.reasoning_amount IS (u.reasoning_tokens)
                AND NEW.reasoning_receipt_id IS u.receipt_id)))
        OR NEW.reasoning_state IS NOT CASE
            WHEN NEW.reasoning_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.reasoning_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.reasoning_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current reasoning usage evidence/knowledge') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.input_tokens+u.output_tokens IS NULL AND NEW.work_amount IS OLD.work_amount
                AND NEW.work_receipt_id IS OLD.work_receipt_id)
            OR (u.input_tokens+u.output_tokens IS NOT NULL AND NEW.work_amount IS (u.input_tokens+u.output_tokens)
                AND NEW.work_receipt_id IS u.receipt_id)))
        OR NEW.work_state IS NOT CASE
            WHEN NEW.work_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.work_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.work_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current work usage evidence/knowledge') END;
    -- Retain known evidence on omission, but never promote it to final.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM usage_receipts u WHERE u.receipt_id=NEW.receipt_id
        AND ((u.input_tokens-u.cached_input_tokens+u.output_tokens IS NULL AND NEW.spend_amount IS OLD.spend_amount
                AND NEW.spend_receipt_id IS OLD.spend_receipt_id)
            OR (u.input_tokens-u.cached_input_tokens+u.output_tokens IS NOT NULL AND NEW.spend_amount IS (u.input_tokens-u.cached_input_tokens+u.output_tokens)
                AND NEW.spend_receipt_id IS u.receipt_id)))
        OR NEW.spend_state IS NOT CASE
            WHEN NEW.spend_amount IS NOT NULL THEN
                CASE WHEN NEW.receipt_state='final' AND NEW.spend_receipt_id=NEW.receipt_id
                    THEN 'final' ELSE 'partial' END
            WHEN NEW.receipt_state='partial' OR OLD.spend_state='partial' THEN 'partial'
            ELSE 'unknown' END
        THEN RAISE(ABORT,'invalid current spend usage evidence/knowledge') END;
END;

CREATE TRIGGER provider_calls_finish_only BEFORE UPDATE ON provider_calls BEGIN
    SELECT CASE WHEN NEW.provider_call_id IS NOT OLD.provider_call_id
        OR NEW.invocation_id IS NOT OLD.invocation_id OR NEW.call_ordinal IS NOT OLD.call_ordinal
        OR NEW.provider IS NOT OLD.provider OR NEW.model IS NOT OLD.model
        OR NEW.reasoning_effort IS NOT OLD.reasoning_effort OR NEW.started_at IS NOT OLD.started_at
        OR OLD.duration_ns IS NOT NULL OR NEW.duration_ns IS NULL
        THEN RAISE(ABORT,'only measured ProviderCall duration may change') END;
END;

CREATE TRIGGER provider_calls_no_delete BEFORE DELETE ON provider_calls BEGIN
    SELECT RAISE(ABORT,'ProviderCalls cannot be deleted');
END;

CREATE TRIGGER provider_calls_start_active BEFORE INSERT ON provider_calls BEGIN
    SELECT CASE WHEN NEW.duration_ns IS NOT NULL
        THEN RAISE(ABORT,'ProviderCall must be registered completion-unknown') END;
END;

CREATE TRIGGER qa_check_evidence_sealed_no_delete BEFORE DELETE ON qa_check_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER qa_check_evidence_sealed_no_insert BEFORE INSERT ON qa_check_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER qa_check_evidence_sealed_no_update BEFORE UPDATE ON qa_check_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER qa_checks_sealed_no_delete BEFORE DELETE ON qa_checks WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER qa_checks_sealed_no_insert BEFORE INSERT ON qa_checks WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER qa_checks_sealed_no_update BEFORE UPDATE ON qa_checks WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER resource_allocation_binding BEFORE INSERT ON resource_allocations BEGIN
    SELECT CASE WHEN NEW.amount <= 0 OR NOT EXISTS (
        SELECT 1 FROM resource_pools p WHERE p.pool_id=NEW.pool_id AND p.run_id=NEW.run_id
        AND p.execution_id IS NEW.execution_id AND p.attempt_id IS NEW.attempt_id
        AND p.resource_type=NEW.resource_type
    ) THEN RAISE(ABORT,'allocation requires positive amount and exact pool binding') END;
    SELECT CASE WHEN NEW.request_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM resource_requests WHERE request_id=NEW.request_id AND run_id=NEW.run_id AND status='PENDING'
    ) THEN RAISE(ABORT,'allocation requires pending resource request') END;
END;

CREATE TRIGGER resource_allocations_no_delete BEFORE DELETE ON resource_allocations BEGIN
    SELECT RAISE(ABORT,'allocations are additive and immutable');
END;

CREATE TRIGGER resource_allocations_no_update BEFORE UPDATE ON resource_allocations BEGIN
    SELECT RAISE(ABORT,'allocations are additive and immutable');
END;

CREATE TRIGGER resource_allocations_sealed_no_update BEFORE UPDATE ON resource_allocations WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE allocation_id=OLD.allocation_id) BEGIN SELECT RAISE(ABORT,'sealed dependency is immutable'); END;

CREATE TRIGGER resource_pool_eligibility_no_delete BEFORE DELETE ON resource_pool_eligibility BEGIN
    SELECT RAISE(ABORT,'pool eligibility cannot be deleted');
END;

CREATE TRIGGER resource_pool_eligibility_no_update BEFORE UPDATE ON resource_pool_eligibility BEGIN
    SELECT RAISE(ABORT,'pool eligibility is immutable');
END;

CREATE TRIGGER resource_pool_eligibility_sealed_no_insert BEFORE INSERT ON resource_pool_eligibility WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE pool_id=NEW.pool_id) BEGIN SELECT RAISE(ABORT,'sealed eligibility is immutable'); END;

CREATE TRIGGER resource_pools_no_delete BEFORE DELETE ON resource_pools BEGIN
    SELECT RAISE(ABORT,'resource pools cannot be deleted');
END;

CREATE TRIGGER resource_pools_no_update BEFORE UPDATE ON resource_pools BEGIN
    SELECT RAISE(ABORT,'resource pool identity and scope are immutable');
END;

CREATE TRIGGER resource_pools_sealed_no_update BEFORE UPDATE ON resource_pools WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE pool_id=OLD.pool_id) BEGIN SELECT RAISE(ABORT,'sealed dependency is immutable'); END;

CREATE TRIGGER role_result_evidence_sealed_no_delete BEFORE DELETE ON role_result_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER role_result_evidence_sealed_no_insert BEFORE INSERT ON role_result_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER role_result_evidence_sealed_no_update BEFORE UPDATE ON role_result_evidence WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) OR EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=NEW.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency contents are immutable'); END;

CREATE TRIGGER role_results_sealed_no_update BEFORE UPDATE ON role_results WHEN EXISTS(SELECT 1 FROM authority_proof_dependencies WHERE result_id=OLD.result_id) BEGIN SELECT RAISE(ABORT,'sealed dependency is immutable'); END;

CREATE TRIGGER run_admission_policy_no_delete BEFORE DELETE ON run_admission_policy BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER run_admission_policy_no_update BEFORE UPDATE ON run_admission_policy BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER run_agent_token_profiles_no_delete BEFORE DELETE ON run_agent_token_profiles BEGIN SELECT RAISE(ABORT,'role profile is immutable'); END;

CREATE TRIGGER run_agent_token_profiles_no_update BEFORE UPDATE ON run_agent_token_profiles BEGIN SELECT RAISE(ABORT,'role profile is immutable'); END;

CREATE TRIGGER run_hard_stops_no_delete BEFORE DELETE ON run_hard_stops BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER run_hard_stops_no_update BEFORE UPDATE ON run_hard_stops BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER run_hard_stops_spend_retired BEFORE INSERT ON run_hard_stops
WHEN NEW.resource_type='SPEND' BEGIN SELECT RAISE(ABORT,'SPEND is informational'); END;

CREATE TRIGGER run_hard_stops_tokens_only_insert BEFORE INSERT ON run_hard_stops
WHEN NEW.resource_type IS NOT NULL AND NEW.resource_type NOT IN ('WORK','SPEND','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS') BEGIN
    SELECT RAISE(ABORT,'only tokens have resource bounds or stops');
END;

CREATE TRIGGER run_resource_budgets_no_delete BEFORE DELETE ON run_resource_budgets BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER run_resource_budgets_no_update BEFORE UPDATE ON run_resource_budgets BEGIN
    SELECT RAISE(ABORT,'admission policy and evidence are immutable');
END;

CREATE TRIGGER runs_initialize_semantic_state AFTER INSERT ON runs BEGIN
    INSERT INTO run_current_semantic_state(run_id, lead_state_id, revision, updated_at)
    VALUES (NEW.run_id, NULL, 0, NEW.updated_at);
END;

CREATE TRIGGER semantic_revision_exhaustion BEFORE UPDATE ON run_current_semantic_state BEGIN
    SELECT CASE WHEN OLD.revision=9223372036854775807
        THEN RAISE(ABORT,'numeric invariant violation: semantic revision exhausted') END;
END;

CREATE TRIGGER usage_receipt_call_binding BEFORE INSERT ON usage_receipts BEGIN
    SELECT CASE WHEN NEW.provider_call_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM provider_calls p WHERE p.provider_call_id=NEW.provider_call_id AND p.invocation_id=NEW.invocation_id
    ) THEN RAISE(ABORT,'receipt requires exact ProviderCall provenance') END;
END;

CREATE TRIGGER usage_receipt_numeric_guard BEFORE INSERT ON usage_receipts BEGIN
    SELECT CASE WHEN NEW.input_tokens<0 OR NEW.cached_input_tokens<0
        OR NEW.output_tokens<0 OR NEW.reasoning_tokens<0
        THEN RAISE(ABORT,'numeric invariant violation: negative usage counter') END;
    SELECT CASE WHEN NEW.input_tokens IS NOT NULL AND NEW.output_tokens IS NOT NULL
        AND NEW.input_tokens>9223372036854775807-NEW.output_tokens
        THEN RAISE(ABORT,'numeric invariant violation: WORK unrepresentable') END;
END;

CREATE TRIGGER usage_receipts_final_closure BEFORE INSERT ON usage_receipts
WHEN EXISTS(SELECT 1 FROM usage_receipts u WHERE u.provider_call_id=NEW.provider_call_id
 AND u.completeness='final' AND u.accepted_at IS NOT NULL)
 OR EXISTS(SELECT 1 FROM provider_call_usage c WHERE c.provider_call_id=NEW.provider_call_id AND c.receipt_state='final')
BEGIN
 SELECT RAISE(ABORT,'ProviderCall receipt sequence is closed by FINAL');
END;

CREATE TRIGGER usage_receipts_no_delete BEFORE DELETE ON usage_receipts BEGIN
    SELECT RAISE(ABORT,'usage receipts are immutable');
END;

CREATE TRIGGER usage_receipts_no_update BEFORE UPDATE ON usage_receipts BEGIN
    SELECT RAISE(ABORT,'usage receipts are immutable');
END;

-- Human budget authorization is immutable evidence; it is never a mutable balance.
CREATE TABLE run_budget_confirmations (
 run_id TEXT PRIMARY KEY REFERENCES run_admission_policy(run_id),
 approval_id TEXT NOT NULL UNIQUE,
 approval_json TEXT NOT NULL CHECK(json_valid(approval_json) AND json_extract(approval_json,'$.Decision') IS 'APPROVE'
 AND json_type(approval_json,'$.Response') IS 'text' AND length(trim(json_extract(approval_json,'$.Response')))>0
 AND json_type(approval_json,'$.Prompt') IS 'text' AND length(trim(json_extract(approval_json,'$.Prompt')))>0
 AND json_extract(approval_json,'$.ApprovalID') IS approval_id
 AND json_type(approval_json,'$.InteractionID') IS 'text' AND length(trim(json_extract(approval_json,'$.InteractionID')))>0),
 confirmed_at TEXT NOT NULL,
 contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64)
) STRICT;
CREATE TABLE run_budget_increases (
 approval_id TEXT PRIMARY KEY,
 run_id TEXT NOT NULL REFERENCES run_budget_confirmations(run_id),
 amount INTEGER NOT NULL CHECK(amount>0),
 approval_json TEXT NOT NULL CHECK(json_valid(approval_json) AND json_extract(approval_json,'$.Decision') IS 'APPROVE'
 AND json_type(approval_json,'$.Response') IS 'text' AND length(trim(json_extract(approval_json,'$.Response')))>0
 AND json_type(approval_json,'$.Prompt') IS 'text' AND length(trim(json_extract(approval_json,'$.Prompt')))>0
 AND json_extract(approval_json,'$.ApprovalID') IS approval_id
 AND json_type(approval_json,'$.InteractionID') IS 'text' AND length(trim(json_extract(approval_json,'$.InteractionID')))>0),
 approved_at TEXT NOT NULL,
 contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64)
) STRICT;
CREATE TABLE hard_limit_requests (
 request_id TEXT PRIMARY KEY,
 run_id TEXT NOT NULL REFERENCES run_budget_confirmations(run_id),
 admission_id TEXT NOT NULL REFERENCES invocation_admissions(admission_id),
 allocation_id TEXT NOT NULL UNIQUE,
 amount INTEGER NOT NULL CHECK(amount>0),
 initial_hard_limit INTEGER NOT NULL CHECK(initial_hard_limit>0),
 proposed_at TEXT NOT NULL,
 deadline TEXT NOT NULL,
 proposal_json TEXT NOT NULL CHECK(json_valid(proposal_json)),
 contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64),
 FOREIGN KEY(admission_id) REFERENCES admission_agent_token_limits(admission_id)
) STRICT;
CREATE TABLE hard_limit_decisions (
 request_id TEXT PRIMARY KEY REFERENCES hard_limit_requests(request_id),
 decision TEXT NOT NULL CHECK(decision IN ('APPROVE','DENY','TIMEOUT','EXPIRED')),
 response_json TEXT CHECK(response_json IS NULL OR (json_valid(response_json)
 AND json_type(response_json,'$.ApprovalID') IS 'text' AND length(trim(json_extract(response_json,'$.ApprovalID')))>0
 AND json_type(response_json,'$.InteractionID') IS 'text' AND length(trim(json_extract(response_json,'$.InteractionID')))>0
 AND json_type(response_json,'$.Prompt') IS 'text' AND length(trim(json_extract(response_json,'$.Prompt')))>0
 AND json_type(response_json,'$.Response') IS 'text' AND length(trim(json_extract(response_json,'$.Response')))>0)),
 decided_at TEXT NOT NULL,
 contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64),
 applied INTEGER NOT NULL CHECK(applied IN (0,1)),
 reason TEXT NOT NULL,
 CHECK(applied=0 OR decision IN ('APPROVE','TIMEOUT')),
 CHECK((decision IN ('APPROVE','DENY') AND response_json IS NOT NULL AND json_type(response_json,'$.Decision') IS 'text' AND json_extract(response_json,'$.Decision') IS decision)
 OR (decision IN ('TIMEOUT','EXPIRED') AND response_json IS NULL))
) STRICT;
CREATE TABLE admission_closures (
 admission_id TEXT PRIMARY KEY REFERENCES invocation_admissions(admission_id),
 proof_id TEXT NOT NULL REFERENCES authority_proofs(proof_id),
 assertion TEXT NOT NULL CHECK(assertion='NO_FURTHER_CALLS'),
 closed_at TEXT NOT NULL,
 contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64)
) STRICT;
CREATE TABLE admission_release_authorizations (
 claim_id TEXT PRIMARY KEY REFERENCES admission_dispatch_claims(claim_id),
 authorized_at TEXT NOT NULL,
 contract_digest TEXT NOT NULL CHECK(length(contract_digest)=64)
) STRICT;
CREATE TRIGGER run_budget_increases_capacity BEFORE INSERT ON run_budget_increases BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM runs WHERE run_id=NEW.run_id AND status='RUNNING')
 THEN RAISE(ABORT,'budget increment requires running Run') END;
 SELECT CASE WHEN (SELECT SUM(value) FROM (
 SELECT normal_budget AS value FROM v_run_token_balance WHERE run_id=NEW.run_id UNION ALL SELECT NEW.amount)) IS NULL
 THEN RAISE(ABORT,'missing normal budget') END;
 SELECT SUM(value) FROM (
 SELECT SUM(value) AS value FROM (SELECT normal_budget AS value FROM v_run_token_balance WHERE run_id=NEW.run_id UNION ALL SELECT NEW.amount)
 UNION ALL SELECT (SELECT SUM(value) FROM (SELECT normal_budget AS value FROM v_run_token_balance WHERE run_id=NEW.run_id UNION ALL SELECT NEW.amount))/10);
END;
CREATE TRIGGER invocation_admissions_confirmation BEFORE INSERT ON invocation_admissions BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM run_budget_confirmations WHERE run_id=NEW.run_id)
 THEN RAISE(ABORT,'human normal-budget confirmation required') END;
END;
CREATE TRIGGER admission_resources_binding BEFORE INSERT ON admission_resources BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocation_admissions a JOIN resource_pools p ON p.pool_id=NEW.pool_id
 WHERE a.admission_id=NEW.admission_id AND a.run_id=p.run_id AND p.resource_type=NEW.resource_type)
 THEN RAISE(ABORT,'admission resource provenance mismatch') END;
END;
CREATE TRIGGER admission_closures_binding BEFORE INSERT ON admission_closures BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocation_admissions a JOIN authority_proofs p ON p.proof_id=NEW.proof_id
 JOIN evidence_artifacts e ON e.evidence_id=p.evidence_id WHERE a.admission_id=NEW.admission_id AND a.run_id=p.run_id
 AND a.invocation_id=p.invocation_id AND p.kind='RECOVERY' AND p.verdict='RECOVERABLE'
 AND json_extract(e.inline_blob,'$.Request.Recovery.Containment')='CONFIRMED')
 OR EXISTS(SELECT 1 FROM provider_calls p JOIN invocation_admissions a USING(invocation_id) WHERE a.admission_id=NEW.admission_id AND p.duration_ns IS NULL)
 THEN RAISE(ABORT,'closure requires confirmed containment and physical completion') END;
END;
CREATE TRIGGER provider_calls_closed_admission BEFORE INSERT ON provider_calls BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM invocation_admissions a JOIN admission_closures c USING(admission_id) WHERE a.invocation_id=NEW.invocation_id)
 OR EXISTS(SELECT 1 FROM invocation_admissions a JOIN admission_non_dispatch_proofs n USING(admission_id) WHERE a.invocation_id=NEW.invocation_id)
 THEN RAISE(ABORT,'admission is closed') END;
END;
CREATE TRIGGER resource_requests_decision BEFORE UPDATE ON resource_requests BEGIN
 SELECT CASE WHEN OLD.status<>'PENDING' OR NEW.status NOT IN ('GRANTED','DENIED') OR NEW.revision<>OLD.revision+1
 OR NEW.request_id IS NOT OLD.request_id OR NEW.run_id IS NOT OLD.run_id OR NEW.cycle_no IS NOT OLD.cycle_no
 OR NEW.request_ordinal IS NOT OLD.request_ordinal OR NEW.requester_invocation_id IS NOT OLD.requester_invocation_id
 OR NEW.source_invocation_id IS NOT OLD.source_invocation_id OR NEW.target_execution_id IS NOT OLD.target_execution_id
 OR NEW.target_attempt_id IS NOT OLD.target_attempt_id OR NEW.target_role IS NOT OLD.target_role
 OR NEW.target_purpose IS NOT OLD.target_purpose OR NEW.semantic_justification IS NOT OLD.semantic_justification
 OR NEW.created_at IS NOT OLD.created_at THEN RAISE(ABORT,'resource request decision is final') END;
 SELECT CASE WHEN NEW.status='GRANTED' AND NOT EXISTS(SELECT 1 FROM resource_allocations WHERE request_id=OLD.request_id)
 THEN RAISE(ABORT,'grant decision requires allocation evidence') END;
END;
CREATE TRIGGER resource_requests_no_delete BEFORE DELETE ON resource_requests BEGIN SELECT RAISE(ABORT,'resource request history is immutable'); END;

CREATE TRIGGER run_budget_confirmations_no_update BEFORE UPDATE ON run_budget_confirmations BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER run_budget_confirmations_no_delete BEFORE DELETE ON run_budget_confirmations BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER run_budget_increases_no_update BEFORE UPDATE ON run_budget_increases BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER run_budget_increases_no_delete BEFORE DELETE ON run_budget_increases BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER hard_limit_requests_no_update BEFORE UPDATE ON hard_limit_requests BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER hard_limit_requests_no_delete BEFORE DELETE ON hard_limit_requests BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER hard_limit_decisions_no_update BEFORE UPDATE ON hard_limit_decisions BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER hard_limit_decisions_no_delete BEFORE DELETE ON hard_limit_decisions BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER admission_closures_no_update BEFORE UPDATE ON admission_closures BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER admission_closures_no_delete BEFORE DELETE ON admission_closures BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER admission_release_authorizations_no_update BEFORE UPDATE ON admission_release_authorizations BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER admission_release_authorizations_no_delete BEFORE DELETE ON admission_release_authorizations BEGIN SELECT RAISE(ABORT,'immutable authorization history'); END;

CREATE TRIGGER run_budget_increases_approval_identity BEFORE INSERT ON run_budget_increases BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM run_budget_confirmations WHERE approval_id=NEW.approval_id)
 OR EXISTS(SELECT 1 FROM hard_limit_decisions WHERE json_extract(response_json,'$.ApprovalID')=NEW.approval_id)
 THEN RAISE(ABORT,'human approval already bound') END;
END;
CREATE UNIQUE INDEX hard_limit_decisions_response ON hard_limit_decisions(json_extract(response_json,'$.ApprovalID')) WHERE response_json IS NOT NULL;
CREATE TRIGGER hard_limit_requests_binding BEFORE INSERT ON hard_limit_requests BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocation_admissions a JOIN admission_agent_token_limits l USING(admission_id)
 WHERE a.admission_id=NEW.admission_id AND a.run_id=NEW.run_id AND a.kind='NORMAL'
 AND json_extract(NEW.proposal_json,'$.RunID') IS NEW.run_id
 AND json_extract(NEW.proposal_json,'$.RequestID') IS NEW.request_id
 AND json_extract(NEW.proposal_json,'$.Grant.AdmissionID') IS NEW.admission_id
 AND json_extract(NEW.proposal_json,'$.Grant.AllocationID') IS NEW.allocation_id
 AND json_extract(NEW.proposal_json,'$.Grant.Amount') IS NEW.amount
 AND json_extract(NEW.proposal_json,'$.PresentedAt') IS NEW.proposed_at
 AND NEW.deadline IS (strftime('%Y-%m-%dT%H:%M:%S',NEW.proposed_at,'+5 minutes')||substr(NEW.proposed_at,20)))
 THEN RAISE(ABORT,'Hard proposal provenance mismatch') END;
END;
CREATE TRIGGER hard_limit_decisions_deadline BEFORE INSERT ON hard_limit_decisions BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM hard_limit_requests r WHERE r.request_id=NEW.request_id AND
 ((NEW.decision<>'EXPIRED' AND (substr(NEW.decided_at,1,19)||'.'||substr(replace(substr(NEW.decided_at,21),'Z','')||'000000000',1,9))<(substr(r.proposed_at,1,19)||'.'||substr(replace(substr(r.proposed_at,21),'Z','')||'000000000',1,9)))
 OR (NEW.decision IN ('APPROVE','DENY') AND (substr(NEW.decided_at,1,19)||'.'||substr(replace(substr(NEW.decided_at,21),'Z','')||'000000000',1,9))>=(substr(r.deadline,1,19)||'.'||substr(replace(substr(r.deadline,21),'Z','')||'000000000',1,9)))
 OR (NEW.decision='TIMEOUT' AND (substr(NEW.decided_at,1,19)||'.'||substr(replace(substr(NEW.decided_at,21),'Z','')||'000000000',1,9))<(substr(r.deadline,1,19)||'.'||substr(replace(substr(r.deadline,21),'Z','')||'000000000',1,9)))))
 THEN RAISE(ABORT,'immutable Hard decision deadline') END;
END;
CREATE TRIGGER admission_grants_hard_authority BEFORE INSERT ON admission_grants
WHEN EXISTS(SELECT 1 FROM admission_grants WHERE admission_id=NEW.admission_id) BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM invocation_admissions a
 WHERE a.admission_id=NEW.admission_id AND
 COALESCE((SELECT SUM(u.work_amount) FROM provider_calls p JOIN provider_call_usage u USING(provider_call_id) WHERE p.invocation_id=a.invocation_id),0)>=
 (SELECT SUM(g.amount) FROM admission_grants b JOIN resource_allocations g USING(allocation_id) WHERE b.admission_id=a.admission_id)
 AND NOT EXISTS(SELECT 1 FROM hard_limit_requests r JOIN hard_limit_decisions d USING(request_id)
 JOIN resource_allocations g ON g.allocation_id=NEW.allocation_id
 WHERE r.admission_id=a.admission_id AND r.run_id=a.run_id AND r.allocation_id=g.allocation_id AND r.amount=g.amount
 AND d.applied=1 AND d.decision IN ('APPROVE','TIMEOUT') AND r.initial_hard_limit=
 (SELECT SUM(g.amount) FROM admission_grants b JOIN resource_allocations g USING(allocation_id) WHERE b.admission_id=a.admission_id)))
 THEN RAISE(ABORT,'specific Hard approval required') END;
END;
CREATE TRIGGER admission_dispatch_claims_deadline BEFORE INSERT ON admission_dispatch_claims BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM invocation_admissions a JOIN execution_admission_deadlines e USING(execution_id)
 WHERE a.admission_id=NEW.admission_id AND (substr(NEW.claimed_at,1,19)||'.'||substr(replace(substr(NEW.claimed_at,21),'Z','')||'000000000',1,9))>=(substr(e.hard_deadline,1,19)||'.'||substr(replace(substr(e.hard_deadline,21),'Z','')||'000000000',1,9)))
 OR EXISTS(SELECT 1 FROM admission_closures WHERE admission_id=NEW.admission_id)
 OR EXISTS(SELECT 1 FROM admission_non_dispatch_proofs WHERE admission_id=NEW.admission_id)
 THEN RAISE(ABORT,'closed or expired admission') END;
END;
CREATE TRIGGER admission_release_authorizations_deadline BEFORE INSERT ON admission_release_authorizations BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM admission_dispatch_claims d JOIN invocation_admissions a USING(admission_id)
 JOIN execution_admission_deadlines e USING(execution_id) WHERE d.claim_id=NEW.claim_id AND
 ((substr(NEW.authorized_at,1,19)||'.'||substr(replace(substr(NEW.authorized_at,21),'Z','')||'000000000',1,9))>=(substr(e.hard_deadline,1,19)||'.'||substr(replace(substr(e.hard_deadline,21),'Z','')||'000000000',1,9)) OR (substr(NEW.authorized_at,1,19)||'.'||substr(replace(substr(NEW.authorized_at,21),'Z','')||'000000000',1,9))<(substr(d.claimed_at,1,19)||'.'||substr(replace(substr(d.claimed_at,21),'Z','')||'000000000',1,9))
 OR EXISTS(SELECT 1 FROM admission_closures c WHERE c.admission_id=a.admission_id)
 OR EXISTS(SELECT 1 FROM admission_non_dispatch_proofs n WHERE n.admission_id=a.admission_id)))
 THEN RAISE(ABORT,'closed or expired pre-release admission') END;
END;
CREATE TRIGGER admission_dispatch_claims_capabilities BEFORE INSERT ON admission_dispatch_claims BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocation_admissions a WHERE a.admission_id=NEW.admission_id
 AND json_extract(NEW.request_json,'$.AdmissionID') IS NEW.admission_id AND json_extract(NEW.request_json,'$.ClaimID') IS NEW.claim_id
 AND json_extract(NEW.request_json,'$.RunID') IS a.run_id
 AND COALESCE(json_extract(NEW.request_json,'$.CompletionOnly'),0) IS NEW.completion_only)
 THEN RAISE(ABORT,'dispatch contract identity mismatch') END;
 SELECT CASE WHEN NEW.provider_call_id IS NOT NULL AND (NOT EXISTS(SELECT 1 FROM provider_calls p WHERE p.provider_call_id=NEW.provider_call_id
 AND p.invocation_id=(SELECT invocation_id FROM invocation_admissions WHERE admission_id=NEW.admission_id)
 AND json_extract(NEW.request_json,'$.ProviderCall.ProviderCallID') IS p.provider_call_id
 AND json_extract(NEW.request_json,'$.ProviderCall.InvocationID') IS p.invocation_id
 AND json_extract(NEW.request_json,'$.ProviderCall.RunID') IS (SELECT run_id FROM invocation_admissions WHERE admission_id=NEW.admission_id)
 AND json_extract(NEW.request_json,'$.ProviderCall.Provider') IS p.provider
 AND json_extract(NEW.request_json,'$.ProviderCall.Model') IS p.model
 AND json_extract(NEW.request_json,'$.ProviderCall.ReasoningEffort') IS p.reasoning_effort
 AND json_extract(NEW.request_json,'$.Capabilities.Provider') IS p.provider
 AND json_extract(NEW.request_json,'$.Capabilities.Model') IS p.model)
 OR typeof(json_extract(NEW.request_json,'$.Capabilities.ContextWindowTokens'))<>'integer'
 OR typeof(json_extract(NEW.request_json,'$.Capabilities.MaxOutputTokens'))<>'integer'
 OR (json_extract(NEW.request_json,'$.Capabilities.MaxInputTokens') IS NOT NULL AND typeof(json_extract(NEW.request_json,'$.Capabilities.MaxInputTokens'))<>'integer')
 OR EXISTS(SELECT 1 FROM json_each(NEW.request_json,'$.Bounds') b WHERE typeof(json_extract(b.value,'$.Amount'))<>'integer' OR json_extract(b.value,'$.Amount')<0
 OR COALESCE(json_extract(b.value,'$.Dimension'),'') NOT IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS'))
 OR (SELECT count(*) FROM json_each(NEW.request_json,'$.Bounds'))<>(SELECT count(DISTINCT json_extract(b.value,'$.Dimension')) FROM json_each(NEW.request_json,'$.Bounds') b)
 OR COALESCE(json_extract(NEW.request_json,'$.Capabilities.ContextWindowTokens'),0)<=0
 OR COALESCE(json_extract(NEW.request_json,'$.Capabilities.MaxOutputTokens'),0)<=0
 OR (SELECT count(*) FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension')='WORK' AND json_extract(b.value,'$.Amount')>0)<>1
 OR (SELECT count(*) FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension')='OUTPUT_TOKENS' AND json_extract(b.value,'$.Amount')>0)<>1
 OR (SELECT count(*) FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension')='INPUT_TOKENS' AND json_extract(b.value,'$.Amount')>=0)<>1)
 THEN RAISE(ABORT,'positive model capabilities and request bounds required') END;
 SELECT CASE WHEN NEW.completion_only=1 AND
 (json_type(NEW.request_json,'$.CompletionCapabilities.ToolsEnabled') IS NOT 'false'
 OR json_type(NEW.request_json,'$.CompletionCapabilities.WorkspaceWriteEnabled') IS NOT 'false'
 OR json_type(NEW.request_json,'$.CompletionCapabilities.FinalResponseOnly') IS NOT 'true')
 THEN RAISE(ABORT,'completion mode requires tools and writes disabled') END;
 SELECT CASE WHEN NEW.completion_only=0 AND json_extract(NEW.request_json,'$.CompletionCapabilities') IS NOT NULL
 THEN RAISE(ABORT,'completion capabilities require completion-only mode') END;
END;
CREATE TRIGGER admission_dispatch_bounds_contract BEFORE INSERT ON admission_dispatch_bounds BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM admission_dispatch_claims d,json_each(d.request_json,'$.Bounds') b
 WHERE d.claim_id=NEW.claim_id AND json_extract(b.value,'$.Dimension') IS NEW.dimension AND json_extract(b.value,'$.Amount') IS NEW.amount)
 THEN RAISE(ABORT,'dispatch bound differs from immutable contract') END;
END;

-- Shared SQL predicate for the original admission's current ownership/lifecycle
-- fence. Authority comes from these rows, never a supplied role or purpose tag.
CREATE VIEW v_current_admission_fences AS
SELECT a.admission_id FROM invocation_admissions a JOIN invocations i USING(invocation_id)
JOIN executions e ON e.execution_id=i.execution_id JOIN attempts t ON t.attempt_id=i.attempt_id
JOIN runs r ON r.run_id=a.run_id JOIN run_active_actor o ON o.run_id=a.run_id
JOIN run_current_semantic_state s ON s.run_id=a.run_id LEFT JOIN invocations own ON own.invocation_id=o.active_invocation_id
WHERE r.status='RUNNING' AND r.current_cycle=a.cycle_no AND i.cycle_no=a.cycle_no
AND i.execution_id=a.execution_id AND i.attempt_id=a.attempt_id AND i.role=a.role AND i.purpose=a.purpose
AND i.content_identity IS a.content_identity AND i.ownership_epoch=a.ownership_epoch AND i.ownership_epoch=o.ownership_epoch
AND i.completed_at IS NULL AND e.completed_at IS NULL AND t.completed_at IS NULL
AND r.revision=a.run_revision AND e.revision=a.execution_revision AND t.revision=a.attempt_revision AND i.revision=a.invocation_revision
AND s.revision=a.semantic_revision AND o.active_role=a.actor_role AND o.active_invocation_id IS a.actor_invocation_id
AND own.content_identity IS a.actor_content_identity AND o.revision=a.actor_revision
AND NOT EXISTS(SELECT 1 FROM role_results WHERE invocation_id=a.invocation_id)
AND ((a.role=o.active_role AND o.active_invocation_id=a.invocation_id) OR (a.role='ORCHESTRATOR' AND a.purpose='RECOVER'));
CREATE TRIGGER admission_dispatch_claims_fence BEFORE INSERT ON admission_dispatch_claims BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v_current_admission_fences WHERE admission_id=NEW.admission_id)
 THEN RAISE(ABORT,'stale authoritative admission fence') END;
END;
CREATE TRIGGER admission_release_authorizations_fence BEFORE INSERT ON admission_release_authorizations BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM admission_dispatch_claims d JOIN v_current_admission_fences f USING(admission_id)
 WHERE d.claim_id=NEW.claim_id) THEN RAISE(ABORT,'stale pre-release admission fence') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM admission_dispatch_claims d JOIN invocation_admissions a USING(admission_id)
 WHERE d.claim_id=NEW.claim_id AND a.kind='NORMAL' AND EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE run_id=a.run_id))
 THEN RAISE(ABORT,'durable hard stop before release') END;
END;
CREATE TRIGGER admission_dispatch_claims_model_limits BEFORE INSERT ON admission_dispatch_claims
WHEN NEW.provider_call_id IS NOT NULL BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM (
 SELECT (SELECT SUM(json_extract(b.value,'$.Amount')) FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension') IN ('INPUT_TOKENS','OUTPUT_TOKENS')) AS combined,
 (SELECT json_extract(b.value,'$.Amount') FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension')='WORK') AS work,
 (SELECT json_extract(b.value,'$.Amount') FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension')='INPUT_TOKENS') AS input,
 (SELECT json_extract(b.value,'$.Amount') FROM json_each(NEW.request_json,'$.Bounds') b WHERE json_extract(b.value,'$.Dimension')='OUTPUT_TOKENS') AS output,
 json_extract(NEW.request_json,'$.Capabilities.ContextWindowTokens') AS context_limit,
 json_extract(NEW.request_json,'$.Capabilities.MaxOutputTokens') AS output_limit,
 json_extract(NEW.request_json,'$.Capabilities.MaxInputTokens') AS input_limit
 ) b WHERE combined>work OR combined>context_limit OR output>output_limit OR output_limit>context_limit
 OR input_limit IS NOT NULL AND (input_limit<0 OR input_limit>context_limit OR input>input_limit))
 THEN RAISE(ABORT,'provider request exceeds model or WORK boundaries') END;
END;
CREATE UNIQUE INDEX run_budget_confirmations_interaction ON run_budget_confirmations(json_extract(approval_json,'$.InteractionID'));
CREATE UNIQUE INDEX run_budget_increases_interaction ON run_budget_increases(json_extract(approval_json,'$.InteractionID'));
CREATE UNIQUE INDEX hard_limit_decisions_interaction ON hard_limit_decisions(json_extract(response_json,'$.InteractionID')) WHERE response_json IS NOT NULL;
CREATE TRIGGER budget_confirmation_interaction BEFORE INSERT ON run_budget_confirmations BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM run_budget_increases WHERE approval_id=NEW.approval_id OR json_extract(approval_json,'$.InteractionID')=json_extract(NEW.approval_json,'$.InteractionID'))
 OR EXISTS(SELECT 1 FROM hard_limit_decisions WHERE json_extract(response_json,'$.ApprovalID')=NEW.approval_id OR json_extract(response_json,'$.InteractionID')=json_extract(NEW.approval_json,'$.InteractionID'))
 THEN RAISE(ABORT,'human interaction already bound') END;
END;
CREATE TRIGGER budget_increase_interaction BEFORE INSERT ON run_budget_increases BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM run_budget_confirmations WHERE json_extract(approval_json,'$.InteractionID')=json_extract(NEW.approval_json,'$.InteractionID'))
 OR EXISTS(SELECT 1 FROM hard_limit_decisions WHERE json_extract(response_json,'$.InteractionID')=json_extract(NEW.approval_json,'$.InteractionID'))
 THEN RAISE(ABORT,'human interaction already bound') END;
END;
CREATE TRIGGER hard_decision_interaction BEFORE INSERT ON hard_limit_decisions WHEN NEW.response_json IS NOT NULL BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM run_budget_confirmations WHERE approval_id=json_extract(NEW.response_json,'$.ApprovalID') OR json_extract(approval_json,'$.InteractionID')=json_extract(NEW.response_json,'$.InteractionID'))
 OR EXISTS(SELECT 1 FROM run_budget_increases WHERE approval_id=json_extract(NEW.response_json,'$.ApprovalID') OR json_extract(approval_json,'$.InteractionID')=json_extract(NEW.response_json,'$.InteractionID'))
 THEN RAISE(ABORT,'human interaction already bound') END;
END;

-- Closure makes a conditional Hard proposal inapplicable. Resolve it in the
-- same transaction so a pending question never survives as grant authority.
CREATE TRIGGER admission_closures_cancel_pending AFTER INSERT ON admission_closures BEGIN
 INSERT INTO hard_limit_decisions(request_id,decision,response_json,decided_at,contract_digest,applied,reason)
 SELECT r.request_id,'EXPIRED',NULL,NEW.closed_at,NEW.contract_digest,0,'admission closed with no further calls'
 FROM hard_limit_requests r WHERE r.admission_id=NEW.admission_id
 AND NOT EXISTS(SELECT 1 FROM hard_limit_decisions d WHERE d.request_id=r.request_id);
END;
CREATE TRIGGER hard_limit_decisions_application BEFORE INSERT ON hard_limit_decisions WHEN NEW.applied=1 BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM hard_limit_requests r JOIN invocation_admissions a USING(admission_id)
 JOIN v_current_admission_fences f USING(admission_id) JOIN v_run_token_balance b ON b.run_id=a.run_id
 JOIN execution_admission_deadlines e ON e.execution_id=a.execution_id
 WHERE (substr(NEW.decided_at,1,19)||'.'||substr(replace(substr(NEW.decided_at,21),'Z','')||'000000000',1,9)) < (substr(e.hard_deadline,1,19)||'.'||substr(replace(substr(e.hard_deadline,21),'Z','')||'000000000',1,9)) AND r.request_id=NEW.request_id AND r.initial_hard_limit=(SELECT SUM(g.amount) FROM admission_grants z JOIN resource_allocations g USING(allocation_id) WHERE z.admission_id=a.admission_id)
 AND r.initial_hard_limit>=COALESCE((SELECT SUM(u.work_amount) FROM provider_calls p JOIN provider_call_usage u USING(provider_call_id) WHERE p.invocation_id=a.invocation_id),0)
 AND r.amount<=b.normal_available AND NOT(b.contingency_used>0 AND b.contingency_available<=0)
 AND NOT EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE run_id=a.run_id)
 AND NOT EXISTS(SELECT 1 FROM admission_closures WHERE admission_id=a.admission_id)
 AND NOT EXISTS(SELECT 1 FROM admission_non_dispatch_proofs WHERE admission_id=a.admission_id))
 THEN RAISE(ABORT,'Hard decision no longer satisfies active admission/capacity') END;
END;

CREATE VIEW v_release_token_capacity AS
WITH agent AS (
 SELECT a.admission_id,a.invocation_id,a.run_id,
 EXISTS(SELECT 1 FROM admission_contingency_authorizations WHERE admission_id=a.admission_id) AS emergency,
 CASE WHEN EXISTS(SELECT 1 FROM admission_contingency_authorizations WHERE admission_id=a.admission_id) THEN r.initial_limit
 ELSE COALESCE((SELECT SUM(g.amount) FROM admission_grants z JOIN resource_allocations g USING(allocation_id) WHERE z.admission_id=a.admission_id),0) END AS hard,
 COALESCE((SELECT SUM(u.work_amount) FROM provider_calls p JOIN provider_call_usage u USING(provider_call_id) WHERE p.invocation_id=a.invocation_id),0) AS used,
 l.initial_soft_limit,l.initial_hard_limit
 FROM invocation_admissions a JOIN admission_resources r USING(admission_id) LEFT JOIN admission_agent_token_limits l USING(admission_id)
)
SELECT a.*,initial_soft_limit+(hard-initial_hard_limit) AS soft,
 (EXISTS(SELECT 1 FROM admission_token_topup_denials WHERE admission_id=a.admission_id AND hard_limit=hard)
 OR EXISTS(SELECT 1 FROM hard_limit_requests r JOIN hard_limit_decisions d USING(request_id)
 WHERE r.admission_id=a.admission_id AND r.initial_hard_limit=hard AND d.decision='DENY')) AS completion_only,
 CASE WHEN emergency THEN b.contingency_available ELSE b.normal_available END AS available,
 (b.contingency_used>0 AND b.contingency_available<=0) AS contingency_exhausted
FROM agent a JOIN v_run_token_balance b USING(run_id);
CREATE TRIGGER admission_release_authorizations_capacity BEFORE INSERT ON admission_release_authorizations BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM admission_dispatch_claims d WHERE d.claim_id=NEW.claim_id AND d.provider_call_id IS NOT NULL
 AND (SELECT count(*) FROM admission_dispatch_bounds WHERE claim_id=d.claim_id)
 <> (SELECT count(*) FROM json_each(d.request_json,'$.Bounds')))
 THEN RAISE(ABORT,'complete normalized provider bounds required before release') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM admission_dispatch_claims d JOIN v_release_token_capacity c USING(admission_id)
 JOIN admission_dispatch_bounds b ON b.claim_id=d.claim_id AND b.dimension='WORK'
 WHERE d.claim_id=NEW.claim_id AND d.provider_call_id IS NOT NULL AND
 (b.amount<=0 OR b.amount>c.hard-c.used OR b.amount>c.available OR c.contingency_exhausted
 OR (c.emergency=0 AND ((d.completion_only=1 AND c.completion_only=0)
 OR (d.completion_only=0 AND (c.completion_only=1 OR c.used>=c.soft))))))
 THEN RAISE(ABORT,'capacity or Soft/denial state changed before release') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM admission_dispatch_claims d JOIN provider_calls p USING(provider_call_id)
 WHERE d.claim_id=NEW.claim_id AND p.duration_ns IS NOT NULL)
 THEN RAISE(ABORT,'completed ProviderCall cannot be released again') END;
END;

-- Minimal fake workflow runtime. These rows select current work; journal events
-- and protocol artifacts never select or reconstruct lifecycle state.
CREATE TABLE workflow_runs (
 run_id TEXT PRIMARY KEY REFERENCES runs(run_id),
 spec_json TEXT NOT NULL CHECK(json_valid(spec_json)),
 current_invocation_id TEXT REFERENCES workflow_steps(invocation_id),
 next_step INTEGER NOT NULL DEFAULT 1 CHECK(next_step>0),
 terminal_proof_id TEXT REFERENCES authority_proofs(proof_id),
 terminal_sha256 TEXT CHECK(terminal_sha256 IS NULL OR length(terminal_sha256)=64),
 FOREIGN KEY(current_invocation_id,run_id) REFERENCES workflow_steps(invocation_id,run_id)
) STRICT;
CREATE TABLE workflow_steps (
 invocation_id TEXT PRIMARY KEY REFERENCES invocations(invocation_id),
 run_id TEXT NOT NULL REFERENCES workflow_runs(run_id),
 step_no INTEGER NOT NULL CHECK(step_no>0),
 action_id TEXT UNIQUE REFERENCES next_actions(action_id),
 output_id TEXT UNIQUE REFERENCES evidence_artifacts(evidence_id),
 UNIQUE(run_id,step_no),
 UNIQUE(invocation_id,run_id),
 FOREIGN KEY(action_id,run_id) REFERENCES next_actions(action_id,run_id),
 FOREIGN KEY(invocation_id,run_id) REFERENCES invocations(invocation_id,run_id)
) STRICT;
CREATE TRIGGER workflow_spec_immutable BEFORE UPDATE OF run_id,spec_json ON workflow_runs BEGIN
 SELECT RAISE(ABORT,'workflow input is immutable');
END;
CREATE TRIGGER workflow_steps_immutable BEFORE UPDATE ON workflow_steps WHEN
 NEW.invocation_id IS NOT OLD.invocation_id OR NEW.run_id IS NOT OLD.run_id OR NEW.step_no IS NOT OLD.step_no
 OR NEW.action_id IS NOT OLD.action_id OR OLD.output_id IS NOT NULL BEGIN
 SELECT RAISE(ABORT,'workflow binding and captured output are immutable');
END;
CREATE TRIGGER workflow_steps_no_delete BEFORE DELETE ON workflow_steps BEGIN
 SELECT RAISE(ABORT,'workflow decisions are retained');
END;
CREATE TRIGGER workflow_terminal_immutable BEFORE UPDATE ON workflow_runs WHEN OLD.terminal_proof_id IS NOT NULL BEGIN
 SELECT RAISE(ABORT,'terminal workflow selection is immutable');
END;
CREATE TRIGGER workflow_runs_no_delete BEFORE DELETE ON workflow_runs BEGIN
 SELECT RAISE(ABORT,'workflow inputs and decisions are retained');
END;
CREATE TRIGGER workflow_output_authorization BEFORE UPDATE OF output_id ON workflow_steps
WHEN NEW.output_id IS NOT NULL AND OLD.output_id IS NULL BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM workflow_runs w JOIN runs r USING(run_id)
 JOIN run_active_actor a USING(run_id) WHERE w.run_id=NEW.run_id
 AND json_extract(w.spec_json,'$.Mode')='EXTERNAL' AND r.status='RUNNING'
 AND a.active_invocation_id=NEW.invocation_id)
 AND NOT EXISTS(SELECT 1 FROM admission_dispatch_claims d
 JOIN admission_release_authorizations z USING(claim_id) JOIN provider_calls p USING(provider_call_id)
 WHERE p.invocation_id=NEW.invocation_id)
 THEN RAISE(ABORT,'captured workflow output requires prior release authorization') END;
END;

CREATE TRIGGER external_workflow_no_provider_calls BEFORE INSERT ON provider_calls
WHEN EXISTS(SELECT 1 FROM workflow_runs w JOIN invocations i USING(run_id) WHERE i.invocation_id=NEW.invocation_id AND json_extract(w.spec_json,'$.Mode')='EXTERNAL') BEGIN
 SELECT RAISE(ABORT,'external Codex requests are outside Orchestrator accounting');
END;
CREATE TRIGGER external_workflow_no_managed_admission BEFORE INSERT ON invocation_admissions
WHEN EXISTS(SELECT 1 FROM workflow_runs WHERE run_id=NEW.run_id AND json_extract(spec_json,'$.Mode')='EXTERNAL') BEGIN
 SELECT RAISE(ABORT,'external workflow steps do not admit model requests');
END;

-- Approved external workflow authorization profiles. These are requested client
-- settings, not observations of Codex's internal requests or model identity.
CREATE VIEW v_workflow_role_profiles(role,level,model,effort) AS VALUES
 ('LEAD',0,'GPT-6.1 Sol','Medium'),('LEAD',1,'GPT-6.1 Sol','High'),('LEAD',2,'GPT-6 Astra','High'),
 ('DEVELOPER',0,'GPT-6 Luna','Medium'),('DEVELOPER',1,'GPT-6 Luna','High'),('DEVELOPER',2,'GPT-6.1 Sol','High'),
 ('QA',0,'GPT-6 Luna','High'),('QA',1,'GPT-6.1 Sol','Medium'),('QA',2,'GPT-6.1 Sol','High'),
 ('REVIEWER',0,'GPT-6.1 Sol','High'),('REVIEWER',1,'GPT-6.1 Sol','XHigh'),('REVIEWER',2,'GPT-6 Astra','High');
CREATE TRIGGER external_workflow_profile BEFORE INSERT ON workflow_steps
WHEN EXISTS(SELECT 1 FROM workflow_runs WHERE run_id=NEW.run_id AND json_extract(spec_json,'$.Mode')='EXTERNAL') BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM invocations i JOIN v_workflow_role_profiles p ON p.role=i.role
 AND p.level=CASE WHEN i.role='LEAD' THEN i.cycle_no-1 ELSE
 (SELECT count(*) FROM workflow_steps s JOIN invocations old USING(invocation_id) WHERE s.run_id=NEW.run_id AND old.role=i.role) END
 WHERE i.invocation_id=NEW.invocation_id AND i.cycle_no<=3 AND i.model=p.model AND i.reasoning_effort=p.effort)
 THEN RAISE(ABORT,'external workflow escalation/profile exhausted or mismatched') END;
END;
