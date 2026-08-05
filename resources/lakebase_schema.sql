-- Lakebase / Postgres app-state schema (Phase 3 wires this up).
-- Path A/B: apply against Lakebase. Path C/D: apply against Neon.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS incidents (
    incident_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_run_id           TEXT NOT NULL UNIQUE,
    detected_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    pipeline_key         TEXT NOT NULL,
    primary_failure_type TEXT,
    severity             TEXT NOT NULL DEFAULT 'medium',
    status               TEXT NOT NULL DEFAULT 'OPEN'
                         CHECK (status IN ('OPEN', 'INVESTIGATING', 'AWAITING_APPROVAL', 'RESOLVED')),
    agent_started_at     TIMESTAMPTZ,
    rca_report_path      TEXT,
    linked_commit_sha    TEXT
);

CREATE TABLE IF NOT EXISTS incident_signals (
    signal_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id   UUID NOT NULL REFERENCES incidents (incident_id),
    failure_type  TEXT NOT NULL,
    detected_by   TEXT NOT NULL CHECK (detected_by IN ('workflow', 'poller')),
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_incident_signals_incident
    ON incident_signals (incident_id);

CREATE TABLE IF NOT EXISTS incident_status_events (
    event_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents (incident_id),
    from_status TEXT,
    to_status   TEXT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_incident_status_events_incident
    ON incident_status_events (incident_id, changed_at);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id      UUID NOT NULL REFERENCES incidents (incident_id),
    decision         TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
    decided_by       TEXT NOT NULL,
    decided_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    remediation_type TEXT,
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS agent_actions (
    action_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id     UUID NOT NULL REFERENCES incidents (incident_id),
    tool_name       TEXT NOT NULL,
    inputs_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
    outputs_summary TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT,
    detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
