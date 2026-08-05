"""Lakebase incident upsert: one incident per job_run_id + append-only signals."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from src.common.constants import FAILURE_TYPE_PRIORITY
from src.detection.signals import DetectedSignal
from src.detection.slack import notify_incident_opened

logger = logging.getLogger(__name__)


def pick_primary_failure_type(failure_types: list[str]) -> str | None:
    if not failure_types:
        return None
    return min(failure_types, key=lambda t: FAILURE_TYPE_PRIORITY.get(t, 99))


@dataclass
class IncidentWriteResult:
    incident_id: str
    job_run_id: str
    created: bool
    primary_failure_type: str | None
    signal_failure_type: str


class IncidentStore(Protocol):
    def record_signal(self, signal: DetectedSignal) -> IncidentWriteResult: ...


class PostgresIncidentStore:
    """Transactional incident writer against Lakebase / Postgres."""

    def __init__(self, conn: Any, *, notify: bool = True, changed_by: str = "detection") -> None:
        self.conn = conn
        self.notify = notify
        self.changed_by = changed_by

    def record_signal(self, signal: DetectedSignal) -> IncidentWriteResult:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO incidents (job_run_id, pipeline_key, primary_failure_type, severity, status)
                VALUES (%s, %s, %s, %s, 'OPEN')
                ON CONFLICT (job_run_id) DO NOTHING
                RETURNING incident_id
                """,
                (signal.job_run_id, signal.pipeline_key, signal.failure_type, signal.severity),
            )
            row = cur.fetchone()
            created = row is not None
            if created:
                incident_id = row[0]
                cur.execute(
                    """
                    INSERT INTO incident_status_events (incident_id, from_status, to_status, changed_by)
                    VALUES (%s, NULL, 'OPEN', %s)
                    """,
                    (incident_id, self.changed_by),
                )
            else:
                cur.execute(
                    "SELECT incident_id FROM incidents WHERE job_run_id = %s",
                    (signal.job_run_id,),
                )
                incident_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO incident_signals (incident_id, failure_type, detected_by, evidence_json)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (incident_id, signal.failure_type, signal.detected_by, signal.evidence_json()),
            )

            cur.execute(
                "SELECT failure_type FROM incident_signals WHERE incident_id = %s",
                (incident_id,),
            )
            types = [r[0] for r in cur.fetchall()]
            primary = pick_primary_failure_type(types)
            cur.execute(
                """
                UPDATE incidents
                SET primary_failure_type = %s,
                    severity = CASE
                      WHEN %s = 'job_crash' OR %s = 'schema_drift' THEN 'high'
                      ELSE severity
                    END
                WHERE incident_id = %s
                """,
                (primary, primary, primary, incident_id),
            )

        self.conn.commit()
        incident_id_str = str(incident_id if isinstance(incident_id, UUID) else incident_id)
        if created and self.notify:
            notify_incident_opened(
                incident_id=incident_id_str,
                job_run_id=signal.job_run_id,
                pipeline_key=signal.pipeline_key,
                primary_failure_type=primary,
                severity=signal.severity,
            )
        return IncidentWriteResult(
            incident_id=incident_id_str,
            job_run_id=signal.job_run_id,
            created=created,
            primary_failure_type=primary,
            signal_failure_type=signal.failure_type,
        )


class InMemoryIncidentStore:
    """Test double that mirrors Lakebase dedup semantics without Postgres."""

    def __init__(self) -> None:
        self.incidents: dict[str, dict[str, Any]] = {}
        self.signals: list[dict[str, Any]] = []
        self.status_events: list[dict[str, Any]] = []
        self._seq = 0

    def record_signal(self, signal: DetectedSignal) -> IncidentWriteResult:
        created = signal.job_run_id not in self.incidents
        if created:
            self._seq += 1
            incident_id = f"inc-{self._seq:04d}"
            self.incidents[signal.job_run_id] = {
                "incident_id": incident_id,
                "job_run_id": signal.job_run_id,
                "pipeline_key": signal.pipeline_key,
                "primary_failure_type": signal.failure_type,
                "severity": signal.severity,
                "status": "OPEN",
            }
            self.status_events.append(
                {
                    "incident_id": incident_id,
                    "from_status": None,
                    "to_status": "OPEN",
                    "changed_by": "detection",
                }
            )
        else:
            incident_id = self.incidents[signal.job_run_id]["incident_id"]

        self.signals.append(
            {
                "incident_id": incident_id,
                "failure_type": signal.failure_type,
                "detected_by": signal.detected_by,
                "evidence": signal.evidence,
            }
        )
        types = [s["failure_type"] for s in self.signals if s["incident_id"] == incident_id]
        primary = pick_primary_failure_type(types)
        self.incidents[signal.job_run_id]["primary_failure_type"] = primary
        return IncidentWriteResult(
            incident_id=incident_id,
            job_run_id=signal.job_run_id,
            created=created,
            primary_failure_type=primary,
            signal_failure_type=signal.failure_type,
        )


def record_signals(store: IncidentStore, signals: list[DetectedSignal]) -> list[IncidentWriteResult]:
    """Persist each signal; dedup across job_run_id is handled by the store."""
    # Stable order: higher-priority failures first so primary settles quickly
    ordered = sorted(
        signals,
        key=lambda s: (s.job_run_id, FAILURE_TYPE_PRIORITY.get(s.failure_type, 99)),
    )
    return [store.record_signal(s) for s in ordered]


def dump_incident_snapshot(store: InMemoryIncidentStore) -> str:
    return json.dumps({"incidents": store.incidents, "signals": store.signals}, indent=2, default=str)
