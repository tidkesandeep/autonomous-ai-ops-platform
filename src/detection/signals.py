"""Detected failure signals produced by rule evaluation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class DetectedSignal:
    """One failure signal for a single pipeline run."""

    job_run_id: str
    pipeline_key: str
    failure_type: str
    detected_by: str  # workflow | poller
    evidence: dict[str, Any] = field(default_factory=dict)
    severity: str = "medium"
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def evidence_json(self) -> str:
        return json.dumps(self.evidence, default=str)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["detected_at"] = self.detected_at.isoformat()
        return row
