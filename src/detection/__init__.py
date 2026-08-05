"""Incident detection engine — evaluate ops signals and open Lakebase incidents."""

from src.detection.baselines import AnomalyDecision, decide_null_rate, decide_volume
from src.detection.engine import load_detection_inputs, run_detection
from src.detection.incidents import (
    IncidentWriteResult,
    InMemoryIncidentStore,
    PostgresIncidentStore,
    pick_primary_failure_type,
    record_signals,
)
from src.detection.rules import evaluate_all
from src.detection.scoring import Scorecard, score_detections
from src.detection.signals import DetectedSignal
from src.detection.slack import notify_incident_opened

__all__ = [
    "AnomalyDecision",
    "DetectedSignal",
    "InMemoryIncidentStore",
    "IncidentWriteResult",
    "PostgresIncidentStore",
    "Scorecard",
    "decide_null_rate",
    "decide_volume",
    "evaluate_all",
    "load_detection_inputs",
    "notify_incident_opened",
    "pick_primary_failure_type",
    "record_signals",
    "run_detection",
    "score_detections",
]
