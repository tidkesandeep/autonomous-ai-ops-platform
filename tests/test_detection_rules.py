"""Unit tests for detection baselines and rules."""

from __future__ import annotations

from src.detection.baselines import decide_null_rate, decide_volume
from src.detection.incidents import InMemoryIncidentStore, pick_primary_failure_type, record_signals
from src.detection.rules import (
    detect_dq_failures,
    detect_job_crashes,
    detect_schema_drift,
    detect_volume_anomalies,
    evaluate_all,
)
from src.detection.scoring import score_detections
from src.detection.signals import DetectedSignal


def test_pick_primary_prefers_crash_over_null():
    assert pick_primary_failure_type(["null_spike", "job_crash"]) == "job_crash"
    assert pick_primary_failure_type(["late_data", "schema_drift"]) == "schema_drift"


def test_null_rate_cold_start():
    hit = decide_null_rate(0.45, [0.01, 0.02])
    assert hit.is_anomaly and hit.mode == "cold_start"
    miss = decide_null_rate(0.05, [0.01])
    assert not miss.is_anomaly


def test_volume_cold_start_ratio():
    hit = decide_volume(10, [100])
    assert hit.is_anomaly and hit.mode == "cold_start"


def test_job_crash_from_poller_logs():
    signals = detect_job_crashes(
        [
            {
                "run_id": "r1",
                "pipeline_key": "orders_ingest",
                "task_key": "bronze",
                "result_state": "FAILED",
                "lifecycle_state": "TERMINATED",
                "error_signature": "Exception",
                "raw_output": "OutOfMemoryError: Java heap space",
            }
        ]
    )
    assert len(signals) == 1
    assert signals[0].failure_type == "job_crash"
    assert signals[0].severity == "high"


def test_dq_maps_duplicate_and_late():
    rows = [
        {
            "run_id": "r2",
            "pipeline_key": "orders_ingest",
            "check_name": "orders_duplicate_rate",
            "metric_name": "duplicate_rate",
            "passed": False,
            "observed_value": 0.5,
            "threshold_value": 0.01,
            "comparator": "<=",
            "table_name": "demo.silver.orders",
        },
        {
            "run_id": "r3",
            "pipeline_key": "events_clickstream",
            "check_name": "events_lag_under_48h",
            "metric_name": "pct_late",
            "passed": False,
            "observed_value": 0.9,
            "threshold_value": 0.05,
            "comparator": "<=",
            "table_name": "demo.silver.events",
        },
    ]
    signals = detect_dq_failures(rows)
    assert {s.failure_type for s in signals} == {"duplicate_explosion", "late_data"}


def test_schema_drift_detects_added_column():
    signals = detect_schema_drift(
        current_snapshots=[
            {
                "run_id": "r4",
                "pipeline_key": "products_catalog",
                "schema_snapshot_json": '{"a":"string","b":"int"}',
            }
        ],
        previous_by_pipeline={"products_catalog": {"a": "string"}},
    )
    assert len(signals) == 1
    assert signals[0].evidence["added_columns"] == ["b"]


def test_volume_anomaly_rule():
    signals = detect_volume_anomalies(
        telemetry=[{"run_id": "r5", "pipeline_key": "orders_ingest", "task_name": "t", "rows_out": 5}],
        history_by_pipeline={"orders_ingest": [100, 110, 105]},
    )
    assert len(signals) == 1
    assert signals[0].failure_type == "volume_anomaly"


def test_incident_dedup_one_per_run():
    store = InMemoryIncidentStore()
    signals = [
        DetectedSignal("run-9", "orders_ingest", "null_spike", "workflow", evidence={"n": 1}),
        DetectedSignal("run-9", "orders_ingest", "job_crash", "poller", evidence={"n": 2}),
    ]
    results = record_signals(store, signals)
    assert len(store.incidents) == 1
    assert sum(1 for r in results if r.created) == 1
    assert store.incidents["run-9"]["primary_failure_type"] == "job_crash"
    assert len(store.signals) == 2


def test_evaluate_all_smoke():
    signals = evaluate_all(
        telemetry_current=[
            {
                "run_id": "run-a",
                "pipeline_key": "orders_ingest",
                "task_name": "load",
                "status": "SUCCESS",
                "rows_out": 10,
                "metadata_json": "{}",
                "schema_snapshot_json": None,
            }
        ],
        telemetry_history=[{"pipeline_key": "orders_ingest", "rows_out": 100, "metadata_json": "{}"}],
        dq_rows=[],
        task_logs=[],
        previous_schemas={},
    )
    assert any(s.failure_type == "volume_anomaly" for s in signals)


def test_scorecard_precision_recall():
    expected = [{"job_run_id": "1", "failure_type": "null_spike"}, {"job_run_id": "2", "failure_type": "job_crash"}]
    detected = [{"job_run_id": "1", "failure_type": "null_spike"}, {"job_run_id": "3", "failure_type": "late_data"}]
    card = score_detections(expected, detected)
    assert card.true_positives == 1
    assert card.false_positives == 1
    assert card.false_negatives == 1
    assert card.precision == 0.5
    assert card.recall == 0.5
