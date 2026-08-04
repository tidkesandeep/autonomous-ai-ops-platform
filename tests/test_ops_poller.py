from __future__ import annotations

from datetime import UTC, datetime

from src.ops.poller import failed_task_logs_from_runs_payload


def test_failed_task_logs_filters_only_failed_tasks():
    payload = {
        "runs": [
            {
                "run_id": 1,
                "job_id": 11,
                "run_name": "orders_ingest",
                "tasks": [
                    {
                        "run_id": 101,
                        "task_key": "ok_task",
                        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
                    },
                    {
                        "run_id": 102,
                        "task_key": "fail_task",
                        "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "FAILED",
                            "state_message": "OutOfMemoryError: Java heap space",
                        },
                    },
                ],
            }
        ]
    }
    rows = failed_task_logs_from_runs_payload(payload, now=datetime(2026, 8, 4, tzinfo=UTC))
    assert len(rows) == 1
    row = rows[0]
    assert row.task_run_id == "102"
    assert row.pipeline_key == "orders_ingest"
    assert row.error_signature.lower() == "outofmemoryerror"


def test_failed_task_logs_include_internal_error_without_result_state():
    payload = {
        "runs": [
            {
                "run_id": 2,
                "job_id": 22,
                "tasks": [
                    {
                        "run_id": 202,
                        "task_key": "internal",
                        "state": {
                            "life_cycle_state": "INTERNAL_ERROR",
                            "state_message": "Driver crashed",
                        },
                    }
                ],
            }
        ]
    }
    rows = failed_task_logs_from_runs_payload(payload)
    assert len(rows) == 1
    assert rows[0].task_key == "internal"
