"""Score detection precision/recall against chaos ground truth."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Scorecard:
    expected: int
    detected: int
    true_positives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "expected": self.expected,
            "detected": self.detected,
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "false_negatives": self.false_negatives,
            "precision": self.precision,
            "recall": self.recall,
        }


def score_detections(
    expected: list[dict[str, Any]],
    detected: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...] = ("job_run_id", "failure_type"),
) -> Scorecard:
    """Compare injected ground truth to detected incident primary/signals.

    ``expected`` rows need the key fields (typically from ops.bronze.injected_failures).
    ``detected`` rows need the same keys (from incidents + signals).
    """
    exp_keys = {_key(row, key_fields) for row in expected}
    det_keys = {_key(row, key_fields) for row in detected}
    tp = len(exp_keys & det_keys)
    fp = len(det_keys - exp_keys)
    fn = len(exp_keys - det_keys)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    return Scorecard(
        expected=len(exp_keys),
        detected=len(det_keys),
        true_positives=tp,
        false_positives=fp,
        false_negatives=fn,
        precision=precision,
        recall=recall,
    )


def _key(row: dict[str, Any], fields: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(row.get(f) for f in fields)
