"""Auto-grade agent RCAs against chaos ground truth + optional human CSV."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Grade:
    incident_id: str
    job_run_id: str
    score: int
    grader: str
    notes: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "job_run_id": self.job_run_id,
            "score": self.score,
            "grader": self.grader,
            "notes": self.notes,
        }


def auto_grade_report(report: dict[str, Any], expected_failure_type: str | None) -> Grade:
    """Rubric auto-grade for injected failures.

    2 = root_cause_type matches injected class and evidence is non-empty
    1 = type matches but evidence thin, or evidence strong but type missing
    0 = wrong / missing type
    """
    iid = str(report.get("incident_id") or "")
    run_id = str(report.get("job_run_id") or "")
    predicted = report.get("root_cause_type")
    evidence = report.get("evidence") or []
    if expected_failure_type and predicted == expected_failure_type and len(evidence) >= 2:
        return Grade(iid, run_id, 2, "auto", "type match + evidence")
    if expected_failure_type and predicted == expected_failure_type:
        return Grade(iid, run_id, 1, "auto", "type match, thin evidence")
    if expected_failure_type and evidence:
        return Grade(iid, run_id, 0, "auto", f"expected {expected_failure_type}, got {predicted}")
    return Grade(iid, run_id, 0, "auto", "missing expected failure type")


def summarize_grades(grades: list[Grade]) -> dict[str, Any]:
    if not grades:
        return {"n": 0, "mean": 0.0, "zeros": 0, "distribution": {}}
    scores = [g.score for g in grades]
    dist = {0: scores.count(0), 1: scores.count(1), 2: scores.count(2)}
    return {
        "n": len(scores),
        "mean": sum(scores) / len(scores),
        "zeros": dist[0],
        "distribution": dist,
        "exit_criteria_met": (sum(scores) / len(scores) >= 1.6) and dist[0] <= 1 and len(scores) >= 18,
    }


def write_grades_csv(path: str | Path, grades: list[Grade]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["incident_id", "job_run_id", "score", "grader", "notes"])
        writer.writeheader()
        for g in grades:
            writer.writerow(g.as_dict())


def load_grades_csv(path: str | Path) -> list[Grade]:
    path = Path(path)
    if not path.exists():
        return []
    out: list[Grade] = []
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            out.append(
                Grade(
                    incident_id=row["incident_id"],
                    job_run_id=row.get("job_run_id", ""),
                    score=int(row["score"]),
                    grader=row.get("grader", "human"),
                    notes=row.get("notes", ""),
                )
            )
    return out
