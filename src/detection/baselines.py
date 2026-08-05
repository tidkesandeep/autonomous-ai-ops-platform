"""Cold-start thresholds and z-score anomaly helpers."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, pstdev

COLD_START_MIN_HISTORY = 7

# Fixed thresholds when history is thin (plan §5 cold start)
NULL_RATE_COLD = 0.20
VOLUME_RATIO_LOW = 0.5
VOLUME_RATIO_HIGH = 2.0
DURATION_RATIO_COLD = 3.0
ZSCORE_THRESHOLD = 3.0


@dataclass(frozen=True)
class AnomalyDecision:
    is_anomaly: bool
    mode: str  # cold_start | zscore
    observed: float
    baseline: float | None
    score: float | None
    reason: str


def decide_null_rate(observed: float, history: list[float]) -> AnomalyDecision:
    if len(history) < COLD_START_MIN_HISTORY:
        hit = observed > NULL_RATE_COLD
        return AnomalyDecision(
            is_anomaly=hit,
            mode="cold_start",
            observed=observed,
            baseline=NULL_RATE_COLD,
            score=None,
            reason=f"null_rate {observed:.3f} > cold threshold {NULL_RATE_COLD}",
        )
    mu = mean(history)
    sigma = pstdev(history) or 1e-9
    z = (observed - mu) / sigma
    hit = z >= ZSCORE_THRESHOLD
    return AnomalyDecision(
        is_anomaly=hit,
        mode="zscore",
        observed=observed,
        baseline=mu,
        score=z,
        reason=f"null_rate z={z:.2f} (mu={mu:.3f}, sigma={sigma:.3f})",
    )


def decide_volume(observed: float, history: list[float]) -> AnomalyDecision:
    if not history:
        return AnomalyDecision(
            is_anomaly=False,
            mode="cold_start",
            observed=observed,
            baseline=None,
            score=None,
            reason="no prior volume history",
        )
    if len(history) < COLD_START_MIN_HISTORY:
        prev = history[-1]
        if prev <= 0:
            hit = observed > 0
            ratio = None
        else:
            ratio = observed / prev
            hit = ratio < VOLUME_RATIO_LOW or ratio > VOLUME_RATIO_HIGH
        return AnomalyDecision(
            is_anomaly=hit,
            mode="cold_start",
            observed=observed,
            baseline=prev,
            score=ratio,
            reason=f"volume ratio vs previous={ratio}",
        )
    mu = mean(history)
    sigma = pstdev(history) or 1e-9
    z = abs(observed - mu) / sigma
    hit = z >= ZSCORE_THRESHOLD
    return AnomalyDecision(
        is_anomaly=hit,
        mode="zscore",
        observed=observed,
        baseline=mu,
        score=z,
        reason=f"volume |z|={z:.2f} (mu={mu:.1f}, sigma={sigma:.1f})",
    )


def decide_duration(observed_ms: float, history_ms: list[float]) -> AnomalyDecision:
    if not history_ms:
        return AnomalyDecision(
            is_anomaly=False,
            mode="cold_start",
            observed=observed_ms,
            baseline=None,
            score=None,
            reason="no prior duration history",
        )
    if len(history_ms) < COLD_START_MIN_HISTORY:
        prev = history_ms[-1]
        ratio = None if prev <= 0 else observed_ms / prev
        hit = ratio is not None and ratio > DURATION_RATIO_COLD
        return AnomalyDecision(
            is_anomaly=hit,
            mode="cold_start",
            observed=observed_ms,
            baseline=prev,
            score=ratio,
            reason=f"duration ratio vs previous={ratio}",
        )
    mu = mean(history_ms)
    sigma = pstdev(history_ms) or 1e-9
    z = (observed_ms - mu) / sigma
    hit = z >= ZSCORE_THRESHOLD
    return AnomalyDecision(
        is_anomaly=hit,
        mode="zscore",
        observed=observed_ms,
        baseline=mu,
        score=z,
        reason=f"duration z={z:.2f} (mu={mu:.1f}, sigma={sigma:.1f})",
    )
