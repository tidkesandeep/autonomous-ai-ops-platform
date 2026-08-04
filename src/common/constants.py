"""Shared constants and catalog naming for the Autonomous AI Ops Platform."""

from __future__ import annotations

DEMO_CATALOG = "demo"
OPS_CATALOG = "ops"

DEMO_BRONZE = f"{DEMO_CATALOG}.bronze"
DEMO_SILVER = f"{DEMO_CATALOG}.silver"
DEMO_GOLD = f"{DEMO_CATALOG}.gold"

OPS_BRONZE = f"{OPS_CATALOG}.bronze"
OPS_SILVER = f"{OPS_CATALOG}.silver"
OPS_GOLD = f"{OPS_CATALOG}.gold"

# Five simulated pipelines (enough for all six failure classes)
PIPELINE_KEYS = (
    "orders_ingest",
    "customers_scd2",
    "products_catalog",
    "events_clickstream",
    "reviews_enrichment",
)

FAILURE_TYPES = (
    "job_crash",
    "schema_drift",
    "duplicate_explosion",
    "null_spike",
    "volume_anomaly",
    "late_data",
)

# Primary failure type priority (most upstream / structural wins)
FAILURE_TYPE_PRIORITY = {
    "job_crash": 0,
    "schema_drift": 1,
    "duplicate_explosion": 2,
    "null_spike": 3,
    "volume_anomaly": 4,
    "late_data": 5,
}

INCIDENT_STATUSES = (
    "OPEN",
    "INVESTIGATING",
    "AWAITING_APPROVAL",
    "RESOLVED",
)
