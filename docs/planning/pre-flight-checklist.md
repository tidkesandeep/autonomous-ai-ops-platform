# Pre-flight Checklist — Free Edition De-risks

Complete these in Week 1 before building past the monitored platform.
Record the chosen deployment path from §8b of the implementation plan in the README.

## Hard dependencies

| # | Check | Pass criteria | Result |
|---|---|---|---|
| 1 | Databricks Apps | Hello-world Streamlit deploys as a Databricks App | ☐ |
| 2 | Lakebase + sync | Create DB + one table; synced table appears in Unity Catalog; document how to force a refresh | ☐ |
| 3a | Embedding API | Databricks job returns vectors via Gemini `text-embedding-004` (LiteLLM) | ☐ |
| 3b | Inter-job auth | Job with secret-stored PAT/SP can call `jobs/run-now` and `runs/get-output` | ☐ |
| 3c | `run_if = ALL_DONE` | Final detection task still runs when an upstream task fails (DABs) | ☐ |
| 4 | Slack + GitHub | Incoming webhook posts a test message; read-only PAT lists commits | ☐ |

## Deployment path (pick one)

- [ ] **A** — Lakebase + Databricks App + synced tables (target)
- [ ] **B** — Lakebase + Streamlit Community Cloud
- [ ] **C** — Neon + Databricks App + JDBC MERGE
- [ ] **D** — Neon + Community Cloud (**last resort; fails Databricks App requirement**)

**Chosen path:** _TBD after pre-flight_

## Notes

_Record blockers, Free Edition limits, and any substitutions here._
