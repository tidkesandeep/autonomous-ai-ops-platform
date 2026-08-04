# Pre-flight Checklist — Free Edition De-risks

Completed against workspace `https://dbc-da72c144-83db.cloud.databricks.com`
(user: `sandeeptidke.work@gmail.com`) on 2026-08-04.

## Hard dependencies

| # | Check | Pass criteria | Result |
|---|---|---|---|
| 1 | Databricks Apps | Hello-world Streamlit deploys as a Databricks App | ✅ `aiops-hello` RUNNING — https://aiops-hello-7474653382320337.aws.databricksapps.com |
| 2 | Lakebase + sync | Create DB + one table; bridge into Delta | ✅ Instance `aiops-lakebase` AVAILABLE. Postgres table `preflight_ping` readable as `lakebase_app.public.preflight_ping` and mirrored to `ops.bronze.preflight_ping_delta` via SQL CTAS. Native "synced tables" product is **Delta → Postgres** (opposite direction); analytics path is **Lakebase UC online catalog → scheduled MERGE/CTAS into `ops` Delta**. |
| 3a | Embedding API | Databricks job returns vectors via Gemini `text-embedding-004` | ⬜ Pending — needs `GEMINI_API_KEY` / `GROQ_API_KEY` in secrets |
| 3b | Inter-job auth | Job with PAT can call `jobs/run-now` and `runs/get-output` | ✅ Created `aiops-preflight-job`, `run-now` → SUCCESS |
| 3c | `run_if = ALL_DONE` | Final detection task still runs when upstream fails | ⬜ Pending — verify when wiring DABs detection task |
| 4 | Slack + GitHub | Webhook + read-only PAT | ⬜ Pending — create when needed |

## Workspace bootstrap (done)

- Catalogs: `demo`, `ops` (created via SQL warehouse; CLI create needs Default Storage path)
- Schemas: `demo.{bronze,silver,gold}`, `ops.{bronze,silver,gold}`
- SQL warehouse: `Serverless Starter Warehouse` (`4a3ce36aae2d0b64`)
- Lakebase: `aiops-lakebase` (CU_1, PG 16) + UC online catalog `lakebase_app`
- Hello app source: `/Workspace/Users/sandeeptidke.work@gmail.com/aiops-hello-app`

## Deployment path (pick one)

- [x] **A** — Lakebase + Databricks App + analytics sync via `lakebase_app` → `ops` Delta (CTAS/MERGE)
- [ ] **B** — Lakebase + Streamlit Community Cloud
- [ ] **C** — Neon + Databricks App + JDBC MERGE
- [ ] **D** — Neon + Community Cloud (**last resort**)

**Chosen path:** **A** (with documented analytics sync mechanism above)


## Demo medallion deploy (2026-08-04)

- Job: `demo-medallion-pipeline` (`605068665132316`) — run SUCCESS
- Workspace code: `/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform`
- Row counts: bronze customers 1000 / products 200 / orders 5000 / events 20k / reviews 2000; gold `fact_orders` 5000, `dim_customer` 1000, `dim_product` 191

## Auth notes (for operators)

| Secret | Where |
|---|---|
| Host | `https://dbc-da72c144-83db.cloud.databricks.com` |
| Token | PAT in local `~/.databrickscfg` or `DATABRICKS_TOKEN` — **never commit; rotate if pasted in chat** |
| Lakebase | Host `ep-snowy-violet-d8t4xovo.database.us-east-2.cloud.databricks.com`, db `databricks_postgres`, user = workspace email, password = `databricks database generate-database-credential` JWT |

## Security action required

A PAT was shared in chat during setup. **Revoke it** in Settings → Developer → Access tokens, generate a new one, and keep the new value only in local env / password manager.
