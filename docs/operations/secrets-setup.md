# Operator secrets setup — points 2, 4, 5, 6, 7

Secrets live in Databricks scope **`aiops`** (never commit values). Jobs hydrate
them into env vars via `src.common.secrets.hydrate_env_from_secret_scope()`.

| Secret key | Env var | Used for |
|---|---|---|
| `slack-webhook-url` | `SLACK_WEBHOOK_URL` | Incident + RCA Slack posts |
| `github-token` | `GITHUB_TOKEN` | Commit/PR correlation |
| `github-repo` | `GITHUB_REPO` | Default `tidkesandeep/autonomous-ai-ops-platform` |
| `gemini-api-key` | `GEMINI_API_KEY` | Embeddings + optional LLM polish |
| `groq-api-key` | `GROQ_API_KEY` | Optional LLM polish (LiteLLM) |
| `embedding-model` | `EMBEDDING_MODEL` | Optional override (default `gemini/text-embedding-004`) |

Local CLI helper: `scripts/put-aiops-secret.sh <key>`.

---

## 2) Rotate Databricks PAT

**Why:** A PAT was shared earlier in chat; treat it as compromised.

### Steps (UI — recommended)

1. Open workspace → **Settings** → **Developer** → **Access tokens**.
2. Click **Generate new token**.
   - Comment: `aiops-local-2026-08`
   - Lifetime: 90 days (or your policy)
3. **Copy the token once** (it will not be shown again).
4. On your laptop / this agent VM, update `~/.databrickscfg`:

```ini
[DEFAULT]
host = https://dbc-da72c144-83db.cloud.databricks.com
token = <NEW_TOKEN_HERE>
```

5. Verify:

```bash
databricks auth profiles
databricks current-user me
```

6. In **Access tokens**, **revoke/delete** every older PAT you no longer need
   (especially any that may have been pasted in chat).
7. Do **not** paste the new token into GitHub issues, Slack, or Cursor chat.

### Steps (CLI)

```bash
databricks tokens create --comment "aiops-rotated" --lifetime-seconds 7776000
# Copy token_value from JSON → write into ~/.databrickscfg
# Then delete old token ids:
databricks tokens list -o json
databricks tokens delete --token-id <OLD_TOKEN_ID>
```

---

## 4) Slack webhook (`SLACK_WEBHOOK_URL`)

1. In Slack: create (or open) a workspace channel, e.g. `#aiops-incidents`.
2. [Incoming Webhooks](https://api.slack.com/messaging/webhooks) → **Add to Slack**
   → pick the channel → **Copy Webhook URL**
   (`https://hooks.slack.com/services/...`).
3. Store in Databricks (do not commit):

```bash
./scripts/put-aiops-secret.sh slack-webhook-url
# paste URL when prompted
```

4. Smoke test after a detection/agent run — you should see messages for
   incident opened and RCA ready. Until set, notifier no-ops but still audits.

---

## 5) GitHub token (`GITHUB_TOKEN`)

1. GitHub → **Settings** → **Developer settings** → **Personal access tokens**.
2. Create a **fine-grained** token (preferred) or classic `ghp_…`:
   - Resource: `tidkesandeep/autonomous-ai-ops-platform`
   - Permissions: **Contents: Read-only** (and Metadata)
3. Store:

```bash
./scripts/put-aiops-secret.sh github-token
./scripts/put-aiops-secret.sh github-repo tidkesandeep/autonomous-ai-ops-platform
```

4. Confirm on next `ops-run-agent` that `correlate_github_commits` returns SHAs
   (not `"GITHUB_TOKEN unset"`).

---

## 6) Gemini / Groq API keys

### Gemini (embeddings + optional polish) — recommended for point 7

1. Open [Google AI Studio](https://aistudio.google.com/apikey) → **Create API key**.
2. Store:

```bash
./scripts/put-aiops-secret.sh gemini-api-key
# optional:
./scripts/put-aiops-secret.sh embedding-model gemini/text-embedding-004
```

### Groq (optional narrative polish)

1. Open [Groq Console](https://console.groq.com/keys) → create key.
2. Store:

```bash
./scripts/put-aiops-secret.sh groq-api-key
```

Agent polish uses Groq if `GROQ_API_KEY` is set, else Gemini if `GEMINI_API_KEY` is set
(`src/agent/graph.py`).

---

## 7) Re-embed runbooks with Gemini

After `gemini-api-key` is in scope `aiops`:

```bash
# Ensure workspace code + secrets hydrate are deployed
databricks bundle deploy -t dev   # or sync notebooks/src as usual

# Run embed job (canonical non-dev job id may differ; prefer bundle)
databricks bundle run ops_embed_runbooks -t dev
# OR
databricks jobs run-now --json '{"job_id":341843632355099}'
```

Check the job output for `backend` / fingerprint starting with `api:gemini/...`
(not `hash`). Then run a short agent investigation to confirm RAG still hits.

---

## Verify all secrets present (names only)

```bash
databricks secrets list-secrets aiops
```

Expected keys: `slack-webhook-url`, `github-token`, `github-repo`,
`gemini-api-key`, and optionally `groq-api-key`, `embedding-model`.
