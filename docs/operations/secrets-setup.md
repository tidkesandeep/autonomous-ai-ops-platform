# Operator secrets — in-depth setup (points 2, 4, 5, 6, 7)

This guide is written for a first-time setup. Check boxes as you go.
**Never paste secret values into GitHub, Slack, or Cursor chat.**

Jobs load secrets from Databricks scope **`aiops`** via
`src.common.secrets.hydrate_env_from_secret_scope()`.

Helper (from repo root, with `databricks` CLI authenticated):

```bash
./scripts/put-aiops-secret.sh <secret-key>
```

| Secret key in Databricks | Env var jobs see | Point |
|---|---|---|
| `slack-webhook-url` | `SLACK_WEBHOOK_URL` | 4 |
| `github-token` | `GITHUB_TOKEN` | 5 |
| `github-repo` | `GITHUB_REPO` | 5 |
| `gemini-api-key` | `GEMINI_API_KEY` | 6 |
| `groq-api-key` | `GROQ_API_KEY` | 6 (optional) |
| `embedding-model` | `EMBEDDING_MODEL` | 6 (optional) |

---

## Point 2 — Rotate Databricks PAT

**Status on agent VM:** already rotated (`aiops-rotated-2026-08-05`); old `cursor-dev` deleted.
Do this on **your laptop** too if that machine still has the old token.

### 2.1 Create a new token (UI)

1. Open your workspace:  
   https://dbc-da72c144-83db.cloud.databricks.com
2. Click your **user icon** (top right) → **Settings**.
3. Left sidebar → **Developer** → **Access tokens**.
4. Click **Generate new token**.
5. Fill in:
   - **Comment:** `aiops-laptop-2026-08`
   - **Lifetime (days):** `90` (or your policy)
6. Click **Generate**.
7. **Copy the token immediately** into a password manager. Slack/chat will never see it again after you leave this page.

### 2.2 Put it in your local CLI config

1. On your machine, open (or create) `~/.databrickscfg`:

```ini
[DEFAULT]
host = https://dbc-da72c144-83db.cloud.databricks.com
token = <PASTE_NEW_TOKEN_HERE>
```

2. Restrict file permissions:

```bash
chmod 600 ~/.databrickscfg
```

3. Verify:

```bash
databricks auth profiles
databricks current-user me
```

You should see `sandeeptidke.work@gmail.com`.

### 2.3 Revoke old tokens

1. Still on **Settings → Developer → Access tokens**.
2. For every token you no longer use (especially anything created before this rotation / anything that may have been pasted in chat): click **…** → **Delete** / **Revoke**.
3. Keep only the new laptop token (and the agent token if you still need the cloud agent).

### 2.2b Alternate — CLI create (optional)

```bash
databricks tokens create --comment "aiops-laptop" --lifetime-seconds 7776000 -o json
# Copy token_value into ~/.databrickscfg
databricks tokens list -o json
databricks tokens delete <OLD_TOKEN_ID>
```

---

## Point 4 — Slack Incoming Webhook

We only need a **one-way post into one channel**. That means a simple Slack **App + Incoming Webhooks**.  
We do **not** need a bot that listens, AI Agent features, or a starter template.

### 4.0 What to choose on “Create an app” (important)

Go to: https://api.slack.com/apps → **Create New App**.

You may see options like:

| Option you might see | Choose it? | Why |
|---|---|---|
| **From scratch** | **YES — pick this** | Manual, minimal app. Perfect for Incoming Webhooks only. |
| **From a manifest** | No | YAML/JSON blueprint for complex apps (events, slash commands). Overkill here. |
| **AI agent** / Agent template | No | Builds an interactive AI agent in Slack. Not what we need. |
| **Starter app** / Bolt / Socket Mode templates | No | Adds servers, OAuth, interactivity we do not use. |

**Decision: Create New App → From scratch.**

### 4.1 Create the app

1. Open https://api.slack.com/apps  
2. Click **Create New App**.  
3. Choose **From scratch**.  
4. **App Name:** `AI Ops Notifier` (any clear name).  
5. **Pick a workspace:** the Slack workspace where you want alerts (must be one you admin/can install apps into).  
6. Click **Create App**.

You land on the app’s **Basic Information** page.

### 4.2 (Recommended) Create the destination channel first

1. In Slack desktop/web, create a channel, e.g. `#aiops-incidents` (private or public).  
2. Stay in that workspace for the next install step.

### 4.3 Turn on Incoming Webhooks

1. In the Slack app config (left sidebar) open **Features → Incoming Webhooks**.  
2. Toggle **Activate Incoming Webhooks** to **On**.  
3. Scroll to **Webhook URLs for Your Workspace**.  
4. Click **Add New Webhook to Workspace**.  
5. In the permission screen:
   - **Post to:** select `#aiops-incidents` (or your channel).  
   - Click **Allow**.  
6. You now see a URL like:

```text
https://hooks.slack.com/services/T…/B…/…
```

7. Click **Copy**. Treat it like a password.

### 4.4 Store it in Databricks (do not commit)

From the repo root on a machine with Databricks CLI auth:

```bash
cd /path/to/autonomous-ai-ops-platform
./scripts/put-aiops-secret.sh slack-webhook-url
# When prompted, paste the webhook URL and press Enter
```

Or non-interactive (still avoid shell history if you can):

```bash
./scripts/put-aiops-secret.sh slack-webhook-url 'https://hooks.slack.com/services/T…/B…/…'
```

Confirm name only (value is never listed):

```bash
databricks secrets list-secrets aiops
# expect: slack-webhook-url
```

### 4.5 Smoke test the webhook itself

```bash
curl -sS -X POST "$SLACK_WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  -d '{"text":":white_check_mark: AI Ops webhook test"}'
```

Slack should reply `ok` and the channel should show the message.

### 4.6 Smoke test from the platform

1. Run detection or agent (either is fine), e.g. open an incident then run `ops-run-agent`.  
2. Expect:
   - Incident-opened style message (from detection), and/or  
   - RCA-ready message (from the agent).  
3. If nothing appears: job log should no longer say `SLACK_WEBHOOK_URL unset` after secrets hydrate prints `loaded: ['SLACK_WEBHOOK_URL', …]`.

---

## Point 5 — GitHub token (commit correlation)

The agent calls GitHub to list recent commits on this repo. **Read-only** is enough.

### 5.1 Prefer fine-grained PAT (recommended)

1. Sign in to GitHub as the user who can read  
   `tidkesandeep/autonomous-ai-ops-platform`.  
2. Click your avatar (top right) → **Settings**.  
3. Scroll left sidebar → **Developer settings**.  
4. **Personal access tokens** → **Fine-grained tokens**.  
5. Click **Generate new token**.  
6. You may need to confirm password / 2FA.

### 5.2 Token form fields (exact choices)

| Field | What to enter |
|---|---|
| **Token name** | `aiops-commit-correlation` |
| **Description** | `Read commits for RCA correlation` |
| **Resource owner** | Your user (or the org that owns the repo) |
| **Expiration** | `90 days` (or custom) |
| **Repository access** | **Only select repositories** → choose `autonomous-ai-ops-platform` |
| **Repository permissions → Contents** | **Read-only** |
| **Repository permissions → Metadata** | **Read-only** (usually auto-selected) |
| **Account permissions** | Leave all **No access** |

Do **not** enable Administration, Secrets, Workflows, etc.

7. Click **Generate token**.  
8. **Copy** the token (`github_pat_…` for fine-grained). Store in a password manager.

### 5.3 Classic PAT alternative (only if fine-grained is blocked)

1. **Developer settings → Personal access tokens → Tokens (classic)**.  
2. **Generate new token (classic)**.  
3. Note: `aiops-commit-correlation`.  
4. Scopes: check **`public_repo`** if the repo is public, or **`repo`** if private (broader — prefer fine-grained).  
5. Generate → copy `ghp_…`.

### 5.4 Store in Databricks

```bash
./scripts/put-aiops-secret.sh github-token
# paste token when prompted

./scripts/put-aiops-secret.sh github-repo tidkesandeep/autonomous-ai-ops-platform
```

```bash
databricks secrets list-secrets aiops
# expect: github-token, github-repo
```

### 5.5 Smoke test

```bash
# Local quick check (token in env only for this shell)
export GITHUB_TOKEN='…'   # do not commit
curl -sS -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://api.github.com/repos/tidkesandeep/autonomous-ai-ops-platform/commits?per_page=1 \
  | head -c 400; echo
```

You should see JSON with a `sha` field (not `Bad credentials`).

Then run `ops-run-agent` on any incident; agent actions / RCA should not say  
`GITHUB_TOKEN unset — skipped live correlation`.

---

## Point 6 — Gemini and (optional) Groq API keys

### 6.1 Gemini (required for real embeddings in point 7)

1. Open Google AI Studio API keys:  
   https://aistudio.google.com/apikey  
   (same as https://aistudio.google.com/app/apikey)  
2. Sign in with the Google account you want billed/quotas on.  
3. Accept Terms if prompted.  
4. Click **Create API key**.  
5. If asked to pick a Google Cloud project:
   - **Create API key in new project**, or  
   - Choose an existing project you own.  
6. Copy the key. New AI Studio keys are typically restricted to the Gemini API — that is fine.  
7. Optional hardening later: Google Cloud Console → APIs & Credentials → restrict key to Generative Language API.

Store:

```bash
./scripts/put-aiops-secret.sh gemini-api-key
# paste key when prompted

# optional explicit model (default already matches our code)
./scripts/put-aiops-secret.sh embedding-model gemini/text-embedding-004
```

### 6.2 Groq (optional — nicer RCA narrative polish)

1. Open https://console.groq.com/keys  
2. Sign up / sign in.  
3. Click **Create API Key**.  
4. Name: `aiops-llm-polish`.  
5. Copy the key.  

Store:

```bash
./scripts/put-aiops-secret.sh groq-api-key
```

**How the agent picks a model** (`src/agent/graph.py`):

- If `GROQ_API_KEY` is set → Groq Llama polish  
- Else if `GEMINI_API_KEY` is set → Gemini flash polish  
- Else → heuristic RCA only (still valid for rubric)

Embeddings use **Gemini** when `GEMINI_API_KEY` (or `GOOGLE_API_KEY`) is set.

### 6.3 Confirm secrets exist

```bash
databricks secrets list-secrets aiops
```

Ideal set:

- `slack-webhook-url`
- `github-token`
- `github-repo`
- `gemini-api-key`
- `groq-api-key` (optional)
- `embedding-model` (optional)

---

## Point 7 — Re-embed runbooks with Gemini

Do this **only after** `gemini-api-key` is in scope `aiops`.

### 7.1 Ensure code is in the workspace

If you have not synced recently:

```bash
# from repo root
databricks workspace import \
  /Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform/src/common/secrets.py \
  --file src/common/secrets.py --format AUTO --overwrite

databricks workspace import \
  /Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform/notebooks/ops/07_embed_runbooks.py \
  --file notebooks/ops/07_embed_runbooks.py --format SOURCE --language PYTHON --overwrite
```

(Or `databricks bundle deploy -t dev`.)

### 7.2 Run the embed job

Canonical job (non-dev):

```bash
databricks jobs run-now --json '{"job_id":341843632355099}' --timeout 30m
```

Or bundle-managed:

```bash
databricks bundle run ops_embed_runbooks -t dev
```

### 7.3 What “success” looks like

1. Job run **SUCCESS**.  
2. Notebook output `secrets` printout includes `GEMINI_API_KEY` under `loaded` (or already set).  
3. Summary / table column `embedding_backend` looks like **`api:gemini/text-embedding-004`** (or similar `api:…`), **not** `hash`.  
4. SQL check:

```sql
SELECT embedding_backend, COUNT(*) AS n
FROM ops.gold.runbook_embeddings
GROUP BY embedding_backend;
```

### 7.4 If it still says `hash`

| Check | Fix |
|---|---|
| `gemini-api-key` missing from `databricks secrets list-secrets aiops` | Re-run put-secret |
| Job printed `missing_secret_keys: ['gemini-api-key', …]` | Scope/key typo; notebook must call hydrate |
| API error then silent fall-back to hash | Open job logs for LiteLLM/Gemini error (quota, bad key) |
| Old hash rows mixed with API rows | Re-run embed; rebuild replaces table |

---

## End-to-end checklist

- [ ] Point 2: New PAT in `~/.databrickscfg`; old tokens revoked  
- [ ] Point 4: Slack app **From scratch** → Incoming Webhooks → URL in `aiops/slack-webhook-url` → curl `ok`  
- [ ] Point 5: Fine-grained PAT Contents:Read → `aiops/github-token` + `github-repo` → commits API works  
- [ ] Point 6: Gemini key in `aiops/gemini-api-key` (Groq optional)  
- [ ] Point 7: `ops-embed-runbooks` shows `api:gemini/...` backends  

When finished, tell the agent “secrets are in `aiops`” (names only) and we can re-run embed + a sample agent job for you without you pasting values into chat.
