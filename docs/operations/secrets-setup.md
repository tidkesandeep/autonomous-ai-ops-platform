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

### 6.1 Gemini API key (Google AI Studio) — required for real embeddings

**What this is for in our project**
- Runbook embeddings (`text-embedding-004` via LiteLLM)
- Optional RCA narrative polish if Groq is not set

**Direct link:** https://aistudio.google.com/app/apikey  
(also works: https://aistudio.google.com/apikey)

#### Step A — Open AI Studio and sign in

1. Open https://aistudio.google.com/app/apikey in Chrome/Firefox.  
2. Click **Sign in** with the Google account you want to own the key  
   (personal Gmail is fine; Workspace accounts also work).  
3. If you see **Get started** / Terms of Service:
   - Read and **Accept** (required once).  
4. If the left nav is collapsed, expand it.

#### Step B — Land on the API Keys page

You should see a page titled something like **API keys** / **Get API key**.

If you landed on the AI Studio home instead:
1. Left sidebar → click **Get API key** (key icon), **or**  
2. Paste https://aistudio.google.com/app/apikey again.

#### Step C — Create the key (project choice)

1. Click **Create API key** (top of the page).  
2. A dialog asks which Google Cloud project to attach the key to. Choose:

| Dialog option | Choose when | Recommendation |
|---|---|---|
| **Create API key in new project** | You are new / want a clean project | **Preferred for this capstone** |
| **Create API key in existing project** | You already have a GCP project for AI | Fine if you know which project |
| **Import projects** (if shown first) | AI Studio does not list your GCP projects yet | Import the project, then Create API key |

3. If you chose **new project**:
   - Google auto-creates a Cloud project behind the scenes.  
   - You usually do **not** need billing enabled for free-tier Gemini API usage, but quotas apply.  
4. If you chose **existing project**:
   - Pick the project from the dropdown → confirm.  
5. Click **Create** / **Create key** in the dialog.

#### Step D — Copy and save the key

1. The key appears on screen. It typically looks like:  
   `AIza…` (about 39 characters).  
2. Click **Copy**.  
3. Paste into a **password manager** immediately.  
4. You can usually re-view keys later on the same API keys page, but treat “copy now” as mandatory habit.

#### Step E — (Optional) name / restrict

1. On the API keys list, open the key’s **⋯** menu if available → rename to `aiops-embeddings`.  
2. Optional hardening (later):  
   [Google Cloud Credentials](https://console.cloud.google.com/apis/credentials) → select the key →  
   **API restrictions** → restrict to **Generative Language API**  
   (`generativelanguage.googleapis.com`).  
   AI Studio keys are often Gemini-restricted by default — that is OK.

#### Step F — Store in Databricks (do not paste in chat)

```bash
cd /path/to/autonomous-ai-ops-platform
./scripts/put-aiops-secret.sh gemini-api-key
# paste AIza… when prompted (input is hidden)

# optional — our code already defaults to this
./scripts/put-aiops-secret.sh embedding-model gemini/text-embedding-004
```

#### Step G — Quick local smoke test (optional)

```bash
export GEMINI_API_KEY='AIza…'   # this shell only; never commit
curl -sS "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent?key=${GEMINI_API_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"model":"models/text-embedding-004","content":{"parts":[{"text":"hello aiops"}]}}' \
  | head -c 300; echo
```

Expect JSON containing an `embedding` / `values` array — **not** `API_KEY_INVALID`.

#### Gemini troubleshooting

| Symptom | What to do |
|---|---|
| Permission / IAM error on Create | Use a personal Gmail, or create key in a project you own (not a locked org project) |
| “Import projects” empty | Create API key in **new** project instead |
| `API_KEY_INVALID` | Regenerated wrong; create a new key and replace `aiops/gemini-api-key` |
| Quota / ResourceExhausted | Wait / check AI Studio usage; free tier has limits |
| Embed job still says `hash` | Secret not hydrated — see Point 7 checklist |

---

### 6.2 Groq API key (optional — RCA narrative polish)

**What this is for in our project**
- Optional LLM “polish” pass in `src/agent/graph.py` (fast Llama models).  
- **Not** required for embeddings (those use Gemini).  
- If Groq is unset but Gemini is set, polish can still use Gemini.  
- If both unset, heuristic RCA still works (Phase 4 rubric already passed).

**Direct link:** https://console.groq.com/keys

#### Step A — Create / sign in to GroqCloud

1. Open https://console.groq.com  
2. Click **Sign up** or **Sign in**.  
3. Choose one of:
   - **Continue with Google** (fastest), or  
   - **Continue with GitHub**, or  
   - Email + password  
4. Verify email if asked.  
5. Accept the Services Agreement on first login.  
6. **No credit card** is required for the free tier.

#### Step B — Open API Keys

1. In the left sidebar, click **API Keys**, **or**  
2. Go directly to https://console.groq.com/keys  

You should see a table of keys (empty at first) and a **Create API Key** button.

> Note: If you are on a Groq **team** org, only **owners** / **developer** roles can create keys.

#### Step C — Create the key

1. Click **Create API Key**.  
2. In the dialog:
   - **Name:** `aiops-llm-polish`  
   - (Leave other fields at defaults unless you need expiry.)  
3. Click **Submit** / **Create**.  

#### Step D — Copy once (critical)

1. Groq shows the full secret **once**. It looks like:  
   `gsk_…`  
2. Click **Copy** immediately.  
3. Save in a password manager.  
4. If you close the dialog without copying → you **cannot** view it again.  
   Create a new key and delete the lost one.

#### Step E — Store in Databricks

```bash
./scripts/put-aiops-secret.sh groq-api-key
# paste gsk_… when prompted
```

#### Step F — Optional smoke test

```bash
export GROQ_API_KEY='gsk_…'
curl -sS https://api.groq.com/openai/v1/models \
  -H "Authorization: Bearer $GROQ_API_KEY" | head -c 400; echo
```

Expect a JSON list of models — not `Invalid API Key`.

#### Groq troubleshooting

| Symptom | What to do |
|---|---|
| Cannot create key | Check org role (need owner/developer) or create a personal org |
| Lost the `gsk_` value | Create a new key; revoke the old unnamed one |
| 429 rate limit | Free tier RPM/TPM limits; retry later or upgrade tier |
| Agent still heuristic-only | Confirm job log `secrets` loaded `GROQ_API_KEY` |

---

### 6.3 How our code uses these keys

| Env var | Source secret | Used for |
|---|---|---|
| `GEMINI_API_KEY` | `aiops/gemini-api-key` | Embeddings + fallback LLM polish |
| `GROQ_API_KEY` | `aiops/groq-api-key` | Preferred LLM polish if set |
| `EMBEDDING_MODEL` | `aiops/embedding-model` | Default `gemini/text-embedding-004` |

Agent polish order (`src/agent/graph.py`): **Groq → Gemini → heuristic**.  
Embeddings: Gemini when keyed, else deterministic **hash** (still valid).

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
