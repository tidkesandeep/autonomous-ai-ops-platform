#!/usr/bin/env bash
# Put a secret into the Databricks `aiops` scope without echoing it to shell history
# when used with a here-string / prompt.
#
# Usage:
#   ./scripts/put-aiops-secret.sh slack-webhook-url
#   ./scripts/put-aiops-secret.sh github-token
#   ./scripts/put-aiops-secret.sh gemini-api-key
#   ./scripts/put-aiops-secret.sh groq-api-key
#   ./scripts/put-aiops-secret.sh github-repo tidkesandeep/autonomous-ai-ops-platform
set -euo pipefail

SCOPE="${AIOPS_SECRET_SCOPE:-aiops}"
KEY="${1:-}"
VALUE_ARG="${2:-}"

if [[ -z "$KEY" ]]; then
  echo "Usage: $0 <secret-key> [value]" >&2
  echo "Keys: slack-webhook-url | github-token | github-repo | gemini-api-key | groq-api-key | embedding-model" >&2
  exit 1
fi

databricks secrets create-scope "$SCOPE" --initial-manage-principal users 2>/dev/null || true

if [[ -n "$VALUE_ARG" ]]; then
  databricks secrets put-secret "$SCOPE" "$KEY" --string-value "$VALUE_ARG"
else
  echo "Enter secret value for ${SCOPE}/${KEY} (input hidden):" >&2
  read -r -s VALUE
  echo >&2
  databricks secrets put-secret "$SCOPE" "$KEY" --string-value "$VALUE"
fi

echo "Stored ${SCOPE}/${KEY}"
databricks secrets list-secrets "$SCOPE"
