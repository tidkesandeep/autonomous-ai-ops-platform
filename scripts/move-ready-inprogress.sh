#!/usr/bin/env bash
# Move Ready / In Progress (/ Done) cards into the correct Status columns on Project #6.
#   ./scripts/move-ready-inprogress.sh

set -euo pipefail

OWNER="tidkesandeep"
PROJECT_NUMBER="${PROJECT_NUMBER:-6}"
PROJECT_ID="PVT_kwHOCATBWM4BfVed"
STATUS_FIELD="PVTSSF_lAHOCATBWM4BfVedzhZpFgE"
READY_ID="61e4505c"
IN_PROGRESS_ID="47fc9ee4"
DONE_ID="98236657"

echo "Updating Status on project #${PROJECT_NUMBER}…"

tmp=$(mktemp)
gh project item-list "$PROJECT_NUMBER" --owner "$OWNER" --format json --limit 100 >"$tmp"

mapfile -t rows < <(python3 - "$tmp" "$READY_ID" "$IN_PROGRESS_ID" "$DONE_ID" <<'PY'
import json, sys
path, ready, in_prog, done = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
data = json.load(open(path))
items = data.get("items") or data
for it in items:
    title = it.get("title") or ""
    item_id = it["id"]
    if title.startswith("[In Progress]"):
        print(f"{item_id}\t{in_prog}\t{title}")
    elif title.startswith("[Todo]"):
        print(f"{item_id}\t{ready}\t{title}")
    elif title.startswith("[Done]"):
        print(f"{item_id}\t{done}\t{title}")
PY
)

rm -f "$tmp"

for row in "${rows[@]}"; do
  IFS=$'\t' read -r item_id option_id title <<<"$row"
  echo "→ $title"
  if gh project item-edit \
    --project-id "$PROJECT_ID" \
    --id "$item_id" \
    --field-id "$STATUS_FIELD" \
    --single-select-option-id "$option_id" >/dev/null
  then
    echo "  ok"
  else
    echo "  FAILED (need project write scope on this token)"
  fi
done

echo "Done. Open: gh project view $PROJECT_NUMBER --owner $OWNER --web"
