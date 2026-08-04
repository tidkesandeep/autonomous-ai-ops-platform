#!/usr/bin/env bash
# Add repo issues to GitHub Project #6 and set Status from the [Done]/[Todo]/… title prefix.
# Run as YOU (tidkesandeep), not the Cursor bot:
#   gh auth login
#   gh auth refresh -s project,read:project
#   ./scripts/sync-project-board.sh

set -euo pipefail

OWNER="tidkesandeep"
REPO="tidkesandeep/autonomous-ai-ops-platform"
PROJECT_NUMBER="${PROJECT_NUMBER:-6}"

echo "Using project #${PROJECT_NUMBER} owned by ${OWNER}"

# Resolve project + Status field + option IDs
meta=$(gh project field-list "$PROJECT_NUMBER" --owner "$OWNER" --format json)
echo "$meta" | python3 -c "import sys,json; d=json.load(sys.stdin);
fields=d.get('fields') or d.get('items') or (d if isinstance(d,list) else [])
print('fields:', [ (f.get('name'), f.get('type') or f.get('dataType')) for f in fields ])"

PROJECT_ID=$(gh project view "$PROJECT_NUMBER" --owner "$OWNER" --format json --jq .id)
STATUS_FIELD_ID=$(gh project field-list "$PROJECT_NUMBER" --owner "$OWNER" --format json \
  --jq '.fields[] | select(.name=="Status") | .id' 2>/dev/null || true)

if [[ -z "${STATUS_FIELD_ID}" ]]; then
  STATUS_FIELD_ID=$(gh project field-list "$PROJECT_NUMBER" --owner "$OWNER" --format json | \
    python3 -c "import sys,json; d=json.load(sys.stdin); fields=d.get('fields',d if isinstance(d,list) else []);
print(next(f['id'] for f in fields if f.get('name')=='Status'))")
fi

# Map title prefix -> Status option name (Board template defaults)
status_option_id() {
  local name="$1"
  gh project field-list "$PROJECT_NUMBER" --owner "$OWNER" --format json | python3 -c "
import sys, json
name = '''$name'''
d = json.load(sys.stdin)
fields = d.get('fields', d if isinstance(d, list) else [])
status = next(f for f in fields if f.get('name') == 'Status')
opts = status.get('options') or []
# fuzzy match
for o in opts:
    if o.get('name','').lower() == name.lower():
        print(o['id']); raise SystemExit
# common aliases
aliases = {
  'Todo': ['Todo', 'To do', 'Ready'],
  'In Progress': ['In Progress', 'In progress', 'Doing'],
  'Done': ['Done', 'Complete', 'Completed'],
  'Backlog': ['Backlog', 'No Status', 'Todo'],
}
for cand in aliases.get(name, [name]):
    for o in opts:
        if o.get('name','').lower() == cand.lower():
            print(o['id']); raise SystemExit
print('OPTIONS:', [(o.get('name'), o.get('id')) for o in opts], file=sys.stderr)
raise SystemExit('missing status option: ' + name)
"
}

DONE_ID=$(status_option_id "Done")
PROG_ID=$(status_option_id "In Progress")
TODO_ID=$(status_option_id "Todo")
# Backlog may not exist on default board — fall back to Todo
BACKLOG_ID=$(status_option_id "Backlog" 2>/dev/null || echo "$TODO_ID")

echo "Status field: $STATUS_FIELD_ID"
echo "Options: Done=$DONE_ID InProgress=$PROG_ID Todo=$TODO_ID Backlog=$BACKLOG_ID"

map_status() {
  case "$1" in
    \[Done\]*) echo "$DONE_ID" ;;
    \[In\ Progress\]*) echo "$PROG_ID" ;;
    \[Todo\]*) echo "$TODO_ID" ;;
    \[Backlog\]*) echo "$BACKLOG_ID" ;;
    *) echo "$TODO_ID" ;;
  esac
}

# Skip the probe issue
gh issue list --repo "$REPO" --state open --limit 100 --json number,title,url | python3 -c "
import json,sys
issues=json.load(sys.stdin)
for i in issues:
    if i['title']=='test-kanban-access':
        continue
    print(f\"{i['number']}\t{i['title']}\t{i['url']}\")
" | while IFS=$'\t' read -r num title url; do
  echo "→ #$num $title"
  item_id=$(gh project item-add "$PROJECT_NUMBER" --owner "$OWNER" --url "$url" --format json --jq .id)
  opt=$(map_status "$title")
  gh project item-edit --project-id "$PROJECT_ID" --id "$item_id" \
    --field-id "$STATUS_FIELD_ID" --single-select-option-id "$opt" >/dev/null
  echo "  added ($item_id) → status option $opt"
done

echo "Board sync complete. Open:"
echo "  gh project view $PROJECT_NUMBER --owner $OWNER --web"
