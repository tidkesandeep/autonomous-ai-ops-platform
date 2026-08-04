#!/usr/bin/env python3
"""Set Ready / In progress / Done Status on GitHub Project #6 from issue title prefixes."""

from __future__ import annotations

import json
import subprocess
import sys

OWNER = "tidkesandeep"
PROJECT_NUMBER = "6"
PROJECT_ID = "PVT_kwHOCATBWM4BfVed"
STATUS_FIELD = "PVTSSF_lAHOCATBWM4BfVedzhZpFgE"
READY_ID = "61e4505c"
IN_PROGRESS_ID = "47fc9ee4"
DONE_ID = "98236657"


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def main() -> int:
    listed = run(
        [
            "gh",
            "project",
            "item-list",
            PROJECT_NUMBER,
            "--owner",
            OWNER,
            "--format",
            "json",
            "--limit",
            "100",
        ]
    )
    if listed.returncode != 0:
        print(listed.stderr, file=sys.stderr)
        return listed.returncode

    data = json.loads(listed.stdout)
    items = data.get("items") or data
    failures = 0

    for it in items:
        title = it.get("title") or ""
        item_id = it["id"]
        if title.startswith("[In Progress]"):
            option_id = IN_PROGRESS_ID
        elif title.startswith("[Todo]"):
            option_id = READY_ID
        elif title.startswith("[Done]"):
            option_id = DONE_ID
        else:
            continue

        print(f"→ {title}")
        edited = run(
            [
                "gh",
                "project",
                "item-edit",
                "--project-id",
                PROJECT_ID,
                "--id",
                item_id,
                "--field-id",
                STATUS_FIELD,
                "--single-select-option-id",
                option_id,
            ]
        )
        if edited.returncode != 0:
            failures += 1
            print(f"  FAILED: {edited.stderr.strip()}")
        else:
            print("  ok")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
