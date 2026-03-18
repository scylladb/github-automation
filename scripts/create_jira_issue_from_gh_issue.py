#!/usr/bin/env python3
"""
create_jira_issue_from_gh_issue.py - Create a Jira issue from a newly opened GitHub issue.

Reads environment variables set by the GitHub Actions workflow and creates
a corresponding Jira issue in the configured project, including:
  - Title, description, labels, issue type mapping
  - Assignee / reporter lookup (best-effort by display name)
  - Clickable cross-links in both the Jira description footer and
    the GitHub issue body footer

Reuses HTTP helpers from jira_sync_modules to avoid code duplication.
"""

import json
import os
import sys

from jira_sync_modules import (
    _jira_get,
    _jira_post,
    _jira_put,
    _gh_api,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JIRA_BASE_URL = "https://scylladb.atlassian.net"

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
JIRA_AUTH = os.environ.get("JIRA_AUTH", "")          # email:api_token
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
JIRA_PROJECT_KEY = os.environ.get("JIRA_PROJECT_KEY", "")

# GitHub issue payload fields (set by the workflow)
ISSUE_TITLE = os.environ.get("ISSUE_TITLE", "")
ISSUE_BODY = os.environ.get("ISSUE_BODY", "")
ISSUE_NUMBER = os.environ.get("ISSUE_NUMBER", "")
ISSUE_HTML_URL = os.environ.get("ISSUE_HTML_URL", "")
ISSUE_LABELS = os.environ.get("ISSUE_LABELS", "")     # comma-separated
ISSUE_TYPE = os.environ.get("ISSUE_TYPE", "")          # GitHub issue type name
ISSUE_MILESTONE = os.environ.get("ISSUE_MILESTONE", "")
OWNER_REPO = os.environ.get("OWNER_REPO", "")

# Assignee / reporter (GitHub login + display name)
GH_ASSIGNEE_NAME = os.environ.get("GH_ASSIGNEE_NAME", "")
GH_REPORTER_NAME = os.environ.get("GH_REPORTER_NAME", "")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _map_issue_type(gh_type_name: str, labels: list[str]) -> str:
    """Map a GitHub issue type / labels to a Jira issue type name."""
    if gh_type_name:
        low = gh_type_name.lower()
        if low == "bug":
            return "Bug"
        if low in ("enhancement", "feature"):
            return "Enhancement"
        if low == "epic":
            return "Epic"
    label_set = {l.lower() for l in labels}
    if "bug" in label_set:
        return "Bug"
    if "enhancement" in label_set:
        return "Enhancement"
    return "Task"


def _find_jira_account_id(display_name: str) -> str | None:
    """Best-effort lookup of a Jira accountId by display name."""
    if not display_name:
        return None
    from urllib.parse import quote
    url = f"{JIRA_BASE_URL}/rest/api/3/user/search?query={quote(display_name)}&maxResults=50"
    result = _jira_get(url, JIRA_AUTH)
    if result and isinstance(result, list):
        for user in result:
            if user.get("displayName", "").lower() == display_name.lower():
                return user["accountId"]
    return None


def _build_description_adf(body_text: str, gh_url: str) -> dict:
    """Build an ADF document from the GH issue body + a footer link."""
    content_nodes = []

    if body_text:
        content_nodes.append({
            "type": "paragraph",
            "content": [{"type": "text", "text": body_text}],
        })

    # Divider before footer
    content_nodes.append({"type": "rule"})

    # Footer with clickable link to original GitHub issue
    content_nodes.append({
        "type": "paragraph",
        "content": [
            {"type": "text", "text": "Original GitHub issue: "},
            {
                "type": "text",
                "text": gh_url,
                "marks": [{"type": "link", "attrs": {"href": gh_url}}],
            },
        ],
    })

    return {"version": 1, "type": "doc", "content": content_nodes}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def create_jira_issue() -> str | None:
    """Create a Jira issue and return its key, or None on failure."""
    labels = [l.strip() for l in ISSUE_LABELS.split(",") if l.strip()]
    issue_type = _map_issue_type(ISSUE_TYPE, labels)

    # Determine priority from labels
    priority = None
    priority_labels = {"P0", "P1", "P2", "P3", "P4"}
    for label in labels:
        if label in priority_labels:
            priority = label
            break

    description_adf = _build_description_adf(ISSUE_BODY, ISSUE_HTML_URL)

    payload: dict = {
        "fields": {
            "project": {"key": JIRA_PROJECT_KEY},
            "summary": ISSUE_TITLE,
            "description": description_adf,
            "issuetype": {"name": issue_type},
            "labels": labels if labels else [],
        }
    }

    if ISSUE_MILESTONE:
        payload["fields"]["fixVersions"] = [{"name": ISSUE_MILESTONE}]

    if priority:
        payload["fields"]["priority"] = {"name": priority}

    # Reporter
    reporter_id = _find_jira_account_id(GH_REPORTER_NAME)
    if reporter_id:
        payload["fields"]["reporter"] = {"accountId": reporter_id}

    print(f"Creating Jira issue in project {JIRA_PROJECT_KEY}")
    print(f"  type     = {issue_type}")
    print(f"  summary  = {ISSUE_TITLE!r}")
    print(f"  labels   = {labels}")
    print(f"  priority = {priority}")
    print(f"  reporter = {GH_REPORTER_NAME!r} -> {reporter_id}")

    url = f"{JIRA_BASE_URL}/rest/api/3/issue"
    status, body = _jira_post(url, payload, JIRA_AUTH)

    if status not in (200, 201):
        print(f"ERROR: Jira issue creation failed (HTTP {status})")
        print(body)
        return None

    jira_key = json.loads(body)["key"]
    print(f"Created Jira issue: {jira_key}")

    # Assign (separate call - assignment often requires different permissions)
    assignee_id = _find_jira_account_id(GH_ASSIGNEE_NAME)
    if assignee_id:
        print(f"  assignee = {GH_ASSIGNEE_NAME!r} -> {assignee_id}")
        assign_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{jira_key}/assignee"
        a_status, a_body = _jira_put(assign_url, {"accountId": assignee_id}, JIRA_AUTH)
        if a_status not in (200, 204):
            print(f"WARNING: Could not assign {jira_key} (HTTP {a_status}): {a_body}")

    return jira_key


def append_jira_link_to_gh_issue(jira_key: str) -> None:
    """Append a Jira link footer to the GitHub issue body."""
    jira_url = f"{JIRA_BASE_URL}/browse/{jira_key}"
    footer = f"\n\n---\nJira issue: [{jira_key}]({jira_url})"

    current_body = ISSUE_BODY or ""
    new_body = current_body + footer

    owner, repo = OWNER_REPO.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/issues/{ISSUE_NUMBER}"

    status, body = _gh_api("PATCH", url, GITHUB_TOKEN, {"body": new_body})

    if status == 200:
        print(f"Updated GitHub issue #{ISSUE_NUMBER} with Jira link to {jira_key}")
    else:
        print(f"WARNING: Failed to update GitHub issue #{ISSUE_NUMBER} (HTTP {status})")
        print(body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print(" sync_gh_issue_to_jira")
    print("=" * 60)

    if not JIRA_AUTH:
        print("ERROR: JIRA_AUTH env var is not set.")
        sys.exit(1)
    if not JIRA_PROJECT_KEY:
        print("ERROR: JIRA_PROJECT_KEY env var is not set.")
        sys.exit(1)
    if not ISSUE_TITLE:
        print("ERROR: ISSUE_TITLE env var is not set.")
        sys.exit(1)
    if not GITHUB_TOKEN:
        print("ERROR: GITHUB_TOKEN env var is not set.")
        sys.exit(1)

    jira_key = create_jira_issue()
    if not jira_key:
        print("ERROR: Failed to create Jira issue. Exiting.")
        sys.exit(1)

    append_jira_link_to_gh_issue(jira_key)

    print("=" * 60)
    print(f" Done. GitHub #{ISSUE_NUMBER} -> {jira_key}")
    print("=" * 60)


if __name__ == "__main__":
    main()