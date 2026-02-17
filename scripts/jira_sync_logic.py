#!/usr/bin/env python3
"""
Jira sync logic for GitHub Actions workflows.

Dispatches to the requested action based on the --action CLI argument.
Currently supports:
  - debug: Log GitHub event context and label-specific transition hints.
  - extract_jira_keys: Extract and validate Jira issue keys from PR title/body.
  - add_label_to_jira_issue: Add a label, priority, or Scylla component to Jira issues.

Usage:
  python3 scripts/jira_sync_logic.py --action debug
  python3 scripts/jira_sync_logic.py --action extract_jira_keys
  python3 scripts/jira_sync_logic.py --action add_label_to_jira_issue

Environment variables (for extract_jira_keys):
  PR_TITLE   - The pull request title
  PR_BODY    - The pull request body
  JIRA_AUTH  - Jira auth credential "<user email>:<api_token>"

Environment variables (for add_label_to_jira_issue):
  JIRA_KEYS_JSON - JSON array of Jira issue keys, e.g. '["STAG-1","STAG-2"]'
  LABEL          - The label to add (plain label, P0-P4 for priority, area/* for Scylla component)
  JIRA_AUTH      - Jira auth credential "<user email>:<api_token>"
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


AVAILABLE_ACTIONS = ['debug', 'extract_jira_keys', 'add_label_to_jira_issue']

KNOWN_PROJECT_PREFIXES = {
    "ANSROLES", "ARGUS", "CE", "CLOUD", "CLOUDEVOPS", "COREPROD",
    "CUSTOMER", "CXTOOLS", "DOCTOR", "DRIVER", "DTEST",
    "FIELDAUTO", "FIELDCLOUD", "FIELDCLUS", "FIELDENG", "ILIAD",
    "OPERATOR", "PKG", "PKGDASH", "PM", "PT", "PUB",
    "QAINFRA", "QATOOLS", "RELENG", "SCT", "SCYLLADB", "SMI",
    "STAG", "TOOLS", "UX", "VECTOR", "WEBINSTALL",
}

JIRA_BASE_URL = "https://scylladb.atlassian.net"
SCYLLA_COMPONENTS_FIELD = "customfield_10321"

# Regex: any JIRA-style key (PROJECT-123) in any text
_JIRA_KEY_RE = re.compile(r'[A-Z]+-[0-9]+')

# Regex: closing keywords followed by a Jira key (optionally as a browse URL)
_CLOSING_KEYWORD_RE = re.compile(
    r'(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)'
    r'\s*[: ]\s*\[?\s*(?:https?://\S*/browse/)?([A-Z]+-[0-9]+)',
    re.IGNORECASE,
)

# Priority names recognised as Jira priority values
_PRIORITY_NAMES = {"P0", "P1", "P2", "P3", "P4"}


def _sanitize(text: str) -> str:
    """Remove carriage returns and backticks (matches the shell workflow)."""
    return text.replace('\r', '').replace('`', ' ')


def _extract_candidate_keys(pr_title: str, pr_body: str) -> list[str]:
    """
    Extract candidate Jira keys from PR title and body.

    Title: any JIRA-style key is accepted.
    Body:  only keys preceded by a closing keyword are accepted.
    Returns a sorted, deduplicated list.
    """
    candidates: set[str] = set()

    title = _sanitize(pr_title)
    body = _sanitize(pr_body)

    # All JIRA keys from title
    candidates.update(_JIRA_KEY_RE.findall(title))

    # Only closing-keyword keys from body
    candidates.update(_CLOSING_KEYWORD_RE.findall(body))

    return sorted(candidates)


def _fetch_jira_project_keys(jira_auth: str) -> set[str]:
    """
    Query the Jira REST API for all project keys.

    jira_auth is expected as "email:api_token" (Basic auth).
    """
    url = f"{JIRA_BASE_URL}/rest/api/3/project/search?maxResults=1000"

    encoded = base64.b64encode(jira_auth.encode()).decode()

    req = Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Basic {encoded}")

    try:
        with urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            return {project["key"] for project in data.get("values", [])}
    except (HTTPError, URLError) as exc:
        print(f"Warning: Jira project lookup failed: {exc}")
        return set()


def extract_jira_keys(pr_title: str, pr_body: str, jira_auth: str) -> list[str]:
    """
    Replicate the extract_jira_keys.yml logic in pure Python.

    1. Extract candidate JIRA keys from the PR title and body.
    2. Accept keys whose project prefix is in the hard-coded set.
    3. For remaining keys, query the Jira API and accept valid prefixes.
    4. Return a sorted, deduplicated list (or ["__NO_KEYS_FOUND__"]).
    """
    candidates = _extract_candidate_keys(pr_title, pr_body)

    if not candidates:
        print("No Jira-like keys found in PR title or body")
        return ["__NO_KEYS_FOUND__"]

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print("Candidate keys:")
    for key in candidates:
        print(f"  {key}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    accepted: list[str] = []
    unknown: list[str] = []

    # --- Pass 1: hard-coded prefixes ---
    print(f"Known project prefixes (hard-coded): {' '.join(sorted(KNOWN_PROJECT_PREFIXES))}")
    for key in candidates:
        prefix = key.split('-', 1)[0]
        if prefix in KNOWN_PROJECT_PREFIXES:
            print(f"Accepting {key} via hard-coded prefix '{prefix}'.")
            accepted.append(key)
        else:
            print(f"Deferring {key} - prefix '{prefix}' not in hard-coded list.")
            unknown.append(key)

    # --- Pass 2: Jira API for unknown prefixes ---
    if unknown:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("Some prefixes not in hard-coded list; querying Jira for project keys...")
        print("Unknown-prefix candidates:")
        for key in unknown:
            print(f"  {key}")

        api_keys = _fetch_jira_project_keys(jira_auth)

        if api_keys:
            print(f"Valid Jira project keys from API (first 20): {' '.join(sorted(api_keys)[:20])}")

        for key in unknown:
            prefix = key.split('-', 1)[0]
            if prefix in api_keys:
                print(f"Accepting {key} via Jira API (valid project prefix '{prefix}').")
                accepted.append(key)
            else:
                print(f"Skipping {key} - unknown project prefix '{prefix}' (not in Jira).")
    else:
        print("All prefixes resolved via hard-coded list; no Jira project lookup needed.")

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    if not accepted:
        print("No valid Jira keys found after validation")
        return ["__NO_KEYS_FOUND__"]

    result = sorted(set(accepted))
    print("Final Jira keys:")
    for key in result:
        print(f"  {key}")

    return result


def _run_extract_jira_keys() -> None:
    """CLI entry-point wrapper for extract_jira_keys.

    Reads PR_TITLE, PR_BODY, and JIRA_AUTH from environment variables.
    """
    pr_title = os.environ.get("PR_TITLE", "")
    pr_body = os.environ.get("PR_BODY", "")
    jira_auth = os.environ.get("JIRA_AUTH", "")

    print(f"PR title: {pr_title}")
    print(f"PR body: {pr_body}")

    if not pr_title:
        print("Warning: PR_TITLE env var is not set or empty.")

    if not jira_auth:
        print("Warning: JIRA_AUTH env var is not set. "
              "Jira API fallback for unknown prefixes will be skipped.")

    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    output = json.dumps(keys)
    print(f"jira-keys-json={output}")

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"jira-keys-json={output}\n")


# ---------------------------------------------------------------------------
# add_label_to_jira_issue
# ---------------------------------------------------------------------------

def _parse_jira_keys_json(raw: str) -> list[str]:
    """Parse and deduplicate a JSON array of Jira keys.

    Returns an empty list when the input is empty, sentinel, or invalid.
    """
    raw = raw.strip()
    print(f"Incoming jira_keys_json: {raw}")

    if not raw or raw in ('[]', '[""]', '["__NO_KEYS_FOUND__"]'):
        print("No usable Jira keys in jira_keys_json; nothing to update.")
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"ERROR: jira_keys_json is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print(f"ERROR: jira_keys_json must be a JSON array; got: {type(data)}", file=sys.stderr)
        sys.exit(1)

    keys: list[str] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, str):
            continue
        k = item.strip()
        if k and k != "__NO_KEYS_FOUND__" and k not in seen:
            seen.add(k)
            keys.append(k)

    print(f"Found {len(keys)} issue(s).")
    return keys


def _determine_mode(label: str) -> tuple[str, str | None, dict | None]:
    """Decide the update mode and build the appropriate JSON payload.

    Returns (mode, priority_name_or_none, payload_dict).
    Modes: "priority", "scylla_component", "label".
    """
    label_upper = label.upper()
    print(f"Incoming label: '{label}'")

    # P0..P4 -> set priority field
    if label_upper in _PRIORITY_NAMES:
        payload = {"fields": {"priority": {"name": label_upper}}}
        return "priority", label_upper, payload

    # area/* -> add Scylla component
    if label.startswith("area/"):
        component_value = label[len("area/"):].replace("_", " ")
        print(f"Derived Scylla component value: '{component_value}' from label '{label}'")
        payload = {
            "update": {
                SCYLLA_COMPONENTS_FIELD: [{"add": {"value": component_value}}]
            }
        }
        return "scylla_component", None, payload

    # Fallback: normal Jira label
    payload = {"update": {"labels": [{"add": label}]}}
    return "label", None, payload


def _jira_put(url: str, payload: dict, jira_auth: str) -> tuple[int, str]:
    """PUT JSON to a Jira REST endpoint. Returns (http_code, response_body)."""
    encoded_auth = base64.b64encode(jira_auth.encode()).decode()
    body = json.dumps(payload).encode()

    req = Request(url, data=body, method="PUT")
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Basic {encoded_auth}")

    try:
        with urlopen(req) as resp:
            return resp.getcode(), resp.read().decode()
    except HTTPError as exc:
        return exc.code, exc.read().decode() if exc.fp else str(exc)


def add_label_to_jira_issue(jira_keys_json: str, label: str, jira_auth: str) -> None:
    """Add a label, priority, or Scylla component to every Jira issue in *jira_keys_json*.

    Replicates the logic of add_label_to_jira_issue.yml in pure Python.

    Modes:
      - P0..P4           -> sets the issue priority field
      - area/<component>  -> adds a Scylla component (customfield_10321)
      - anything else     -> adds a plain Jira label
    """
    keys = _parse_jira_keys_json(jira_keys_json)
    if not keys:
        return

    mode, priority_name, payload = _determine_mode(label)

    if mode == "priority":
        action_desc = "Set priority"
        print(f"Will set priority to: {priority_name}")
    elif mode == "scylla_component":
        action_desc = "Add Scylla component"
        print(f"Will add Scylla component derived from label: {label}")
    else:
        action_desc = "Add label"
        print(f"Will add label: {label}")

    ok = 0
    skipped = 0
    failed = 0

    for key in keys:
        issue_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{key}"
        print(f"{action_desc} on {key} ...")

        code, body_text = _jira_put(issue_url, payload, jira_auth)

        if code in (200, 204):
            print(f"OK {key} ({code})")
            ok += 1

        elif code == 400 and mode == "label":
            print(f"SKIP {key} ({code}) likely already has the label. First 200 chars:")
            print(body_text[:200])
            skipped += 1

        elif code == 400 and mode == "scylla_component":
            print(f"SKIP {key} ({code}) invalid Scylla component option. First 200 chars:")
            print(body_text[:200])
            skipped += 1

        else:
            print(f"FAIL {key} ({code}) First 400 chars:")
            print(body_text[:400])
            failed += 1

        time.sleep(0.2)

    print(f"Summary: ok={ok} skipped={skipped} failed={failed}")
    if failed > 0:
        sys.exit(1)


def _run_add_label_to_jira_issue() -> None:
    """CLI entry-point wrapper for add_label_to_jira_issue.

    Reads JIRA_KEYS_JSON, LABEL, and JIRA_AUTH from environment variables.
    """
    jira_keys_json = os.environ.get("JIRA_KEYS_JSON", "")
    label = os.environ.get("LABEL", "")
    jira_auth = os.environ.get("JIRA_AUTH", "")

    if not jira_keys_json:
        print("Error: JIRA_KEYS_JSON env var is not set or empty.")
        sys.exit(1)

    if not label:
        print("Error: LABEL env var is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: JIRA_AUTH env var is not set or empty.")
        sys.exit(1)

    add_label_to_jira_issue(jira_keys_json, label, jira_auth)


def debug_sync_context():
    """Log GitHub event context and label-specific transition hints."""
    event_name = os.environ.get('GITHUB_EVENT_NAME', '')
    action = os.environ.get('GITHUB_EVENT_ACTION', '')
    jira_keys_json = os.environ.get('JIRA_KEYS_JSON', '')
    label = os.environ.get('TRIGGERING_LABEL', '')
    repository = os.environ.get('GITHUB_REPOSITORY', '')
    github_context = os.environ.get('GITHUB_CONTEXT', '')

    print(f"event_name='{event_name}'")
    print(f"action='{action}'")
    print(f"jira-keys-json='{jira_keys_json}'")
    print(f"triggering-label='{label}'")
    print(f"repository='{repository}'")

    if label == 'status/merge_candidate':
        print("Try to transition Jira issue to 'Ready For Merge'")

    if label == 'promoted-to-master':
        print("Try to close Jira issue (promoted-to-master label added)")

    print("~~~~~~~~~~~ GitHub Context ~~~~~~~~~~~")
    if github_context:
        try:
            parsed = json.loads(github_context)
            print(json.dumps(parsed, indent=2))
        except json.JSONDecodeError:
            print(github_context)
    else:
        print("(GITHUB_CONTEXT not set)")


ACTION_DISPATCH = {
    'debug': debug_sync_context,
    'extract_jira_keys': _run_extract_jira_keys,
    'add_label_to_jira_issue': _run_add_label_to_jira_issue,
}


def main():
    parser = argparse.ArgumentParser(
        description='Jira sync logic for GitHub Actions workflows'
    )
    parser.add_argument(
        '--action',
        required=True,
        choices=AVAILABLE_ACTIONS,
        help='The action to execute'
    )
    args = parser.parse_args()

    print(f"=== Jira Sync: {args.action} ===")

    handler = ACTION_DISPATCH.get(args.action)
    if not handler:
        print(f"Error: Unknown action '{args.action}'")
        sys.exit(1)

    handler()
    return 0


if __name__ == '__main__':
    sys.exit(main())