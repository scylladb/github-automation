#!/usr/bin/env python3
"""
Jira sync logic for GitHub Actions workflows.

Dispatches to the requested action based on the --action CLI argument.
Currently supports:
  - debug: Log GitHub event context and label-specific transition hints.
  - extract_jira_keys: Extract and validate Jira issue keys from PR title/body.
  - add_label_to_jira_issue: Add a label, priority, or Scylla component to Jira issues.
  - extract_jira_issue_details: Fetch Jira issue details and produce a CSV.

Usage:
  python3 scripts/jira_sync_logic.py --action debug
  python3 scripts/jira_sync_logic.py --action extract_jira_keys
  python3 scripts/jira_sync_logic.py --action add_label_to_jira_issue
  python3 scripts/jira_sync_logic.py --action extract_jira_issue_details

Environment variables (for extract_jira_keys):
  PR_TITLE   - The pull request title
  PR_BODY    - The pull request body
  JIRA_AUTH  - Jira auth credential "<user email>:<api_token>"

Environment variables (for extract_jira_issue_details):
  JIRA_KEYS_JSON - JSON array of Jira issue keys
  JIRA_AUTH       - Jira auth credential "<user email>:<api_token>"

Environment variables (for add_label_to_jira_issue):
  JIRA_KEYS_JSON - JSON array of Jira issue keys, e.g. '["STAG-1","STAG-2"]'
  LABEL          - The label to add (plain label, P0-P4 for priority, area/* for Scylla component, symptom/* for symptom)
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


AVAILABLE_ACTIONS = ['debug', 'extract_jira_keys', 'add_label_to_jira_issue', 'extract_jira_issue_details']

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
SYMPTOM_FIELD = "customfield_11120"

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
    Modes: "priority", "scylla_component", "symptom", "label".
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

    # symptom/* -> add symptom custom field
    if label.startswith("symptom/"):
        symptom_value = label[len("symptom/"):].replace("_", " ")
        print(f"Derived symptom value: '{symptom_value}' from label '{label}'")
        payload = {
            "update": {
                SYMPTOM_FIELD: [{"add": {"value": symptom_value}}]
            }
        }
        return "symptom", None, payload

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
      - area/<component>   -> adds a Scylla component (customfield_10321)
      - symptom/<symptom>  -> adds a symptom (customfield_11120)
      - anything else      -> adds a plain Jira label
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
    elif mode == "symptom":
        action_desc = "Add symptom"
        print(f"Will add symptom derived from label: {label}")
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

        elif mode in ("scylla_component", "symptom") and code not in (200, 204):
            print(f"WARN {key} ({code}) custom field update failed. First 200 chars:")
            print(body_text[:200])
            print(f"Falling back to adding '{label}' as a plain Jira label on {key} ...")
            fallback_payload = {"update": {"labels": [{"add": label}]}}
            fb_code, fb_body = _jira_put(issue_url, fallback_payload, jira_auth)
            if fb_code in (200, 204):
                print(f"OK {key} (fallback label, {fb_code})")
                ok += 1
            elif fb_code == 400:
                print(f"SKIP {key} (fallback label, {fb_code}) likely already has the label.")
                skipped += 1
            else:
                print(f"FAIL {key} (fallback label, {fb_code}) First 400 chars:")
                print(fb_body[:400])
                failed += 1

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


# ---------------------------------------------------------------------------
# extract_jira_issue_details
# ---------------------------------------------------------------------------

# CSV columns produced by this action
_CSV_HEADER = "key,status,labels,assignee,priority,fixVersions,scylla_components,startDate,dueDate"
START_DATE_FIELD = "customfield_10015"
DUE_DATE_FIELD = "duedate"
_DETAIL_DELIM = ";"


def _jira_get(url: str, jira_auth: str) -> dict | None:
    """GET JSON from a Jira REST endpoint. Returns parsed JSON or None on failure."""
    encoded_auth = base64.b64encode(jira_auth.encode()).decode()

    req = Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Basic {encoded_auth}")

    try:
        with urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except (HTTPError, URLError) as exc:
        print(f"Warning: GET {url} failed: {exc}")
        return None


def _csv_escape(value: str) -> str:
    """Wrap a value in double-quotes for CSV, escaping internal quotes."""
    return '"' + value.replace('"', '""') + '"'


def extract_jira_issue_details(jira_keys_json: str, jira_auth: str) -> tuple[str, str]:
    """Fetch Jira issue details and produce a CSV plus a deduplicated labels string.

    Replicates the logic of extract_jira_issue_details.yml in pure Python.

    Returns (csv_content, labels_csv).
    """
    keys = _parse_jira_keys_json(jira_keys_json)

    if not keys:
        print("---------------------------------------------------")
        print("Generated CSV (empty-keys short-circuit):")
        print("---------------------------------------------------")
        print(_CSV_HEADER)
        print("---------------------------------------------------")
        return _CSV_HEADER + "\n", ""

    fields_param = ",".join([
        "status", "labels", "assignee", "priority", "fixVersions",
        SCYLLA_COMPONENTS_FIELD, START_DATE_FIELD, DUE_DATE_FIELD,
    ])

    csv_lines: list[str] = [_CSV_HEADER]
    all_labels: list[str] = []

    for key in keys:
        url = f"{JIRA_BASE_URL}/rest/api/3/issue/{key}?fields={fields_param}"
        print(f"Fetching Jira issue: {key}")

        resp = _jira_get(url, jira_auth)
        if resp is None:
            print(f"Skipping {key} - fetch failed")
            continue

        fields = resp.get("fields", {})

        status = (fields.get("status") or {}).get("name", "")
        assignee = (fields.get("assignee") or {}).get("displayName", "")
        priority = (fields.get("priority") or {}).get("name", "")

        labels_list = fields.get("labels") or []
        labels_str = _DETAIL_DELIM.join(labels_list)
        all_labels.extend(labels_list)

        fix_versions_raw = fields.get("fixVersions") or []
        fix_versions = _DETAIL_DELIM.join(
            v.get("name", "") for v in fix_versions_raw
        )

        components_raw = fields.get(SCYLLA_COMPONENTS_FIELD)
        if isinstance(components_raw, list):
            components = _DETAIL_DELIM.join(
                c.get("value", "") if isinstance(c, dict) else str(c)
                for c in components_raw
            )
        elif components_raw is not None:
            components = str(components_raw)
        else:
            components = ""

        start_date = fields.get(START_DATE_FIELD) or ""
        due_date = fields.get(DUE_DATE_FIELD) or ""

        row = ",".join([
            _csv_escape(key),
            _csv_escape(status),
            _csv_escape(labels_str),
            _csv_escape(assignee),
            _csv_escape(priority),
            _csv_escape(fix_versions),
            _csv_escape(components),
            _csv_escape(start_date),
            _csv_escape(due_date),
        ])
        csv_lines.append(row)

    # Deduplicate labels
    if all_labels:
        labels_csv = ",".join(sorted(set(all_labels)))
    else:
        labels_csv = ""

    csv_content = "\n".join(csv_lines) + "\n"

    print("---------------------------------------------------")
    print("Generated CSV (after fetching issues):")
    print("Showing first 20 lines:")
    print("---------------------------------------------------")
    for line in csv_lines[:20]:
        print(line)
    print("---------------------------------------------------")

    return csv_content, labels_csv


def _run_extract_jira_issue_details() -> None:
    """CLI entry-point wrapper for extract_jira_issue_details.

    Reads JIRA_KEYS_JSON and JIRA_AUTH from environment variables.
    Writes labels_csv and csv to GITHUB_OUTPUT.
    """
    jira_keys_json = os.environ.get("JIRA_KEYS_JSON", "")
    jira_auth = os.environ.get("JIRA_AUTH", "")

    if not jira_keys_json:
        print("Error: JIRA_KEYS_JSON env var is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: JIRA_AUTH env var is not set or empty.")
        sys.exit(1)

    csv_content, labels_csv = extract_jira_issue_details(jira_keys_json, jira_auth)

    print(f"labels_csv={labels_csv}")

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"labels_csv={labels_csv}\n")
            f.write(f"csv<<EOF\n")
            f.write(csv_content)
            f.write("EOF\n")

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
    'extract_jira_issue_details': _run_extract_jira_issue_details,
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