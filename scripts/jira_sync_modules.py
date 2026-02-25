#!/usr/bin/env python3
"""
jira_sync_modules.py - Shared constants, helpers, and action functions
for the Jira/GitHub sync workflows.

Contains:
  - All imports and shared constants
  - Private HTTP/utility helpers
  - Public action functions:
      extract_jira_keys
      add_label_to_jira_issue
      remove_label_from_jira_issue
      extract_jira_issue_details
      apply_jira_labels_to_pr
      jira_status_transition
      add_comment_to_jira

The orchestrator functions and CLI dispatcher live in jira_sync_logic.py
which imports from this module.
"""

import base64
import csv
import io
import json
import os
import re
import sys
import time
from datetime import date
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError



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
    print(f"PR title: {pr_title}")
    print(f"PR body: {pr_body}")

    if not pr_title:
        print("Warning: pr_title is not set or empty.")

    if not jira_auth:
        print("Warning: jira_auth is not set. "
              "Jira API fallback for unknown prefixes will be skipped.")

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
    except URLError as exc:
        print(f"Warning: network error - {exc}")
        return 0, str(exc)


def add_label_to_jira_issue(jira_keys_json: str, label: str, jira_auth: str) -> list[str]:
    """Add a label, priority, or Scylla component to every Jira issue in *jira_keys_json*.

    Replicates the logic of add_label_to_jira_issue.yml in pure Python.

    Modes:
      - P0..P4           -> sets the issue priority field
      - area/<component>   -> adds a Scylla component (customfield_10321)
      - symptom/<symptom>  -> adds a symptom (customfield_11120)
      - anything else      -> adds a plain Jira label
    """
    if not label:
        print("Error: label is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: jira_auth is not set or empty.")
        sys.exit(1)

    keys = _parse_jira_keys_json(jira_keys_json)
    if not keys:
        return []

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
    not_found_keys: list[str] = []

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
            elif fb_code == 404:
                print(f"SKIP {key} (fallback label, {fb_code}) issue not found or no permission. Removing from further processing.")
                skipped += 1
                not_found_keys.append(key)
            else:
                print(f"FAIL {key} (fallback label, {fb_code}) First 400 chars:")
                print(fb_body[:400])
                failed += 1

        elif code == 404:
            print(f"SKIP {key} ({code}) issue not found or no permission. Removing from further processing.")
            skipped += 1
            not_found_keys.append(key)

        else:
            print(f"FAIL {key} ({code}) First 400 chars:")
            print(body_text[:400])
            failed += 1

        time.sleep(0.2)

    print(f"Summary: ok={ok} skipped={skipped} failed={failed}")
    if not_found_keys:
        print(f"Not-found keys (will be removed from further processing): {not_found_keys}")
    if failed > 0:
        sys.exit(1)
    return not_found_keys


def remove_label_from_jira_issue(jira_keys_json: str, label: str, jira_auth: str) -> list[str]:
    """Remove a label or Scylla component from every Jira issue in *jira_keys_json*.

    Replicates the logic of remove_label_from_jira_issue.yml in pure Python.

    Modes:
      - area/<component>  -> removes a Scylla component (customfield_10321)
      - symptom/<symptom> -> removes a Problem Symptom (customfield_11120)
      - anything else     -> removes a plain Jira label
    """
    if not label:
        print("Error: label is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: jira_auth is not set or empty.")
        sys.exit(1)

    keys = _parse_jira_keys_json(jira_keys_json)
    if not keys:
        return []

    print(f"Incoming removed label: '{label}'")

    if label.startswith("area/"):
        mode = "scylla_component"
        component_value = label[len("area/"):].replace("_", " ")
        payload = {
            "update": {
                SCYLLA_COMPONENTS_FIELD: [{"remove": {"value": component_value}}]
            }
        }
        action_desc = "Remove Scylla component"
        print(f"Will remove Scylla component: '{component_value}'")
    elif label.startswith("symptom/"):
        mode = "symptom"
        symptom_value = label[len("symptom/"):].replace("_", " ")
        payload = {
            "update": {
                SYMPTOM_FIELD: [{"remove": {"value": symptom_value}}]
            }
        }
        action_desc = "Remove symptom"
        print(f"Will remove symptom: '{symptom_value}'")
    else:
        mode = "label"
        payload = {"update": {"labels": [{"remove": label}]}}
        action_desc = "Remove label"
        print(f"Will remove label: '{label}'")

    ok = 0
    skipped = 0
    failed = 0
    not_found_keys: list[str] = []

    for key in keys:
        issue_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{key}"
        print(f"{action_desc} on {key} ...")

        code, body_text = _jira_put(issue_url, payload, jira_auth)

        if code in (200, 204):
            print(f"OK {key} ({code})")
            ok += 1

        elif code == 400:
            print(f"SKIP {key} ({code})  value may not exist in Jira. First 200 chars:")
            print(body_text[:200])
            skipped += 1

        elif code == 404:
            print(f"SKIP {key} ({code}) issue not found or no permission. Removing from further processing.")
            skipped += 1
            not_found_keys.append(key)

        else:
            print(f"FAIL {key} ({code}) First 400 chars:")
            print(body_text[:400])
            failed += 1

        time.sleep(0.2)

    print(f"Summary: ok={ok} skipped={skipped} failed={failed}")
    if not_found_keys:
        print(f"Not-found keys (will be removed from further processing): {not_found_keys}")
    if failed > 0:
        sys.exit(1)
    return not_found_keys


# ---------------------------------------------------------------------------
# extract_jira_issue_details
# ---------------------------------------------------------------------------

# CSV columns produced by this action
_CSV_HEADER = "key,status,labels,assignee,priority,fixVersions,scylla_components,symptoms,startDate,dueDate"
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


def extract_jira_issue_details(jira_keys_json: str, jira_auth: str) -> tuple[str, str, list[str]]:
    """Fetch Jira issue details and produce a CSV plus a deduplicated labels string.

    Replicates the logic of extract_jira_issue_details.yml in pure Python.

    Returns (csv_content, labels_csv, not_found_keys).
    not_found_keys lists issue keys that returned 404 or other fetch errors.
    """
    if not jira_auth:
        print("Error: jira_auth is not set or empty.")
        sys.exit(1)

    keys = _parse_jira_keys_json(jira_keys_json)

    if not keys:
        print("---------------------------------------------------")
        print("Generated CSV (empty-keys short-circuit):")
        print("---------------------------------------------------")
        print(_CSV_HEADER)
        print("---------------------------------------------------")
        return _CSV_HEADER + "\n", "", []

    fields_param = ",".join([
        "status", "labels", "assignee", "priority", "fixVersions",
        SCYLLA_COMPONENTS_FIELD, SYMPTOM_FIELD, START_DATE_FIELD, DUE_DATE_FIELD,
    ])

    csv_lines: list[str] = [_CSV_HEADER]
    all_labels: list[str] = []
    not_found_keys: list[str] = []

    for key in keys:
        url = f"{JIRA_BASE_URL}/rest/api/3/issue/{key}?fields={fields_param}"
        print(f"Fetching Jira issue: {key}")

        resp = _jira_get(url, jira_auth)
        if resp is None:
            print(f"Skipping {key} - fetch failed")
            not_found_keys.append(key)
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

        symptoms_raw = fields.get(SYMPTOM_FIELD)
        if isinstance(symptoms_raw, list):
            symptoms = _DETAIL_DELIM.join(
                s.get("value", "") if isinstance(s, dict) else str(s)
                for s in symptoms_raw
            )
        elif symptoms_raw is not None:
            symptoms = str(symptoms_raw)
        else:
            symptoms = ""

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
            _csv_escape(symptoms),
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

    return csv_content, labels_csv, not_found_keys


# ---------------------------------------------------------------------------
# apply_jira_labels_to_pr
# ---------------------------------------------------------------------------

# Jira priority name -> P* rank mapping
_PRIORITY_RANK_MAP = {
    "p0": 0, "highest": 0, "blocker": 0,
    "p1": 1, "critical": 1, "high": 1,
    "p2": 2, "medium": 2, "major": 2,
    "p3": 3, "low": 3, "minor": 3,
    "p4": 4, "lowest": 4, "trivial": 4,
}

_GH_API_VERSION = "2022-11-28"


def _gh_api(method: str, url: str, gh_token: str, payload: dict | None = None) -> tuple[int, str]:
    """Make a GitHub REST API request. Returns (http_code, response_body)."""
    body = json.dumps(payload).encode() if payload else None

    req = Request(url, data=body, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {gh_token}")
    req.add_header("X-GitHub-Api-Version", _GH_API_VERSION)
    if body:
        req.add_header("Content-Type", "application/json; charset=utf-8")

    try:
        with urlopen(req) as resp:
            return resp.getcode(), resp.read().decode()
    except HTTPError as exc:
        return exc.code, exc.read().decode() if exc.fp else str(exc)


def _compute_labels(labels_csv: str, details_csv: str, new_priority_label: str) -> list[str]:
    """Compute the final list of labels to apply to a PR.

    1. Parse labels_csv into a deduped list.
    2. Strip any existing P0..P4 from that list.
    3. Parse details_csv to derive the best Jira priority (P*) and area/* labels.
    4. If the triggering event label is P0..P4 it overrides the Jira priority.
    5. Append area/* labels.
    6. Append symptom/* labels.
    """
    # 1) Parse labels_csv
    raw_labels = [s.strip() for s in labels_csv.split(",")]
    seen: set[str] = set()
    labels: list[str] = []
    for s in raw_labels:
        if s and s not in seen:
            seen.add(s)
            labels.append(s)

    # 2) Remove P0..P4 from base list
    priority_names = {"P0", "P1", "P2", "P3", "P4"}
    labels = [lb for lb in labels if lb not in priority_names]

    # 3) Parse details CSV for priority + scylla_components
    best_rank = None
    area_labels: list[str] = []
    area_seen: set[str] = set()
    symptom_labels: list[str] = []
    symptom_seen: set[str] = set()

    stripped_csv = details_csv.strip()
    if stripped_csv:
        reader = csv.DictReader(io.StringIO(stripped_csv))
        for row in reader:
            prio = (row.get("priority") or "").strip()
            if prio:
                rank = _PRIORITY_RANK_MAP.get(prio.lower())
                if rank is not None:
                    if best_rank is None or rank < best_rank:
                        best_rank = rank

            comp_raw = (row.get("scylla_components") or "").strip()
            if comp_raw:
                for part in comp_raw.split(";"):
                    comp = part.strip()
                    if not comp:
                        continue
                    safe = re.sub(r"\s+", "_", comp)
                    label = f"area/{safe}"
                    if label not in area_seen:
                        area_seen.add(label)
                        area_labels.append(label)

            symp_raw = (row.get("symptoms") or "").strip()
            if symp_raw:
                for part in symp_raw.split(";"):
                    symp = part.strip()
                    if not symp:
                        continue
                    safe = re.sub(r"\s+", "_", symp)
                    label = f"symptom/{safe}"
                    if label not in symptom_seen:
                        symptom_seen.add(label)
                        symptom_labels.append(label)

    # 4) Decide P* label
    priority_label = None
    if best_rank is not None:
        priority_label = f"P{best_rank}"

    new_p = (new_priority_label or "").strip()
    if new_p and re.match(r"(?i)^P[0-4]$", new_p):
        print(f"Overriding Jira priority with PR-added label: {new_p}")
        priority_label = new_p.upper()

    if priority_label:
        print(f"Effective priority label: {priority_label}")
        if priority_label not in labels:
            labels.insert(0, priority_label)
    else:
        print("No effective P* priority (from Jira or event); not adding P* label.")

    # 5) Append area/* labels
    for area in area_labels:
        if area not in labels:
            labels.append(area)

    # 6) Append symptom/* labels
    for symp in symptom_labels:
        if symp not in labels:
            labels.append(symp)

    print(f"Final labels to apply: {labels}")
    return labels


def _remove_stale_priority_labels(
    owner_repo: str,
    pr_number: int,
    desired_labels: list[str],
    gh_token: str,
) -> None:
    """Remove P0-P4 labels from a PR that are not in the desired set."""
    issue_api = f"https://api.github.com/repos/{owner_repo}/issues/{pr_number}"

    desired_p = {lb for lb in desired_labels if re.match(r"^P[0-4]$", lb)}
    if not desired_p:
        print("No desired P* labels computed; keeping existing P* labels unchanged.")
        return

    print(f"Fetching existing labels for PR #{pr_number}...")
    code, body = _gh_api("GET", f"{issue_api}/labels", gh_token)
    if code != 200:
        print(f"Warning: failed to fetch existing labels (HTTP {code})")
        return

    existing = {item["name"] for item in json.loads(body)}
    existing_p = {lb for lb in existing if re.match(r"^P[0-4]$", lb)}

    for p in sorted(existing_p):
        if p in desired_p:
            print(f"Keeping existing priority label: {p} (also desired)")
        else:
            print(f"Removing existing priority label not desired anymore: {p}")
            del_code, _ = _gh_api("DELETE", f"{issue_api}/labels/{p}", gh_token)
            print(f"Delete {p} -> HTTP {del_code}")


def apply_jira_labels_to_pr(
    pr_number: int,
    labels_csv: str,
    details_csv: str,
    new_priority_label: str,
    owner_repo: str,
    gh_token: str,
) -> None:
    """Apply Jira-derived labels to a GitHub PR.

    Replicates the logic of apply_labels_to_pr.yml in pure Python.

    1. Compute the final label set (priority, area/*, plain labels).
    2. Remove stale P0-P4 labels from the PR.
    3. Add each computed label to the PR via the GitHub API.
    """
    if not owner_repo:
        print("Error: owner_repo is not set or empty.")
        sys.exit(1)

    if not gh_token:
        print("Error: gh_token is not set or empty.")
        sys.exit(1)

    print("======================================================")
    print(" Apply Labels to PR -- Input Parameters")
    print("======================================================")
    print(f"PR Number:       {pr_number}")
    print(f"labels_csv:      {labels_csv}")
    print(f"NEW_PRIORITY_LABEL (from event): {new_priority_label or '<none>'}")
    print("details_csv (first 5 lines):")
    print("------------------------------------------------------")
    for i, line in enumerate(details_csv.splitlines()):
        print(line)
        if i >= 4:
            break
    print("------------------------------------------------------\n")

    labels = _compute_labels(labels_csv, details_csv, new_priority_label)

    if not labels:
        print("No labels to apply. Skipping.")
        return

    _remove_stale_priority_labels(owner_repo, pr_number, labels, gh_token)

    issue_api = f"https://api.github.com/repos/{owner_repo}/issues/{pr_number}/labels"

    ok = 0
    failed = 0
    for lb in labels:
        lb = lb.strip()
        if not lb:
            continue

        print("----------------------------------------")
        print(f"Applying label: '{lb}'")

        code, body_text = _gh_api("POST", issue_api, gh_token, {"labels": [lb]})

        if code in (200, 201):
            print(f"Result: success (HTTP {code}).")
            ok += 1
        else:
            print(f"Result: failed (HTTP {code}). Body (first 200 chars):")
            print(body_text[:200])
            failed += 1

    print(f"Summary: ok={ok} failed={failed}")


# ---------------------------------------------------------------------------
# jira_status_transition
# ---------------------------------------------------------------------------

_WORKING_STATES = {"in progress", "in review", "ready for merge"}
_CLOSED_STATES = {"done", "won't fix", "duplicate"}


def _jira_post(url: str, payload: dict, jira_auth: str) -> tuple[int, str]:
    """POST JSON to a Jira REST endpoint. Returns (http_code, response_body)."""
    encoded_auth = base64.b64encode(jira_auth.encode()).decode()
    body = json.dumps(payload).encode()

    req = Request(url, data=body, method="POST")
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Basic {encoded_auth}")

    try:
        with urlopen(req) as resp:
            return resp.getcode(), resp.read().decode()
    except HTTPError as exc:
        return exc.code, exc.read().decode() if exc.fp else str(exc)
    except URLError as exc:
        print(f"Warning: network error - {exc}")
        return 0, str(exc)


def _plan_transitions(
    details_csv: str, transition_name: str,
) -> tuple[list[tuple[str, str, str, str]], list[tuple[str, str]], list[tuple[str, str]]]:
    """Parse the details CSV and categorize issues.

    Returns (to_transition, already_ok, done_issues).
    Each to_transition item is (key, current_status, start_date, due_date).
    Each already_ok / done_issues item is (key, current_status).
    """
    to_transition: list[tuple[str, str, str, str]] = []
    already_ok: list[tuple[str, str]] = []
    done_issues: list[tuple[str, str]] = []

    stripped = details_csv.strip()
    if not stripped:
        return to_transition, already_ok, done_issues

    reader = csv.DictReader(io.StringIO(stripped))
    fieldmap = {(h or "").strip().lower(): h for h in reader.fieldnames or []}

    def get(row: dict, name: str) -> str:
        h = fieldmap.get(name.lower())
        return (row.get(h) or "").strip() if h else ""

    for row in reader:
        key = get(row, "key")
        status = get(row, "status")
        start_dt = get(row, "startdate") or get(row, "startDate")
        due_dt = get(row, "duedate") or get(row, "dueDate")
        if not key:
            continue

        if status.lower() == transition_name.lower():
            already_ok.append((key, status))
        elif status.lower() in _CLOSED_STATES:
            done_issues.append((key, status))
        else:
            to_transition.append((key, status, start_dt, due_dt))

    return to_transition, already_ok, done_issues

def _set_date_field(key: str, field_id: str, field_label: str, jira_auth: str) -> None:
    """Set a date field on a Jira issue to today's date (UTC)."""
    today = date.today().isoformat()
    print(f"Setting {field_label} to {today} for {key} (field: {field_id})")
    payload = {"fields": {field_id: today}}
    code, body_text = _jira_put(f"{JIRA_BASE_URL}/rest/api/3/issue/{key}", payload, jira_auth)
    if code not in (200, 204):
        print(f"Warning: Failed to set {field_label} for {key} (HTTP {code})")
        print(body_text[:200])
    time.sleep(0.2)


def jira_status_transition(
    details_csv: str,
    transition_name: str,
    transition_id: str,
    jira_auth: str,
) -> None:
    """Transition Jira issues to a target status.

    Replicates the logic of jira_transition.yml in pure Python.

    1. Parse the details CSV and categorize issues.
    2. For issues moving to a working state, set start date if empty.
    3. For issues moving to a closed state, set due date if empty.
    4. POST the transition for each issue that needs it.
    """
    if not details_csv:
        print("Error: details_csv is not set or empty.")
        sys.exit(1)

    if not transition_name:
        print("Error: transition_name is not set or empty.")
        sys.exit(1)

    if not transition_id:
        print("Error: transition_id is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: jira_auth is not set or empty.")
        sys.exit(1)

    print("======================================================")
    print(" Jira Status Transition -- Input Parameters")
    print("======================================================")
    print(f"Transition name: {transition_name}")
    print(f"Transition ID:   {transition_id}")
    print("details_csv (first 5 lines):")
    print("------------------------------------------------------")
    for i, line in enumerate(details_csv.splitlines()):
        print(line)
        if i >= 4:
            break
    print("------------------------------------------------------\n")

    to_transition, already_ok, done_issues = _plan_transitions(details_csv, transition_name)

    print(f"Target status:           {transition_name}")
    print(f"Issues already at target: {len(already_ok)}")
    print(f"Issues in closed state:   {len(done_issues)}")
    print(f"Issues to transition:     {len(to_transition)}")

    if already_ok:
        print("----- Already OK (up to 10) -----")
        for key, status in already_ok[:10]:
            print(f"  {key} ({status})")

    if done_issues:
        print("----- Done / closed issues (up to 10) -----")
        for key, status in done_issues[:10]:
            print(f"  {key} ({status})")

    if not to_transition:
        print("No issues require transition. Done.")
        return

    target_lower = transition_name.lower()
    is_working = target_lower in _WORKING_STATES
    is_closed = target_lower in _CLOSED_STATES

    ok = 0
    failed = 0
    skipped = 0

    for key, current_status, start_dt, due_dt in to_transition:
        print(f"Transitioning {key} from '{current_status}' -> '{transition_name}' (id={transition_id})")

        # Set start date for working states if empty
        if is_working and (not start_dt or start_dt == "null"):
            _set_date_field(key, START_DATE_FIELD, "start date", jira_auth)

        # Set due date for closed states if empty
        if is_closed and (not due_dt or due_dt == "null"):
            _set_date_field(key, DUE_DATE_FIELD, "due date", jira_auth)

        # POST the transition
        url = f"{JIRA_BASE_URL}/rest/api/3/issue/{key}/transitions"
        payload = {"transition": {"id": transition_id}}
        code, body_text = _jira_post(url, payload, jira_auth)

        if code in (200, 204):
            print(f"OK {key} ({code})")
            ok += 1
        elif code == 404:
            print(f"SKIP {key} ({code}) issue not found or no permission. Continuing.")
            skipped += 1
        else:
            print(f"FAIL {key} ({code}) First 400 chars:")
            print(body_text[:400])
            failed += 1

        time.sleep(0.2)

    print(f"Summary: ok={ok} skipped={skipped} failed={failed}")
    if failed > 0:
        print(f"WARNING: {failed} comment(s) failed. Continuing.")


# ---------------------------------------------------------------------------
# add_comment_to_jira
# ---------------------------------------------------------------------------


def _build_adf_comment(comment: str, link_text: str, link_url: str) -> dict:
    """Build an Atlassian Document Format (ADF) comment payload.

    If *link_text* and *link_url* are provided the comment text is followed
    by a clickable link.  Otherwise the comment is rendered as plain text.
    """
    if link_text and link_url:
        content = [
            {"type": "text", "text": comment},
            {
                "type": "text",
                "text": link_text,
                "marks": [
                    {"type": "link", "attrs": {"href": link_url}}
                ],
            },
        ]
    else:
        content = [{"type": "text", "text": comment}]

    return {
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {"type": "paragraph", "content": content}
            ],
        }
    }


def add_comment_to_jira(
    jira_keys_json: str,
    comment: str,
    jira_auth: str,
    link_text: str = "",
    link_url: str = "",
) -> None:
    """Add a comment to one or more Jira issues.

    Replicates the logic of add_comment_to_jira.yml in pure Python.

    Parameters
    ----------
    jira_keys_json : str
        JSON array of Jira issue keys, e.g. '["STAG-1","STAG-2"]'.
    comment : str
        The comment text (prefix before an optional link).
    jira_auth : str
        Jira auth credential "email:api_token".
    link_text : str, optional
        Display text for a clickable link appended to the comment.
    link_url : str, optional
        URL for the clickable link.
    """
    if not jira_auth:
        print("Error: jira_auth is not set or empty.")
        sys.exit(1)

    keys = _parse_jira_keys_json(jira_keys_json)
    if not keys:
        print("No Jira keys to comment on.")
        return

    if not comment:
        print("Comment text is empty; nothing to do.")
        return

    payload = _build_adf_comment(comment, link_text, link_url)

    print(f"Adding comment to {len(keys)} issue(s)")
    print(f"Comment: {comment}")
    if link_text:
        print(f"Link text: {link_text}")
    if link_url:
        print(f"Link URL: {link_url}")

    ok = 0
    skipped = 0
    failed = 0

    for key in keys:
        url = f"{JIRA_BASE_URL}/rest/api/3/issue/{key}/comment"
        print(f"Posting comment on {key} ...")

        code, body_text = _jira_post(url, payload, jira_auth)

        if code in (200, 201):
            print(f"OK {key} ({code})")
            ok += 1
        elif code == 404:
            print(f"SKIP {key} ({code}) issue not found or no permission. Continuing.")
            skipped += 1
        else:
            print(f"FAIL {key} ({code}) First 400 chars:")
            print(body_text[:400])
            failed += 1

        time.sleep(0.2)

    print(f"Summary: ok={ok} skipped={skipped} failed={failed}")
    if failed > 0:
        print(f"WARNING: {failed} comment(s) failed. Continuing.")


