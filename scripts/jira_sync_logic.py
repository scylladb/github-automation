#!/usr/bin/env python3
"""
Jira sync logic for GitHub Actions workflows.

Dispatches to the requested action based on the --action CLI argument.
Currently supports:
  - debug: Log GitHub event context and label-specific transition hints.
  - extract_jira_keys: Extract and validate Jira issue keys from PR title/body.

Usage:
  python3 scripts/jira_sync_logic.py --action debug
  python3 scripts/jira_sync_logic.py --action extract_jira_keys \
      --pr-title "STAG-123 fix something" \
      --pr-body "Fixes: PM-456" \
      --jira-auth "user@example.com:api_token"
"""

import argparse
import json
import os
import re
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


AVAILABLE_ACTIONS = ['debug', 'extract_jira_keys']

KNOWN_PROJECT_PREFIXES = {
    "RELENG", "CLOUD", "SCYLLADB", "PKG", "PM", "CUSTOMER",
    "VECTOR", "ANSROLES", "CLOUDEVOPS", "CXTOOLS", "DOCTOR",
    "FIELDAUTO", "FIELDENG", "ILIAD", "OPERATOR", "PKGDASH",
    "PUB", "SMI", "WEBINSTALL", "STAG",
}

JIRA_BASE_URL = "https://scylladb.atlassian.net"

# Regex: any JIRA-style key (PROJECT-123) in any text
_JIRA_KEY_RE = re.compile(r'[A-Z]+-[0-9]+')

# Regex: closing keywords followed by a Jira key (optionally as a browse URL)
_CLOSING_KEYWORD_RE = re.compile(
    r'(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)'
    r'\s*[: ]\s*\[?\s*(?:https?://\S*/browse/)?([A-Z]+-[0-9]+)',
    re.IGNORECASE,
)


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

    import base64
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


def _run_extract_jira_keys(args: argparse.Namespace) -> None:
    """CLI entry-point wrapper for extract_jira_keys."""
    pr_title = args.pr_title or ""
    pr_body = args.pr_body or ""
    jira_auth = args.jira_auth or os.environ.get("JIRA_AUTH", "")

    if not jira_auth:
        print("Warning: No --jira-auth provided and JIRA_AUTH env var not set. "
              "Jira API fallback for unknown prefixes will be skipped.")

    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    output = json.dumps(keys)
    print(f"jira-keys-json={output}")

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"jira-keys-json={output}\n")


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
    'extract_jira_keys': None,  # handled separately (needs parsed args)
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
    parser.add_argument(
        '--pr-title',
        default=None,
        help='PR title text (for extract_jira_keys)'
    )
    parser.add_argument(
        '--pr-body',
        default=None,
        help='PR body text (for extract_jira_keys)'
    )
    parser.add_argument(
        '--jira-auth',
        default=None,
        help='Jira auth credential "email:api_token" (for extract_jira_keys). '
             'Falls back to JIRA_AUTH env var.'
    )
    args = parser.parse_args()

    print(f"=== Jira Sync: {args.action} ===")

    if args.action == 'extract_jira_keys':
        _run_extract_jira_keys(args)
        return 0

    handler = ACTION_DISPATCH.get(args.action)
    if not handler:
        print(f"Error: Unknown action '{args.action}'")
        sys.exit(1)

    handler()
    return 0


if __name__ == '__main__':
    sys.exit(main())