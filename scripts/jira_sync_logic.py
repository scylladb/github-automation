#!/usr/bin/env python3
"""
jira_sync_logic.py - Top-level orchestrator and CLI dispatcher for Jira sync.

Contains manage_labeled_gh_event (the main orchestration function),
debug_sync_context, the ACTION_DISPATCH table, and main().

All helpers, constants, and individual action implementations live in
jira_sync_modules.py.
"""

import json
import os
import sys
import argparse

from jira_sync_modules import (
    AVAILABLE_ACTIONS,
    extract_jira_keys,
    add_label_to_jira_issue,
    extract_jira_issue_details,
    apply_jira_labels_to_pr,
    jira_status_transition,
    add_comment_to_jira,
    _run_extract_jira_keys,
    _run_add_label_to_jira_issue,
    _run_extract_jira_issue_details,
    _run_apply_jira_labels_to_pr,
    _run_jira_status_transition,
    _run_add_comment_to_jira,
)

# Sentinel value returned by extract_jira_keys when no keys are found.
_NO_KEYS = '["__NO_KEYS_FOUND__"]'

def manage_labeled_gh_event(
    pr_title: str,
    pr_body: str,
    pr_number: int,
    triggering_label: str,
    owner_repo: str,
    gh_token: str,
    jira_auth: str,
) -> None:
    """Orchestrate every label-sync step in a single invocation.

    Replicates the full job graph of main_jira_sync_add_label.yml:

      1.  extract_jira_keys
      2.  add_label_to_jira_issue  (triggering label)
      3.  if label is status/release_blocker  -> also add P0
      4.  extract_jira_issue_details
      5.  apply_jira_labels_to_pr
      6.  if label is status/merge_candidate  -> transition to Ready for Merge
      7.  if label is promoted-to-master AND repo is scylladb/staging:
            a. add comment  b. transition to Done
    """
    print("=" * 60)
    print(" manage_labeled_gh_event  input parameters")
    print("=" * 60)
    print(f"  pr_title        = {pr_title!r}")
    print(f"  pr_body          = {pr_body!r}")
    print(f"  pr_number        = {pr_number!r}")
    print(f"  triggering_label = {triggering_label!r}")
    print(f"  owner_repo       = {owner_repo!r}")
    print(f"  gh_token         = {gh_token[:4]}***")
    print(f"  jira_auth        = {jira_auth[:4]}***")

    # --- Step 1: extract jira keys ---
    print("=" * 60)
    print(" Step 1 / extract_jira_keys")
    print("=" * 60)
    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    jira_keys_json = json.dumps(keys)
    print(f"jira-keys-json={jira_keys_json}")

    if jira_keys_json == _NO_KEYS:
        print("No Jira keys found. Nothing to do.")
        return

    # --- Step 2: add the triggering label ---
    print("\n" + "=" * 60)
    print(" Step 2 / add_label_to_jira_issue")
    print("=" * 60)
    not_found = add_label_to_jira_issue(jira_keys_json, triggering_label, jira_auth)

    # Remove issues that were not found (404) from all subsequent steps
    if not_found:
        keys = [k for k in keys if k not in not_found]
        jira_keys_json = json.dumps(keys)
        print(f"Filtered jira-keys-json (removed {len(not_found)} not-found): {jira_keys_json}")
        if not keys:
            print("All Jira keys were not found. Nothing more to do.")
            return

    # --- Step 3: add P0 when status/release_blocker ---
    print("\n" + "=" * 60)
    print(" Step 3 / add P0 (release_blocker)")
    print("=" * 60)
    if triggering_label == "status/release_blocker":
        add_label_to_jira_issue(jira_keys_json, "P0", jira_auth)
    else:
        print(f"SKIPPED: triggering_label is '{triggering_label}', not 'status/release_blocker'")

    # --- Step 4: extract issue details ---
    print("\n" + "=" * 60)
    print(" Step 4 / extract_jira_issue_details")
    print("=" * 60)
    csv_content, labels_csv = extract_jira_issue_details(jira_keys_json, jira_auth)

    # --- Step 5: apply labels to PR ---
    print("\n" + "=" * 60)
    print(" Step 5 / apply_jira_labels_to_pr")
    print("=" * 60)
    apply_jira_labels_to_pr(
        pr_number, labels_csv, csv_content, triggering_label, owner_repo, gh_token,
    )

    # --- Step 6: status/merge_candidate -> Ready for Merge ---
    print("\n" + "=" * 60)
    print(" Step 6 / jira_status_transition -> Ready for Merge")
    print("=" * 60)
    if triggering_label == "status/merge_candidate":
        jira_status_transition(csv_content, "Ready for Merge", "131", jira_auth)
    else:
        print(f"SKIPPED: triggering_label is '{triggering_label}', not 'status/merge_candidate'")

    # --- Step 7: promoted-to-master (scylladb/staging only) ---
    print("\n" + "=" * 60)
    print(" Step 7a / add_comment_to_jira (promoted-to-master)")
    print("=" * 60)
    if triggering_label == "promoted-to-master" and owner_repo == "scylladb/staging":
        pr_url = f"https://github.com/{owner_repo}/pull/{pr_number}"
        add_comment_to_jira(
            jira_keys_json,
            "Closed via promoted-to-master label on PR ",
            jira_auth,
            link_text=pr_title,
            link_url=pr_url,
        )
    else:
        print(f"SKIPPED: triggering_label is '{triggering_label}' and owner_repo is '{owner_repo}'"
              f" (requires 'promoted-to-master' on 'scylladb/staging')")

    print("\n" + "=" * 60)
    print(" Step 7b / jira_status_transition -> Done")
    print("=" * 60)
    if triggering_label == "promoted-to-master" and owner_repo == "scylladb/staging":
        jira_status_transition(csv_content, "Done", "141", jira_auth)
    else:
        print(f"SKIPPED: triggering_label is '{triggering_label}' and owner_repo is '{owner_repo}'"
              f" (requires 'promoted-to-master' on 'scylladb/staging')")

    print("\n" + "=" * 60)
    print(" manage_labeled_gh_event completed successfully")
    print("=" * 60)


def _run_manage_labeled_gh_event() -> None:
    """CLI entry-point wrapper for manage_labeled_gh_event.

    Reads PR_TITLE, PR_BODY, PR_NUMBER, TRIGGERING_LABEL,
    OWNER_REPO, GITHUB_TOKEN, and JIRA_AUTH from environment variables.
    """
    pr_title = os.environ.get("PR_TITLE", "")
    pr_body = os.environ.get("PR_BODY", "")
    pr_number_str = os.environ.get("PR_NUMBER", "")
    triggering_label = os.environ.get("TRIGGERING_LABEL", "")
    owner_repo = os.environ.get("OWNER_REPO", "")
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    jira_auth = os.environ.get("JIRA_AUTH", "")

    if not pr_number_str:
        print("Error: PR_NUMBER env var is not set or empty.")
        sys.exit(1)

    try:
        pr_number = int(pr_number_str)
    except ValueError:
        print(f"Error: PR_NUMBER '{pr_number_str}' is not a valid integer.")
        sys.exit(1)

    if not triggering_label:
        print("Error: TRIGGERING_LABEL env var is not set or empty.")
        sys.exit(1)

    if not owner_repo:
        print("Error: OWNER_REPO env var is not set or empty.")
        sys.exit(1)

    if not gh_token:
        print("Error: GITHUB_TOKEN env var is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: JIRA_AUTH env var is not set or empty.")
        sys.exit(1)

    manage_labeled_gh_event(
        pr_title, pr_body, pr_number, triggering_label,
        owner_repo, gh_token, jira_auth,
    )



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
    'apply_jira_labels_to_pr': _run_apply_jira_labels_to_pr,
    'jira_status_transition': _run_jira_status_transition,
    'add_comment_to_jira': _run_add_comment_to_jira,
    'manage_labeled_gh_event': _run_manage_labeled_gh_event,
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
