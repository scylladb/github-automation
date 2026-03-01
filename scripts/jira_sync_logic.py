#!/usr/bin/env python3
"""
jira_sync_logic.py - Top-level orchestrator and CLI dispatcher for Jira sync.

Contains manage_labeled_gh_event, manage_unlabeled_gh_event, manage_review_gh_event (orchestration functions),
debug_sync_context, the ACTION_DISPATCH table, and main().

All helpers, constants, and individual action implementations live in
jira_sync_modules.py.
"""

import json
import os
import sys
import argparse

from jira_sync_modules import (
    extract_jira_keys,
    add_label_to_jira_issue,
    extract_jira_issue_details,
    apply_jira_labels_to_pr,
    jira_status_transition,
    add_comment_to_jira,
    remove_label_from_jira_issue,
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
    csv_content, labels_csv, details_not_found = extract_jira_issue_details(jira_keys_json, jira_auth)

    if details_not_found:
        keys = [k for k in keys if k not in details_not_found]
        jira_keys_json = json.dumps(keys)
        print(f"Filtered jira-keys-json after details (removed {len(details_not_found)} not-found): {jira_keys_json}")
        if not keys:
            print("All Jira keys were not found. Nothing more to do.")
            return

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



def manage_review_gh_event(
    pr_title: str,
    pr_body: str,
    pr_number: int,
    owner_repo: str,
    gh_token: str,
    requested_reviewer: str,
    jira_auth: str,
) -> None:
    """Orchestrate the "In Review" sync in a single invocation.

    Replicates the full job graph of main_jira_sync_in_review.yml:

      1.  extract_jira_keys
      2.  extract_jira_issue_details
      3.  apply_jira_labels_to_pr
      4.  jira_status_transition -> "In Review" (id 121)
    """
    print("=" * 60)
    print(" manage_review_gh_event  input parameters")
    print("=" * 60)
    print(f"  pr_title   = {pr_title!r}")
    print(f"  pr_body    = {pr_body!r}")
    print(f"  pr_number  = {pr_number!r}")
    print(f"  owner_repo = {owner_repo!r}")
    print(f"  requested_reviewer = {requested_reviewer!r}")

    # --- Step 1: extract jira keys ---
    print("\n" + "=" * 60)
    print(" Step 1 / extract_jira_keys")
    print("=" * 60)
    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    jira_keys_json = json.dumps(keys)
    print(f"jira-keys-json={jira_keys_json}")

    if jira_keys_json == _NO_KEYS:
        print("No Jira keys found. Nothing to do.")
        return

    # --- Step 2: extract issue details ---
    print("\n" + "=" * 60)
    print(" Step 2 / extract_jira_issue_details")
    print("=" * 60)
    csv_content, labels_csv, details_not_found = extract_jira_issue_details(jira_keys_json, jira_auth)

    if details_not_found:
        keys = [k for k in keys if k not in details_not_found]
        jira_keys_json = json.dumps(keys)
        print(f"Filtered jira-keys-json after details (removed {len(details_not_found)} not-found): {jira_keys_json}")
        if not keys:
            print("All Jira keys were not found. Nothing more to do.")
            return

    # --- Step 3: apply labels to PR ---
    print("\n" + "=" * 60)
    print(" Step 3 / apply_jira_labels_to_pr")
    print("=" * 60)
    apply_jira_labels_to_pr(
        pr_number, labels_csv, csv_content, "", owner_repo, gh_token,
    )

    # --- Step 4: transition to In Review ---
    print("\n" + "=" * 60)
    print(" Step 4 / jira_status_transition -> In Review")
    print("=" * 60)
    jira_status_transition(csv_content, "In Review", "121", jira_auth)

    print("\n" + "=" * 60)
    print(" manage_review_gh_event completed successfully")
    print("=" * 60)


def _run_manage_review_gh_event() -> None:
    """CLI entry-point wrapper for manage_review_gh_event.

    Reads PR_TITLE, PR_BODY, PR_NUMBER, OWNER_REPO,
    GITHUB_TOKEN, and JIRA_AUTH from environment variables.
    """
    pr_title = os.environ.get("PR_TITLE", "")
    pr_body = os.environ.get("PR_BODY", "")
    pr_number_str = os.environ.get("PR_NUMBER", "")
    owner_repo = os.environ.get("OWNER_REPO", "")
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    requested_reviewer = os.environ.get("REQUESTED_REVIEWER", "")
    jira_auth = os.environ.get("JIRA_AUTH", "")

    if not pr_number_str:
        print("Error: PR_NUMBER env var is not set or empty.")
        sys.exit(1)

    try:
        pr_number = int(pr_number_str)
    except ValueError:
        print(f"Error: PR_NUMBER '{pr_number_str}' is not a valid integer.")
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

    manage_review_gh_event(
        pr_title, pr_body, pr_number,
        owner_repo, gh_token, requested_reviewer, jira_auth,
    )



def manage_closed_gh_event(
    pr_title: str,
    pr_body: str,
    pr_number: int,
    pr_merged: bool,
    owner_repo: str,
    gh_token: str,
    jira_auth: str,
) -> None:
    """Orchestrate the "PR Closed" sync in a single invocation.

    Replicates the full job graph of main_jira_sync_pr_closed.yml:

      1.  extract_jira_keys
      2.  extract_jira_issue_details
      3.  apply_jira_labels_to_pr
      4.  add_comment_to_jira (merged: "Closed via PR merge"; not merged: "PR closed without merge")
      5.  if merged: jira_status_transition -> "Done" (id 141)
    """
    print("=" * 60)
    print(" manage_closed_gh_event  input parameters")
    print("=" * 60)
    print(f"  pr_title   = {pr_title!r}")
    print(f"  pr_body    = {pr_body!r}")
    print(f"  pr_number  = {pr_number!r}")
    print(f"  pr_merged  = {pr_merged!r}")
    print(f"  owner_repo = {owner_repo!r}")

    # --- Step 1: extract jira keys ---
    print("\n" + "=" * 60)
    print(" Step 1 / extract_jira_keys")
    print("=" * 60)
    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    jira_keys_json = json.dumps(keys)
    print(f"jira-keys-json={jira_keys_json}")

    if jira_keys_json == _NO_KEYS:
        print("No Jira keys found. Nothing to do.")
        return

    # --- Step 2: extract issue details ---
    print("\n" + "=" * 60)
    print(" Step 2 / extract_jira_issue_details")
    print("=" * 60)
    csv_content, labels_csv, details_not_found = extract_jira_issue_details(jira_keys_json, jira_auth)

    if details_not_found:
        keys = [k for k in keys if k not in details_not_found]
        jira_keys_json = json.dumps(keys)
        print(f"Filtered jira-keys-json after details (removed {len(details_not_found)} not-found): {jira_keys_json}")
        if not keys:
            print("All Jira keys were not found. Nothing more to do.")
            return

    # --- Step 3: apply labels to PR ---
    print("\n" + "=" * 60)
    print(" Step 3 / apply_jira_labels_to_pr")
    print("=" * 60)
    apply_jira_labels_to_pr(
        pr_number, labels_csv, csv_content, "", owner_repo, gh_token,
    )

    # --- Step 4: add "PR closed" comment ---
    print("\n" + "=" * 60)
    print(" Step 4 / add_comment_to_jira (PR closed)")
    print("=" * 60)
    pr_url = f"https://github.com/{owner_repo}/pull/{pr_number}"
    if pr_merged:
        add_comment_to_jira(
            jira_keys_json,
            "Closed via PR merge ",
            jira_auth,
            link_text=pr_title,
            link_url=pr_url,
        )
    else:
        add_comment_to_jira(
            jira_keys_json,
            "PR closed without merge ",
            jira_auth,
            link_text=pr_title,
            link_url=pr_url,
        )

    # --- Step 5: transition to Done (merged PRs only) ---
    print("\n" + "=" * 60)
    print(" Step 5 / jira_status_transition -> Done")
    print("=" * 60)
    if pr_merged:
        jira_status_transition(csv_content, "Done", "141", jira_auth)
    else:
        print("SKIPPED: PR was closed without merge")

    print("\n" + "=" * 60)
    print(" manage_closed_gh_event completed successfully")
    print("=" * 60)


def _run_manage_closed_gh_event() -> None:
    """CLI entry-point wrapper for manage_closed_gh_event.

    Reads PR_TITLE, PR_BODY, PR_NUMBER, PR_MERGED, OWNER_REPO,
    GITHUB_TOKEN, and JIRA_AUTH from environment variables.
    """
    pr_title = os.environ.get("PR_TITLE", "")
    pr_body = os.environ.get("PR_BODY", "")
    pr_number_str = os.environ.get("PR_NUMBER", "")
    pr_merged_str = os.environ.get("PR_MERGED", "false")
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

    pr_merged = pr_merged_str.lower() == "true"

    if not owner_repo:
        print("Error: OWNER_REPO env var is not set or empty.")
        sys.exit(1)

    if not gh_token:
        print("Error: GITHUB_TOKEN env var is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: JIRA_AUTH env var is not set or empty.")
        sys.exit(1)

    manage_closed_gh_event(
        pr_title, pr_body, pr_number, pr_merged,
        owner_repo, gh_token, jira_auth,
    )


def manage_opened_gh_event(
    pr_title: str,
    pr_body: str,
    pr_number: int,
    owner_repo: str,
    gh_token: str,
    jira_auth: str,
) -> None:
    """Orchestrate the "PR Opened" sync in a single invocation.

    Replicates the full job graph of main_jira_sync_pr_opened.yml:

      1.  extract_jira_keys
      2.  extract_jira_issue_details
      3.  apply_jira_labels_to_pr
      4.  jira_status_transition -> "In Progress" (id 111)
    """
    print("=" * 60)
    print(" manage_opened_gh_event  input parameters")
    print("=" * 60)
    print(f"  pr_title   = {pr_title!r}")
    print(f"  pr_body    = {pr_body!r}")
    print(f"  pr_number  = {pr_number!r}")
    print(f"  owner_repo = {owner_repo!r}")

    # --- Step 1: extract jira keys ---
    print("\n" + "=" * 60)
    print(" Step 1 / extract_jira_keys")
    print("=" * 60)
    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    jira_keys_json = json.dumps(keys)
    print(f"jira-keys-json={jira_keys_json}")

    if jira_keys_json == _NO_KEYS:
        print("No Jira keys found. Nothing to do.")
        return

    # --- Step 2: extract issue details ---
    print("\n" + "=" * 60)
    print(" Step 2 / extract_jira_issue_details")
    print("=" * 60)
    csv_content, labels_csv, details_not_found = extract_jira_issue_details(jira_keys_json, jira_auth)

    if details_not_found:
        keys = [k for k in keys if k not in details_not_found]
        jira_keys_json = json.dumps(keys)
        print(f"Filtered jira-keys-json after details (removed {len(details_not_found)} not-found): {jira_keys_json}")
        if not keys:
            print("All Jira keys were not found. Nothing more to do.")
            return

    # --- Step 3: apply labels to PR ---
    print("\n" + "=" * 60)
    print(" Step 3 / apply_jira_labels_to_pr")
    print("=" * 60)
    apply_jira_labels_to_pr(
        pr_number, labels_csv, csv_content, "", owner_repo, gh_token,
    )

    # --- Step 4: transition to In Progress ---
    print("\n" + "=" * 60)
    print(" Step 4 / jira_status_transition -> In Progress")
    print("=" * 60)
    jira_status_transition(csv_content, "In Progress", "111", jira_auth)

    print("\n" + "=" * 60)
    print(" manage_opened_gh_event completed successfully")
    print("=" * 60)


def _run_manage_opened_gh_event() -> None:
    """CLI entry-point wrapper for manage_opened_gh_event.

    Reads PR_TITLE, PR_BODY, PR_NUMBER, OWNER_REPO,
    GITHUB_TOKEN, and JIRA_AUTH from environment variables.
    """
    pr_title = os.environ.get("PR_TITLE", "")
    pr_body = os.environ.get("PR_BODY", "")
    pr_number_str = os.environ.get("PR_NUMBER", "")
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

    if not owner_repo:
        print("Error: OWNER_REPO env var is not set or empty.")
        sys.exit(1)

    if not gh_token:
        print("Error: GITHUB_TOKEN env var is not set or empty.")
        sys.exit(1)

    if not jira_auth:
        print("Error: JIRA_AUTH env var is not set or empty.")
        sys.exit(1)

    manage_opened_gh_event(
        pr_title, pr_body, pr_number,
        owner_repo, gh_token, jira_auth,
    )

def manage_unlabeled_gh_event(
    pr_title: str,
    pr_body: str,
    pr_number: int,
    removed_label: str,
    owner_repo: str,
    gh_token: str,
    jira_auth: str,
) -> None:
    """Orchestrate the "PR Unlabeled" sync in a single invocation.

    Replicates the full job graph of main_jira_sync_remove_label.yml:

      1.  extract_jira_keys
      2.  remove_label_from_jira_issue  (skip P0-P4 labels)
      3.  extract_jira_issue_details
      4.  apply_jira_labels_to_pr
    """
    print("=" * 60)
    print(" manage_unlabeled_gh_event  input parameters")
    print("=" * 60)
    print(f"  pr_title       = {pr_title!r}")
    print(f"  pr_body        = {pr_body!r}")
    print(f"  pr_number      = {pr_number!r}")
    print(f"  removed_label  = {removed_label!r}")
    print(f"  owner_repo     = {owner_repo!r}")

    # --- Step 1: extract jira keys ---
    print("\n" + "=" * 60)
    print(" Step 1 / extract_jira_keys")
    print("=" * 60)
    keys = extract_jira_keys(pr_title, pr_body, jira_auth)
    jira_keys_json = json.dumps(keys)
    print(f"jira-keys-json={jira_keys_json}")

    if jira_keys_json == _NO_KEYS:
        print("No Jira keys found. Nothing to do.")
        return

    # --- Step 2: remove label from Jira (skip priority labels) ---
    print("\n" + "=" * 60)
    print(" Step 2 / remove_label_from_jira_issue")
    print("=" * 60)
    _PRIORITY_LABELS = {"P0", "P1", "P2", "P3", "P4"}
    if removed_label in _PRIORITY_LABELS:
        print(f"SKIPPED: removed_label '{removed_label}' is a priority label (P0-P4)")
    else:
        not_found = remove_label_from_jira_issue(jira_keys_json, removed_label, jira_auth)

        # Remove issues that were not found (404) from subsequent steps
        if not_found:
            keys = [k for k in keys if k not in not_found]
            jira_keys_json = json.dumps(keys)
            print(f"Filtered jira-keys-json (removed {len(not_found)} not-found): {jira_keys_json}")
            if not keys:
                print("All Jira keys were not found. Nothing more to do.")
                return

    # --- Step 3: extract issue details ---
    print("\n" + "=" * 60)
    print(" Step 3 / extract_jira_issue_details")
    print("=" * 60)
    csv_content, labels_csv, details_not_found = extract_jira_issue_details(jira_keys_json, jira_auth)

    if details_not_found:
        keys = [k for k in keys if k not in details_not_found]
        jira_keys_json = json.dumps(keys)
        print(f"Filtered jira-keys-json after details (removed {len(details_not_found)} not-found): {jira_keys_json}")
        if not keys:
            print("All Jira keys were not found. Nothing more to do.")
            return

    # --- Step 4: apply labels to PR ---
    print("\n" + "=" * 60)
    print(" Step 4 / apply_jira_labels_to_pr")
    print("=" * 60)
    apply_jira_labels_to_pr(
        pr_number, labels_csv, csv_content, "", owner_repo, gh_token,
    )

    print("\n" + "=" * 60)
    print(" manage_unlabeled_gh_event completed successfully")
    print("=" * 60)


def _run_manage_unlabeled_gh_event() -> None:
    """CLI entry-point wrapper for manage_unlabeled_gh_event.

    Reads PR_TITLE, PR_BODY, PR_NUMBER, REMOVED_LABEL,
    OWNER_REPO, GITHUB_TOKEN, and JIRA_AUTH from environment variables.
    """
    pr_title = os.environ.get("PR_TITLE", "")
    pr_body = os.environ.get("PR_BODY", "")
    pr_number_str = os.environ.get("PR_NUMBER", "")
    removed_label = os.environ.get("REMOVED_LABEL", "")
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

    if not removed_label:
        print("Error: REMOVED_LABEL env var is not set or empty.")
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

    manage_unlabeled_gh_event(
        pr_title, pr_body, pr_number, removed_label,
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
    'manage_labeled_gh_event': _run_manage_labeled_gh_event,
    'manage_review_gh_event': _run_manage_review_gh_event,
    'manage_closed_gh_event': _run_manage_closed_gh_event,
    'manage_opened_gh_event': _run_manage_opened_gh_event,
    'manage_unlabeled_gh_event': _run_manage_unlabeled_gh_event,
}


def main():
    parser = argparse.ArgumentParser(
        description='Jira sync logic for GitHub Actions workflows'
    )
    parser.add_argument(
        '--action',
        required=True,
        choices=ACTION_DISPATCH.keys(),
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
