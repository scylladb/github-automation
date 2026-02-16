#!/usr/bin/env python3
"""
Debug and diagnostic logging for the Jira sync add-label workflow.

Replaces the inline 'debug' job that was previously defined in
main_jira_sync_add_label.yml. Logs GitHub event context, extracted
Jira keys, and label-specific transition hints.
"""

import json
import os
import sys


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


def main():
    print("=== Jira Sync Debug ===")
    debug_sync_context()
    return 0


if __name__ == '__main__':
    sys.exit(main())
