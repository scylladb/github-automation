#!/usr/bin/env python3
"""
reconcile_merged_prs.py - Safety net for the PR "closed" Jira sync (RELENG-1033).

main_pr_events_jira_sync.yml closes the Jira issues a PR fixes only when the
PR's "closed" event produces a run that actually executes. GitHub sometimes
drops those runs (stuck in "queued" with no jobs, or no hosted runner ever
assigned), and nothing retries them, so the issues stay open.

This script runs on a schedule, independent of PR events. For every PR merged
in the last --days days it finds the Jira issues the "closed" handler would
have closed, and closes the ones the sync missed: issues that are not in a
closed state and whose status has not changed since the merge. An issue whose
status changed after the merge (e.g. reopened by hand) is left alone, and so
are the issues of a PR whose title or body was edited after the merge.

Only the Jira side is reconciled (comment + transition); PR labels are not.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from urllib.error import URLError

from jira_sync_logic import extract_closed_pr_jira_keys
from jira_sync_modules import (
    MERGED_STATUS_NAME,
    MERGED_TRANSITION_ID,
    _gh_api,
    _plan_transitions,
    add_comment_to_jira,
    extract_jira_issue_details,
    get_issue_status_category,
    get_jira_account_id,
    get_sync_failures,
    jira_status_transition,
    record_sync_failure,
    status_changed_since,
)

_NO_KEYS = ["__NO_KEYS_FOUND__"]

# The search API returns at most 1000 results (10 pages of 100).
_SEARCH_MAX_PAGES = 10

# The search API allows 30 requests per minute; space every request (across
# all repositories, not just between pages of one) to stay under it.
_SEARCH_MIN_INTERVAL = 2.1
_last_search_at = 0.0

# How many times a page GitHub marks as incomplete (search timed out) is retried.
_SEARCH_INCOMPLETE_RETRIES = 2


def _throttle_search() -> None:
    global _last_search_at
    wait = _last_search_at + _SEARCH_MIN_INTERVAL - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_search_at = time.monotonic()


def _search_merged_prs(owner_repo: str, since: datetime, gh_token: str) -> list[dict] | None:
    """Return the PRs of *owner_repo* merged at or after *since*, or None on error."""
    query = f"repo:{owner_repo} is:pr is:merged merged:>={since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    items: list[dict] = []
    seen: set[int] = set()
    for page in range(1, _SEARCH_MAX_PAGES + 1):
        # Sort by creation time: unlike "updated", it does not reorder while we
        # page, so a PR commented on mid-run is not skipped at a page boundary.
        url = (f"https://api.github.com/search/issues?q={quote(query)}"
               f"&sort=created&order=asc&per_page=100&page={page}")
        for _attempt in range(_SEARCH_INCOMPLETE_RETRIES + 1):
            _throttle_search()
            try:
                code, body = _gh_api("GET", url, gh_token)
                data = json.loads(body) if code == 200 else None
            except (URLError, ValueError) as exc:
                code, data = 0, None
                body = str(exc)
            if not isinstance(data, dict):
                print(f"Error: PR search for {owner_repo} failed (page {page}, HTTP {code}): {body[:300]}")
                return None
            if not data.get("incomplete_results"):
                break
            print(f"Warning: GitHub returned incomplete search results for {owner_repo} (page {page}).")
        else:
            # A short incomplete page would also end the paging below, so the
            # merged PRs it left out would be skipped without any error.
            record_sync_failure(f"{owner_repo}: GitHub search results stayed incomplete (page {page}), "
                                "some merged PRs may not have been reconciled")
        page_items = data.get("items") or []
        for item in page_items:
            if item.get("number") not in seen:
                seen.add(item.get("number"))
                items.append(item)
        if len(page_items) < 100:
            break
    else:
        record_sync_failure(f"{owner_repo}: more than {_SEARCH_MAX_PAGES * 100} merged PRs, "
                            "the rest were not reconciled (lower --days)")
    return items


def _parse_github_time(value: str) -> datetime | None:
    """Parse a GitHub timestamp such as 2026-10-07T15:04:00Z."""
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


_EDITS_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      lastEditedAt
      timelineItems(itemTypes: [RENAMED_TITLE_EVENT], last: 1) {
        nodes { ... on RenamedTitleEvent { createdAt } }
      }
    }
  }
}
"""


def _edited_after(owner_repo: str, pr_number: int, merged_at: datetime, gh_token: str) -> bool | None:
    """Whether the title or body of a PR was edited after *merged_at*, or None on error."""
    owner, name = owner_repo.split("/", 1)
    payload = {"query": _EDITS_QUERY, "variables": {"owner": owner, "name": name, "number": pr_number}}
    try:
        code, body = _gh_api("POST", "https://api.github.com/graphql", gh_token, payload)
        pull = json.loads(body)["data"]["repository"]["pullRequest"] if code == 200 else None
    except (URLError, ValueError, KeyError, TypeError):
        pull = None
    if not isinstance(pull, dict):
        return None
    edits = [pull.get("lastEditedAt")]
    edits += [(node or {}).get("createdAt") for node in (pull.get("timelineItems") or {}).get("nodes") or []]
    times = [_parse_github_time(t) for t in edits if t is not None]
    # An edit time we cannot parse may well be after the merge: treat it as an error.
    if None in times:
        return None
    return any(t > merged_at for t in times)


def reconcile_pr(
    owner_repo: str, pr: dict, gh_token: str, jira_auth: str, dry_run: bool,
    sync_account_id: str = "",
) -> list[str]:
    """Close the Jira issues of one merged PR that the sync missed.

    Status changes made by *sync_account_id* (the account the sync runs as) do
    not count as a decision to keep the issue open.
    Returns the keys it closed (with dry_run: the keys it would close).
    """
    pr_number = pr["number"]
    pr_title = pr.get("title") or ""
    pr_body = pr.get("body") or ""
    pr_url = pr.get("html_url") or f"https://github.com/{owner_repo}/pull/{pr_number}"
    merged_at = _parse_github_time((pr.get("pull_request") or {}).get("merged_at"))
    if merged_at is None:
        record_sync_failure(f"{owner_repo}#{pr_number}: could not read the merge time")
        return []

    keys = extract_closed_pr_jira_keys(pr_title, pr_body, pr_number, owner_repo, gh_token, jira_auth)
    if keys == _NO_KEYS:
        return []

    missed: list[str] = []
    for key in keys:
        code, category = get_issue_status_category(key, jira_auth)
        if code == 404:
            print(f"Skipping {key}: issue not found or no permission.")
            continue
        if code != 200:
            record_sync_failure(f"{owner_repo}#{pr_number}: could not read the status of {key} (HTTP {code})")
            continue
        if category == "done":
            continue
        changed = status_changed_since(key, merged_at, jira_auth, ignore_account_id=sync_account_id)
        if changed is None:
            record_sync_failure(f"{owner_repo}#{pr_number}: could not read the changelog of {key}")
        elif changed:
            print(f"Leaving {key} as is: someone changed its status after {owner_repo}#{pr_number} was merged.")
        else:
            missed.append(key)

    if not missed:
        return []

    details_csv, _, details_failed = extract_jira_issue_details(json.dumps(missed), jira_auth)
    if details_failed:
        record_sync_failure(f"{owner_repo}#{pr_number}: could not read the details of {details_failed}")
    # Keep only the issues jira_status_transition would actually move: a status
    # named like a closed state (e.g. "Merged") whose category is not "done" is
    # left alone by the transition, so it must not be reported as missed either.
    to_transition = {row[0] for row in _plan_transitions(details_csv, MERGED_STATUS_NAME)[0]}
    missed = [key for key in missed if key in to_transition]
    if not missed:
        return []

    # The keys are read from the current title and body, which can differ from
    # what was reviewed and merged: do not act on a PR edited after the merge.
    edited = _edited_after(owner_repo, pr_number, merged_at, gh_token)
    if edited is None:
        record_sync_failure(f"{owner_repo}#{pr_number}: could not read the edit history")
        return []
    if edited:
        print(f"::warning::{owner_repo}#{pr_number}: title or body edited after the merge, "
              "not reconciling its Jira issues - check them by hand.")
        return []

    print(f"MISSED: {owner_repo}#{pr_number} (merged {merged_at.isoformat()}) left open: {missed}")
    if dry_run:
        print("Dry run: not changing Jira.")
        return missed

    # Transition first and comment only on the issues that actually closed: an
    # issue whose transition failed is retried by the next run, and commenting
    # before the transition would add a new comment on every retry.
    closed = jira_status_transition(details_csv, MERGED_STATUS_NAME, MERGED_TRANSITION_ID, jira_auth)
    if closed:
        add_comment_to_jira(
            json.dumps(closed),
            "Closed via PR merge (reconciled: the PR sync had not closed this issue) ",
            jira_auth,
            link_text=pr_title,
            link_url=pr_url,
        )
    return closed


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repositories', type=str, required=True,
                        help='Comma-separated repositories to reconcile (e.g. scylladb/scylla-cluster-tests)')
    parser.add_argument('--days', type=int, default=7,
                        help='Reconcile PRs merged in the last N days')
    parser.add_argument('--min-age-minutes', type=int, default=60,
                        help='Skip PRs merged more recently than this, whose closed event may still be running')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report the missed issues without changing Jira')
    return parser.parse_args()


def main() -> int:
    args = parser()
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    jira_auth = os.environ.get("JIRA_AUTH", "")
    if not gh_token or not jira_auth:
        print("Error: GITHUB_TOKEN and JIRA_AUTH must be set.")
        return 1

    repositories = [r.strip() for r in args.repositories.split(',') if r.strip()]
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=args.days)
    newest = now - timedelta(minutes=args.min_age_minutes)

    # Without it every status change counts, which can only close fewer issues.
    sync_account_id = get_jira_account_id(jira_auth)
    if sync_account_id is None:
        print("Warning: could not read the Jira account of JIRA_AUTH; "
              "status changes made by the sync itself will also keep issues open.")
        sync_account_id = ""

    reconciled: list[str] = []
    for owner_repo in repositories:
        prs = _search_merged_prs(owner_repo, since, gh_token)
        if prs is None:
            record_sync_failure(f"could not list the merged PRs of {owner_repo}")
            continue
        print(f"{owner_repo}: {len(prs)} PR(s) merged since {since.date().isoformat()}")
        for pr in prs:
            merged_at = _parse_github_time((pr.get("pull_request") or {}).get("merged_at"))
            if merged_at is not None and merged_at > newest:
                continue
            print(f"::group::{owner_repo}#{pr['number']}")
            try:
                missed = reconcile_pr(owner_repo, pr, gh_token, jira_auth, args.dry_run,
                                      sync_account_id=sync_account_id)
            except Exception as exc:  # keep going: one bad PR must not skip the other repos
                record_sync_failure(f"{owner_repo}#{pr['number']}: unexpected error: {exc!r}")
                missed = []
            finally:
                print("::endgroup::")
            reconciled.extend(f"{key} ({owner_repo}#{pr['number']})" for key in missed)

    verb = "Would reconcile" if args.dry_run else "Reconciled"
    print(f"{verb} {len(reconciled)} issue(s)")
    for item in reconciled:
        print(f"  {item}")

    failures = get_sync_failures()
    for failure in failures:
        print(f"::error::Jira reconcile: {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
