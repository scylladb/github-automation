#!/usr/bin/env python3
"""
Reconciliation safety net for promoted-to-* labels.

search_commits.py labels a PR as promoted the moment its commit lands on
master/branch-X.Y, but it only ever runs off a single push event's exact
before..after commit range. If that push's webhook is ever dropped by
GitHub (observed for scylladb/scylladb#30992 - RELENG-824: the commit
promoting the PR landed on master, but no "push" webhook fired for it at
all, so search_commits.py never got a chance to see it), the PR is left
without its promoted-to-* label with nothing to ever retry it.

This script re-scans the last `--lookback` commits of each given branch on
a schedule, independent of push events, and re-applies the same labeling
logic. Re-running it over commits that were already labeled is harmless -
adding a label a PR already has is a no-op.
"""

import argparse
import os
import sys

from github import Github

from search_commits import GITHUB_RETRY, label_promoted_commits

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=str, required=True,
                         help='Github repository name (e.g., scylladb/scylladb)')
    parser.add_argument('--branches', type=str, required=True,
                         help='Comma-separated branches to reconcile (e.g. master,branch-2026.3)')
    parser.add_argument('--lookback', type=int, default=500,
                         help='How many of the most recent commits per branch to re-scan')
    return parser.parse_args()


def main():
    args = parser()
    branches = [b.strip() for b in args.branches.split(',') if b.strip()]
    if not branches:
        print("No branches given, nothing to reconcile.")
        return

    g = Github(github_token, retry=GITHUB_RETRY)
    repo = g.get_repo(args.repository, lazy=False)

    for branch in branches:
        print(f"::group::Reconciling promoted labels on {args.repository}@{branch} "
              f"(last {args.lookback} commits)")
        commits = repo.get_commits(sha=branch)[:args.lookback]
        label_promoted_commits(
            repository=args.repository,
            commits=commits,
            ref=f'refs/heads/{branch}',
            label='promoted-to-master',
            github_token=github_token,
        )
        print("::endgroup::")


if __name__ == "__main__":
    main()
