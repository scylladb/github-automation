#!/usr/bin/env python3

import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from github import Github, GithubRetry
import argparse
import sys
import os

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)

# PyGithub's default GithubRetry only retries a handful of attempts before
# giving up, which isn't enough to ride out a transient GitHub API outage
# (e.g. a burst of 503s). Give it more attempts and a longer backoff so a
# temporary blip doesn't abort the whole promoted-to-master labeling run.
GITHUB_RETRY = GithubRetry(
    total=8,
    backoff_factor=10,
    backoff_max=120,
    status_forcelist=list(range(500, 600)) + [403, 429],
)


def requests_session_with_retries() -> requests.Session:
    """Build a requests Session that retries transient errors (5xx/403/429)
    for the plain 'requests' calls this script makes against the GitHub
    REST API, mirroring the resilience given to the PyGithub client."""
    session = requests.Session()
    retry = Retry(
        total=8,
        backoff_factor=10,
        status_forcelist=list(range(500, 600)) + [403, 429],
        allowed_methods=frozenset(["GET", "POST", "DELETE"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


http = requests_session_with_retries()


def _raise_for_status(response, action, ok_statuses=()):
    """Raise loudly on a failed GitHub API call instead of letting the
    caller silently treat it as 'no match' or 'nothing to do'. `http`
    already retries transient 5xx/403/429 extensively, so a response that
    still isn't ok here is a persistent failure worth stopping for."""
    if response.ok or response.status_code in ok_statuses:
        return
    raise RuntimeError(f"{action} failed: HTTP {response.status_code} {response.text}")


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=str, default='scylladb/scylla-pkg', help='Github repository name')
    parser.add_argument('--commits', type=str, required=True, help='Range of promoted commits.')
    parser.add_argument('--label', type=str, default='promoted-to-master', help='Label to use')
    parser.add_argument('--ref', type=str, required=True, help='PR target branch')
    return parser.parse_args()


def get_promoted_label_for_ref(ref: str) -> str:
    """
    Compute the correct promoted-to label based on the branch reference.
    - refs/heads/master or refs/heads/main -> promoted-to-master
    - refs/heads/next -> promoted-to-master (gating for master)
    - refs/heads/branch-X.Y -> promoted-to-branch-X.Y
    - refs/heads/next-X.Y -> promoted-to-branch-X.Y (gating for branch-X.Y)
    - refs/heads/manager-X.Y -> promoted-to-manager-X.Y
    """
    # Extract branch name from ref
    branch = ref.replace('refs/heads/', '') if ref.startswith('refs/heads/') else ref

    if branch in ('master', 'main', 'next'):
        return 'promoted-to-master'
    # For gating branches (next-X.Y), map to the stable branch label
    if branch.startswith('next-'):
        stable = branch.replace('next-', 'branch-', 1)
        return f'promoted-to-{stable}'
    return f'promoted-to-{branch}'


def label_promoted_commits(repository, commits, ref, label, github_token):
    """
    Scan `commits` (an iterable of PyGithub Commit objects) for PRs that were
    promoted to `ref`, and apply the appropriate promoted-to-* label.

    Shared between the push-triggered path (main(), fed with the exact
    before..after range of a single push) and the periodic reconciliation
    path (reconcile_promoted_labels.py, fed with a lookback window of recent
    commits), which exists because a dropped push webhook otherwise leaves a
    promoted PR unlabeled forever - see RELENG-824.

    Re-running this over the same commits is safe: adding a label a PR
    already has, or removing one it no longer has, is a no-op on GitHub's side.
    """
    # Skip gating branches (next-X.Y, next) - labels should only be added
    # when commits are promoted to the stable branch (branch-X.Y, master)
    branch = ref.replace('refs/heads/', '') if ref.startswith('refs/heads/') else ref
    if branch.startswith('next-') or branch == 'next':
        print(f"Skipping push to gating branch {branch} - waiting for promotion to stable branch")
        return set()

    # Compute the correct promoted label based on the branch
    promoted_label = get_promoted_label_for_ref(ref) if label == 'promoted-to-master' else label
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    processed_prs = set()
    for commit in commits:
        search_url = 'https://api.github.com/search/issues'
        query = f"repo:{repository} is:pr is:closed sha:{commit.sha}"
        params = {
            "q": query,
        }
        response = http.get(search_url, headers=headers, params=params)
        _raise_for_status(response, f"Searching for PRs matching commit {commit.sha}")
        prs = response.json().get("items", [])
        # Fallback: if the commit message has "Closes" references, include those PRs too
        # This handles PRs closed by pushing a rebased commit directly (different SHA than the PR's head)
        # IMPORTANT: Only include PRs whose base branch matches the branch being pushed to,
        # because promoter commits may contain Closes references to PRs from other branches.
        found_pr_numbers = {pr["number"] for pr in prs}
        close_refs = re.findall(
            rf'Closes\s+(?:{re.escape(repository)}#|#)(\d+)',
            commit.commit.message
        )
        # Extract the short branch name from the ref for comparison
        target_branch = branch
        for close_ref in close_refs:
            pr_num = int(close_ref)
            if pr_num not in found_pr_numbers and pr_num not in processed_prs:
                pr_url = f'https://api.github.com/repos/{repository}/pulls/{pr_num}'
                pr_response = http.get(pr_url, headers=headers)
                # A 404 here means the referenced PR number genuinely doesn't
                # exist (e.g. a stale/typo'd reference) - not a failure to raise on.
                _raise_for_status(pr_response, f"Fetching PR #{pr_num}", ok_statuses=(404,))
                if pr_response.ok:
                    pr_data = pr_response.json()
                    if pr_data.get("state") == "closed":
                        # Only process PRs that target this branch to avoid adding
                        # wrong promoted-to labels to PRs from other branches
                        pr_base_branch = pr_data.get("base", {}).get("ref", "")
                        if pr_base_branch != target_branch:
                            print(f"Skipping PR #{pr_num} from 'Closes' reference: targets {pr_base_branch}, not {target_branch}")
                            continue
                        prs.append(pr_data)
        for pr in prs:
            # Body can legitimately be None (empty PR description) - don't let
            # that crash the run and strand every later PR/commit unprocessed.
            pr_body = pr.get("body") or ""
            match = re.findall(r'Parent PR: #(\d+)', pr_body)
            pr_number = int(match[0]) if match else pr["number"]
            # Multiple scanned commits can resolve to the same PR (e.g. several
            # commits of one squashed/merged PR all matching the sha search) -
            # skip it once already labeled instead of re-issuing the same POST.
            if pr_number in processed_prs:
                continue
            if match:
                version_ref = re.search(r'-(\d+\.\d+)', ref)
                label_to_add = f'backport/{version_ref.group(1)}-done'
                label_to_remove = f'backport/{version_ref.group(1)}'
                remove_label_url = f'https://api.github.com/repos/{repository}/issues/{pr_number}/labels/{label_to_remove}'
                del_data = {
                    "labels": [f'{label_to_remove}']
                }
                response = http.delete(remove_label_url, headers=headers, json=del_data)
                # A 404 means the PR simply didn't have that label anymore - fine.
                _raise_for_status(response, f"Removing label {label_to_remove} from PR #{pr_number}", ok_statuses=(404,))
                print(f'Label {label_to_remove} removed successfully')
            else:
                label_to_add = promoted_label
            data = {
                "labels": [f'{label_to_add}']
            }
            add_label_url = f'https://api.github.com/repos/{repository}/issues/{pr_number}/labels'
            response = http.post(add_label_url, headers=headers, json=data)
            _raise_for_status(response, f"Adding label {label_to_add} to PR #{pr_number}")
            print(f"Label added successfully to {add_label_url}")
            # Only mark a PR processed once its label update actually succeeded,
            # so a failure here surfaces loudly instead of the run reporting
            # success while the PR is still unlabeled.
            processed_prs.add(pr_number)
    return processed_prs


def main():
    args = parser()
    g = Github(github_token, retry=GITHUB_RETRY)
    repo = g.get_repo(args.repository, lazy=False)
    start_commit, end_commit = args.commits.split('..')
    commits = repo.compare(start_commit, end_commit).commits
    label_promoted_commits(args.repository, commits, args.ref, args.label, github_token)


if __name__ == "__main__":
    main()
