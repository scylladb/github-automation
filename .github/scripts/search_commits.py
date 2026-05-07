#!/usr/bin/env python3

import re
import requests
from github import Github
import argparse
import sys
import os

try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


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


def main():
    args = parser()

    # Skip gating branches (next-X.Y, next) - labels should only be added
    # when commits are promoted to the stable branch (branch-X.Y, master)
    branch = args.ref.replace('refs/heads/', '') if args.ref.startswith('refs/heads/') else args.ref
    if branch.startswith('next-') or branch == 'next':
        print(f"Skipping push to gating branch {branch} - waiting for promotion to stable branch")
        return

    g = Github(github_token)
    repo = g.get_repo(args.repository, lazy=False)
    start_commit, end_commit = args.commits.split('..')
    commits = repo.compare(start_commit, end_commit).commits
    # Compute the correct promoted label based on the branch
    promoted_label = get_promoted_label_for_ref(args.ref) if args.label == 'promoted-to-master' else args.label
    processed_prs = set()
    for commit in commits:
        search_url = f'https://api.github.com/search/issues'
        query = f"repo:{args.repository} is:pr is:closed sha:{commit.sha}"
        params = {
            "q": query,
        }
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        response = requests.get(search_url, headers=headers, params=params)
        prs = response.json().get("items", [])
        # Fallback: if the commit message has "Closes" references, include those PRs too
        # This handles PRs closed by pushing a rebased commit directly (different SHA than the PR's head)
        found_pr_numbers = {pr["number"] for pr in prs}
        close_refs = re.findall(
            rf'Closes\s+(?:{re.escape(args.repository)}#|#)(\d+)',
            commit.commit.message
        )
        for ref in close_refs:
            pr_num = int(ref)
            if pr_num not in found_pr_numbers and pr_num not in processed_prs:
                pr_url = f'https://api.github.com/repos/{args.repository}/pulls/{pr_num}'
                pr_response = requests.get(pr_url, headers=headers)
                if pr_response.ok:
                    pr_data = pr_response.json()
                    if pr_data.get("state") == "closed":
                        prs.append(pr_data)
        for pr in prs:
            match = re.findall(r'Parent PR: #(\d+)', pr["body"])
            if match:
                pr_number = int(match[0])
                if pr_number in processed_prs:
                    continue
                ref = re.search(r'-(\d+\.\d+)', args.ref)
                label_to_add = f'backport/{ref.group(1)}-done'
                label_to_remove = f'backport/{ref.group(1)}'
                remove_label_url = f'https://api.github.com/repos/{args.repository}/issues/{pr_number}/labels/{label_to_remove}'
                del_data = {
                    "labels": [f'{label_to_remove}']
                }
                response = requests.delete(remove_label_url, headers=headers, json=del_data)
                if response.ok:
                    print(f'Label {label_to_remove} removed successfully')
                else:
                    print(f'Label {label_to_remove} cant be removed')
            else:
                pr_number = pr["number"]
                label_to_add = promoted_label
            data = {
                "labels": [f'{label_to_add}']
            }
            add_label_url = f'https://api.github.com/repos/{args.repository}/issues/{pr_number}/labels'
            response = requests.post(add_label_url, headers=headers, json=data)
            if response.ok:
                print(f"Label added successfully to {add_label_url}")
            else:
                print(f"No label was added to {add_label_url}")
            processed_prs.add(pr_number)


if __name__ == "__main__":
    main()
