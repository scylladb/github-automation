#!/usr/bin/env python3

import argparse
import base64
import os
import re
import sys
import tempfile
import time
import logging
import requests
from typing import Optional, List, Dict, Tuple

from github import Github, GithubException
from git import Repo, GitCommandError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# GitHub token
try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)

# Jira credentials (optional - only needed for Jira integration)
# Supports USER_AND_KEY_FOR_JIRA_AUTOMATION in "user:token" format
JIRA_AUTH = os.environ.get("JIRA_AUTH", "")
if ":" in JIRA_AUTH:
    JIRA_USER, JIRA_API_TOKEN = JIRA_AUTH.split(":", 1)
else:
    JIRA_USER = None
    JIRA_API_TOKEN = None
JIRA_BASE_URL = "https://scylladb.atlassian.net"

# GitHub Actions run URL for error reporting
GITHUB_RUN_URL = os.environ.get("GITHUB_SERVER_URL", "https://github.com") + "/" + \
                 os.environ.get("GITHUB_REPOSITORY", "") + "/actions/runs/" + \
                 os.environ.get("GITHUB_RUN_ID", "")

# Label for Jira sub-issue creation failure
JIRA_FAILURE_LABEL = "jira-sub-issue-creation-failed"
SCYLLADB_REPO_NAME = "scylladb/scylladb"
SCYLLA_PKG_REPO_NAME = "scylladb/scylla-pkg"
MILESTONE_REPOS = {SCYLLADB_REPO_NAME, SCYLLA_PKG_REPO_NAME}
_scylladb_repo_cache = None


def is_pull_request():
    return '--pull-request' in sys.argv[1:]


def is_chain_backport():
    return '--chain-backport' in sys.argv[1:]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=str, required=True, help='Github repository name')
    parser.add_argument('--base-branch', type=str, default='refs/heads/next', help='Base branch')
    parser.add_argument('--commits', default=None, type=str, help='Range of promoted commits.')
    parser.add_argument('--pull-request', type=int, help='Pull request number to be backported')
    parser.add_argument('--head-commit', type=str, required=is_pull_request(), help='The HEAD of target branch after the pull request specified by --pull-request is merged')
    parser.add_argument('--label', type=str, required=is_pull_request(), help='Backport label name when --pull-request is defined')
    parser.add_argument('--chain-backport', action='store_true', help='Process chain backport from merged backport PR')
    parser.add_argument('--merged-pr', type=int, help='Merged backport PR number for chain processing')
    parser.add_argument('--promoted-to-branch', type=str, help='Branch name for push events to version branches (e.g., branch-2025.4)')
    return parser.parse_args()


# ============================================================================
# Branch naming helpers
# ============================================================================

def get_branch_prefix(repo_name: str) -> str:
    """
    Get the branch prefix based on repository.
    
    scylladb/scylladb: Uses 'branch-X.Y' as target for backport PRs.
                       Has gating for both master and releases:
                       - Master: PR → next → master
                       - Releases: PR → next-X.Y → branch-X.Y
    
    Other repos: Use 'next-X.Y' as target for backport PRs.
                 Gating: PR → next-X.Y → branch-X.Y
    """
    if repo_name == "scylladb/scylladb":
        return "branch-"
    return "next-"


def is_manager_version(version: str) -> bool:
    """
    Check if the version is a manager version (prefixed with 'manager-').
    Example: 'manager-3.4' returns True, '2025.4' returns False.
    """
    return version.startswith("manager-")


def get_branch_name(repo_name: str, version: str) -> str:
    """
    Get the full branch name for a version.
    For manager versions (manager-X.Y), returns 'manager-X.Y'.
    For regular versions, returns 'branch-X.Y' or 'next-X.Y' based on repo.
    """
    if is_manager_version(version):
        # Manager versions use the version as-is for branch name
        return version
    prefix = get_branch_prefix(repo_name)
    return f"{prefix}{version}"


def parse_version(version_str: str) -> Tuple[int, int]:
    """
    Parse version string like '2025.4' or 'manager-3.4' into tuple for sorting.
    Manager versions are sorted separately (lower priority than regular versions).
    """
    # Strip 'manager-' prefix if present for parsing
    clean_version = version_str.replace("manager-", "") if version_str.startswith("manager-") else version_str
    parts = clean_version.split('.')
    # Manager versions get a lower sort priority (0 prefix) vs regular versions (1 prefix)
    prefix = 0 if is_manager_version(version_str) else 1
    return (prefix, int(parts[0]), int(parts[1]))


def sort_versions_descending(versions: List[str]) -> List[str]:
    """
    Sort versions in descending order (highest first).
    Regular versions (2025.4, 2025.3) come before manager versions (manager-3.4).
    """
    return sorted(versions, key=parse_version, reverse=True)


# ============================================================================
# Milestone helpers
# ============================================================================

def get_scylladb_repo():
    global _scylladb_repo_cache
    if _scylladb_repo_cache is None:
        g = Github(github_token)
        _scylladb_repo_cache = g.get_repo(SCYLLADB_REPO_NAME)
    return _scylladb_repo_cache


def parse_version_triplet(version_str: str) -> Optional[Tuple[int, int, int]]:
    try:
        major, minor, patch = version_str.split('.')
        return int(major), int(minor), int(patch)
    except ValueError:
        return None


def find_master_version_from_file() -> Optional[str]:
    """
    Fetch the VERSION from SCYLLA-VERSION-GEN file in scylladb/scylladb master branch.
    The file contains a line like: VERSION=2026.2.0-dev
    Returns the version without the -dev suffix (e.g., '2026.2.0').
    """
    try:
        url = "https://raw.githubusercontent.com/scylladb/scylladb/master/SCYLLA-VERSION-GEN"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Look for VERSION=X.Y.Z-dev pattern
        match = re.search(r'^VERSION=(\d+\.\d+\.\d+)-dev\s*$', response.text, re.MULTILINE)
        if match:
            return match.group(1)
        
        logging.warning("Could not find VERSION=X.Y.Z-dev pattern in SCYLLA-VERSION-GEN")
        return None
    except Exception as e:
        logging.warning(f"Failed to fetch SCYLLA-VERSION-GEN: {e}")
        return None


def find_latest_patch_for_branch(repo, version: str) -> Optional[int]:
    """
    Find the latest patch number for a branch version by scanning tags.
    
    Looks for:
    - Released tags: scylla-X.Y.Z or scylla-X.Y.Z-candidate-...
    - RC tags: scylla-X.Y.Z-rcN or scylla-X.Y.Z-rcN-candidate-...
    
    For branches with only rc tags (no GA release yet), returns -1 so milestone becomes X.Y.0.
    """
    escaped_version = re.escape(version)
    
    # Pattern for released versions: scylla-X.Y.Z or scylla-X.Y.Z-candidate-...
    release_pattern = re.compile(rf'^scylla-{escaped_version}\.(\d+)(?:-candidate-[\w\.-]+)?$')
    # Pattern for rc versions: scylla-X.Y.Z-rcN or scylla-X.Y.Z-rcN-candidate-...
    rc_pattern = re.compile(rf'^scylla-{escaped_version}\.(\d+)-rc\d+(?:-candidate-[\w\.-]+)?$')
    
    latest_patch = None
    has_rc_tags = False
    
    for tag in repo.get_tags():
        # Check for released version first
        match = release_pattern.match(tag.name)
        if match:
            patch = int(match.group(1))
            if latest_patch is None or patch > latest_patch:
                latest_patch = patch
            continue
        
        # Check for rc version (branch exists but no GA release yet)
        rc_match = rc_pattern.match(tag.name)
        if rc_match:
            has_rc_tags = True
            patch = int(rc_match.group(1))
            # Track rc patch but don't override a real release
            if latest_patch is None:
                latest_patch = patch - 1  # Will result in patch+1 = the rc version
    
    # If we found rc tags but no releases, the first release will be X.Y.0
    if latest_patch is None and has_rc_tags:
        return -1  # Results in milestone X.Y.0
    
    return latest_patch


def resolve_master_milestone_title() -> Optional[str]:
    """
    Resolve milestone for master PRs from SCYLLA-VERSION-GEN file.
    E.g., if VERSION=2026.2.0-dev, milestone is '2026.2.0'.
    """
    try:
        master_version = find_master_version_from_file()
        if not master_version:
            logging.warning("Could not resolve master milestone from SCYLLA-VERSION-GEN")
            return None
        return master_version
    except Exception as e:
        logging.warning(f"Failed to resolve master milestone: {e}")
        return None


def resolve_backport_milestone_title(version: str) -> Optional[str]:
    if not version or is_manager_version(version):
        return None
    try:
        repo = get_scylladb_repo()
        latest_patch = find_latest_patch_for_branch(repo, version)
        if latest_patch is None:
            logging.warning(f"No tags found for backport version {version}")
            return None
        return f"{version}.{latest_patch + 1}"
    except Exception as e:
        logging.warning(f"Failed to resolve backport milestone for {version}: {e}")
        return None


def find_or_create_milestone(repo, title: str):
    for milestone in repo.get_milestones(state='all'):
        if milestone.title == title:
            return milestone
    try:
        return repo.create_milestone(title=title)
    except GithubException as e:
        logging.warning(f"Failed to create milestone '{title}': {e}")
        return None


def set_pr_milestone(pr, milestone_title: Optional[str]) -> bool:
    if not milestone_title:
        return False
    if pr.milestone and pr.milestone.title == milestone_title:
        return True
    milestone = find_or_create_milestone(pr.base.repo, milestone_title)
    if not milestone:
        return False
    try:
        # Use as_issue() since milestone is an Issue property, not a PullRequest property
        issue = pr.as_issue()
        issue.edit(milestone=milestone)
        logging.info(f"Set milestone '{milestone_title}' on PR #{pr.number}")
        return True
    except GithubException as e:
        logging.warning(f"Failed to set milestone '{milestone_title}' on PR #{pr.number}: {e}")
        return False


# ============================================================================
# Jira API functions
# ============================================================================

def jira_api_request(method: str, endpoint: str, data: dict = None) -> Optional[dict]:
    """Make a request to Jira API."""
    if not JIRA_USER or not JIRA_API_TOKEN:
        logging.warning("Jira credentials not configured")
        return None
    
    url = f"{JIRA_BASE_URL}/rest/api/3/{endpoint}"
    auth_string = base64.b64encode(f"{JIRA_USER}:{JIRA_API_TOKEN}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data)
        elif method == "PUT":
            response = requests.put(url, headers=headers, json=data)
        else:
            logging.error(f"Unsupported HTTP method: {method}")
            return None
        
        response.raise_for_status()
        return response.json() if response.text else {}
    except requests.exceptions.RequestException as e:
        logging.error(f"Jira API request failed: {e}")
        return None


def get_jira_issue(issue_key: str) -> Optional[dict]:
    """Get a Jira issue by key."""
    return jira_api_request("GET", f"issue/{issue_key}")


def find_jira_user_by_email(email: str) -> Optional[str]:
    """
    Find a Jira user's accountId by their email address.
    Returns the accountId if found, None otherwise.
    """
    if not email or not JIRA_USER or not JIRA_API_TOKEN:
        return None
    
    try:
        # URL encode the email for the query parameter
        encoded_email = requests.utils.quote(email)
        result = jira_api_request("GET", f"user/search?query={encoded_email}")
        
        if result and len(result) > 0:
            # Return the first matching user's accountId
            account_id = result[0].get("accountId")
            if account_id:
                logging.info(f"Found Jira user for email {email}: {account_id}")
                return account_id
        
        logging.warning(f"No Jira user found for email: {email}")
    except Exception as e:
        logging.warning(f"Error searching for Jira user by email {email}: {e}")
    
    return None


def get_jira_user_from_github_user(github_user) -> Optional[str]:
    """
    Get Jira accountId from a GitHub user object.
    Tries to find the Jira user by:
    1. GitHub user's public email (if available)
    2. Constructed email using GitHub username @scylladb.com (as fallback)
    
    Args:
        github_user: PyGithub NamedUser object
        
    Returns:
        Jira accountId if found, None otherwise
    """
    if not github_user:
        return None
    
    try:
        # Try to get the user's email from GitHub
        email = github_user.email
        if email:
            account_id = find_jira_user_by_email(email)
            if account_id:
                return account_id
        
        # Fallback: try to find Jira user by constructed email using GitHub username
        # Many organizations use username@company.com pattern
        constructed_email = f"{github_user.login}@scylladb.com"
        logging.info(f"No public email for GitHub user {github_user.login}, trying constructed email: {constructed_email}")
        account_id = find_jira_user_by_email(constructed_email)
        if account_id:
            return account_id
        
        # Fallback: try firstname.lastname@scylladb.com from the user's display name
        if github_user.name:
            name_parts = github_user.name.lower().split()
            if len(name_parts) >= 2:
                name_email = f"{'.'.join(name_parts)}@scylladb.com"
                logging.info(f"Trying name-based email for GitHub user {github_user.login}: {name_email}")
                account_id = find_jira_user_by_email(name_email)
                if account_id:
                    return account_id
        
        logging.warning(f"Could not find Jira user for GitHub user {github_user.login} by email")
    except Exception as e:
        logging.warning(f"Error getting Jira user from GitHub user: {e}")
    
    return None


def assign_jira_issue(issue_key: str, account_id: str) -> bool:
    """
    Assign a Jira issue to a user by their accountId.
    
    Args:
        issue_key: The Jira issue key (e.g., 'PROJ-123')
        account_id: The Jira user's accountId
        
    Returns:
        True if assignment was successful, False otherwise
    """
    if not issue_key or not account_id:
        return False
    
    try:
        assign_data = {"accountId": account_id}
        result = jira_api_request("PUT", f"issue/{issue_key}/assignee", assign_data)
        if result is not None:  # PUT returns empty response on success
            logging.info(f"Assigned Jira issue {issue_key} to user {account_id}")
            return True
    except Exception as e:
        logging.warning(f"Error assigning Jira issue {issue_key}: {e}")
    
    return False


def extract_jira_key_from_pr_body(pr_body: str) -> Optional[str]:
    """Extract first Jira issue key from PR body 'Fixes:' line."""
    if not pr_body:
        return None
    
    # Match patterns like:
    #   "Fixes: PROJ-123"
    #   "Fixes:PROJ-123"
    #   "Fixes: https://scylladb.atlassian.net/browse/PROJ-123"
    #   "Fixes: [PROJ-123](https://scylladb.atlassian.net/browse/PROJ-123)"
    match = re.search(r'[Ff]ixes:\s*(?:\[([A-Z]+-\d+)\]\([^)]*\)|(?:https?://[^\s/]+/browse/)?([A-Z]+-\d+))', pr_body)
    if match:
        return match.group(1) or match.group(2)
    return None


def extract_all_jira_keys_from_pr_body(pr_body: str) -> List[str]:
    """Extract ALL Jira issue keys from PR body 'Fixes:' lines."""
    if not pr_body:
        return []
    
    # Find all patterns like:
    #   "Fixes: PROJ-123"
    #   "Fixes:PROJ-123"
    #   "Fixes: https://scylladb.atlassian.net/browse/PROJ-123"
    #   "Fixes: [PROJ-123](https://scylladb.atlassian.net/browse/PROJ-123)"
    matches = re.findall(r'[Ff]ixes:\s*(?:\[([A-Z]+-\d+)\]\([^)]*\)|(?:https?://[^\s/]+/browse/)?([A-Z]+-\d+))', pr_body)
    # findall with groups returns list of tuples; pick the non-empty group from each
    return [m[0] or m[1] for m in matches]


def has_fixes_reference(pr_body: str) -> bool:
    """
    Check if PR body contains a valid Fixes reference.
    Supports Jira keys and GitHub issues/PRs.
    """
    if not pr_body:
        return False

    jira_match = re.search(r'[Ff]ixes:?\s*(?:\[([A-Z]+-\d+)\]\([^)]*\)|(?:https?://[^\s/]+/browse/)?([A-Z]+-\d+))', pr_body)
    github_match = re.search(
        r'[Ff]ixes:?\s*(?:https?://github\.com/[^\s/]+/[^\s/]+/(?:issues|pull)/\d+|[^#\s]+/[^#\s]+#\d+|#\d+)\b',
        pr_body
    )
    return bool(jira_match or github_match)


def extract_project_from_jira_key(jira_key: str) -> str:
    """Extract project key from Jira issue key (e.g., 'SCYLLADB-123' -> 'SCYLLADB')."""
    return jira_key.split('-')[0]


def find_existing_sub_issue(parent_key: str, version: str) -> Optional[str]:
    """
    Search for an existing sub-issue for a specific backport version.
    Returns the issue key if found, None otherwise.
    
    Uses the parent issue's subtasks field (direct REST API) as the primary method,
    with JQL search as a fallback. The subtasks field is always consistent (no index delay),
    unlike JQL which has eventual consistency and may miss recently created issues.
    """
    if not JIRA_USER or not JIRA_API_TOKEN:
        return None
    
    try:
        # Primary method: fetch subtasks directly from the parent issue.
        # This is always consistent (no JQL index delay).
        parent_issue = get_jira_issue(parent_key)
        if parent_issue:
            subtasks = parent_issue.get("fields", {}).get("subtasks", [])
            for subtask in subtasks:
                summary = subtask.get("fields", {}).get("summary", "")
                if f"Backport {version}]" in summary or f"Backport {version} " in summary or summary.endswith(f"Backport {version}"):
                    existing_key = subtask["key"]
                    logging.info(f"Found existing Jira sub-issue for {parent_key} version {version}: {existing_key} (via parent subtasks)")
                    return existing_key
        
        # Fallback: JQL search (may have eventual consistency delay, but catches
        # edge cases where the parent's subtasks field is truncated or unavailable)
        jql = f'parent = {parent_key} AND summary ~ "Backport {version}" AND issuetype = Sub-task'
        result = jira_api_request("POST", "search/jql", data={"jql": jql, "maxResults": 10})
        
        if result and result.get("issues"):
            # Check for exact version match to avoid matching 2025.4 when looking for 2025.40
            for issue in result["issues"]:
                summary = issue["fields"]["summary"]
                # Check if the version appears as a complete version number
                if f"Backport {version}]" in summary or f"Backport {version} " in summary or summary.endswith(f"Backport {version}"):
                    existing_key = issue["key"]
                    logging.info(f"Found existing Jira sub-issue for {parent_key} version {version}: {existing_key} (via JQL fallback)")
                    return existing_key
        
        logging.info(f"No existing Jira sub-issue found for {parent_key} version {version}")
    except Exception as e:
        logging.warning(f"Error searching for existing sub-issue: {e}")
        
    return None


def is_subtask_issue(issue: dict) -> bool:
    """
    Check if a Jira issue is a sub-task.
    
    Args:
        issue: The Jira issue dict from get_jira_issue()
        
    Returns True if the issue is a sub-task, False otherwise.
    """
    try:
        issue_type = issue.get("fields", {}).get("issuetype", {})
        return issue_type.get("subtask", False)
    except Exception:
        return False


def get_parent_key_if_subtask(issue: dict) -> Optional[str]:
    """
    Get the parent issue key if the given issue is a sub-task.
    
    Args:
        issue: The Jira issue dict from get_jira_issue()
        
    Returns the parent issue key if this is a sub-task, None otherwise.
    """
    try:
        if is_subtask_issue(issue):
            parent = issue.get("fields", {}).get("parent", {})
            return parent.get("key")
    except Exception:
        pass
    return None


def create_jira_sub_issue(parent_key: str, version: str, original_title: str, assignee_account_id: str = None) -> Optional[str]:
    """
    Create a Jira sub-issue for a backport.
    
    If the parent issue is already a sub-task (Jira only allows 2 levels of hierarchy),
    the new sub-task will be created under the parent's parent instead, with the
    description updated to reference the original sub-task.
    
    Args:
        parent_key: The parent Jira issue key (e.g., 'PROJ-123')
        version: The backport version (e.g., '2025.4')
        original_title: The original issue/PR title
        assignee_account_id: Optional Jira accountId to assign the sub-issue to
        
    Returns the new issue key or None on failure.
    """
    # Get the issue to check if it's a sub-task
    original_issue = get_jira_issue(parent_key)
    if not original_issue:
        logging.error(f"Failed to fetch Jira issue: {parent_key}")
        return None
    
    # Determine the actual parent for the new sub-task
    # If the original issue is already a sub-task, use its parent instead
    actual_parent_key = parent_key
    original_was_subtask = False
    grandparent_key = get_parent_key_if_subtask(original_issue)
    
    if grandparent_key:
        original_was_subtask = True
        actual_parent_key = grandparent_key
        logging.info(f"Issue {parent_key} is already a sub-task of {grandparent_key}. "
                     f"Creating new sub-task under {grandparent_key} instead.")
    
    # First check if sub-issue already exists under the actual parent
    existing_key = find_existing_sub_issue(actual_parent_key, version)
    if existing_key:
        # If sub-issue exists but we have an assignee, try to assign it
        if assignee_account_id:
            assign_jira_issue(existing_key, assignee_account_id)
        return existing_key
    
    project_key = extract_project_from_jira_key(actual_parent_key)
    
    sub_issue_title = f"[Backport {version}] - {original_title}"
    
    # Build description based on whether original was a sub-task
    if original_was_subtask:
        description = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": "Backporting of "
                        },
                        {
                            "type": "text",
                            "text": parent_key,
                            "marks": [
                                {
                                    "type": "link",
                                    "attrs": {
                                        "href": f"{JIRA_BASE_URL}/browse/{parent_key}"
                                    }
                                }
                            ]
                        },
                        {
                            "type": "text",
                            "text": " (sub-task of "
                        },
                        {
                            "type": "text",
                            "text": actual_parent_key,
                            "marks": [
                                {
                                    "type": "link",
                                    "attrs": {
                                        "href": f"{JIRA_BASE_URL}/browse/{actual_parent_key}"
                                    }
                                }
                            ]
                        },
                        {
                            "type": "text",
                            "text": f") to version {version}"
                        }
                    ]
                }
            ]
        }
    else:
        description = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": "Backporting of "
                        },
                        {
                            "type": "text",
                            "text": parent_key,
                            "marks": [
                                {
                                    "type": "link",
                                    "attrs": {
                                        "href": f"{JIRA_BASE_URL}/browse/{parent_key}"
                                    }
                                }
                            ]
                        },
                        {
                            "type": "text",
                            "text": f" to version {version}"
                        }
                    ]
                }
            ]
        }
    
    # Create the sub-task
    issue_data = {
        "fields": {
            "project": {"key": project_key},
            "parent": {"key": actual_parent_key},
            "summary": sub_issue_title,
            "description": description,
            "issuetype": {"name": "Sub-task"}
        }
    }
    
    # Add assignee if provided
    if assignee_account_id:
        issue_data["fields"]["assignee"] = {"accountId": assignee_account_id}
    
    result = jira_api_request("POST", "issue", issue_data)
    if result and "key" in result:
        logging.info(f"Created Jira sub-issue: {result['key']} under parent {actual_parent_key}")
        return result["key"]
    
    logging.error(f"Failed to create Jira sub-issue for {parent_key} version {version}")
    return None


def add_jira_comment(issue_key: str, comment: str) -> bool:
    """Add a comment to a Jira issue with Atlassian Document Format."""
    comment_data = {
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": comment.split("[")[0].strip() if "[" in comment else comment
                        }
                    ]
                }
            ]
        }
    }
    
    # If there's a link in the comment, add it properly
    if "[" in comment and "|" in comment:
        # Parse Jira markup link: [text|url]
        link_match = re.search(r'\[([^\|]+)\|([^\]]+)\]', comment)
        if link_match:
            link_text = link_match.group(1)
            link_url = link_match.group(2)
            comment_data = {
                "body": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [
                                {
                                    "type": "text",
                                    "text": comment.split("[")[0].strip() + " "
                                },
                                {
                                    "type": "text",
                                    "text": link_text,
                                    "marks": [
                                        {
                                            "type": "link",
                                            "attrs": {"href": link_url}
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
    
    result = jira_api_request("POST", f"issue/{issue_key}/comment", comment_data)
    return result is not None


def report_jira_failure(main_jira_key: str, version: str):
    """Report Jira sub-issue creation failure by adding a comment to the main issue."""
    comment = f"Failed to create backport sub-issue for version {version}. [View workflow run|{GITHUB_RUN_URL}]"
    if add_jira_comment(main_jira_key, comment):
        logging.info(f"Added failure comment to {main_jira_key}")
    else:
        logging.error(f"Failed to add comment to {main_jira_key}")


# ============================================================================
# PR body parsing for chain backports
# ============================================================================

def extract_main_pr_link_from_body(pr_body: str) -> Optional[str]:
    """Extract the main PR link from backport PR body.
    
    Supports both formats:
    - New format: "This PR is a backport of PR scylladb/repo#1234" or "backport of PR #1234"
    - Old format: "Parent PR: #1234"
    """
    if not pr_body:
        return None
    
    # Try new format first: "backport of PR ..."
    match = re.search(r'backport of PR\s+(\S+)', pr_body, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Try old format: "Parent PR: #1234"
    parent_match = re.search(r'Parent PR:\s*(#\d+)', pr_body, re.IGNORECASE)
    if parent_match:
        return parent_match.group(1)
    
    return None


def get_original_pr_from_backport(repo, backport_pr) -> Optional[object]:
    """
    Get the original PR object from a backport PR.
    Extracts the PR number from the backport PR body and fetches the PR.
    
    Supports formats:
    - "This PR is a backport of PR scylladb/scylla-pkg#1234"
    - "This PR is a backport of PR #1234"
    - "Parent PR: #1234"
    
    Returns the PR object or None if not found.
    """
    pr_body = backport_pr.body
    if not pr_body:
        return None
    
    # Try new format first: "backport of PR scylladb/repo#1234" or "backport of PR #1234"
    main_pr_link = extract_main_pr_link_from_body(pr_body)
    
    # Try old format: "Parent PR: #1234"
    if not main_pr_link:
        parent_match = re.search(r'Parent PR:\s*#(\d+)', pr_body, re.IGNORECASE)
        if parent_match:
            main_pr_link = f"#{parent_match.group(1)}"
    
    if not main_pr_link:
        return None
    
    # Extract PR number from link
    pr_number_match = re.search(r'#(\d+)', main_pr_link)
    if not pr_number_match:
        return None
    
    try:
        return repo.get_pull(int(pr_number_match.group(1)))
    except Exception as e:
        logging.warning(f"Could not fetch original PR from link '{main_pr_link}': {e}")
        return None


def extract_main_jira_from_body(pr_body: str) -> Optional[str]:
    """Extract the main Jira issue from backport PR body."""
    if not pr_body:
        return None
    
    match = re.search(r'main Jira issue is\s+([A-Z]+-\d+)', pr_body, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def is_backport_pr(pr_title: str, pr_body: str) -> bool:
    """Check if this PR is a backport PR created by automation."""
    # Check title format: [Backport X.Y] or [Backport manager-X.Y] ...
    if pr_title and re.match(r'\[Backport (manager-)?\d+\.\d+\]', pr_title):
        return True
    
    # Check body for new format
    if pr_body and 'backport of PR' in pr_body.lower():
        return True
    
    # Check body for old format (Parent PR: #1234)
    if pr_body and 'parent pr:' in pr_body.lower():
        return True
    
    return False


def get_root_original_pr(repo, pr, max_depth: int = 10) -> Optional[object]:
    """
    Trace back through the backport chain to find the root original PR.
    This follows the chain of "backport of PR #X" links until we find a PR
    that is not itself a backport.
    
    Args:
        repo: PyGithub Repository object
        pr: Starting PR (can be a backport or original)
        max_depth: Maximum number of hops to prevent infinite loops
        
    Returns:
        The root original PR object, or the input PR if it's not a backport
    """
    current_pr = pr
    depth = 0
    
    while depth < max_depth:
        # If current PR is not a backport, we found the root
        if not is_backport_pr(current_pr.title, current_pr.body):
            logging.info(f"Found root original PR #{current_pr.number} (author: {current_pr.user.login})")
            return current_pr
        
        # Try to get the parent PR
        parent_pr = get_original_pr_from_backport(repo, current_pr)
        if not parent_pr:
            # Can't trace further, return current
            logging.warning(f"Could not trace parent of backport PR #{current_pr.number}, returning current")
            return current_pr
        
        logging.info(f"Tracing backport chain: PR #{current_pr.number} -> PR #{parent_pr.number}")
        current_pr = parent_pr
        depth += 1
    
    logging.warning(f"Max depth ({max_depth}) reached while tracing backport chain, returning last PR #{current_pr.number}")
    return current_pr


def extract_original_title(pr_title: str) -> str:
    """
    Extract the original PR title by stripping any [Backport X.Y] or [Backport manager-X.Y] prefixes.
    This prevents title stacking like '[Backport 2025.3] [Backport 2025.4] Original title'.
    
    Examples:
        '[Backport 2025.4] Fix bug' -> 'Fix bug'
        '[Backport manager-3.4] Fix bug' -> 'Fix bug'
        '[Backport 2025.3] [Backport 2025.4] Fix bug' -> 'Fix bug'
        'Fix bug' -> 'Fix bug'
    """
    if not pr_title:
        return pr_title
    
    # Keep stripping [Backport X.Y] or [Backport manager-X.Y] prefixes until none remain
    result = pr_title
    while True:
        match = re.match(r'\[Backport (manager-)?\d+\.\d+\]\s*', result)
        if match:
            result = result[match.end():]
        else:
            break
    
    return result.strip() if result else pr_title


# ============================================================================
# PR body generation
# ============================================================================

def strip_cherry_pick_info(body: str) -> str:
    """
    Strip existing cherry-pick markers and Parent PR references from a PR body.
    This prevents stacking of cherry-pick info when doing chain backports.
    
    Removes lines like:
    - (cherry picked from commit <sha>)
    Parent PR: #1234
    """
    if not body:
        return body
    
    # Remove cherry-pick lines and Parent PR lines
    lines = body.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip cherry-pick marker lines
        if re.match(r'^-?\s*\(?cherry picked from commit [0-9a-f]+\)?$', stripped):
            continue
        # Skip Parent PR reference lines
        if re.match(r'^Parent PR:\s*#\d+$', stripped):
            continue
        cleaned_lines.append(line)
    
    # Remove trailing blank lines that were left after stripping
    while cleaned_lines and cleaned_lines[-1].strip() == '':
        cleaned_lines.pop()
    
    return '\n'.join(cleaned_lines)


def replace_fixes_in_body(original_body: str, jira_mapping: Dict[str, str]) -> str:
    """
    Replace Fixes references in PR body with new Jira keys based on mapping.
    If no Fixes reference exists, returns the original body unchanged.
    
    Args:
        original_body: The original PR body
        jira_mapping: Dict mapping original Jira keys to their sub-task keys
                     e.g., {'SCYLLADB-123': 'SCYLLADB-9012', 'SCYLLADB-456': 'SCYLLADB-9013'}
    """
    if not original_body or not jira_mapping:
        return original_body
    
    # Match Jira Fixes patterns:
    #   "Fixes: PROJ-123"
    #   "Fixes: https://...browse/PROJ-123"
    #   "Fixes: [PROJ-123](https://...browse/PROJ-123)"
    jira_pattern = r'([Ff]ixes:\s*)(?:\[([A-Z]+-\d+)\]\([^)]*\)|(?:https?://[^\s/]+/browse/)?([A-Z]+-\d+))'
    
    def replace_match(match):
        prefix = match.group(1)  # "Fixes: "
        original_key = match.group(2) or match.group(3)  # "PROJ-123"
        # Use the mapped sub-task key if available, otherwise keep original
        new_key = jira_mapping.get(original_key, original_key)
        return f"{prefix}{new_key}"
    
    # Replace all Jira keys with their corresponding sub-tasks
    new_body = re.sub(jira_pattern, replace_match, original_body)
    return new_body


def generate_backport_pr_body(
    original_pr_body: str,
    main_pr_link: str,
    jira_mapping: Dict[str, str],
    commits: List[str]
) -> str:
    """
    Generate the PR body for a backport PR.
    Uses the original PR body and replaces Fixes references with the sub-tasks.
    
    Args:
        original_pr_body: Original PR body text
        main_pr_link: Link to the parent PR (e.g., "scylladb/scylladb#5678")
        jira_mapping: Dict mapping original Jira keys to their sub-task keys
        commits: List of commit SHAs that were cherry-picked
    """
    body = ""
    
    # Add original PR body with modified Fixes references
    if original_pr_body:
        # Strip any existing cherry-pick markers and Parent PR lines to prevent
        # stacking in chain backports (e.g., master -> 2026.1 -> 2025.4)
        clean_body = strip_cherry_pick_info(original_pr_body)
        modified_body = replace_fixes_in_body(clean_body, jira_mapping)
        body += modified_body
        # Ensure there's spacing before cherry-pick info
        if not body.endswith('\n\n'):
            body += '\n\n' if body.endswith('\n') else '\n\n'
    
    # Add cherry-pick info
    for commit in commits:
        body += f"- (cherry picked from commit {commit})\n"
    
    # Add parent PR reference at the end
    # Extract just the PR number from the link (e.g., "scylladb/scylladb#5678" -> "5678")
    pr_number_match = re.search(r'#(\d+)', main_pr_link)
    if pr_number_match:
        body += f"\nParent PR: #{pr_number_match.group(1)}"
    
    return body


def create_pull_request(repo, new_branch_name, base_branch_name, pr, backport_pr_title, commits, 
                        is_draft=False, pr_body=None, jira_failed=False, remaining_backport_labels=None,
                        original_pr=None, warn_missing_fixes=False, backport_version: Optional[str] = None):
    """
    Create a backport pull request.
    
    Args:
        original_pr: The original PR from master (for assignment). If None, uses pr.
                     This ensures backport PRs are always assigned to the original author,
                     not to scylladbbot or intermediate backport PR authors.
    """
    if pr_body is None:
        # Fallback to original body format if no custom body provided
        pr_body = f'{pr.body}\n\n'
        for commit in commits:
            pr_body += f'- (cherry picked from commit {commit})\n\n'
        pr_body += f'Parent PR: #{pr.number}'
    
    # Determine who to assign the PR to - always use original PR author
    assign_to_pr = original_pr if original_pr else pr
    
    try:
        backport_pr = repo.create_pull(
            title=backport_pr_title,
            body=pr_body,
            head=f'scylladbbot:{new_branch_name}',
            base=base_branch_name,
            draft=is_draft
        )
        logging.info(f"Pull request created: {backport_pr.html_url}")
        backport_pr.add_to_assignees(assign_to_pr.user)
        logging.info(f"Assigned PR to original author: {assign_to_pr.user.login}")

        if warn_missing_fixes:
            warning_comment = (f"@{assign_to_pr.user.login} This backport PR can't be merged without a valid Fixes reference ")
            backport_pr.create_issue_comment(warning_comment)
        
        # Add labels to the backport PR
        labels_to_add = []
        
        # Check for priority labels (P0 or P1) in parent PR and add them to backport PR
        # Skip force_on_cloud for scylla-pkg and RELENG projects
        priority_labels = {"P0", "P1"}
        parent_pr_labels = [label.name for label in pr.labels]
        skip_force_on_cloud_repos = {"scylladb/scylla-pkg"}
        for label in priority_labels:
            if label in parent_pr_labels:
                labels_to_add.append(label)
                # Add force_on_cloud only for repos that need it (not scylla-pkg or RELENG)
                if repo.full_name not in skip_force_on_cloud_repos:
                    labels_to_add.append("force_on_cloud")
                    logging.info(f"Adding {label} and force_on_cloud labels from parent PR to backport PR")
                else:
                    logging.info(f"Adding {label} label from parent PR to backport PR (skipping force_on_cloud for {repo.full_name})")
                break  # Only apply the highest priority label
        
        # Add conflicts label if PR is in draft mode
        if is_draft:
            labels_to_add.append("conflicts")
            pr_comment = f"@{assign_to_pr.user.login} - This PR has conflicts, therefore it was moved to `draft` \n"
            pr_comment += "Please resolve them and mark this PR as ready for review by removing the conflicts label"
            backport_pr.create_issue_comment(pr_comment)
        
        # Note: promoted-to-<branch> label is NOT added here.
        # It will be added by the workflow when the commit is actually pushed/promoted to the branch.
        # This allows chain backports to trigger only after successful merge.
        
        # Add Jira failure label if sub-issue creation failed
        if jira_failed:
            labels_to_add.append(JIRA_FAILURE_LABEL)
            logging.info(f"Adding {JIRA_FAILURE_LABEL} label due to Jira sub-issue creation failure")
        
        # Add remaining backport labels for chain continuation
        if remaining_backport_labels:
            labels_to_add.extend(remaining_backport_labels)
            logging.info(f"Adding remaining backport labels for chain: {remaining_backport_labels}")
            
        # Apply all labels at once if we have any
        if labels_to_add:
            backport_pr.add_to_labels(*labels_to_add)
            logging.info(f"Added labels to backport PR: {labels_to_add}")
            
        if backport_version:
            milestone_title = resolve_backport_milestone_title(backport_version)
            set_pr_milestone(backport_pr, milestone_title)
        return backport_pr
    except GithubException as e:
        if 'A pull request already exists' in str(e):
            logging.warning(f'A pull request already exists for scylladbbot:{new_branch_name}')
            # Try to find and return the existing PR
            try:
                pulls = repo.get_pulls(state='open', head=f'scylladbbot:{new_branch_name}')
                for existing_pr in pulls:
                    logging.info(f"Found existing PR: {existing_pr.html_url}")
                    # Update the body with the latest info
                    existing_pr.edit(body=pr_body)
                    if warn_missing_fixes:
                        warning_comment = (f"@{assign_to_pr.user.login} This backport PR can't be merged without a valid Fixes reference ")
                        existing_pr.create_issue_comment(warning_comment)
                    if backport_version:
                        milestone_title = resolve_backport_milestone_title(backport_version)
                        set_pr_milestone(existing_pr, milestone_title)
                    return existing_pr
            except Exception as find_error:
                logging.warning(f"Could not find existing PR: {find_error}")
        else:
            logging.error(f'Failed to create PR: {e}')


def get_pr_commits(repo, pr, stable_branch, start_commit=None):
    commits = []
    logging.info(f"get_pr_commits: PR #{pr.number} state={pr.state}, merged={pr.merged}, "
                 f"merge_commit_sha={pr.merge_commit_sha}, stable_branch={stable_branch}, "
                 f"start_commit={start_commit}")
    if pr.merged:
        merge_commit = repo.get_commit(pr.merge_commit_sha)
        if len(merge_commit.parents) > 1:  # Check if this merge commit includes multiple commits
            commits.append(pr.merge_commit_sha)
        else:
            if start_commit:
                promoted_commits = list(repo.compare(start_commit, stable_branch).commits)
            else:
                promoted_commits = list(repo.get_commits(sha=stable_branch))
            
            if not promoted_commits:
                # If compare returns no commits (e.g., start_commit IS the branch tip),
                # fall back to using the merge_commit_sha directly.
                # This handles squash merges where head_commit == merge_commit_sha.
                logging.info(f"No promoted commits found between {start_commit} and {stable_branch}, "
                             f"using merge_commit_sha {pr.merge_commit_sha} directly")
                commits.append(pr.merge_commit_sha)
            else:
                pr_commits_list = list(pr.get_commits())
                for commit in pr_commits_list:
                    for promoted_commit in promoted_commits:
                        commit_title = commit.commit.message.splitlines()[0]
                        # In Scylla-pkg and scylla-dtest, for example,
                        # we don't create a merge commit for a PR with multiple commits,
                        # according to the GitHub API, the last commit will be the merge commit,
                        # which is not what we need when backporting (we need all the commits).
                        # So here, we are validating the correct SHA for each commit so we can cherry-pick
                        if promoted_commit.commit.message.startswith(commit_title):
                            commits.append(promoted_commit.sha)
                if len(commits) != len(pr_commits_list):
                    logging.warning(f"get_pr_commits: PR #{pr.number} has {len(pr_commits_list)} commits "
                                    f"but only {len(commits)} were matched by title against promoted commits. "
                                    f"PR commit titles: {[c.commit.message.splitlines()[0] for c in pr_commits_list]}")

    # For non-merged closed PRs, or as a fallback when the merged path found no commits,
    # look for the commit SHA from the close event. This handles PRs closed by direct push
    # (e.g., in scylladb/scylladb where commits are pushed directly to the branch).
    # commit.get_pulls() does not return such non-merged PRs.
    # We use the *last* referenced event because a PR may be queued (creating commit A),
    # dequeued, and re-queued (creating commit B that replaces A). The first referenced
    # event would point to commit A which may no longer exist on the branch.
    if not commits and pr.state == 'closed':
        events = pr.get_issue_events()
        last_referenced_commit = None
        for event in events:
            if event.event == 'referenced' and event.commit_id:
                last_referenced_commit = event.commit_id
        if last_referenced_commit:
            commits.append(last_referenced_commit)
            logging.info(f"Found close event commit for PR #{pr.number}: {last_referenced_commit}")
    return commits


def is_commit_in_branch(repo, commit_sha: str, branch_name: str) -> bool:
    """
    Check if a commit (or its cherry-pick) is already in the target branch.
    This handles both exact SHA matches and cherry-picked commits (by commit message).
    """
    try:
        # Get the commit message to search for
        commit = repo.get_commit(commit_sha)
        commit_title = commit.commit.message.splitlines()[0]
        
        # Search for commits in the target branch with the same title
        # This catches both the original commit and cherry-picks
        branch_commits = repo.get_commits(sha=branch_name)
        for branch_commit in branch_commits[:100]:  # Check last 100 commits
            branch_commit_title = branch_commit.commit.message.splitlines()[0]
            # Check if titles match (ignoring cherry-pick markers)
            if commit_title in branch_commit_title or branch_commit_title in commit_title:
                logging.info(f"Commit '{commit_title}' already exists in branch {branch_name}")
                return True
            # Also check for exact SHA match
            if branch_commit.sha == commit_sha:
                logging.info(f"Commit {commit_sha} already exists in branch {branch_name}")
                return True
    except Exception as e:
        logging.warning(f"Error checking if commit exists in branch: {e}")
    return False


def replace_backport_label_with_done(repo, pr, version: str):
    """
    Replace backport/X.Y, backport/manager-X.Y, or backport/X.Y-pending label with backport/X.Y-done on the original PR.
    
    This handles three cases:
    1. backport/X.Y -> backport/X.Y-done (direct backport from master)
    2. backport/X.Y-pending -> backport/X.Y-done (chain backport completion)
    3. No label found (already done or never existed)
    """
    original_label = f"backport/{version}"
    pending_label = f"backport/{version}-pending"
    done_label = f"backport/{version}-done"
    
    try:
        # Get labels from the PR
        labels = [label.name for label in pr.labels]
        
        # Check for original label first
        if original_label in labels:
            pr.remove_from_labels(original_label)
            logging.info(f"Removed label '{original_label}' from PR #{pr.number}")
            pr.add_to_labels(done_label)
            logging.info(f"Added label '{done_label}' to PR #{pr.number}")
            return True
        
        # Check for pending label (from chain backport)
        if pending_label in labels:
            pr.remove_from_labels(pending_label)
            logging.info(f"Removed label '{pending_label}' from PR #{pr.number}")
            pr.add_to_labels(done_label)
            logging.info(f"Added label '{done_label}' to PR #{pr.number}")
            return True
        
        # If done label already exists, we're good
        if done_label in labels:
            logging.info(f"Label '{done_label}' already exists on PR #{pr.number}")
            return True
            
        logging.warning(f"No backport label found for version {version} on PR #{pr.number}")
    except Exception as e:
        logging.warning(f"Error replacing backport label: {e}")
    return False


def backport(repo, pr, version, commits, backport_base_branch, pr_body=None, jira_failed=False, original_pr=None, remaining_backport_labels=None, warn_missing_fixes=False, jira_mapping=None):
    """
    Create a backport PR.
    
    Args:
        pr: The PR to cherry-pick from (could be original or a backport PR in chain)
        original_pr: The original PR from master (for label updates and assignment).
                     If None, uses pr. This ensures backport PRs are always assigned
                     to the original author, not to scylladbbot.
        remaining_backport_labels: List of backport/X.Y labels to add for chain continuation
        jira_mapping: Dict mapping original Jira keys to their sub-task keys.
                     If provided, commit messages are amended to replace Fixes references
                     with the version-specific sub-task keys.
    """
    # Check if commits are already in the target branch
    logging.info(f"Backporting commits {commits} to {backport_base_branch} for version {version}")
    for commit in commits:
        if is_commit_in_branch(repo, commit, backport_base_branch):
            logging.info(f"Commit {commit} already in {backport_base_branch}, skipping backport for version {version}")
            # Still mark the label as done since the backport exists
            target_pr = original_pr if original_pr else pr
            replace_backport_label_with_done(repo, target_pr, version)
            return None
    
    new_branch_name = f'backport/{pr.number}/to-{version}'
    # Extract original title to prevent stacking like '[Backport 2025.3] [Backport 2025.4] Title'
    original_title = extract_original_title(pr.title)
    backport_pr_title = f'[Backport {version}] {original_title}'
    repo_url = f'https://scylladbbot:{github_token}@github.com/{repo.full_name}.git'
    fork_repo = f'https://scylladbbot:{github_token}@github.com/scylladbbot/{repo.name}.git'
    with (tempfile.TemporaryDirectory() as local_repo_path):
        try:
            repo_local = Repo.clone_from(repo_url, local_repo_path, branch=backport_base_branch)
            repo_local.git.checkout(b=new_branch_name)
            is_draft = False
            # Use -x only when cherry-picking from master (direct backport).
            # Chain backports (branch-N → branch-{N-1}) omit -x to preserve the original cherry-pick marker.
            is_from_master = (original_pr is None) or (original_pr.number == pr.number)
            for commit in commits:
                try:
                    # Only use -m1 for merge commits (multiple parents).
                    # Non-merge commits (e.g. from closed-but-not-merged PRs) don't need -m1.
                    commit_obj = repo.get_commit(commit)
                    is_merge_commit = len(commit_obj.parents) > 1
                    cherry_pick_args = [commit]
                    if is_merge_commit:
                        cherry_pick_args.append('-m1')
                    if is_from_master:
                        cherry_pick_args.append('-x')
                    repo_local.git.cherry_pick(*cherry_pick_args)
                except GitCommandError as e:
                    logging.warning(f'Cherry-pick conflict on commit {commit}: {e}')
                    is_draft = True
                    repo_local.git.add(A=True)
                    repo_local.git.cherry_pick('--continue')
                # Amend the just-cherry-picked commit to replace Fixes references
                # with the version-specific sub-task key
                if jira_mapping:
                    try:
                        old_msg = repo_local.git.log('-1', '--format=%B')
                        new_msg = replace_fixes_in_body(old_msg, jira_mapping)
                        if new_msg != old_msg:
                            repo_local.git.commit('--amend', '-m', new_msg, '--no-verify')
                            logging.info(f"Amended commit message to update Fixes reference with sub-task key")
                    except GitCommandError as e:
                        logging.warning(f"Failed to amend commit message: {e}")
            repo_local.git.push(fork_repo, new_branch_name, force=True)
            return create_pull_request(repo, new_branch_name, backport_base_branch, pr, backport_pr_title, commits,
                                is_draft=is_draft, pr_body=pr_body, jira_failed=jira_failed,
                                remaining_backport_labels=remaining_backport_labels,
                                original_pr=original_pr, warn_missing_fixes=warn_missing_fixes,
                                backport_version=version)
        except GitCommandError as e:
            logging.warning(f"GitCommandError: {e}")
            return None


def find_existing_backport_pr(repo, original_pr_number: int, version: str):
    """
    Find an existing backport PR for a specific version.
    
    Checks for:
    1. Open PRs with the backport branch pattern
    2. Recently merged PRs (to handle race conditions)
    3. Any PR in any state with the backport branch pattern
    
    Returns the PR object if found, None otherwise.
    """
    # Search for PRs with the backport branch naming pattern
    branch_pattern = f"backport/{original_pr_number}/to-{version}"
    try:
        # Check open PRs first
        pulls = repo.get_pulls(state='open', head=f'scylladbbot:{branch_pattern}')
        for pull in pulls:
            logging.info(f"Found existing open backport PR #{pull.number} for version {version}")
            return pull
        
        # Also check all PRs (including merged/closed) to prevent re-creating backports
        # This handles race conditions and cases where the PR was just merged
        pulls = repo.get_pulls(state='all', head=f'scylladbbot:{branch_pattern}', sort='created', direction='desc')
        for pull in pulls:
            logging.info(f"Found existing backport PR #{pull.number} (state: {pull.state}) for version {version}")
            return pull
    except Exception as e:
        logging.warning(f"Error searching for existing backport PR: {e}")
    return None


def _replace_labels_with_pending(pr, backport_labels: List[str]):
    """
    Replace backport/X.Y labels with backport/X.Y-pending on a PR.
    
    An external actor (e.g., github-actions[bot]) can race with us and re-add
    the original labels right after we remove them.  To defend against this we:
    1. Do a first pass: remove each label, add its -pending counterpart.
    2. Re-read labels from the API and remove any backport/X.Y labels that
       were re-added during the first pass.
    """
    labels_to_replace = set()
    for label in backport_labels:
        version = label.replace('backport/', '')
        labels_to_replace.add(label)
        pending_label = f"backport/{version}-pending"
        try:
            pr.remove_from_labels(label)
            pr.add_to_labels(pending_label)
            logging.info(f"Replaced '{label}' with '{pending_label}' on PR #{pr.number}")
        except Exception as e:
            logging.warning(f"Failed to replace label '{label}' on PR #{pr.number}: {e}")

    # Second pass: re-read labels and remove any that were re-added by a concurrent actor
    time.sleep(2)
    try:
        current_labels = {label.name for label in pr.get_labels()}
        for label in labels_to_replace:
            if label in current_labels:
                logging.warning(f"Label '{label}' was re-added to PR #{pr.number} by a concurrent actor, removing again")
                try:
                    pr.remove_from_labels(label)
                except Exception as e:
                    logging.warning(f"Failed to remove re-added label '{label}' on PR #{pr.number}: {e}")
    except Exception as e:
        logging.warning(f"Failed to verify label cleanup on PR #{pr.number}: {e}")


def backport_with_jira(repo, pr, versions: List[str], commits: List[str], main_jira_key: str, repo_name: str):
    """
    Perform backport with Jira sub-issue creation.
    
    If 'parallel_backport' label is present on the PR, creates backport PRs for ALL versions
    simultaneously (useful for security fixes that need to go to all branches at once).
    
    Otherwise, creates sub-issues for all versions, then creates PR for the highest version only.
    Remaining versions are tracked via backport labels on the created PR.
    """
    # Check if parallel backport is requested
    pr_labels = [label.name for label in pr.labels]
    parallel_backport = "parallel_backport" in pr_labels
    
    if parallel_backport:
        logging.info("parallel_backport label detected - will create backport PRs for ALL versions simultaneously")
    
    # Sort versions descending (highest first)
    sorted_versions = sort_versions_descending(versions)
    logging.info(f"Processing backports for versions (sorted descending): {sorted_versions}")
    
    # Extract ALL Jira keys from PR body
    all_jira_keys = extract_all_jira_keys_from_pr_body(pr.body)
    if not all_jira_keys:
        # Fallback to main_jira_key if provided (for backward compatibility)
        all_jira_keys = [main_jira_key] if main_jira_key else []
    
    if all_jira_keys:
        logging.info(f"Found {len(all_jira_keys)} Jira issue(s) in PR body: {all_jira_keys}")
    
    # Use the PR title for sub-issue naming (strip any existing [Backport X.Y] prefix)
    original_title = extract_original_title(pr.title)
    
    # Determine if we should warn about missing Fixes reference (scylladb/scylladb only)
    warn_missing_fixes = repo_name == "scylladb/scylladb" and not has_fixes_reference(pr.body)

    # Get Jira accountId for the original PR author to assign sub-issues
    # Trace back to the root original PR to get the actual author (not a bot)
    root_pr = get_root_original_pr(repo, pr)
    root_author = root_pr.user if root_pr else pr.user
    assignee_account_id = get_jira_user_from_github_user(root_author)
    if assignee_account_id:
        logging.info(f"Will assign Jira sub-issues to accountId: {assignee_account_id} (author: {root_author.login})")
    else:
        logging.warning(f"Could not find Jira user for GitHub user {root_author.login}, sub-issues will be unassigned")
    
    # Create Jira sub-issues for ALL parent issues and ALL versions
    # Structure: version_to_jira_mapping[version] = {original_key: sub_task_key}
    version_to_jira_mapping = {}
    jira_failures = []
    
    for version in sorted_versions:
        version_to_jira_mapping[version] = {}
        
        for parent_jira_key in all_jira_keys:
            if JIRA_USER and JIRA_API_TOKEN:
                sub_issue_key = create_jira_sub_issue(parent_jira_key, version, original_title, assignee_account_id)
                if sub_issue_key:
                    version_to_jira_mapping[version][parent_jira_key] = sub_issue_key
                    logging.info(f"Created sub-issue {sub_issue_key} for {parent_jira_key} version {version}")
                else:
                    jira_failures.append((version, parent_jira_key))
                    report_jira_failure(parent_jira_key, version)
                    # Use parent jira key as fallback
                    version_to_jira_mapping[version][parent_jira_key] = parent_jira_key
            else:
                # No Jira integration, use parent key as-is
                version_to_jira_mapping[version][parent_jira_key] = parent_jira_key
    
    # Create PR for highest version only (first in sorted list)
    if not sorted_versions:
        logging.warning("No versions to backport")
        return
    
    # Generate main PR link
    main_pr_link = f"{repo_name}#{pr.number}"
    
    # =========================================================================
    # PARALLEL BACKPORT MODE: Create PRs for ALL versions simultaneously
    # =========================================================================
    if parallel_backport:
        logging.info(f"Creating backport PRs for ALL {len(sorted_versions)} versions in parallel")
        created_prs = []
        
        for version in sorted_versions:
            jira_mapping = version_to_jira_mapping[version]
            jira_failed = any((version, key) in jira_failures for key in all_jira_keys)
            
            # Check if a backport PR already exists for this version
            existing_pr = find_existing_backport_pr(repo, pr.number, version)
            if existing_pr:
                logging.info(f"Backport PR #{existing_pr.number} already exists for version {version}, skipping")
                if warn_missing_fixes:
                    warning_comment = (f"@{pr.user.login} This backport PR can't be merged without a valid Fixes reference ")
                    existing_pr.create_issue_comment(warning_comment)
                continue
            
            # Generate PR body for this version
            pr_body = generate_backport_pr_body(
                original_pr_body=pr.body,
                main_pr_link=main_pr_link,
                jira_mapping=jira_mapping,
                commits=commits
            )
            
            target_branch = get_branch_name(repo_name, version)
            logging.info(f"Creating parallel backport PR for version {version} to branch {target_branch}")
            
            # No remaining backport labels in parallel mode - each PR is independent
            backport_pr = backport(repo, pr, version, commits, target_branch, pr_body=pr_body, 
                                   jira_failed=jira_failed, original_pr=pr, 
                                   remaining_backport_labels=None, warn_missing_fixes=warn_missing_fixes,
                                   jira_mapping=jira_mapping)
            
            if backport_pr:
                created_prs.append(backport_pr)
        
        logging.info(f"Parallel backport complete: created {len(created_prs)} PRs")
        return created_prs[0] if created_prs else None
    
    # =========================================================================
    # CHAINED BACKPORT MODE (default): Create PR for highest version only
    # =========================================================================
    highest_version = sorted_versions[0]
    jira_mapping = version_to_jira_mapping[highest_version]
    jira_failed = any((highest_version, key) in jira_failures for key in all_jira_keys)
    
    # Build remaining backport labels for versions after the highest
    remaining_backport_labels = [f"backport/{v}" for v in sorted_versions[1:]]
    
    # Generate PR body
    pr_body = generate_backport_pr_body(
        original_pr_body=pr.body,
        main_pr_link=main_pr_link,
        jira_mapping=jira_mapping,
        commits=commits
    )
    
    # Check if a backport PR already exists for the highest version
    existing_pr = find_existing_backport_pr(repo, pr.number, highest_version)
    if existing_pr:
        logging.info(f"Backport PR #{existing_pr.number} already exists for version {highest_version}, skipping")
        if warn_missing_fixes:
            warning_comment = (f"@{pr.user.login} This backport PR can't be merged without a valid Fixes reference ")
            existing_pr.create_issue_comment(warning_comment)
        return existing_pr
    
    # Create backport PR for highest version
    target_branch = get_branch_name(repo_name, highest_version)
    logging.info(f"Creating backport PR for version {highest_version} to branch {target_branch}")
    backport_pr = backport(repo, pr, highest_version, commits, target_branch, pr_body=pr_body, 
                           jira_failed=jira_failed, original_pr=pr, 
                           remaining_backport_labels=remaining_backport_labels, warn_missing_fixes=warn_missing_fixes,
                           jira_mapping=jira_mapping)
    
    # If backport PR was successfully created, replace remaining backport labels on the original PR
    # with "pending" labels. This:
    # 1. Prevents the 'labeled' event from triggering again (label name doesn't match backport/X.Y pattern)
    # 2. Keeps visibility on the original PR about what versions are still pending
    # 3. Allows tracking if the chain breaks mid-way
    # The actual backport labels are now on the backport PR for chain continuation.
    if backport_pr and remaining_backport_labels:
        _replace_labels_with_pending(pr, remaining_backport_labels)
    
    # Note: The highest version label is NOT marked as done here - it will be marked when 
    # the backport PR is merged/promoted. This happens in process_chain_backport or process_branch_push.
    
    return backport_pr


def process_chain_backport(repo, merged_pr, repo_name: str, promoted_commit_sha: str = None):
    """
    Process the next backport in the chain when a backport PR is merged.
    Uses backport labels on the merged PR to determine next versions.
    Also marks the backport label as done on the original PR.
    
    Label flow on merged backport PR:
    - backport/X.Y (next version) -> removed, transferred to new backport PR
    - backport/X.Y (remaining versions) -> backport/X.Y-pending (for tracking)
    """
    pr_body = merged_pr.body
    
    if not is_backport_pr(merged_pr.title, pr_body):
        logging.info("Merged PR is not a backport PR, skipping chain processing")
        return
    
    # Extract the version that was just merged from the PR title
    # Title format: "[Backport X.Y] Original title" or "[Backport manager-X.Y] Original title"
    merged_version_match = re.search(r'\[Backport ((manager-)?\d+\.\d+)\]', merged_pr.title)
    merged_version = merged_version_match.group(1) if merged_version_match else None
    
    # Trace back to the root original PR (not just the immediate parent) for label updates
    original_pr = get_root_original_pr(repo, merged_pr)
    # If get_root_original_pr returned the merged_pr itself, it means we couldn't find the original
    if original_pr and original_pr.number == merged_pr.number:
        original_pr = None

    # Determine if we should warn about missing Fixes reference (scylladb/scylladb only)
    warn_missing_fixes = False
    if repo_name == "scylladb/scylladb" and original_pr:
        warn_missing_fixes = not has_fixes_reference(original_pr.body)
    
    # Mark the merged version's label as done on the root original PR
    # This changes backport/X.Y-pending -> backport/X.Y-done on the original PR
    if original_pr and merged_version:
        replace_backport_label_with_done(repo, original_pr, merged_version)
    
    # Get remaining backport versions from the merged PR's labels
    backport_label_pattern = re.compile(r'^backport/((manager-)?\d+\.\d+)$')
    merged_pr_labels = [label.name for label in merged_pr.labels]
    
    remaining_versions = []
    remaining_labels = []
    for label in merged_pr_labels:
        match = backport_label_pattern.match(label)
        if match:
            version = match.group(1)
            # Skip the version that was just merged -- it may have been
            # re-added by an external workflow (e.g. Jira label sync) and
            # must not be picked as the next backport target.
            if version == merged_version:
                logging.info(f"Skipping backport/{version} label on merged PR (same as just-merged version)")
                continue
            remaining_versions.append(version)
            remaining_labels.append(label)
    
    if not remaining_versions:
        logging.info("No more backport labels on merged PR, chain complete")
        return
    
    # Sort versions descending and get the highest (next in chain)
    remaining_versions = sort_versions_descending(remaining_versions)
    next_version = remaining_versions[0]
    
    logging.info(f"Processing chain backport: next version={next_version}, remaining={remaining_versions}")
    
    # Get Jira key for this version - try to create sub-issue or use main Jira
    main_jira_key = extract_main_jira_from_body(pr_body)
    main_pr_link = extract_main_pr_link_from_body(pr_body)
    
    # Get Jira accountId for the original PR author to assign sub-issues
    # Trace back to the root original PR to get the actual author (not a bot)
    assignee_account_id = None
    root_pr = get_root_original_pr(repo, merged_pr)
    if root_pr:
        assignee_account_id = get_jira_user_from_github_user(root_pr.user)
        if assignee_account_id:
            logging.info(f"Will assign Jira sub-issue to root original PR author: {root_pr.user.login}")
        else:
            logging.warning(f"Could not find Jira user for root author {root_pr.user.login}")
    
    # Extract ALL Jira keys from the original PR body (or backport PR body if original not found)
    source_body = original_pr.body if original_pr else merged_pr.body
    all_jira_keys = extract_all_jira_keys_from_pr_body(source_body)
    if not all_jira_keys and main_jira_key:
        all_jira_keys = [main_jira_key]
    
    # Look up existing Jira sub-issues (already created by the main PR flow)
    # Do NOT create new sub-tasks from backport PRs to avoid duplicates
    jira_mapping = {}
    if all_jira_keys:
        for parent_jira_key in all_jira_keys:
            if JIRA_USER and JIRA_API_TOKEN:
                # Resolve actual parent (in case the key is a sub-task, search under grandparent)
                actual_parent_key = parent_jira_key
                original_issue = get_jira_issue(parent_jira_key)
                if original_issue:
                    grandparent_key = get_parent_key_if_subtask(original_issue)
                    if grandparent_key:
                        actual_parent_key = grandparent_key

                existing_key = find_existing_sub_issue(actual_parent_key, next_version)
                if existing_key:
                    jira_mapping[parent_jira_key] = existing_key
                    logging.info(f"Found existing sub-issue {existing_key} for {parent_jira_key} version {next_version}")
                    if assignee_account_id:
                        assign_jira_issue(existing_key, assignee_account_id)
                else:
                    # Sub-issue not found - use parent key as fallback
                    jira_mapping[parent_jira_key] = parent_jira_key
                    logging.info(f"No existing sub-issue found for {parent_jira_key} version {next_version}, using parent key")
            else:
                jira_mapping[parent_jira_key] = parent_jira_key
    
    # Get commits from the merged/closed PR
    # For superseded (closed, not merged) PRs, use the promoted commit SHA if provided.
    # This handles the case where a backport PR was closed because the promoter pushed
    # the commit directly to the branch (different SHA than the PR's commits).
    if promoted_commit_sha and not merged_pr.merged:
        commits = [promoted_commit_sha]
        logging.info(f"Using promoted commit SHA for superseded PR #{merged_pr.number}: {promoted_commit_sha}")
    elif merged_pr.merged:
        if merged_pr.merge_commit_sha:
            merge_commit = repo.get_commit(merged_pr.merge_commit_sha)
            if len(merge_commit.parents) > 1:
                # True merge commit (multi-parent): cherry-pick with -m1 handles it
                commits = [merged_pr.merge_commit_sha]
            else:
                # Rebase merge (single-parent merge_commit_sha): this SHA is just the
                # LAST commit in the PR. For multi-commit PRs we must collect ALL commits.
                pr_commits = list(merged_pr.get_commits())
                if len(pr_commits) > 1:
                    commits = [c.sha for c in pr_commits]
                    logging.info(f"Rebase-merged PR #{merged_pr.number} has {len(commits)} commits: {commits}")
                else:
                    commits = [merged_pr.merge_commit_sha]
        else:
            commits = []
    else:
        # For PRs closed by direct push (not merged through GitHub UI),
        # get the actual commit SHA from the close event
        commits = get_pr_commits(repo, merged_pr, merged_pr.base.ref)
    
    if not commits:
        logging.warning(f"No commits found for PR #{merged_pr.number}, skipping chain backport")
        return
    
    # Build remaining backport labels for the NEW backport PR (exclude the one we're processing)
    remaining_backport_labels = [f"backport/{v}" for v in remaining_versions[1:]]
    
    # Update labels on the MERGED backport PR:
    # - Remove all backport/X.Y labels
    # - Add backport/X.Y-pending for versions after the next one (for tracking)
    # - The next version label will be on the new backport PR
    logging.info(f"Updating labels on merged backport PR #{merged_pr.number}")
    labels_to_make_pending = [label for label in remaining_labels if label.replace('backport/', '') != next_version]
    labels_to_just_remove = [label for label in remaining_labels if label.replace('backport/', '') == next_version]
    
    # Remove the next version label (it moves to the new backport PR)
    for label in labels_to_just_remove:
        try:
            merged_pr.remove_from_labels(label)
            logging.info(f"Removed '{label}' from merged PR #{merged_pr.number} (transferred to new backport PR)")
        except Exception as e:
            logging.warning(f"Failed to remove label '{label}' on merged PR #{merged_pr.number}: {e}")
    
    # Replace remaining labels with -pending
    if labels_to_make_pending:
        _replace_labels_with_pending(merged_pr, labels_to_make_pending)
    
    # Get the source branch - this is the branch the merged PR was merged into
    source_branch = merged_pr.base.ref
    
    logging.info(f"Processing chain backport: version={next_version}, source={source_branch}, jira_keys={list(jira_mapping.keys())}")
    
    # Get original PR body for the backport
    original_pr_body = original_pr.body if original_pr else merged_pr.body
    
    # Generate PR body for next backport
    # Resolve the main PR link: prefer extracted link from body, then root original PR,
    # then fall back to the merged PR number.
    if not main_pr_link:
        if original_pr:
            main_pr_link = f"#{original_pr.number}"
        else:
            main_pr_link = f"#{merged_pr.number}"
    new_pr_body = generate_backport_pr_body(
        original_pr_body=original_pr_body,
        main_pr_link=main_pr_link,
        jira_mapping=jira_mapping,
        commits=commits
    )
    
    # Create backport PR - cherry-pick from the source branch
    target_branch = get_branch_name(repo_name, next_version)
    # Create backport PR, passing original_pr for label updates if commit already exists
    backport_pr = backport(repo, merged_pr, next_version, commits, target_branch, pr_body=new_pr_body, 
                           jira_failed=False, original_pr=original_pr, 
                           remaining_backport_labels=remaining_backport_labels, warn_missing_fixes=warn_missing_fixes,
                           jira_mapping=jira_mapping)
    
    return backport_pr


def create_pr_comment_and_remove_label(pr):
    comment_body = f':warning:  @{pr.user.login} PR body does not contain a valid reference to an issue '
    comment_body += ' based on [linking-a-pull-request-to-an-issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword)'
    comment_body += ' and can not be backported\n\n'
    comment_body += 'The following labels were removed:\n'
    labels = pr.get_labels()
    pattern = re.compile(r"backport/(manager-)?\d+\.\d+$")
    for label in labels:
        if pattern.match(label.name):
            print(f"Removing label: {label.name}")
            comment_body += f'- {label.name}\n'
            pr.remove_from_labels(label)
    comment_body += f'\nPlease add the relevant backport labels after PR body is fixed'
    pr.create_issue_comment(comment_body)


def get_promoted_label(base_branch: str) -> str:
    """
    Get the promoted label based on the base branch.
    For master/main/next: 'promoted-to-master'
    For branch-X.Y: 'promoted-to-branch-X.Y'
    For next-X.Y (gating): 'promoted-to-branch-X.Y' (maps to the stable branch label,
        since promotion from next-X.Y to branch-X.Y is what triggers backports)
    """
    if base_branch in ('master', 'main', 'next'):
        return 'promoted-to-master'
    # For gating branches (next-X.Y), map to the stable branch label (promoted-to-branch-X.Y)
    # since the promoted-to label is added when commits land on the stable branch
    if base_branch.startswith('next-'):
        stable_branch = base_branch.replace('next-', 'branch-', 1)
        return f'promoted-to-{stable_branch}'
    return f'promoted-to-{base_branch}'


def process_branch_push(repo, commits_range: str, branch_name: str, repo_name: str):
    """
    Process a push event to a stable branch (e.g., master, branch-2025.4, or manager-3.4).
    This handles:
    1. Finding backport PRs that were merged in this push
    2. Adding promoted-to-<branch> label to them
    3. Marking backport/X.Y or backport/manager-X.Y as done on the original PR
    4. Continuing the chain backport if there are more versions
    5. For original (non-backport) PRs on version branches: adding promoted label
       and initiating backport chain if backport labels are present
    
    For repos with version branch gating (scylla-pkg):
    - Push to next-X.Y: Ignored (PR merge event, not a promotion)
    - Push to branch-X.Y: Adds promoted-to-branch-X.Y label, marks done, continues chain
      For original PRs targeting next-X.Y directly: initiates backport chain
    
    For repos with master gating (scylladb/scylladb):
    - Push to next: Ignored (PR merge to gating branch, not a promotion)
    - Push to master: Adds promoted-to-master label, triggers backports
    - Push to branch-X.Y: Adds promoted-to-branch-X.Y label, marks done, continues chain
    
    Manager branches have no gating, so chain continues immediately.
    
    Args:
        repo: GitHub repo object
        commits_range: Commit range like 'abc123..def456'
        branch_name: Short branch name like 'master', 'branch-2025.4' or 'manager-3.4'
        repo_name: Full repo name like 'scylladb/scylla-pkg' or 'scylladb/scylladb'
    """
    # Skip gating branches:
    # - next-X.Y: version branch gating (scylla-pkg)
    # - next: master branch gating (scylladb/scylladb)
    # Only process stable branches: master, branch-X.Y, or manager-X.Y
    if branch_name.startswith('next-') or branch_name == 'next':
        logging.info(f"Skipping push to gating branch {branch_name} - waiting for promotion to stable branch")
        return
    
    promoted_label = f"promoted-to-{branch_name}"
    
    # Extract version from branch name
    # For manager branches (manager-X.Y), the version IS 'manager-X.Y'
    # For regular branches (branch-X.Y), extract just 'X.Y'
    if branch_name.startswith('manager-'):
        version = branch_name  # manager-3.4 -> manager-3.4
    else:
        version_match = re.search(r'(\d+\.\d+)$', branch_name)
        version = version_match.group(1) if version_match else None
    
    logging.info(f"Processing push to stable branch {branch_name}, version={version}, will add label '{promoted_label}' and continue chain")
    
    # Parse commit range
    start_commit, end_commit = commits_range.split('..')
    
    # Get commits in this push
    comparison = repo.compare(start_commit, end_commit)
    
    # Find PRs associated with these commits
    processed_prs = set()
    for commit in comparison.commits:
        # Get PRs from GitHub's commit-PR association (works for merged PRs)
        prs_for_commit = list(commit.get_pulls())
        seen_in_commit = {pr.number for pr in prs_for_commit}

        # Also find PRs referenced via "Closes #X" in commit messages.
        # This handles single-commit PRs that are closed (not merged) with a commit event
        # (e.g., in scylladb/scylladb), which commit.get_pulls() does not return.
        for match in re.finditer(r'Closes\s+(?:[\w-]+/[\w-]+)?#(\d+)', commit.commit.message):
            pr_number = int(match.group(1))
            if pr_number not in seen_in_commit and pr_number not in processed_prs:
                try:
                    closed_pr = repo.get_pull(pr_number)
                    if closed_pr.state == 'closed':
                        prs_for_commit.append(closed_pr)
                        logging.info(f"Found closed-with-commit PR #{pr_number} via 'Closes' reference")
                except Exception as e:
                    logging.warning(f"Could not fetch PR #{pr_number} from 'Closes' reference: {e}")

        for pr in prs_for_commit:
            if pr.number in processed_prs:
                continue
            processed_prs.add(pr.number)
            
            # For non-backport (original) PRs, add the promoted label
            if not is_backport_pr(pr.title, pr.body):
                try:
                    pr.add_to_labels(promoted_label)
                    logging.info(f"Added '{promoted_label}' label to original PR #{pr.number}")
                except Exception as e:
                    logging.warning(f"Failed to add label to original PR #{pr.number}: {e}")
                
                # For PRs that originated directly on a version branch (not master),
                # check if they have backport labels and initiate the backport chain.
                # This handles the case where a PR targets next-X.Y (or branch-X.Y)
                # directly instead of going through master first.
                if branch_name not in ('master', 'main'):
                    backport_label_pattern = re.compile(r'backport/(manager-)?\d+\.\d+$')
                    pr_labels = [label.name for label in pr.labels]
                    backport_labels = [label for label in pr_labels if backport_label_pattern.match(label)]
                    if backport_labels:
                        logging.info(f"Original PR #{pr.number} on version branch {branch_name} has backport labels: {backport_labels}")
                        versions = [label.replace('backport/', '') for label in backport_labels]
                        stable_branch = branch_name
                        commits = get_pr_commits(repo, pr, stable_branch)
                        if commits:
                            main_jira_key = extract_jira_key_from_pr_body(pr.body)
                            backport_with_jira(repo, pr, versions, commits, main_jira_key, repo_name)
                        else:
                            logging.warning(f"No commits found for PR #{pr.number} on branch {stable_branch}")
                continue
            
            logging.info(f"Found merged backport PR #{pr.number}: {pr.title}")
            
            # Add promoted-to-<branch> label to the backport PR
            try:
                pr.add_to_labels(promoted_label)
                logging.info(f"Added '{promoted_label}' label to PR #{pr.number}")
            except Exception as e:
                logging.warning(f"Failed to add label to PR #{pr.number}: {e}")
            
            # Mark backport/X.Y as done on the root original PR
            if version:
                original_pr = get_root_original_pr(repo, pr)
                # If get_root_original_pr returned the PR itself, it means we couldn't find the original
                if original_pr and original_pr.number != pr.number:
                    replace_backport_label_with_done(repo, original_pr, version)
                    logging.info(f"Marked backport/{version} as done on original PR #{original_pr.number}")
                else:
                    logging.warning(f"Could not find original PR for backport PR #{pr.number}")
            
            # Process chain backport (continue to next version if any)
            process_chain_backport(repo, pr, repo_name)

    # Find and close open backport PRs superseded by this push.
    # This handles the parallel backport case where the promoter cherry-picks the commit
    # directly to the branch (new SHA), so commit.get_pulls() can't find the backport PR,
    # and GitHub's "Closes #X" keyword only auto-closes on the default branch.
    _close_superseded_backport_prs(repo, comparison.commits, branch_name, version, promoted_label, processed_prs, repo_name)


def _close_superseded_backport_prs(repo, pushed_commits, branch_name: str, version: str,
                                   promoted_label: str, already_processed: set, repo_name: str):
    """
    Find and close open backport PRs whose commits have been promoted to the branch.
    Uses commit title matching and 'Closes' references from promoted commit messages.
    """
    # Build mappings of promoted commit titles and 'Closes' PR numbers to commit SHAs
    promoted_titles = {}  # title -> commit sha
    closes_pr_numbers = {}  # pr_number -> commit sha
    for commit in pushed_commits:
        message = commit.commit.message
        title = message.splitlines()[0].strip()
        promoted_titles[title] = commit.sha
        # Extract PR numbers from 'Closes' references (added by promoter)
        for match in re.finditer(r'Closes\s+(?:[\w-]+/[\w-]+)?#(\d+)', message):
            closes_pr_numbers[int(match.group(1))] = commit.sha

    # First, try directly via 'Closes' references (most efficient)
    for pr_number, commit_sha in closes_pr_numbers.items():
        if pr_number in already_processed:
            continue
        try:
            pr = repo.get_pull(pr_number)
            if pr.state != 'open':
                continue
            if not is_backport_pr(pr.title, pr.body):
                continue
            if pr.base.ref != branch_name:
                continue
            _close_promoted_backport_pr(repo, pr, branch_name, version, promoted_label, repo_name, commit_sha)
            already_processed.add(pr_number)
        except Exception as e:
            logging.warning(f"Error checking PR #{pr_number} from Closes reference: {e}")

    # Second, search for open backport PRs targeting this branch and match by commit title
    try:
        open_pulls = repo.get_pulls(state='open', base=branch_name)
        for pr in open_pulls:
            if pr.number in already_processed:
                continue
            if not is_backport_pr(pr.title, pr.body):
                continue
            # Compare the PR's latest commit title with promoted commit titles
            try:
                pr_commits = list(pr.get_commits())
                if not pr_commits:
                    continue
                pr_commit_title = pr_commits[-1].commit.message.splitlines()[0].strip()
                if pr_commit_title in promoted_titles:
                    commit_sha = promoted_titles[pr_commit_title]
                    _close_promoted_backport_pr(repo, pr, branch_name, version, promoted_label, repo_name, commit_sha)
                    already_processed.add(pr.number)
            except Exception as e:
                logging.warning(f"Error checking commits for PR #{pr.number}: {e}")
    except Exception as e:
        logging.warning(f"Error searching for open backport PRs on {branch_name}: {e}")


def _close_promoted_backport_pr(repo, pr, branch_name: str, version: str,
                                promoted_label: str, repo_name: str, commit_sha: str):
    """Close a backport PR that was superseded by a direct promotion to the branch."""
    logging.info(f"Found superseded backport PR #{pr.number}: {pr.title}")

    try:
        pr.add_to_labels(promoted_label)
        logging.info(f"Added '{promoted_label}' label to PR #{pr.number}")
    except Exception as e:
        logging.warning(f"Failed to add label to PR #{pr.number}: {e}")

    try:
        pr.create_issue_comment(
            f"Closed via {commit_sha}"
        )
        pr.edit(state='closed')
        logging.info(f"Closed superseded backport PR #{pr.number}")
    except Exception as e:
        logging.warning(f"Failed to close PR #{pr.number}: {e}")

    # Mark backport/X.Y as done on the root original PR
    if version:
        try:
            original_pr = get_root_original_pr(repo, pr)
            if original_pr and original_pr.number != pr.number:
                replace_backport_label_with_done(repo, original_pr, version)
                logging.info(f"Marked backport/{version} as done on original PR #{original_pr.number}")
            else:
                logging.warning(f"Could not find original PR for backport PR #{pr.number}")
        except Exception as e:
            logging.warning(f"Error marking backport as done for PR #{pr.number}: {e}")

    # Continue chain backport to remaining versions
    process_chain_backport(repo, pr, repo_name, promoted_commit_sha=commit_sha)


def main():
    args = parse_args()
    base_branch = args.base_branch.split('/')[2]
    promoted_label = get_promoted_label(base_branch)
    repo_name = args.repo

    # Determine branch prefix based on repository
    backport_branch = get_branch_prefix(repo_name)
    stable_branch = 'master' if base_branch in ('next', 'main') else base_branch.replace('next-', 'branch-', 1)
    backport_label_pattern = re.compile(r'backport/(manager-)?\d+\.\d+$')

    g = Github(github_token)
    repo = g.get_repo(repo_name)
    
    # Handle push to version branch (chain continuation)
    if args.promoted_to_branch and args.commits:
        process_branch_push(repo, args.commits, args.promoted_to_branch, repo_name)
        return
    
    # Handle chain backport mode (legacy - for PR merge events)
    if args.chain_backport and args.merged_pr:
        merged_pr = repo.get_pull(args.merged_pr)
        if merged_pr.merged:
            process_chain_backport(repo, merged_pr, repo_name)
        else:
            logging.warning(f"PR #{args.merged_pr} is not merged, skipping chain processing")
        return
    
    closed_prs = []
    start_commit = None

    if args.commits:
        start_commit, end_commit = args.commits.split('..')
        commits = repo.compare(start_commit, end_commit).commits
        seen_pr_numbers = set()
        for commit in commits:
            for pr in commit.get_pulls():
                if pr.number not in seen_pr_numbers:
                    closed_prs.append(pr)
                    seen_pr_numbers.add(pr.number)

            # Also find PRs referenced via "Closes #X" in commit messages.
            # This handles PRs that are closed by direct push (not merged via
            # GitHub UI), e.g. in scylladb/scylladb where PRs target 'next'
            # and commits are promoted to master with a different flow.
            # commit.get_pulls() does not return such non-merged PRs.
            for match in re.finditer(r'Closes\s+(?:[\w-]+/[\w-]+)?#(\d+)', commit.commit.message):
                pr_number = int(match.group(1))
                if pr_number not in seen_pr_numbers:
                    try:
                        closed_pr = repo.get_pull(pr_number)
                        if closed_pr.state == 'closed':
                            closed_prs.append(closed_pr)
                            seen_pr_numbers.add(pr_number)
                            logging.info(f"Found closed-with-commit PR #{pr_number} via 'Closes' reference")
                    except Exception as e:
                        logging.warning(f"Could not fetch PR #{pr_number} from 'Closes' reference: {e}")
    if args.pull_request:
        start_commit = args.head_commit
        pr = repo.get_pull(args.pull_request)
        closed_prs = [pr]

    for pr in closed_prs:
        if args.commits and repo_name in MILESTONE_REPOS and base_branch in ('next', 'master'):
            master_milestone_title = resolve_master_milestone_title()
            set_pr_milestone(pr, master_milestone_title)

        labels = [label.name for label in pr.labels]
        # Always get all backport labels from the PR, not just the one that triggered the event
        backport_labels = [label for label in labels if backport_label_pattern.match(label)]
        
        if promoted_label not in labels:
            print(f'no {promoted_label} label: {pr.number}')
            continue
        if not backport_labels:
            print(f'no backport label: {pr.number}')
            continue
        
        # If triggered by a specific label, verify it's in the backport labels
        if args.label and args.label not in backport_labels:
            print(f'label {args.label} not found in PR labels')
            continue
        
        # Extract versions from backport labels
        versions = [label.replace('backport/', '') for label in backport_labels]
        
        # Check if this is triggered by a specific label and a backport already exists for the highest version
        # This prevents race conditions where the 'labeled' event triggers after chain backport has already started
        sorted_versions = sort_versions_descending(versions)
        highest_version = sorted_versions[0] if sorted_versions else None
        
        if highest_version:
            existing_pr = find_existing_backport_pr(repo, pr.number, highest_version)
            if existing_pr:
                logging.info(f"Backport PR #{existing_pr.number} already exists for highest version {highest_version}")
                logging.info(f"Skipping backport creation for PR #{pr.number} - chain is already in progress")
                continue
            
        commits = get_pr_commits(repo, pr, stable_branch, start_commit)
        logging.info(f"Found PR #{pr.number} with commit {commits} and the following labels: {backport_labels}")
        
        if not commits:
            logging.warning(f"No commits found for PR #{pr.number}, skipping backport. "
                            f"This may happen if the merge commit SHA could not be resolved "
                            f"against the stable branch '{stable_branch}'.")
            continue
        
        # Extract Jira key from PR body
        main_jira_key = extract_jira_key_from_pr_body(pr.body)
        if main_jira_key:
            logging.info(f"Found Jira issue in PR body: {main_jira_key}")
        else:
            logging.warning(f"No Jira issue found in PR #{pr.number} body")
        
        # Use the new Jira-aware backport function
        backport_with_jira(repo, pr, versions, commits, main_jira_key, repo_name)


if __name__ == "__main__":
    main()
