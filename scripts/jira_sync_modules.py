"""
Jira synchronization modules for GitHub Actions workflows.

This module provides functions to synchronize GitHub PRs with Jira issues.
Currently implements:
- Jira status transitions
"""

import argparse
import csv
import io
import os
import sys
import time
from typing import Dict, List, Tuple

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    print("Error: 'requests' library is required. Install it with: pip install requests")
    sys.exit(1)


def transition_issues_from_csv(
    csv_content: str,
    transition_id: str,
    transition_name: str,
    jira_auth: str
) -> int:
    """
    Transition Jira issues based on CSV input.
    
    Args:
        csv_content: CSV string with columns: key, status, done
        transition_id: ID of the Jira transition to apply
        transition_name: Name of the transition (for logging)
        jira_auth: Authentication string (Bearer token or username:password)
    
    Returns:
        Exit code (0 for success, 1 for errors)
    """
    # Parse CSV
    reader = csv.DictReader(io.StringIO(csv_content))
    issues = list(reader)
    
    if not issues:
        print("No issues found in CSV")
        return 0
    
    # Categorize issues
    to_transition = []
    already_ok = []
    done_issues = []
    
    for issue in issues:
        key = issue.get('key', '')
        status = issue.get('status', '')
        done = issue.get('done', '').lower() == 'true'
        
        if not key:
            continue
            
        if done:
            done_issues.append(f"{key} (status={status})")
        elif status == transition_name:
            already_ok.append(f"{key} (already at '{transition_name}')")
        else:
            to_transition.append((key, status))
    
    # Print summary
    print(f"\n=== Jira Transition Summary ===")
    print(f"Target transition: {transition_name} (ID: {transition_id})")
    print(f"Total issues: {len(issues)}")
    print(f"To transition: {len(to_transition)}")
    print(f"Already at target status: {len(already_ok)}")
    print(f"Done issues (skipped): {len(done_issues)}")
    
    if already_ok:
        print(f"\nAlready OK:")
        for item in already_ok:
            print(f"  - {item}")
    
    if done_issues:
        print(f"\nDone issues (skipped):")
        for item in done_issues:
            print(f"  - {item}")
    
    # Perform transitions
    if not to_transition:
        print("\nNo transitions needed.")
        return 0
    
    print(f"\nTransitioning {len(to_transition)} issue(s):")
    
    # Setup authentication
    headers = {}
    auth = None
    
    if jira_auth.startswith('Bearer '):
        headers['Authorization'] = jira_auth
    else:
        # Basic auth: username:password
        if ':' in jira_auth:
            username, password = jira_auth.split(':', 1)
            auth = HTTPBasicAuth(username, password)
        else:
            print("Error: Invalid jira_auth format")
            return 1
    
    headers['Content-Type'] = 'application/json'
    
    # Transition each issue
    success_count = 0
    error_count = 0
    
    for key, old_status in to_transition:
        url = f"https://scylladb.atlassian.net/rest/api/3/issue/{key}/transitions"
        payload = {
            "transition": {
                "id": transition_id
            }
        }
        
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                auth=auth,
                timeout=30
            )
            
            if response.status_code == 204:
                print(f"   {key}: {old_status}  {transition_name}")
                success_count += 1
            else:
                print(f"   {key}: Failed (HTTP {response.status_code})")
                print(f"    Response: {response.text}")
                error_count += 1
        
        except Exception as e:
            print(f"   {key}: Error - {e}")
            error_count += 1
        
        # Rate limiting
        time.sleep(0.2)
    
    print(f"\n=== Results ===")
    print(f"Success: {success_count}")
    print(f"Errors: {error_count}")
    
    return 1 if error_count > 0 else 0


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Transition Jira issues based on CSV input'
    )
    parser.add_argument(
        '--csv-content',
        required=True,
        help='CSV content with columns: key, status, done'
    )
    parser.add_argument(
        '--transition-id',
        required=True,
        help='Jira transition ID'
    )
    parser.add_argument(
        '--transition-name',
        required=True,
        help='Jira transition name (for logging)'
    )
    parser.add_argument(
        '--jira-auth',
        help='Jira authentication (Bearer token or username:password). Can also use JIRA_AUTH env var.'
    )
    
    args = parser.parse_args()
    
    # Get auth from args or environment
    jira_auth = args.jira_auth or os.environ.get('JIRA_AUTH')
    if not jira_auth:
        print("Error: --jira-auth or JIRA_AUTH environment variable is required")
        sys.exit(1)
    
    exit_code = transition_issues_from_csv(
        args.csv_content,
        args.transition_id,
        args.transition_name,
        jira_auth
    )
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
