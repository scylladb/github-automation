# GitHub Automation - Backport with Jira Integration

This document describes the automated backport system with Jira sub-issue integration for ScyllaDB repositories.

## Overview

The backport automation system handles:
1. Creating backport PRs when the `backport/X.Y` label is added to a promoted PR
2. Creating Jira sub-issues for each backport version
3. Chaining backports from higher to lower versions to avoid repeated conflict resolution
4. Managing labels throughout the backport lifecycle
5. Setting milestones on PRs (for scylladb/scylladb and scylladb/scylla-pkg)

## Features

### 1. Jira Sub-Issue Creation

When a backport PR is created, the system automatically:
- Extracts all Jira issue keys from the `Fixes:` line in the PR body
- Creates a sub-issue under each Jira parent for the backport version
- Updates the backport PR body to reference the sub-issue instead of the parent
- Assigns the sub-issue to the original PR author (by matching GitHub email to Jira)

**Supported Fixes formats:**
```
Fixes: SCYLLADB-12345
Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-12345
Fixes: SCYLLADB-12345
Fixes: SCYLLADB-67890
```

**Sub-issue naming:**
```
[Backport 2025.4] - Original Issue Title
```

**Jira hierarchy handling:**
If the referenced Jira issue is already a sub-task (Jira only allows 2 levels), the new sub-task is created under the parent's parent with a description referencing the original sub-task.

### 2. Chained Backports

Instead of creating all backport PRs at once (which can cause repeated conflict resolution), the system chains backports:

1. PR promoted to master with labels: `backport/2025.4`, `backport/2025.3`, `backport/2025.2`
2. System creates backport PR for **highest version only** (2025.4)
3. Remaining backport labels (`backport/2025.3`, `backport/2025.2`) are added to the backport PR
4. Original PR labels are changed to `backport/2025.3-pending`, `backport/2025.2-pending`
5. When backport PR merges and is promoted to `branch-2025.4`:
   - `backport/2025.4-done` is added to original PR
   - Next backport PR (2025.3) is created from `branch-2025.4` (not master!)
   - This continues until all versions are done

**Benefits:**
- Conflict resolution in 2025.4 is inherited by 2025.3
- Less manual work when the same conflict exists across versions
- Clear tracking of pending backports on original PR

### 3. Parallel Backports

For security fixes or urgent patches that need immediate backporting to all versions, add the `parallel_backport` label to the original PR.

When this label is present:
- Backport PRs are created for **ALL versions simultaneously**
- Each backport PR cherry-picks from master (not from previous branch)
- No chaining occurs

### 4. Label Lifecycle

| Label | Meaning |
|-------|---------|
| `backport/X.Y` | Backport requested for version X.Y |
| `backport/X.Y-pending` | Backport PR exists, waiting to be processed in chain |
| `backport/X.Y-done` | Backport completed for version X.Y |
| `promoted-to-master` | PR has been promoted to master branch |
| `promoted-to-branch-X.Y` | Backport PR has been promoted to branch-X.Y |
| `conflicts` | Backport PR has cherry-pick conflicts (auto-set) |
| `jira-sub-issue-creation-failed` | Jira API call failed (PR still created) |
| `parallel_backport` | Create all backport PRs at once (no chaining) |
| `P0`, `P1` | Priority labels inherited by backport PRs |
| `force_on_cloud` | Auto-added to backport PRs with P0/P1 (except scylla-pkg) |

### 5. Milestone Management

For `scylladb/scylladb` and `scylladb/scylla-pkg` repositories:

**Master PRs:**
- Milestone is set from `SCYLLA-VERSION-GEN` file (e.g., `2026.2.0`)

**Backport PRs:**
- Milestone is calculated from git tags
- For `branch-2025.4` with latest tag `scylla-2025.4.2`, milestone = `2025.4.3`
- For new branches with only RC tags, milestone = `X.Y.0`

### 6. Branch Naming Convention

| Repository | Backport Target Branch |
|------------|----------------------|
| `scylladb/scylladb` | `branch-X.Y` |
| Other repos | `next-X.Y` |
| Manager versions | `manager-X.Y` |

### 7. Missing Fixes Reference Warning

For `scylladb/scylladb` repository only:
- If a PR lacks a valid `Fixes:` reference (Jira or GitHub issue)
- A warning comment is added to the backport PR
- The warning mentions the PR cannot be merged without a valid reference

## Workflow Triggers

### Push to Master
```yaml
on:
  push:
    branches: [master]
```
- Searches for promoted PRs with backport labels
- Creates backport PR for highest version
- Triggers Jira sub-issue creation

### Label Added to PR
```yaml
on:
  pull_request_target:
    types: [labeled]
```
- Triggered when `backport/X.Y` label is added
- Only processes if PR is closed/merged and has `promoted-to-master`
- 30-second debounce to allow adding multiple labels

### Push to Version Branch
```yaml
on:
  push:
    branches: ['branch-*', 'manager-*']
```
- Processes merged backport PRs in the push
- Adds `promoted-to-branch-X.Y` label
- Marks `backport/X.Y` as done on original PR
- Continues chain to next version

### Backport PR Merged (Chain Event)
```yaml
on:
  pull_request_target:
    types: [closed]
    branches: ['branch-*', 'next-*', 'manager-*']
```
- Triggered when backport PR is merged
- Continues chain to next version if labels exist

## Example Workflows

### Example 1: Standard Chained Backport

**Initial state:** PR #100 merged to master with labels:
- `promoted-to-master`
- `backport/2025.4`
- `backport/2025.3`
- `backport/2025.2`
- `Fixes: SCYLLADB-12345`

**Step 1:** System creates:
- Jira sub-issues: `SCYLLADB-12346` (2025.4), `SCYLLADB-12347` (2025.3), `SCYLLADB-12348` (2025.2)
- Backport PR #101 targeting `branch-2025.4` with labels:
  - `backport/2025.3`
  - `backport/2025.2`
- PR body contains: `Fixes: SCYLLADB-12346`
- PR #100 labels become:
  - `promoted-to-master`
  - `backport/2025.4`
  - `backport/2025.3-pending`
  - `backport/2025.2-pending`

**Step 2:** PR #101 merged and promoted to `branch-2025.4`:
- PR #100 labels: `backport/2025.4-done`, `backport/2025.3-pending`, `backport/2025.2-pending`
- System creates backport PR #102 targeting `branch-2025.3` (cherry-picks from `branch-2025.4`)
- PR #102 has labels: `backport/2025.2`
- PR body contains: `Fixes: SCYLLADB-12347`

**Step 3:** PR #102 merged and promoted to `branch-2025.3`:
- PR #100 labels: `backport/2025.4-done`, `backport/2025.3-done`, `backport/2025.2-pending`
- System creates backport PR #103 targeting `branch-2025.2` (cherry-picks from `branch-2025.3`)
- PR body contains: `Fixes: SCYLLADB-12348`

**Step 4:** PR #103 merged and promoted to `branch-2025.2`:
- PR #100 labels: `backport/2025.4-done`, `backport/2025.3-done`, `backport/2025.2-done`
- Chain complete

### Example 2: Parallel Backport (Security Fix)

**Initial state:** PR #200 with labels:
- `promoted-to-master`
- `backport/2025.4`
- `backport/2025.3`
- `backport/2025.2`
- `parallel_backport`
- `Fixes: SCYLLADB-99999`

**Immediately creates:**
- Backport PR #201 → `branch-2025.4` (cherry-picks from master)
- Backport PR #202 → `branch-2025.3` (cherry-picks from master)
- Backport PR #203 → `branch-2025.2` (cherry-picks from master)

No chaining - all PRs can be reviewed and merged independently.

### Example 3: Multiple Jira Issues

**PR body:**
```
This change fixes two issues.

Fixes: SCYLLADB-11111
Fixes: SCYLLADB-22222
```

**Result for backport to 2025.4:**
- Sub-issue `SCYLLADB-33333` created under `SCYLLADB-11111`
- Sub-issue `SCYLLADB-44444` created under `SCYLLADB-22222`
- Backport PR body contains:
  ```
  Fixes: SCYLLADB-33333
  Fixes: SCYLLADB-44444
  ```

### Example 4: Cherry-Pick Conflict

**Backport PR created with conflicts:**
- PR is created in **draft** state
- `conflicts` label is added
- Comment posted: `@author - This PR has conflicts, therefore it was moved to draft. Please resolve them and mark this PR as ready for review`

### Example 5: Jira API Failure

**If Jira sub-issue creation fails:**
- Comment added to main Jira issue: `Failed to create backport sub-issue for version 2025.4. [View workflow run]`
- `jira-sub-issue-creation-failed` label added to backport PR
- Backport PR is still created (uses parent Jira key in Fixes line)

## Configuration

### Required Secrets

| Secret | Description |
|--------|-------------|
| `GITHUB_TOKEN` | GitHub token with repo access |
| `JIRA_AUTH` | Jira authentication in `user:token` format |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GITHUB_TOKEN` | GitHub token (required) |
| `JIRA_AUTH` | Jira auth string (optional - Jira features disabled if not set) |

## Files

| File | Description |
|------|-------------|
| `.github/scripts/auto-backport-jira.py` | Main backport automation script |
| `.github/scripts/search_commits.py` | Label management for promoted commits |
| `.github/workflows/backport-with-jira.yaml` | Reusable workflow for other repos |

## Usage in Other Repositories

To use this automation in another ScyllaDB repository, create a workflow that calls the reusable workflow:

```yaml
name: Backport Automation

on:
  push:
    branches:
      - master
      - 'branch-*'
      - 'next-*'
      - 'manager-*'
  pull_request_target:
    types: [labeled, closed]

jobs:
  backport:
    uses: scylladb/github-automation/.github/workflows/backport-with-jira.yaml@main
    with:
      event_type: ${{ github.event_name == 'push' && 'push' || (github.event.action == 'labeled' && 'labeled' || 'chain') }}
      base_branch: ${{ github.event_name == 'push' && github.ref || format('refs/heads/{0}', github.event.pull_request.base.ref) }}
      commits: ${{ github.event_name == 'push' && format('{0}..{1}', github.event.before, github.event.after) || '' }}
      pull_request_number: ${{ github.event.pull_request.number || 0 }}
      head_commit: ${{ github.event.pull_request.head.sha || '' }}
      label_name: ${{ github.event.label.name || '' }}
      pr_state: ${{ github.event.pull_request.state || '' }}
      pr_body: ${{ github.event.pull_request.body || '' }}
    secrets:
      gh_token: ${{ secrets.AUTO_BACKPORT_TOKEN }}
      jira_auth: ${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}
```

## Troubleshooting

### Backport PR not created
1. Check if `promoted-to-master` label exists
2. Verify backport label format is `backport/X.Y` (not `backport/X.Y-done` or `-pending`)
3. Check GitHub Actions logs for errors
4. Verify the target branch exists (e.g., `branch-2025.4`)

### Jira sub-issue not created
1. Verify `JIRA_AUTH` secret is set correctly
2. Check if the Jira issue key format is valid
3. Review workflow logs for Jira API errors
4. The `jira-sub-issue-creation-failed` label indicates API failure

### Chain stopped mid-way
1. Check if the backport PR was actually merged (not just closed)
2. Verify the backport PR was promoted to the version branch
3. Check for `backport/X.Y-pending` labels on original PR to see which versions are still pending
4. Manually add `backport/X.Y` label to backport PR to restart chain

### Milestone not set
1. Only applies to `scylladb/scylladb` and `scylladb/scylla-pkg`
2. Check if `SCYLLA-VERSION-GEN` file is accessible
3. For backports, verify tags exist for the version (e.g., `scylla-2025.4.0`)
