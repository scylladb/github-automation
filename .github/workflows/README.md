# GitHub Automation – Reusable Workflows for Jira + PR Synchronization

This repository contains reusable workflows that provide automation between GitHub Pull Requests and Jira issues.  
They allow repositories to consistently synchronize labels, metadata, and status transitions between the two systems.

All workflows are located under:

```
.github/workflows/
```

This document describes what each workflow does, how they relate to one another, and how the orchestration layer in product repositories calls them.

---

## 1. Jira Key Extraction Workflows

### `extract_jira_keys.yml`

**Purpose:**  
Extract Jira issue keys from a Pull Request title or body. The extractor supports patterns such as:

- `ABC-123` - only in the PR title 
- `Fixes: ABC-123`  
- `Closes: [ABC-123](...)`
- `Resolve: ABC-123`

**Output:**  
A JSON array containing the detected Jira keys, for example:

```
["ABC-123", "ABC-777"]
```

All subsequent workflows depend on this output.

---

## 2. Jira Issue Detail Retrieval

### `extract_jira_issue_details.yml`

**Purpose:**  
Fetch metadata for all Jira issues associated with a PR. The workflow retrieves:

- status  
- labels  
- assignee  
- priority  
- fixVersions  
- scylla_components  

**Outputs:**

- **csv** – Full Jira details as CSV  
- **labels_csv** – Flattened list of Jira labels, used to generate normalized PR labels  

These outputs are used by PR label normalization and Jira transitions.

---

## 3. Pull Request Label Normalization

### `apply_labels_to_pr.yml`

**Purpose:**  
Normalize and apply labels to the PR based on:

- Labels pulled from Jira  
- Jira priority (converted to GitHub labels P0–P4)  
- Jira scylla_components (converted into `area/*` labels)

**Key behaviors:**

1. Remove outdated P* priority labels on the PR  
2. Compute correct priority from Jira metadata  
3. Add normalized `area/*` labels  
4. Apply deduplicated, canonical labels to the PR  

This ensures PR labels accurately reflect Jira’s metadata.

---

## 4. Writing PR Metadata into Jira

### `fill_jira_with_github_data.yml`

**Purpose:**  
Write PR metadata back into Jira custom fields, for example:

```
This issue will be closed by <PR URL>
```
This is used by Jira automation to distinguish which Jira issue should be closed when its linked PR is merged.

---

## 5. Jira Status Transition Workflow

### `jira_transition.yml`

**Purpose:**  
Transition Jira issues into a new workflow state.

**Inputs:**

- `transition_name`  
- `transition_id`  
- `details_csv`  

**Behavior:**

1. Identify which Jira issues require transition  
2. Skip issues already in the target state  
3. Skip issues that are Done, Duplicate, or Won’t Fix  
4. Transition all remaining issues  

This workflow keeps Jira issue status synchronized with PR lifecycle.

---

# 6. High-Level Orchestration Workflows

These workflows live in product repositories and call the reusable workflows in this repo.

---

## `main_update_jira_status_to_in_progress.yml`

**Trigger:**  
PR is **opened**.

**Actions:**

1. Extract Jira keys  
2. Retrieve Jira issue details  
3. Apply normalized labels to the PR  
4. Transition Jira issues to **In Progress**

---

## `main_update_jira_status_to_in_review.yml`

**Trigger:**  
PR receives label:

```
status/in_review
```

**Actions:**

1. Extract Jira keys  
2. Add this label to Jira  
3. Fetch updated Jira details  
4. Normalize PR labels  
5. Transition Jira issues to **In Review**  
6. Update Jira fields with PR metadata  

---

## `main_update_jira_status_to_ready_for_merge.yml`

This workflow is triggered **whenever a label is added to a PR**, and it reacts differently depending on the label.

### Why this workflow triggers

GitHub sends an event **every time a new label is added**.  
This workflow listens to that event, evaluates which label was applied, and synchronizes behavior with Jira.

### Label Synchronization Logic

When a label is applied to a PR:

| GitHub label added | Action taken |
|--------------------|--------------|
| any label | Add the same label to Jira |
| `status/release_blocker` | Also add Jira label `P0` |
| `status/merge_candidate` | Transition Jira → *Ready for Merge* |
| anything else | Only sync the label to Jira |

### Workflow Sequence

1. Extract Jira keys  
2. Add the triggering label to Jira  
3. If label is `status/release_blocker`, also add Jira label `P0`  
4. Fetch Jira issue details  
5. Normalize PR labels (priority, area/*)  
6. **Only if the label is `status/merge_candidate`**, transition Jira → Ready for Merge  
7. Write PR metadata into Jira fields  

### Important Notes

- This workflow **only** transitions Jira when the correct triggering label is applied.  
- It ensures that Ready-for-Merge transitions are **intentional** and explicitly signaled by developers.  
- All labels added in GitHub remain synchronized into Jira.

---

# 7. The general Workflow Interaction Diagram

```
call action is triggered by a PR Event
   │
   ▼
main High-Level Orchestration Workflows
   │
   ▼
extract_jira_keys
   │
   ▼
extract_jira_issue_details
   │
   ▼
apply_labels_to_pr
   │
   ▼
jira_transition (conditional)
   │
   ▼
fill_jira_with_github_data
```

---

# 8. Summary

These automation workflows provide a complete integration layer between GitHub and Jira:

- Extract Jira keys from PRs  
- Synchronize labels between systems  
- Compute normalized priority and area labels  
- Write PR information into Jira  
- Transition Jira issues based on PR workflow  

They ensure consistent project tracking and reduce manual Jira updates.

---
