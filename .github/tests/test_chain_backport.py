"""
Unit tests for chain backport processing in auto-backport-jira.py.

Tests:
  - process_chain_backport
  - process_branch_push
  - _close_superseded_backport_prs
"""

import re
import pytest
from unittest.mock import patch, MagicMock, call


class TestProcessChainBackport:
    def test_skips_non_backport_pr(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        merged_pr = make_pr(number=10, title="Fix bug", body="Normal body", merged=True)

        with patch.object(bp_module, "backport") as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")
            mock_bp.assert_not_called()

    def test_no_remaining_labels_stops_chain(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            merged=True,
            labels=[]  # No remaining backport labels
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "backport") as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")
            mock_bp.assert_not_called()

    def test_continues_chain_to_next_version(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3", "backport/2025.2"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_bp.assert_called_once()
            call_args = mock_bp.call_args[0]
            assert call_args[2] == "2025.3"  # Next version (highest remaining)

    def test_rebase_merged_pr_uses_all_commits(self, bp_module, make_pr, make_repo, make_commit):
        """Rebase-merged backport PR with multiple commits should cherry-pick ALL commits,
        not just the last one (merge_commit_sha).

        Regression test for: https://github.com/scylladb/scylla-dtest/pull/6865#issuecomment-4236622352
        PR #6856 had 2 commits. The chain backport from PR #6863 (rebase-merged) only
        cherry-picked the last commit (merge_commit_sha), dropping the first commit.
        """
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")

        # PR commit objects (as returned by pr.get_commits())
        commit1 = make_commit(sha="commit1_sha", message="Fix the actual bug")
        commit2 = make_commit(sha="commit2_sha", message="Add comment documenting workaround")

        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="commit2_sha",  # Rebase merge: last commit = merge_commit_sha
            labels=["backport/2025.3"]
        )
        merged_pr.get_commits.return_value = [commit1, commit2]

        # repo.get_commit for the merge_commit_sha returns single-parent (rebase merge)
        rebase_commit = make_commit(sha="commit2_sha", parents=[MagicMock()])
        repo.get_commit.return_value = rebase_commit

        captured_commits = {}

        def capture_backport(*args, **kwargs):
            captured_commits['commits'] = args[3]  # 4th positional arg is commits
            return make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", side_effect=capture_backport) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_bp.assert_called_once()
            # Must cherry-pick BOTH commits, not just the last one
            assert captured_commits['commits'] == ["commit1_sha", "commit2_sha"], \
                f"Expected both commits but got: {captured_commits['commits']}"

    def test_true_merge_commit_uses_single_sha(self, bp_module, make_pr, make_repo, make_commit):
        """True merge commit (multi-parent) should use only merge_commit_sha."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge_sha",
            labels=["backport/2025.3"]
        )

        # True merge commit has 2 parents
        merge_commit = make_commit(sha="merge_sha", parents=[MagicMock(), MagicMock()])
        repo.get_commit.return_value = merge_commit

        captured_commits = {}

        def capture_backport(*args, **kwargs):
            captured_commits['commits'] = args[3]
            return make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", side_effect=capture_backport) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_bp.assert_called_once()
            assert captured_commits['commits'] == ["merge_sha"]

    def test_single_commit_rebase_merge_uses_merge_sha(self, bp_module, make_pr, make_repo, make_commit):
        """Rebase-merged PR with a single commit should use merge_commit_sha directly."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")

        single_commit = make_commit(sha="only_commit_sha", message="Fix bug")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="only_commit_sha",
            labels=["backport/2025.3"]
        )
        merged_pr.get_commits.return_value = [single_commit]

        # Single parent = rebase merge
        rebase_commit = make_commit(sha="only_commit_sha", parents=[MagicMock()])
        repo.get_commit.return_value = rebase_commit

        captured_commits = {}

        def capture_backport(*args, **kwargs):
            captured_commits['commits'] = args[3]
            return make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", side_effect=capture_backport) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_bp.assert_called_once()
            assert captured_commits['commits'] == ["only_commit_sha"]

    def test_marks_merged_version_done_on_original(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            merged=True,
            labels=[]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")
            mock_done.assert_called_once_with(repo, original, "2025.4")

    def test_updates_labels_on_merged_pr(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3", "backport/2025.2"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)):
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            # Should remove backport/2025.3 (next version) from merged PR
            merged_pr.remove_from_labels.assert_any_call("backport/2025.3")
            # Should add pending label for 2025.2 (remaining after next)
            merged_pr.add_to_labels.assert_any_call("backport/2025.2-pending")

    def test_passes_remaining_labels_to_new_backport(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3", "backport/2025.2"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            _, kwargs = mock_bp.call_args
            assert "backport/2025.2" in kwargs.get("remaining_backport_labels", [])

    def test_uses_existing_jira_sub_issues(self, bp_module, make_pr, make_repo):
        """Chain backports should look up existing sub-issues, not create new ones."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Normal body\nFixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value="user-id"), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value="SCYLLADB-888") as mock_find, \
             patch.object(bp_module, "assign_jira_issue") as mock_assign, \
             patch.object(bp_module, "create_jira_sub_issue") as mock_create, \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)):
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_find.assert_called_once()
            mock_assign.assert_called_once_with("SCYLLADB-888", "user-id")
            mock_create.assert_not_called()

    def test_chain_backport_uses_parent_pr_format_for_pr_link(self, bp_module, make_pr, make_repo):
        """Chain backport from a PR using 'Parent PR: #N' format should correctly
        resolve the original PR link, not fall back to the merged PR number.
        
        Regression test for: https://scylladb.atlassian.net/browse/DTEST-162
        PR #6800 (2026.1 backport) had 'Parent PR: #6398'. When chain-backporting
        to 2025.4, the automation incorrectly used #6800 as the parent PR instead
        of #6398 because extract_main_pr_link_from_body didn't handle the
        'Parent PR' format.
        """
        repo = make_repo()
        # Original PR (e.g., #6398)
        original = make_pr(number=6398, title="Fix bug", body="Fix\nFixes: DTEST-93")
        # Merged 2026.1 backport PR (e.g., #6800) uses "Parent PR:" format
        merged_pr = make_pr(
            number=6800,
            title="[Backport 2026.1] Fix bug",
            body="Fix\nFixes: DTEST-161\n\n- (cherry picked from commit abc123)\n\nParent PR: #6398",
            merged=True,
            merge_commit_sha="merge456",
            labels=["backport/2025.4"]
        )

        captured_pr_body = {}

        def capture_backport(*args, **kwargs):
            captured_pr_body['body'] = kwargs.get('pr_body', '')
            return make_pr(number=6824)

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value="DTEST-162"), \
             patch.object(bp_module, "backport", side_effect=capture_backport) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylla-dtest")

            mock_bp.assert_called_once()
            pr_body = captured_pr_body['body']
            # Parent PR should point to original #6398, not intermediate #6800
            assert "Parent PR: #6398" in pr_body, f"Expected 'Parent PR: #6398' but got body:\n{pr_body}"
            assert "Parent PR: #6800" not in pr_body
            # Fixes should reference the sub-issue DTEST-162, not the parent DTEST-93
            assert "DTEST-162" in pr_body
            assert "DTEST-93" not in pr_body

    def test_chain_backport_fallback_to_original_pr_when_no_link(self, bp_module, make_pr, make_repo):
        """When neither 'backport of PR' nor 'Parent PR' is found in body,
        should fall back to the root original PR number, not the merged PR."""
        repo = make_repo()
        original = make_pr(number=100, title="Fix bug", body="Fix\nFixes: PROJ-1")
        # Merged PR with no parent PR link in body (edge case)
        merged_pr = make_pr(
            number=200,
            title="[Backport 2025.4] Fix bug",
            body="Fix\nFixes: PROJ-2",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3"]
        )

        captured_pr_body = {}

        def capture_backport(*args, **kwargs):
            captured_pr_body['body'] = kwargs.get('pr_body', '')
            return make_pr(number=300)

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", side_effect=capture_backport):
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            pr_body = captured_pr_body['body']
            # Should fall back to original PR #100, not merged PR #200
            assert "Parent PR: #100" in pr_body, f"Expected 'Parent PR: #100' but got body:\n{pr_body}"
            assert "Parent PR: #200" not in pr_body


class TestProcessBranchPush:
    def test_skips_gating_branch_next(self, bp_module, make_repo):
        repo = make_repo()
        with patch.object(bp_module, "backport") as mock_bp:
            bp_module.process_branch_push(repo, "abc..def", "next", "scylladb/scylladb")
            mock_bp.assert_not_called()

    def test_skips_gating_branch_next_version(self, bp_module, make_repo):
        repo = make_repo()
        with patch.object(bp_module, "backport") as mock_bp:
            bp_module.process_branch_push(repo, "abc..def", "next-2025.4", "scylladb/scylla-pkg")
            mock_bp.assert_not_called()

    def test_adds_promoted_label_to_backport_pr(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        backport_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            labels=[]
        )
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        commit = make_commit(sha="abc123", message="Fix bug")
        commit.get_pulls.return_value = [backport_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport"), \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            backport_pr.add_to_labels.assert_called_with("promoted-to-branch-2025.4")

    def test_marks_done_on_original_pr(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        backport_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            labels=[]
        )
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [backport_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done, \
             patch.object(bp_module, "process_chain_backport"), \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            mock_done.assert_called_once_with(repo, original, "2025.4")

    def test_continues_chain_backport(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        backport_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            labels=["backport/2025.3"]
        )
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [backport_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport") as mock_chain, \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            mock_chain.assert_called_once_with(repo, backport_pr, "scylladb/scylladb")

    def test_original_pr_on_version_branch_with_backport_labels(self, bp_module, make_pr, make_repo, make_commit):
        """Original PR targeting version branch with backport labels should initiate backport."""
        repo = make_repo()
        original_pr = make_pr(
            number=10,
            title="Fix bug",
            body="Fix for version branch\nFixes: SCYLLADB-123",
            labels=["backport/2025.3"]
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [original_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_pr_commits", return_value=["abc123"]), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj, \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            mock_bpj.assert_called_once()
            call_args = mock_bpj.call_args
            assert call_args[0][2] == ["2025.3"]  # versions

    def test_adds_promoted_label_to_original_pr_on_version_branch(self, bp_module, make_pr, make_repo, make_commit):
        """Non-backport PRs should get promoted-to label."""
        repo = make_repo()
        original_pr = make_pr(number=10, title="Fix bug", body="Normal", labels=[])

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [original_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            original_pr.add_to_labels.assert_called_with("promoted-to-branch-2025.4")

    def test_manager_branch_version(self, bp_module, make_pr, make_repo, make_commit):
        """Manager branch version should be 'manager-X.Y'."""
        repo = make_repo()
        backport_pr = make_pr(
            number=10,
            title="[Backport manager-3.4] Fix bug",
            body="This PR is a backport of PR #1",
            labels=[]
        )
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [backport_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done, \
             patch.object(bp_module, "process_chain_backport"), \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "manager-3.4", "scylladb/scylladb")

            mock_done.assert_called_once_with(repo, original, "manager-3.4")

    def test_finds_closed_prs_via_closes_reference(self, bp_module, make_pr, make_repo, make_commit):
        """Should find PRs referenced via 'Closes #X' in commit messages."""
        repo = make_repo()
        closed_pr = make_pr(number=99, title="Fix bug", body="Normal", state="closed", labels=[])

        commit = make_commit(sha="abc123", message="Fix bug\n\nCloses #99")
        commit.get_pulls.return_value = []  # Not returned by get_pulls

        repo.get_pull.return_value = closed_pr

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "master", "scylladb/scylladb")

            closed_pr.add_to_labels.assert_called_with("promoted-to-master")


class TestCloseSupersededBackportPrs:
    def test_closes_pr_by_closes_reference(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        backport_pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open",
            base_ref="branch-2025.4"
        )
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        commit = make_commit(sha="new123", message="Fix bug\n\nCloses #42")
        repo.get_pull.return_value = backport_pr

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport"):
            bp_module._close_superseded_backport_prs(
                repo, [commit], "branch-2025.4", "2025.4",
                "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
            )

            backport_pr.add_to_labels.assert_called_with("promoted-to-branch-2025.4")
            backport_pr.edit.assert_called_with(state='closed')

    def test_closes_pr_by_commit_title_match(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        backport_pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open",
            base_ref="branch-2025.4"
        )

        # PR's latest commit has the same title as the promoted commit
        pr_commit = make_commit(sha="old456", message="Fix bug")
        backport_pr.get_commits.return_value = [pr_commit]

        promoted_commit = make_commit(sha="new123", message="Fix bug")

        original = make_pr(number=1, title="Fix bug", body="Normal body")

        repo.get_pulls.return_value = [backport_pr]

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport"):
            bp_module._close_superseded_backport_prs(
                repo, [promoted_commit], "branch-2025.4", "2025.4",
                "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
            )

            backport_pr.edit.assert_called_with(state='closed')

    def test_skips_already_processed_prs(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit(sha="abc123", message="Fix bug\n\nCloses #42")

        already_processed = {42}
        repo.get_pull = MagicMock()

        bp_module._close_superseded_backport_prs(
            repo, [commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", already_processed, "scylladb/scylladb"
        )

        repo.get_pull.assert_not_called()


class TestProcessChainBackportEdgeCases:
    """Edge case tests for process_chain_backport (lines 1530-1531, 1584, 1598, 1611, 1619-1623, 1644-1645)."""

    def test_original_pr_equals_merged_pr_sets_none(self, bp_module, make_pr, make_repo):
        """When get_root_original_pr returns merged_pr itself, original_pr should become None (line 1530-1531)."""
        repo = make_repo()
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            merged=True,
            labels=[]
        )

        # get_root_original_pr returns the merged_pr itself
        with patch.object(bp_module, "get_root_original_pr", return_value=merged_pr), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            # When original_pr == merged_pr, original_pr is set to None,
            # so replace_backport_label_with_done should NOT be called
            mock_done.assert_not_called()

    def test_all_jira_keys_fallback_to_main_jira_key(self, bp_module, make_pr, make_repo):
        """When extract_all_jira_keys_from_pr_body returns empty, falls back to [main_jira_key] (line 1583-1584)."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="No jira keys in body")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "extract_all_jira_keys_from_pr_body", return_value=[]), \
             patch.object(bp_module, "extract_main_jira_from_body", return_value="SCYLLADB-123"), \
             patch.object(bp_module, "get_jira_issue", return_value={"fields": {"issuetype": {"subtask": False}}}), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_bp.assert_called_once()

    def test_grandparent_key_lookup(self, bp_module, make_pr, make_repo):
        """When parent key is a subtask, should look up under grandparent (line 1596-1598)."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Fixes: SCYLLADB-100")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-100",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3"]
        )

        # Simulate: SCYLLADB-100 is a subtask of SCYLLADB-50
        jira_issue = {"fields": {"issuetype": {"subtask": True}, "parent": {"key": "SCYLLADB-50"}}}

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value=jira_issue), \
             patch.object(bp_module, "get_parent_key_if_subtask", return_value="SCYLLADB-50") as mock_parent, \
             patch.object(bp_module, "find_existing_sub_issue", return_value="SCYLLADB-888") as mock_find, \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)):
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            # Should search under grandparent SCYLLADB-50
            mock_find.assert_called_once_with("SCYLLADB-50", "2025.3")

    def test_no_jira_integration_branch(self, bp_module, make_pr, make_repo):
        """When JIRA_USER/JIRA_API_TOKEN are empty, uses parent key as-is (line 1611)."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Fixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "JIRA_USER", ""), \
             patch.object(bp_module, "JIRA_API_TOKEN", ""), \
             patch.object(bp_module, "get_jira_issue") as mock_get_jira, \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_get_jira.assert_not_called()
            mock_bp.assert_called_once()

    def test_non_merged_pr_uses_get_pr_commits(self, bp_module, make_pr, make_repo):
        """For non-merged (closed by push) PRs, should use get_pr_commits (line 1619)."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Fixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=False,  # Not merged through GitHub UI
            merge_commit_sha=None,
            labels=["backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value=None), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "get_pr_commits", return_value=["commit123"]) as mock_get_commits, \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)):
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_get_commits.assert_called_once()

    def test_no_commits_found_returns_early(self, bp_module, make_pr, make_repo):
        """When no commits found (lines 1621-1623), should return early without calling backport."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Fixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha=None,  # No merge commit SHA
            labels=["backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value=None), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport") as mock_bp:
            bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")

            mock_bp.assert_not_called()

    def test_label_update_exception_continues(self, bp_module, make_pr, make_repo):
        """Exception updating labels on merged PR should be caught (lines 1644-1645)."""
        repo = make_repo()
        original = make_pr(number=1, title="Fix bug", body="Fixes: SCYLLADB-123")
        merged_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1\nThe main Jira issue is SCYLLADB-123",
            merged=True,
            merge_commit_sha="merge123",
            labels=["backport/2025.3", "backport/2025.2"]
        )
        merged_pr.remove_from_labels.side_effect = Exception("label API error")

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "get_jira_issue", return_value=None), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)):
            # Should not raise despite label exception
            result = bp_module.process_chain_backport(repo, merged_pr, "scylladb/scylladb")
            # Should still return the backport PR
            assert result is not None


class TestProcessBranchPushEdgeCases:
    """Edge case tests for process_branch_push (lines 1781-1834)."""

    def test_dedup_guard_skips_already_processed(self, bp_module, make_pr, make_repo, make_commit):
        """PR already in processed_prs should be skipped (line 1786)."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1"
        )
        # Same PR returned by two different commits
        commit1 = make_commit(sha="abc123")
        commit1.get_pulls.return_value = [pr]
        commit2 = make_commit(sha="def456")
        commit2.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit1, commit2]
        repo.compare.return_value = comparison

        original = make_pr(number=1, title="Fix bug", body="Normal body")

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done, \
             patch.object(bp_module, "process_chain_backport"), \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            # replace_backport_label_with_done should only be called once despite PR appearing twice
            mock_done.assert_called_once()

    def test_add_label_exception_on_original_pr(self, bp_module, make_pr, make_repo, make_commit):
        """Exception adding promoted label to original PR should be caught (lines 1794-1795)."""
        repo = make_repo()
        original_pr = make_pr(number=10, title="Fix bug", body="Normal", labels=[])
        original_pr.add_to_labels.side_effect = Exception("label API error")

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [original_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "_close_superseded_backport_prs"):
            # Should not raise
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

    def test_add_label_exception_on_backport_pr(self, bp_module, make_pr, make_repo, make_commit):
        """Exception adding promoted label to backport PR should be caught (lines 1823-1824)."""
        repo = make_repo()
        backport_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            labels=[]
        )
        backport_pr.add_to_labels.side_effect = Exception("label API error")
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [backport_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport"), \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            # Should not raise
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

    def test_could_not_find_original_pr_warning(self, bp_module, make_pr, make_repo, make_commit):
        """When get_root_original_pr returns the PR itself, should warn and skip done marking (line 1830-1834)."""
        repo = make_repo()
        backport_pr = make_pr(
            number=10,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            labels=[]
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [backport_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        # get_root_original_pr returns the backport PR itself (couldn't trace back)
        with patch.object(bp_module, "get_root_original_pr", return_value=backport_pr), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done, \
             patch.object(bp_module, "process_chain_backport"), \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            # Should NOT call replace_backport_label_with_done since original == backport
            mock_done.assert_not_called()

    def test_no_commits_for_pr_on_branch(self, bp_module, make_pr, make_repo, make_commit):
        """No commits found for original PR on version branch (line 1814)."""
        repo = make_repo()
        original_pr = make_pr(
            number=10,
            title="Fix bug",
            body="Fix for version branch\nFixes: SCYLLADB-123",
            labels=["backport/2025.3"]
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [original_pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "get_pr_commits", return_value=[]), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj, \
             patch.object(bp_module, "_close_superseded_backport_prs"):
            bp_module.process_branch_push(repo, "before..after", "branch-2025.4", "scylladb/scylladb")

            mock_bpj.assert_not_called()

    def test_closes_reference_exception(self, bp_module, make_pr, make_repo, make_commit):
        """Exception fetching PR from Closes reference should be caught (lines 1781-1782)."""
        repo = make_repo()
        commit = make_commit(sha="abc123", message="Fix bug\n\nCloses #99")
        commit.get_pulls.return_value = []

        repo.get_pull.side_effect = Exception("API error")

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        with patch.object(bp_module, "_close_superseded_backport_prs"):
            # Should not raise
            bp_module.process_branch_push(repo, "before..after", "master", "scylladb/scylladb")


class TestCloseSupersededBackportPrsEdgeCases:
    """Edge case tests for _close_superseded_backport_prs (lines 1870-1901)."""

    def test_skips_non_open_pr_from_closes_ref(self, bp_module, make_pr, make_repo, make_commit):
        """PR from Closes reference that is not open should be skipped (line 1870)."""
        repo = make_repo()
        closed_pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="closed",
            base_ref="branch-2025.4"
        )
        commit = make_commit(sha="new123", message="Fix bug\n\nCloses #42")
        repo.get_pull.return_value = closed_pr

        bp_module._close_superseded_backport_prs(
            repo, [commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

        # Should not try to close an already-closed PR
        closed_pr.edit.assert_not_called()

    def test_skips_non_backport_pr_from_closes_ref(self, bp_module, make_pr, make_repo, make_commit):
        """PR from Closes reference that is not a backport should be skipped (line 1872)."""
        repo = make_repo()
        regular_pr = make_pr(
            number=42,
            title="Fix bug",
            body="Regular PR",
            state="open",
            base_ref="branch-2025.4"
        )
        commit = make_commit(sha="new123", message="Fix bug\n\nCloses #42")
        repo.get_pull.return_value = regular_pr

        bp_module._close_superseded_backport_prs(
            repo, [commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

        regular_pr.edit.assert_not_called()

    def test_skips_pr_targeting_different_branch(self, bp_module, make_pr, make_repo, make_commit):
        """PR targeting different branch should be skipped (line 1874)."""
        repo = make_repo()
        pr = make_pr(
            number=42,
            title="[Backport 2025.3] Fix bug",
            body="This PR is a backport of PR #1",
            state="open",
            base_ref="branch-2025.3"  # Different branch
        )
        commit = make_commit(sha="new123", message="Fix bug\n\nCloses #42")
        repo.get_pull.return_value = pr

        bp_module._close_superseded_backport_prs(
            repo, [commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

        pr.edit.assert_not_called()

    def test_exception_checking_pr_from_closes_ref(self, bp_module, make_repo, make_commit):
        """Exception checking PR from Closes reference should be caught (lines 1877-1878)."""
        repo = make_repo()
        commit = make_commit(sha="new123", message="Fix bug\n\nCloses #42")
        repo.get_pull.side_effect = Exception("API error")

        # Should not raise
        bp_module._close_superseded_backport_prs(
            repo, [commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

    def test_skips_non_backport_in_title_match(self, bp_module, make_pr, make_repo, make_commit):
        """Open PR that is not a backport should be skipped in title matching (line 1887)."""
        repo = make_repo()
        regular_pr = make_pr(
            number=42,
            title="Fix bug",
            body="Regular PR",
            state="open",
            base_ref="branch-2025.4"
        )
        repo.get_pulls.return_value = [regular_pr]

        promoted_commit = make_commit(sha="new123", message="Fix bug")

        bp_module._close_superseded_backport_prs(
            repo, [promoted_commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

        regular_pr.edit.assert_not_called()

    def test_skips_pr_with_no_commits(self, bp_module, make_pr, make_repo, make_commit):
        """Open backport PR with no commits should be skipped (line 1892)."""
        repo = make_repo()
        backport_pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open",
            base_ref="branch-2025.4"
        )
        backport_pr.get_commits.return_value = []
        repo.get_pulls.return_value = [backport_pr]

        promoted_commit = make_commit(sha="new123", message="Fix bug")

        bp_module._close_superseded_backport_prs(
            repo, [promoted_commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

        backport_pr.edit.assert_not_called()

    def test_exception_checking_commits_for_pr(self, bp_module, make_pr, make_repo, make_commit):
        """Exception checking commits for PR should be caught (lines 1898-1899)."""
        repo = make_repo()
        backport_pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open",
            base_ref="branch-2025.4"
        )
        backport_pr.get_commits.side_effect = Exception("API error")
        repo.get_pulls.return_value = [backport_pr]

        promoted_commit = make_commit(sha="new123", message="Fix bug")

        # Should not raise
        bp_module._close_superseded_backport_prs(
            repo, [promoted_commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

    def test_exception_searching_open_prs(self, bp_module, make_repo, make_commit):
        """Exception searching open PRs should be caught (lines 1900-1901)."""
        repo = make_repo()
        repo.get_pulls.side_effect = Exception("API error")

        promoted_commit = make_commit(sha="new123", message="Fix bug")

        # Should not raise
        bp_module._close_superseded_backport_prs(
            repo, [promoted_commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", set(), "scylladb/scylladb"
        )

    def test_already_processed_in_title_match_skipped(self, bp_module, make_pr, make_repo, make_commit):
        """PR already in processed set should be skipped in title matching (line 1885)."""
        repo = make_repo()
        backport_pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open",
            base_ref="branch-2025.4"
        )
        repo.get_pulls.return_value = [backport_pr]

        promoted_commit = make_commit(sha="new123", message="Fix bug")

        already_processed = {42}  # Already processed

        bp_module._close_superseded_backport_prs(
            repo, [promoted_commit], "branch-2025.4", "2025.4",
            "promoted-to-branch-2025.4", already_processed, "scylladb/scylladb"
        )

        backport_pr.edit.assert_not_called()


class TestClosePromotedBackportPrEdgeCases:
    """Edge case tests for _close_promoted_backport_pr (lines 1909-1934)."""

    def test_add_label_exception(self, bp_module, make_pr, make_repo):
        """Exception adding label should be caught (lines 1912-1913)."""
        repo = make_repo()
        pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open"
        )
        pr.add_to_labels.side_effect = Exception("label API error")
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport"):
            # Should not raise
            bp_module._close_promoted_backport_pr(
                repo, pr, "branch-2025.4", "2025.4",
                "promoted-to-branch-2025.4", "scylladb/scylladb", "newsha123"
            )

            # Should still try to close the PR
            pr.edit.assert_called_with(state='closed')

    def test_close_pr_exception(self, bp_module, make_pr, make_repo):
        """Exception closing PR should be caught (lines 1921-1922)."""
        repo = make_repo()
        pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open"
        )
        pr.edit.side_effect = Exception("close API error")
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done"), \
             patch.object(bp_module, "process_chain_backport"):
            # Should not raise
            bp_module._close_promoted_backport_pr(
                repo, pr, "branch-2025.4", "2025.4",
                "promoted-to-branch-2025.4", "scylladb/scylladb", "newsha123"
            )

    def test_could_not_find_original_pr(self, bp_module, make_pr, make_repo):
        """When get_root_original_pr returns the PR itself, should warn (line 1932)."""
        repo = make_repo()
        pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open"
        )

        # get_root_original_pr returns the PR itself
        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done, \
             patch.object(bp_module, "process_chain_backport"):
            bp_module._close_promoted_backport_pr(
                repo, pr, "branch-2025.4", "2025.4",
                "promoted-to-branch-2025.4", "scylladb/scylladb", "newsha123"
            )

            # Should NOT call replace_backport_label_with_done
            mock_done.assert_not_called()

    def test_exception_marking_as_done(self, bp_module, make_pr, make_repo):
        """Exception in replace_backport_label_with_done should be caught (lines 1933-1934)."""
        repo = make_repo()
        pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open"
        )
        original = make_pr(number=1, title="Fix bug", body="Normal body")

        with patch.object(bp_module, "get_root_original_pr", return_value=original), \
             patch.object(bp_module, "replace_backport_label_with_done", side_effect=Exception("API error")), \
             patch.object(bp_module, "process_chain_backport"):
            # Should not raise
            bp_module._close_promoted_backport_pr(
                repo, pr, "branch-2025.4", "2025.4",
                "promoted-to-branch-2025.4", "scylladb/scylladb", "newsha123"
            )

    def test_no_version_skips_done_marking(self, bp_module, make_pr, make_repo):
        """When version is None, should skip done marking entirely (line 1925)."""
        repo = make_repo()
        pr = make_pr(
            number=42,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1",
            state="open"
        )

        with patch.object(bp_module, "get_root_original_pr") as mock_root, \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done, \
             patch.object(bp_module, "process_chain_backport"):
            bp_module._close_promoted_backport_pr(
                repo, pr, "branch-2025.4", None,
                "promoted-to-branch-2025.4", "scylladb/scylladb", "newsha123"
            )

            mock_root.assert_not_called()
            mock_done.assert_not_called()
