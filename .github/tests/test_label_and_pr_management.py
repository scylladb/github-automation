"""
Unit tests for label management and PR creation functions in auto-backport-jira.py.

Tests:
  - replace_backport_label_with_done
  - get_promoted_label
  - create_pull_request
  - find_existing_backport_pr
  - create_pr_comment_and_remove_label
"""

import pytest
from unittest.mock import patch, MagicMock, call


class TestGetPromotedLabel:
    def test_master(self, bp_module):
        assert bp_module.get_promoted_label("master") == "promoted-to-master"

    def test_main(self, bp_module):
        assert bp_module.get_promoted_label("main") == "promoted-to-master"

    def test_next(self, bp_module):
        assert bp_module.get_promoted_label("next") == "promoted-to-master"

    def test_branch(self, bp_module):
        assert bp_module.get_promoted_label("branch-2025.4") == "promoted-to-branch-2025.4"

    def test_next_gating_branch(self, bp_module):
        # next-X.Y maps to promoted-to-branch-X.Y
        assert bp_module.get_promoted_label("next-2025.4") == "promoted-to-branch-2025.4"

    def test_manager_branch(self, bp_module):
        assert bp_module.get_promoted_label("manager-3.4") == "promoted-to-manager-3.4"


class TestReplaceBackportLabelWithDone:
    def test_replace_original_label(self, bp_module, make_pr, make_label):
        pr = make_pr(labels=["backport/2025.4", "promoted-to-master"])
        result = bp_module.replace_backport_label_with_done(MagicMock(), pr, "2025.4")
        assert result is True
        pr.remove_from_labels.assert_called_once_with("backport/2025.4")
        pr.add_to_labels.assert_called_once_with("backport/2025.4-done")

    def test_replace_pending_label(self, bp_module, make_pr):
        pr = make_pr(labels=["backport/2025.4-pending"])
        result = bp_module.replace_backport_label_with_done(MagicMock(), pr, "2025.4")
        assert result is True
        pr.remove_from_labels.assert_called_once_with("backport/2025.4-pending")
        pr.add_to_labels.assert_called_once_with("backport/2025.4-done")

    def test_already_done(self, bp_module, make_pr):
        pr = make_pr(labels=["backport/2025.4-done"])
        result = bp_module.replace_backport_label_with_done(MagicMock(), pr, "2025.4")
        assert result is True
        pr.remove_from_labels.assert_not_called()

    def test_no_matching_label(self, bp_module, make_pr):
        pr = make_pr(labels=["backport/2025.3"])
        result = bp_module.replace_backport_label_with_done(MagicMock(), pr, "2025.4")
        assert result is False


class TestFindExistingBackportPr:
    def test_found_open_pr(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        existing_pr = make_pr(number=42)
        repo.get_pulls.return_value = [existing_pr]

        result = bp_module.find_existing_backport_pr(repo, 10, "2025.4")
        assert result.number == 42
        repo.get_pulls.assert_called_with(state='open', head='scylladbbot:backport/10/to-2025.4')

    def test_found_closed_pr(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        # First call (open) returns empty, second call (all) returns the PR
        existing_pr = make_pr(number=42, state="closed")
        repo.get_pulls.side_effect = [[], [existing_pr]]

        result = bp_module.find_existing_backport_pr(repo, 10, "2025.4")
        assert result.number == 42

    def test_not_found(self, bp_module, make_repo):
        repo = make_repo()
        repo.get_pulls.return_value = []

        result = bp_module.find_existing_backport_pr(repo, 10, "2025.4")
        assert result is None

    def test_error_returns_none(self, bp_module, make_repo):
        repo = make_repo()
        repo.get_pulls.side_effect = Exception("API error")

        result = bp_module.find_existing_backport_pr(repo, 10, "2025.4")
        assert result is None


class TestCreatePullRequest:
    def test_basic_creation(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )

        assert result.number == 42
        repo.create_pull.assert_called_once()
        created_pr.add_to_assignees.assert_called_once()

    def test_draft_pr_adds_conflicts_label(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                is_draft=True, pr_body="Test body", backport_version="2025.4"
            )

        created_pr.add_to_labels.assert_called_once()
        labels_added = created_pr.add_to_labels.call_args[0]
        assert "conflicts" in labels_added
        created_pr.create_issue_comment.assert_called_once()

    def test_jira_failure_label_added(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", jira_failed=True, backport_version="2025.4"
            )

        labels_added = created_pr.add_to_labels.call_args[0]
        assert bp_module.JIRA_FAILURE_LABEL in labels_added

    def test_priority_labels_inherited(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=["P0", "promoted-to-master"])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )

        labels_added = created_pr.add_to_labels.call_args[0]
        assert "P0" in labels_added
        assert "force_on_cloud" in labels_added

    def test_p0_no_force_on_cloud_for_scylla_pkg(self, bp_module, make_pr, make_repo):
        repo = make_repo(full_name="scylladb/scylla-pkg", name="scylla-pkg")
        pr = make_pr(number=10, labels=["P0"])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "next-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )

        labels_added = created_pr.add_to_labels.call_args[0]
        assert "P0" in labels_added
        assert "force_on_cloud" not in labels_added

    def test_remaining_backport_labels_added(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", remaining_backport_labels=["backport/2025.3", "backport/2025.2"],
                backport_version="2025.4"
            )

        labels_added = created_pr.add_to_labels.call_args[0]
        assert "backport/2025.3" in labels_added
        assert "backport/2025.2" in labels_added

    def test_pr_already_exists(self, bp_module, make_pr, make_repo):
        from github import GithubException
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        existing_pr = make_pr(number=42)
        repo.create_pull.side_effect = GithubException(
            422, {"message": "A pull request already exists"}
        )
        repo.get_pulls.return_value = [existing_pr]

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )

        assert result.number == 42

    def test_original_pr_used_for_assignment(self, bp_module, make_pr, make_repo):
        """When original_pr is provided, PR is assigned to original PR's assignee."""
        repo = make_repo()
        pr = make_pr(number=10, user_login="bot")
        original_pr = make_pr(number=5, user_login="real-author")
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", original_pr=original_pr, backport_version="2025.4"
            )

        # Should assign to original_pr's assignee, not pr's user
        created_pr.add_to_assignees.assert_called_once_with(original_pr.assignees[0])

    def test_warn_missing_fixes(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", warn_missing_fixes=True, backport_version="2025.4"
            )

        created_pr.create_issue_comment.assert_called_once()
        comment = created_pr.create_issue_comment.call_args[0][0]
        assert "can't be merged without a valid Fixes reference" in comment

    def test_milestone_set_on_creation(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value="2025.4.2") as mock_ms, \
             patch.object(bp_module, "set_pr_milestone") as mock_set:
            bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )
            mock_set.assert_called_once_with(created_pr, "2025.4.2")


class TestCreatePrCommentAndRemoveLabel:
    def test_removes_backport_labels_and_adds_comment(self, bp_module, make_pr, make_label):
        labels = [make_label("backport/2025.4"), make_label("backport/2025.3"), make_label("other-label")]
        pr = make_pr(labels=["backport/2025.4", "backport/2025.3", "other-label"])
        pr.get_labels.return_value = labels

        bp_module.create_pr_comment_and_remove_label(pr)

        # Should remove backport labels but not other labels
        assert pr.remove_from_labels.call_count == 2
        pr.create_issue_comment.assert_called_once()
        comment = pr.create_issue_comment.call_args[0][0]
        assert "backport/2025.4" in comment
        assert "backport/2025.3" in comment


class TestReplaceBackportLabelWithDoneErrors:
    def test_exception_returns_false(self, bp_module, make_pr):
        """Line 1230-1231: Exception in replace_backport_label_with_done returns False."""
        pr = make_pr(labels=["backport/2025.4"])
        pr.remove_from_labels.side_effect = Exception("API error")
        result = bp_module.replace_backport_label_with_done(MagicMock(), pr, "2025.4")
        assert result is False


class TestCreatePullRequestEdgeCases:
    def test_fallback_body_when_pr_body_is_none(self, bp_module, make_pr, make_repo):
        """Line 1020-1023: When pr_body is None, uses fallback format with cherry-pick SHAs."""
        repo = make_repo()
        pr = make_pr(number=10, body="Original body", labels=[])
        created_pr = make_pr(number=42)
        repo.create_pull.return_value = created_pr

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123", "def456"],
                pr_body=None, backport_version="2025.4"
            )

        call_kwargs = repo.create_pull.call_args[1]
        body = call_kwargs["body"]
        assert "cherry picked from commit abc123" in body
        assert "cherry picked from commit def456" in body
        assert f"Parent PR: #{pr.number}" in body

    def test_pr_already_exists_warn_missing_fixes(self, bp_module, make_pr, make_repo):
        """Line 1104-1105: Warn missing fixes on existing PR found during 'already exists'."""
        from github import GithubException
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        existing_pr = make_pr(number=42)
        repo.create_pull.side_effect = GithubException(
            422, {"message": "A pull request already exists"}
        )
        repo.get_pulls.return_value = [existing_pr]

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", warn_missing_fixes=True, backport_version="2025.4"
            )

        existing_pr.create_issue_comment.assert_called_once()
        comment = existing_pr.create_issue_comment.call_args[0][0]
        assert "can't be merged without a valid Fixes reference" in comment

    def test_pr_already_exists_find_fails(self, bp_module, make_pr, make_repo):
        """Line 1110-1111: Exception finding existing PR after 'already exists'."""
        from github import GithubException
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        repo.create_pull.side_effect = GithubException(
            422, {"message": "A pull request already exists"}
        )
        repo.get_pulls.side_effect = Exception("API error")

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )

        assert result is None

    def test_generic_github_exception(self, bp_module, make_pr, make_repo):
        """Line 1112-1113: GithubException that is NOT 'already exists'."""
        from github import GithubException
        repo = make_repo()
        pr = make_pr(number=10, labels=[])
        repo.create_pull.side_effect = GithubException(500, {"message": "Internal error"})

        with patch.object(bp_module, "resolve_backport_milestone_title", return_value=None):
            result = bp_module.create_pull_request(
                repo, "backport/10/to-2025.4", "branch-2025.4",
                pr, "[Backport 2025.4] Fix bug", ["abc123"],
                pr_body="Test body", backport_version="2025.4"
            )

        assert result is None


class TestGetPrCommitsEdgeCases:
    def test_no_start_commit_fallback(self, bp_module, make_pr, make_repo, make_commit):
        """Line 1129: When no start_commit, falls back to repo.get_commits(sha=stable_branch)."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="merge123")
        # Single parent (not a merge commit)
        merge_commit = make_commit(sha="merge123", parents=[MagicMock()])
        repo.get_commit.return_value = merge_commit

        # get_commits returns some commits, and pr.get_commits has matching one
        promoted_commit = make_commit(sha="abc123", message="Fix bug")
        repo.get_commits.return_value = [promoted_commit]

        pr_commit = make_commit(sha="abc123", message="Fix bug")
        pr.get_commits.return_value = [pr_commit]

        result = bp_module.get_pr_commits(repo, pr, "branch-2025.4", start_commit=None)
        repo.get_commits.assert_called_once_with(sha="branch-2025.4")
