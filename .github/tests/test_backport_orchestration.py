"""
Unit tests for backport orchestration in auto-backport-jira.py.

Tests:
  - backport_with_jira (chained mode - default)
  - backport_with_jira (parallel mode)
  - backport (the git clone / cherry-pick / push function)
"""

import pytest
from unittest.mock import patch, MagicMock, call


class TestBackportFunction:
    """Tests for the backport() function that does git clone, cherry-pick, push."""

    def test_skips_when_commit_already_in_branch(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, title="Fix bug")

        with patch.object(bp_module, "is_commit_in_branch", return_value=True), \
             patch.object(bp_module, "replace_backport_label_with_done") as mock_done:
            result = bp_module.backport(repo, pr, "2025.4", ["abc123"], "branch-2025.4")
            assert result is None
            mock_done.assert_called_once()

    def test_successful_cherry_pick(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        pr = make_pr(number=10, title="Fix bug")
        commit = make_commit(sha="abc123", parents=[MagicMock()])  # single parent
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        mock_created_pr = make_pr(number=42)

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request", return_value=mock_created_pr) as mock_create:
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            result = bp_module.backport(repo, pr, "2025.4", ["abc123"], "branch-2025.4")
            assert result is not None
            mock_local_repo.git.cherry_pick.assert_called()
            mock_local_repo.git.push.assert_called_once()

    def test_cherry_pick_conflict_creates_draft(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        pr = make_pr(number=10, title="Fix bug")
        commit = make_commit(sha="abc123", parents=[MagicMock()])
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        from git import GitCommandError
        # First call (the cherry-pick) raises; second call (--continue) succeeds
        mock_local_repo.git.cherry_pick.side_effect = [
            GitCommandError("cherry-pick", "conflict"),
            None,  # cherry_pick('--continue') succeeds
        ]

        mock_created_pr = make_pr(number=42)

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request", return_value=mock_created_pr) as mock_create:
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            result = bp_module.backport(repo, pr, "2025.4", ["abc123"], "branch-2025.4")

            # Should have called add(A=True) and cherry_pick('--continue') after conflict
            mock_local_repo.git.add.assert_called_with(A=True)
            # create_pull_request should be called with is_draft=True
            _, kwargs = mock_create.call_args
            assert kwargs.get("is_draft") is True

    def test_uses_x_flag_for_master_backport(self, bp_module, make_pr, make_repo, make_commit):
        """Direct backport from master uses -x flag."""
        repo = make_repo()
        pr = make_pr(number=10, title="Fix bug")
        commit = make_commit(sha="abc123", parents=[MagicMock()])
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        mock_created_pr = make_pr(number=42)

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request", return_value=mock_created_pr):
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            # When original_pr is None, it's a direct backport from master
            bp_module.backport(repo, pr, "2025.4", ["abc123"], "branch-2025.4", original_pr=None)

            cherry_pick_call = mock_local_repo.git.cherry_pick.call_args
            assert "-x" in cherry_pick_call[0]

    def test_omits_x_flag_for_chain_backport(self, bp_module, make_pr, make_repo, make_commit):
        """Chain backport omits -x flag to preserve original cherry-pick marker."""
        repo = make_repo()
        pr = make_pr(number=10, title="[Backport 2025.4] Fix bug")
        original_pr = make_pr(number=5, title="Fix bug")
        commit = make_commit(sha="abc123", parents=[MagicMock()])
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        mock_created_pr = make_pr(number=42)

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request", return_value=mock_created_pr):
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            bp_module.backport(repo, pr, "2025.3", ["abc123"], "branch-2025.3", original_pr=original_pr)

            cherry_pick_call = mock_local_repo.git.cherry_pick.call_args
            assert "-x" not in cherry_pick_call[0]

    def test_uses_m1_for_merge_commits(self, bp_module, make_pr, make_repo, make_commit):
        """Merge commits use -m1 flag."""
        repo = make_repo()
        pr = make_pr(number=10, title="Fix bug")
        # Merge commit has 2 parents
        commit = make_commit(sha="abc123", parents=[MagicMock(), MagicMock()])
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        mock_created_pr = make_pr(number=42)

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request", return_value=mock_created_pr):
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            bp_module.backport(repo, pr, "2025.4", ["abc123"], "branch-2025.4")

            cherry_pick_call = mock_local_repo.git.cherry_pick.call_args
            assert "-m1" in cherry_pick_call[0]

    def test_extracts_original_title_to_prevent_stacking(self, bp_module, make_pr, make_repo, make_commit):
        """Backport title should not stack [Backport X.Y] prefixes."""
        repo = make_repo()
        pr = make_pr(number=10, title="[Backport 2025.4] Fix bug")
        commit = make_commit(sha="abc123", parents=[MagicMock()])
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        mock_created_pr = make_pr(number=42)

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request", return_value=mock_created_pr) as mock_create:
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            bp_module.backport(repo, pr, "2025.3", ["abc123"], "branch-2025.3")

            # Title should be [Backport 2025.3] Fix bug, NOT [Backport 2025.3] [Backport 2025.4] Fix bug
            _, kwargs = mock_create.call_args
            # The backport_pr_title is passed as a positional arg (5th positional: index 4)
            call_args = mock_create.call_args[0]
            backport_title = call_args[4]  # 5th positional: backport_pr_title
            assert backport_title == "[Backport 2025.3] Fix bug"


class TestBackportWithJiraChainedMode:
    """Tests for backport_with_jira in default (chained) mode."""

    def test_creates_pr_for_highest_version_only(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(
            number=10,
            title="Fix bug",
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4", "backport/2025.3"]
        )

        mock_backport_pr = make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value="user-id"), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", return_value=mock_backport_pr) as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            # Should create backport for highest version (2025.4) only
            mock_bp.assert_called_once()
            call_args = mock_bp.call_args
            assert call_args[0][2] == "2025.4"  # version
            # Remaining labels should include 2025.3
            assert "backport/2025.3" in call_args[1].get("remaining_backport_labels", [])

    def test_replaces_remaining_labels_with_pending(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4", "backport/2025.3"]
        )
        mock_backport_pr = make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", return_value=mock_backport_pr):
            bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            # Should replace backport/2025.3 with backport/2025.3-pending on original PR
            pr.remove_from_labels.assert_called_with("backport/2025.3")
            pr.add_to_labels.assert_called_with("backport/2025.3-pending")

    def test_creates_jira_subtasks_for_all_versions(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-100\nFixes: SCYLLADB-200",
            labels=["promoted-to-master", "backport/2025.4", "backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value="user-id"), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999") as mock_create, \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)):
            bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-100", "scylladb/scylladb"
            )

            # Should create subtasks for all versions x all jira keys = 4 calls
            assert mock_create.call_count == 4

    def test_skips_when_backport_pr_exists(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, body="Fixes: SCYLLADB-123", labels=["backport/2025.4"])
        existing_pr = make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=existing_pr), \
             patch.object(bp_module, "backport") as mock_bp:
            result = bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            mock_bp.assert_not_called()
            assert result.number == 42

    def test_no_versions_returns_early(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=10, body="Fixes: SCYLLADB-123")

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "backport") as mock_bp:
            bp_module.backport_with_jira(repo, pr, [], ["abc123"], None, "scylladb/scylladb")
            mock_bp.assert_not_called()

    def test_warn_missing_fixes_for_scylladb(self, bp_module, make_pr, make_repo):
        """scylladb/scylladb PRs without Fixes reference should trigger warning."""
        repo = make_repo()
        pr = make_pr(number=10, body="No fixes reference here", labels=["backport/2025.4"])

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport") as mock_bp:
            mock_bp.return_value = make_pr(number=42)
            bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], None, "scylladb/scylladb"
            )
            _, kwargs = mock_bp.call_args
            assert kwargs.get("warn_missing_fixes") is True

    def test_no_warn_for_other_repos(self, bp_module, make_pr, make_repo):
        """Non-scylladb/scylladb repos should NOT warn about missing Fixes."""
        repo = make_repo(full_name="scylladb/scylla-pkg")
        pr = make_pr(number=10, body="No fixes reference here", labels=["backport/2025.4"])

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport") as mock_bp:
            mock_bp.return_value = make_pr(number=42)
            bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], None, "scylladb/scylla-pkg"
            )
            _, kwargs = mock_bp.call_args
            assert kwargs.get("warn_missing_fixes") is False


class TestBackportWithJiraParallelMode:
    """Tests for backport_with_jira with parallel_backport label."""

    def test_creates_prs_for_all_versions(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4", "backport/2025.3", "parallel_backport"]
        )

        call_count = {"n": 0}
        def mock_backport_fn(*args, **kwargs):
            call_count["n"] += 1
            return make_pr(number=40 + call_count["n"])

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", side_effect=mock_backport_fn) as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            # Should create backport for BOTH versions
            assert mock_bp.call_count == 2

    def test_parallel_no_remaining_labels(self, bp_module, make_pr, make_repo):
        """In parallel mode, no remaining backport labels should be attached."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["parallel_backport", "backport/2025.4", "backport/2025.3"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            for c in mock_bp.call_args_list:
                assert c[1].get("remaining_backport_labels") is None

    def test_parallel_skips_existing_prs(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["parallel_backport", "backport/2025.4", "backport/2025.3"]
        )
        existing_pr = make_pr(number=42)

        def side_effect(repo_obj, pr_num, version):
            if version == "2025.4":
                return existing_pr
            return None

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", side_effect=side_effect), \
             patch.object(bp_module, "backport", return_value=make_pr(number=43)) as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            # Should only create PR for 2025.3 (2025.4 already exists)
            assert mock_bp.call_count == 1
            call_args = mock_bp.call_args[0]
            assert call_args[2] == "2025.3"


class TestBackportGitCommandErrorOnPush:
    """Test the outer GitCommandError in backport() (lines 1293-1295)."""

    def test_git_push_error_returns_none(self, bp_module, make_pr, make_repo, make_commit):
        """When git push raises GitCommandError, backport() should return None."""
        from git import GitCommandError
        repo = make_repo()
        pr = make_pr(number=10, title="Fix bug")
        commit = make_commit(sha="abc123", parents=[MagicMock()])
        repo.get_commit.return_value = commit

        mock_local_repo = MagicMock()
        # Cherry-pick succeeds, but push fails
        mock_local_repo.git.push.side_effect = GitCommandError("push", "failed")

        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=mock_local_repo), \
             patch.object(bp_module, "create_pull_request") as mock_create:
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            result = bp_module.backport(repo, pr, "2025.4", ["abc123"], "branch-2025.4")
            assert result is None
            mock_create.assert_not_called()


class TestBackportWithJiraSubIssueFailure:
    """Test Jira sub-issue creation failure path in backport_with_jira (lines 1389-1396)."""

    def test_jira_failure_records_and_falls_back(self, bp_module, make_pr, make_repo):
        """When create_jira_sub_issue returns None, should record failure, call report_jira_failure, and fallback to parent key."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value=None) as mock_create, \
             patch.object(bp_module, "report_jira_failure") as mock_report, \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            mock_create.assert_called_once()
            mock_report.assert_called_once_with("SCYLLADB-123", "2025.4")
            # backport should still be called (with jira_failed=True)
            mock_bp.assert_called_once()
            _, kwargs = mock_bp.call_args
            assert kwargs.get("jira_failed") is True

    def test_no_jira_integration_uses_parent_key(self, bp_module, make_pr, make_repo):
        """When JIRA_USER/JIRA_API_TOKEN are empty, should use parent key as-is."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "JIRA_USER", ""), \
             patch.object(bp_module, "JIRA_API_TOKEN", ""), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue") as mock_create, \
             patch.object(bp_module, "backport", return_value=make_pr(number=42)) as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )

            # create_jira_sub_issue should NOT be called when no Jira integration
            mock_create.assert_not_called()
            mock_bp.assert_called_once()


class TestBackportWithJiraExistingPrWarnMissingFixes:
    """Test warn_missing_fixes on existing PR paths (lines 1421-1424, 1470-1472)."""

    def test_parallel_existing_pr_warns_missing_fixes(self, bp_module, make_pr, make_repo):
        """In parallel mode, existing PR should get warn_missing_fixes comment."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="No fixes reference",
            labels=["parallel_backport", "backport/2025.4"]
        )
        existing_pr = make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=existing_pr), \
             patch.object(bp_module, "backport") as mock_bp:
            bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], None, "scylladb/scylladb"
            )

            existing_pr.create_issue_comment.assert_called_once()
            mock_bp.assert_not_called()

    def test_chained_existing_pr_warns_missing_fixes(self, bp_module, make_pr, make_repo):
        """In chained mode, existing PR for highest version should get warn_missing_fixes comment."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="No fixes reference",
            labels=["backport/2025.4"]
        )
        existing_pr = make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=existing_pr), \
             patch.object(bp_module, "backport") as mock_bp:
            result = bp_module.backport_with_jira(
                repo, pr, ["2025.4"], ["abc123"], None, "scylladb/scylladb"
            )

            existing_pr.create_issue_comment.assert_called_once()
            assert result == existing_pr
            mock_bp.assert_not_called()


class TestBackportWithJiraPendingLabelException:
    """Test exception in pending label replacement (lines 1497-1498)."""

    def test_label_replacement_exception_continues(self, bp_module, make_pr, make_repo):
        """Exception replacing labels should be caught and logged, not crash."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4", "backport/2025.3"]
        )
        # Make remove_from_labels raise on the pending label replacement
        pr.remove_from_labels.side_effect = Exception("label API error")

        mock_backport_pr = make_pr(number=42)

        with patch.object(bp_module, "get_root_original_pr", return_value=pr), \
             patch.object(bp_module, "get_jira_user_from_github_user", return_value=None), \
             patch.object(bp_module, "create_jira_sub_issue", return_value="SCYLLADB-999"), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport", return_value=mock_backport_pr):
            # Should not raise even though label ops fail
            result = bp_module.backport_with_jira(
                repo, pr, ["2025.4", "2025.3"], ["abc123"], "SCYLLADB-123", "scylladb/scylladb"
            )
            assert result == mock_backport_pr
