"""
Unit tests for main() entry point and argument parsing in auto-backport-jira.py.

Tests:
  - parse_args
  - main() dispatching to different modes
  - is_pull_request / is_chain_backport
"""

import sys
import pytest
from unittest.mock import patch, MagicMock


class TestParseArgs:
    def test_push_mode_args(self, bp_module):
        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/master",
            "--commits", "abc123..def456"
        ]
        with patch.object(sys, "argv", test_args):
            args = bp_module.parse_args()
            assert args.repo == "scylladb/scylladb"
            assert args.base_branch == "refs/heads/master"
            assert args.commits == "abc123..def456"

    def test_labeled_mode_args(self, bp_module):
        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/master",
            "--pull-request", "123",
            "--head-commit", "abc123",
            "--label", "backport/2025.4"
        ]
        with patch.object(sys, "argv", test_args):
            args = bp_module.parse_args()
            assert args.pull_request == 123
            assert args.head_commit == "abc123"
            assert args.label == "backport/2025.4"

    def test_chain_backport_args(self, bp_module):
        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/branch-2025.4",
            "--chain-backport",
            "--merged-pr", "456"
        ]
        with patch.object(sys, "argv", test_args):
            args = bp_module.parse_args()
            assert args.chain_backport is True
            assert args.merged_pr == 456

    def test_promoted_to_branch_args(self, bp_module):
        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--commits", "abc..def",
            "--promoted-to-branch", "branch-2025.4"
        ]
        with patch.object(sys, "argv", test_args):
            args = bp_module.parse_args()
            assert args.promoted_to_branch == "branch-2025.4"
            assert args.commits == "abc..def"

    def test_default_base_branch(self, bp_module):
        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--commits", "abc..def"
        ]
        with patch.object(sys, "argv", test_args):
            args = bp_module.parse_args()
            assert args.base_branch == "refs/heads/next"


class TestIsPullRequest:
    def test_true_when_flag_present(self, bp_module):
        with patch.object(sys, "argv", ["prog", "--pull-request", "123"]):
            assert bp_module.is_pull_request() is True

    def test_false_when_flag_absent(self, bp_module):
        with patch.object(sys, "argv", ["prog", "--commits", "abc..def"]):
            assert bp_module.is_pull_request() is False


class TestIsChainBackport:
    def test_true_when_flag_present(self, bp_module):
        with patch.object(sys, "argv", ["prog", "--chain-backport"]):
            assert bp_module.is_chain_backport() is True

    def test_false_when_flag_absent(self, bp_module):
        with patch.object(sys, "argv", ["prog", "--commits", "abc..def"]):
            assert bp_module.is_chain_backport() is False


class TestMainDispatch:
    """Test that main() correctly dispatches to the right code path."""

    def test_promoted_to_branch_mode(self, bp_module, make_repo):
        repo = make_repo()
        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/branch-2025.4",
            "--commits", "abc..def",
            "--promoted-to-branch", "branch-2025.4"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "process_branch_push") as mock_push:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_push.assert_called_once_with(repo, "abc..def", "branch-2025.4", "scylladb/scylladb")

    def test_chain_backport_mode(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        merged_pr = make_pr(number=456, merged=True)
        repo.get_pull.return_value = merged_pr

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/branch-2025.4",
            "--chain-backport",
            "--merged-pr", "456"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "process_chain_backport") as mock_chain:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_chain.assert_called_once_with(repo, merged_pr, "scylladb/scylladb")

    def test_chain_backport_skips_unmerged_pr(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        unmerged_pr = make_pr(number=456, merged=False)
        repo.get_pull.return_value = unmerged_pr

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/branch-2025.4",
            "--chain-backport",
            "--merged-pr", "456"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "process_chain_backport") as mock_chain:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_chain.assert_not_called()

    def test_push_mode_processes_prs(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "get_pr_commits", return_value=["abc123"]), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "resolve_master_milestone_title", return_value="2026.2.0"), \
             patch.object(bp_module, "set_pr_milestone"), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_called_once()

    def test_push_mode_skips_pr_without_promoted_label(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        pr = make_pr(
            number=10,
            labels=["backport/2025.4"]  # Missing promoted-to-master
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_not_called()

    def test_push_mode_skips_pr_without_backport_label(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        pr = make_pr(
            number=10,
            labels=["promoted-to-master"]  # No backport labels
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_not_called()

    def test_labeled_mode(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(
            number=123,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )
        repo.get_pull.return_value = pr

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--pull-request", "123",
            "--head-commit", "abc123",
            "--label", "backport/2025.4"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "get_pr_commits", return_value=["abc123"]), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_called_once()

    def test_push_mode_sets_milestone_for_scylladb(self, bp_module, make_pr, make_repo, make_commit):
        """Push to master for scylladb/scylladb should set milestone."""
        repo = make_repo()
        pr = make_pr(number=10, labels=["promoted-to-master"])

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "resolve_master_milestone_title", return_value="2026.2.0"), \
             patch.object(bp_module, "set_pr_milestone") as mock_ms:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_ms.assert_called_once_with(pr, "2026.2.0")

    def test_push_mode_skips_existing_backport(self, bp_module, make_pr, make_repo, make_commit):
        """If backport PR already exists for highest version, skip."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )
        existing = make_pr(number=42)

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "find_existing_backport_pr", return_value=existing), \
             patch.object(bp_module, "resolve_master_milestone_title", return_value=None), \
             patch.object(bp_module, "set_pr_milestone"), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_not_called()


class TestMainClosesReferenceInCommits:
    """Test Closes #X parsing in main() commits path (lines 1987-1996)."""

    def test_closes_reference_adds_pr_to_list(self, bp_module, make_pr, make_repo, make_commit):
        """Commit message with 'Closes #X' should add the closed PR to processing."""
        repo = make_repo()
        closed_pr = make_pr(
            number=99,
            body="Fixes: SCYLLADB-123",
            state="closed",
            labels=["promoted-to-master", "backport/2025.4"]
        )
        repo.get_pull.return_value = closed_pr

        commit = make_commit(sha="abc123", message="Fix bug\n\nCloses #99")
        commit.get_pulls.return_value = []  # Not returned by get_pulls

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "get_pr_commits", return_value=["abc123"]), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "resolve_master_milestone_title", return_value="2026.2.0"), \
             patch.object(bp_module, "set_pr_milestone"), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_called_once()

    def test_closes_reference_exception_caught(self, bp_module, make_pr, make_repo, make_commit):
        """Exception fetching PR from Closes reference should be caught (lines 1995-1996)."""
        repo = make_repo()
        repo.get_pull.side_effect = Exception("API error")

        commit = make_commit(sha="abc123", message="Fix bug\n\nCloses #99")
        commit.get_pulls.return_value = []

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            # Should not raise
            bp_module.main()
            mock_bpj.assert_not_called()


class TestMainLabelMismatch:
    """Test label mismatch skip in main() (lines 2019-2021)."""

    def test_label_not_in_pr_labels_skips(self, bp_module, make_pr, make_repo, make_commit):
        """When --label is specified but not in PR's labels, should skip."""
        repo = make_repo()
        pr = make_pr(
            number=123,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )
        repo.get_pull.return_value = pr

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--pull-request", "123",
            "--head-commit", "abc123",
            "--label", "backport/2025.3"  # Not in PR's labels
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_not_called()


class TestMainNoCommitsFound:
    """Test no commits found path in main() (lines 2041-2045)."""

    def test_no_commits_skips_backport(self, bp_module, make_pr, make_repo, make_commit):
        """When get_pr_commits returns empty list, should skip backport."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="Fixes: SCYLLADB-123",
            labels=["promoted-to-master", "backport/2025.4"]
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "get_pr_commits", return_value=[]), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "resolve_master_milestone_title", return_value=None), \
             patch.object(bp_module, "set_pr_milestone"), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_not_called()


class TestMainNoJiraIssueWarning:
    """Test no Jira issue found warning in main() (line 2052)."""

    def test_no_jira_key_still_calls_backport(self, bp_module, make_pr, make_repo, make_commit):
        """When no Jira key found, should still call backport_with_jira with None key."""
        repo = make_repo()
        pr = make_pr(
            number=10,
            body="No jira reference here",
            labels=["promoted-to-master", "backport/2025.4"]
        )

        commit = make_commit(sha="abc123")
        commit.get_pulls.return_value = [pr]

        comparison = MagicMock()
        comparison.commits = [commit]
        repo.compare.return_value = comparison

        test_args = [
            "auto-backport-jira.py",
            "--repo", "scylladb/scylladb",
            "--base-branch", "refs/heads/next",
            "--commits", "before..after"
        ]

        with patch.object(sys, "argv", test_args), \
             patch.object(bp_module, "Github") as mock_gh, \
             patch.object(bp_module, "get_pr_commits", return_value=["abc123"]), \
             patch.object(bp_module, "find_existing_backport_pr", return_value=None), \
             patch.object(bp_module, "resolve_master_milestone_title", return_value=None), \
             patch.object(bp_module, "set_pr_milestone"), \
             patch.object(bp_module, "backport_with_jira") as mock_bpj:
            mock_gh.return_value.get_repo.return_value = repo
            bp_module.main()
            mock_bpj.assert_called_once()
            call_args = mock_bpj.call_args[0]
            assert call_args[4] is None  # main_jira_key is None
