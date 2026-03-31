"""
Unit tests for milestone helpers in auto-backport-jira.py.

Tests:
  - parse_version_triplet
  - find_master_version_from_file
  - find_latest_patch_for_branch
  - resolve_master_milestone_title
  - resolve_backport_milestone_title
  - find_or_create_milestone
  - set_pr_milestone
"""

import pytest
from unittest.mock import patch, MagicMock


class TestParseVersionTriplet:
    def test_valid_version(self, bp_module):
        assert bp_module.parse_version_triplet("2026.2.0") == (2026, 2, 0)

    def test_with_nonzero_patch(self, bp_module):
        assert bp_module.parse_version_triplet("2025.4.3") == (2025, 4, 3)

    def test_invalid_format(self, bp_module):
        assert bp_module.parse_version_triplet("2025.4") is None

    def test_non_numeric(self, bp_module):
        assert bp_module.parse_version_triplet("abc.def.ghi") is None

    def test_empty_string(self, bp_module):
        assert bp_module.parse_version_triplet("") is None


class TestFindMasterVersionFromFile:
    def test_success(self, bp_module):
        mock_response = MagicMock()
        mock_response.text = "#!/bin/bash\nVERSION=2026.2.0-dev\n"
        mock_response.raise_for_status = MagicMock()

        with patch.object(bp_module.requests, "get", return_value=mock_response):
            result = bp_module.find_master_version_from_file()
            assert result == "2026.2.0"

    def test_no_version_line(self, bp_module):
        mock_response = MagicMock()
        mock_response.text = "#!/bin/bash\nSOMETHING_ELSE=foo\n"
        mock_response.raise_for_status = MagicMock()

        with patch.object(bp_module.requests, "get", return_value=mock_response):
            result = bp_module.find_master_version_from_file()
            assert result is None

    def test_network_error(self, bp_module):
        with patch.object(bp_module.requests, "get", side_effect=Exception("network error")):
            result = bp_module.find_master_version_from_file()
            assert result is None


class TestFindLatestPatchForBranch:
    def test_release_tags(self, bp_module, make_tag):
        repo = MagicMock()
        repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0"),
            make_tag("scylla-2025.4.1"),
            make_tag("scylla-2025.4.2"),
            make_tag("unrelated-tag"),
        ]
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        assert result == 2

    def test_candidate_tags_ignored_for_patch(self, bp_module, make_tag):
        repo = MagicMock()
        repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0"),
            make_tag("scylla-2025.4.1-candidate-20250101"),
            make_tag("scylla-2025.4.1"),
        ]
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        assert result == 1

    def test_only_rc_tags(self, bp_module, make_tag):
        repo = MagicMock()
        repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0-rc1"),
            make_tag("scylla-2025.4.0-rc2"),
        ]
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        # Returns -1 when only rc tags exist
        assert result == -1

    def test_no_matching_tags(self, bp_module, make_tag):
        repo = MagicMock()
        repo.get_tags.return_value = [
            make_tag("scylla-2024.1.0"),
        ]
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        assert result is None

    def test_empty_tags(self, bp_module):
        repo = MagicMock()
        repo.get_tags.return_value = []
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        assert result is None

    def test_release_and_rc_tags_mixed(self, bp_module, make_tag):
        """When both release and rc tags exist, release tags take priority."""
        repo = MagicMock()
        repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0"),
            make_tag("scylla-2025.4.1-rc1"),
        ]
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        assert result == 0


class TestResolveBackportMilestoneTitle:
    def test_success(self, bp_module, make_tag):
        mock_repo = MagicMock()
        mock_repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0"),
            make_tag("scylla-2025.4.1"),
        ]
        with patch.object(bp_module, "get_scylladb_repo", return_value=mock_repo):
            result = bp_module.resolve_backport_milestone_title("2025.4")
            assert result == "2025.4.2"

    def test_manager_version_returns_none(self, bp_module):
        result = bp_module.resolve_backport_milestone_title("manager-3.4")
        assert result is None

    def test_empty_version_returns_none(self, bp_module):
        result = bp_module.resolve_backport_milestone_title("")
        assert result is None

    def test_no_tags_returns_none(self, bp_module):
        mock_repo = MagicMock()
        mock_repo.get_tags.return_value = []
        with patch.object(bp_module, "get_scylladb_repo", return_value=mock_repo):
            result = bp_module.resolve_backport_milestone_title("2025.4")
            assert result is None

    def test_only_rc_tags(self, bp_module, make_tag):
        mock_repo = MagicMock()
        mock_repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0-rc1"),
        ]
        with patch.object(bp_module, "get_scylladb_repo", return_value=mock_repo):
            result = bp_module.resolve_backport_milestone_title("2025.4")
            assert result == "2025.4.0"


class TestResolveMasterMilestoneTitle:
    def test_success(self, bp_module):
        with patch.object(bp_module, "find_master_version_from_file", return_value="2026.2.0"):
            result = bp_module.resolve_master_milestone_title()
            assert result == "2026.2.0"

    def test_no_version(self, bp_module):
        with patch.object(bp_module, "find_master_version_from_file", return_value=None):
            result = bp_module.resolve_master_milestone_title()
            assert result is None


class TestFindOrCreateMilestone:
    def test_finds_existing_milestone(self, bp_module, make_milestone):
        repo = MagicMock()
        m = make_milestone("2025.4.2")
        repo.get_milestones.return_value = [m]
        result = bp_module.find_or_create_milestone(repo, "2025.4.2")
        assert result == m
        repo.create_milestone.assert_not_called()

    def test_creates_milestone_when_not_found(self, bp_module):
        repo = MagicMock()
        repo.get_milestones.return_value = []
        new_m = MagicMock()
        repo.create_milestone.return_value = new_m
        result = bp_module.find_or_create_milestone(repo, "2025.4.2")
        repo.create_milestone.assert_called_once_with(title="2025.4.2")
        assert result == new_m

    def test_create_milestone_fails(self, bp_module):
        from github import GithubException
        repo = MagicMock()
        repo.get_milestones.return_value = []
        repo.create_milestone.side_effect = GithubException(422, "error", None)
        result = bp_module.find_or_create_milestone(repo, "2025.4.2")
        assert result is None


class TestSetPrMilestone:
    def test_sets_milestone(self, bp_module, make_pr, make_milestone):
        pr = make_pr()
        m = make_milestone("2025.4.2")
        with patch.object(bp_module, "find_or_create_milestone", return_value=m):
            result = bp_module.set_pr_milestone(pr, "2025.4.2")
            assert result is True
            pr.as_issue().edit.assert_called_once_with(milestone=m)

    def test_milestone_already_set(self, bp_module, make_pr):
        pr = make_pr()
        pr.milestone = MagicMock()
        pr.milestone.title = "2025.4.2"
        result = bp_module.set_pr_milestone(pr, "2025.4.2")
        assert result is True

    def test_empty_milestone_title(self, bp_module, make_pr):
        pr = make_pr()
        result = bp_module.set_pr_milestone(pr, None)
        assert result is False

    def test_milestone_not_found(self, bp_module, make_pr):
        pr = make_pr()
        with patch.object(bp_module, "find_or_create_milestone", return_value=None):
            result = bp_module.set_pr_milestone(pr, "2025.4.2")
            assert result is False

    def test_github_exception_returns_false(self, bp_module, make_pr, make_milestone):
        """Line 277-279: GithubException when setting milestone returns False."""
        from github import GithubException
        pr = make_pr()
        m = make_milestone("2025.4.2")
        pr.as_issue.return_value.edit.side_effect = GithubException(422, "error")
        with patch.object(bp_module, "find_or_create_milestone", return_value=m):
            result = bp_module.set_pr_milestone(pr, "2025.4.2")
            assert result is False


class TestResolveMasterMilestoneTitleErrors:
    def test_exception_returns_none(self, bp_module):
        """Line 232-234: Exception in resolve_master_milestone_title returns None."""
        with patch.object(bp_module, "find_master_version_from_file", side_effect=Exception("boom")):
            result = bp_module.resolve_master_milestone_title()
            assert result is None


class TestResolveBackportMilestoneTitleErrors:
    def test_exception_returns_none(self, bp_module):
        """Line 247-249: Exception in resolve_backport_milestone_title returns None."""
        with patch.object(bp_module, "get_scylladb_repo", side_effect=Exception("API down")):
            result = bp_module.resolve_backport_milestone_title("2025.4")
            assert result is None


class TestGetScylladbRepo:
    def test_caches_repo(self, bp_module):
        """Line 139-142: get_scylladb_repo initialises lazily and caches."""
        original_cache = bp_module._scylladb_repo_cache
        try:
            bp_module._scylladb_repo_cache = None
            mock_repo = MagicMock()
            with patch.object(bp_module, "Github") as mock_gh:
                mock_gh.return_value.get_repo.return_value = mock_repo
                result = bp_module.get_scylladb_repo()
                assert result is mock_repo
                # Second call should use cache
                result2 = bp_module.get_scylladb_repo()
                assert result2 is mock_repo
                mock_gh.return_value.get_repo.assert_called_once()
        finally:
            bp_module._scylladb_repo_cache = original_cache


class TestFindLatestPatchRcOnly:
    def test_rc_only_returns_minus_one(self, bp_module, make_tag):
        """Line 216: RC tags exist but no GA release -> returns -1."""
        repo = MagicMock()
        repo.get_tags.return_value = [
            make_tag("scylla-2025.4.0-rc1"),
            make_tag("scylla-2025.4.0-rc2"),
        ]
        result = bp_module.find_latest_patch_for_branch(repo, "2025.4")
        assert result == -1
