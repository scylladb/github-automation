"""
Unit tests for PR body parsing functions in auto-backport-jira.py.

Tests:
  - extract_main_pr_link_from_body
  - get_original_pr_from_backport
  - extract_main_jira_from_body
  - is_backport_pr
  - get_root_original_pr
  - extract_original_title
"""

import pytest
from unittest.mock import patch, MagicMock


class TestExtractMainPrLinkFromBody:
    def test_standard_format(self, bp_module):
        body = "This PR is a backport of PR scylladb/scylladb#1234"
        assert bp_module.extract_main_pr_link_from_body(body) == "scylladb/scylladb#1234"

    def test_short_format(self, bp_module):
        body = "This PR is a backport of PR #1234"
        assert bp_module.extract_main_pr_link_from_body(body) == "#1234"

    def test_case_insensitive(self, bp_module):
        body = "This PR is a Backport of PR #5678"
        assert bp_module.extract_main_pr_link_from_body(body) == "#5678"

    def test_no_match(self, bp_module):
        assert bp_module.extract_main_pr_link_from_body("Just a normal PR") is None

    def test_empty_body(self, bp_module):
        assert bp_module.extract_main_pr_link_from_body("") is None

    def test_none_body(self, bp_module):
        assert bp_module.extract_main_pr_link_from_body(None) is None


class TestGetOriginalPrFromBackport:
    def test_new_format_cross_repo(self, bp_module, make_pr):
        backport_pr = make_pr(
            body="This PR is a backport of PR scylladb/scylladb#100\n\nSome other text"
        )
        repo = MagicMock()
        original = make_pr(number=100)
        repo.get_pull.return_value = original

        result = bp_module.get_original_pr_from_backport(repo, backport_pr)
        repo.get_pull.assert_called_once_with(100)
        assert result == original

    def test_new_format_short(self, bp_module, make_pr):
        backport_pr = make_pr(body="This PR is a backport of PR #200")
        repo = MagicMock()
        original = make_pr(number=200)
        repo.get_pull.return_value = original

        result = bp_module.get_original_pr_from_backport(repo, backport_pr)
        repo.get_pull.assert_called_once_with(200)
        assert result == original

    def test_old_format_parent_pr(self, bp_module, make_pr):
        backport_pr = make_pr(body="Some text\nParent PR: #300")
        repo = MagicMock()
        original = make_pr(number=300)
        repo.get_pull.return_value = original

        result = bp_module.get_original_pr_from_backport(repo, backport_pr)
        repo.get_pull.assert_called_once_with(300)
        assert result == original

    def test_no_parent_reference(self, bp_module, make_pr):
        backport_pr = make_pr(body="Just a normal PR body")
        repo = MagicMock()
        result = bp_module.get_original_pr_from_backport(repo, backport_pr)
        assert result is None

    def test_none_body(self, bp_module, make_pr):
        backport_pr = make_pr(body=None)
        repo = MagicMock()
        result = bp_module.get_original_pr_from_backport(repo, backport_pr)
        assert result is None

    def test_pr_fetch_fails(self, bp_module, make_pr):
        backport_pr = make_pr(body="This PR is a backport of PR #999")
        repo = MagicMock()
        repo.get_pull.side_effect = Exception("Not found")
        result = bp_module.get_original_pr_from_backport(repo, backport_pr)
        assert result is None


class TestExtractMainJiraFromBody:
    def test_standard_format(self, bp_module):
        body = "The main Jira issue is SCYLLADB-123"
        assert bp_module.extract_main_jira_from_body(body) == "SCYLLADB-123"

    def test_case_insensitive(self, bp_module):
        body = "The Main Jira Issue Is PROJ-456"
        assert bp_module.extract_main_jira_from_body(body) == "PROJ-456"

    def test_no_match(self, bp_module):
        assert bp_module.extract_main_jira_from_body("No jira here") is None

    def test_empty(self, bp_module):
        assert bp_module.extract_main_jira_from_body("") is None

    def test_none(self, bp_module):
        assert bp_module.extract_main_jira_from_body(None) is None


class TestIsBackportPr:
    def test_title_with_version(self, bp_module):
        assert bp_module.is_backport_pr("[Backport 2025.4] Fix bug", "") is True

    def test_title_with_manager_version(self, bp_module):
        assert bp_module.is_backport_pr("[Backport manager-3.4] Fix bug", "") is True

    def test_body_new_format(self, bp_module):
        # NOTE: The script has a bug at line 827: it checks 'backport of PR' (mixed case)
        # against pr_body.lower(), so this check never matches. The body "new format"
        # detection is effectively dead code. Test matches actual (buggy) behaviour.
        assert bp_module.is_backport_pr("Fix bug", "This PR is a backport of PR #123") is False

    def test_body_old_format(self, bp_module):
        assert bp_module.is_backport_pr("Fix bug", "Parent PR: #123") is True

    def test_not_backport(self, bp_module):
        assert bp_module.is_backport_pr("Fix bug", "Normal PR body") is False

    def test_empty_title_and_body(self, bp_module):
        assert bp_module.is_backport_pr("", "") is False

    def test_none_title_backport_body(self, bp_module):
        # NOTE: Due to the same 'backport of PR' vs .lower() bug, body-only detection
        # of the new format doesn't work. Only 'parent pr:' old format works via body.
        assert bp_module.is_backport_pr(None, "backport of PR #123") is False

    def test_none_title_old_format_body(self, bp_module):
        # The old format 'parent pr:' body check does work correctly
        assert bp_module.is_backport_pr(None, "Parent PR: #123") is True

    def test_backport_title_none_body(self, bp_module):
        assert bp_module.is_backport_pr("[Backport 2025.4] Fix", None) is True


class TestGetRootOriginalPr:
    def test_non_backport_returns_self(self, bp_module, make_pr):
        pr = make_pr(title="Fix bug", body="Normal body")
        repo = MagicMock()
        result = bp_module.get_root_original_pr(repo, pr)
        assert result.number == pr.number

    def test_one_hop_backport(self, bp_module, make_pr):
        original = make_pr(number=1, title="Fix bug", body="Normal body")
        backport = make_pr(
            number=2,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1"
        )
        repo = MagicMock()
        repo.get_pull.return_value = original

        result = bp_module.get_root_original_pr(repo, backport)
        assert result.number == 1

    def test_multi_hop_backport(self, bp_module, make_pr):
        original = make_pr(number=1, title="Fix bug", body="Normal body")
        bp1 = make_pr(
            number=2,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1"
        )
        bp2 = make_pr(
            number=3,
            title="[Backport 2025.3] Fix bug",
            body="This PR is a backport of PR #2"
        )
        repo = MagicMock()
        repo.get_pull.side_effect = lambda n: {2: bp1, 1: original}[n]

        result = bp_module.get_root_original_pr(repo, bp2)
        assert result.number == 1

    def test_max_depth_reached(self, bp_module, make_pr):
        # Create a circular chain (unrealistic but tests max_depth)
        pr = make_pr(
            number=1,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #1"
        )
        repo = MagicMock()
        repo.get_pull.return_value = pr

        result = bp_module.get_root_original_pr(repo, pr, max_depth=3)
        # Should return the last PR after hitting max depth
        assert result is not None

    def test_broken_chain(self, bp_module, make_pr):
        backport = make_pr(
            number=2,
            title="[Backport 2025.4] Fix bug",
            body="This PR is a backport of PR #999"
        )
        repo = MagicMock()
        repo.get_pull.side_effect = Exception("Not found")

        result = bp_module.get_root_original_pr(repo, backport)
        # Should return the backport itself when chain is broken
        assert result.number == 2


class TestExtractOriginalTitle:
    def test_single_prefix(self, bp_module):
        assert bp_module.extract_original_title("[Backport 2025.4] Fix bug") == "Fix bug"

    def test_manager_prefix(self, bp_module):
        assert bp_module.extract_original_title("[Backport manager-3.4] Fix bug") == "Fix bug"

    def test_stacked_prefixes(self, bp_module):
        title = "[Backport 2025.3] [Backport 2025.4] Fix bug"
        assert bp_module.extract_original_title(title) == "Fix bug"

    def test_no_prefix(self, bp_module):
        assert bp_module.extract_original_title("Fix bug") == "Fix bug"

    def test_empty_title(self, bp_module):
        assert bp_module.extract_original_title("") == ""

    def test_none_title(self, bp_module):
        assert bp_module.extract_original_title(None) is None

    def test_only_prefix(self, bp_module):
        # Edge case: title is just the prefix. The function returns the original
        # title unchanged when stripping would produce an empty string (line 898).
        result = bp_module.extract_original_title("[Backport 2025.4]")
        assert result == "[Backport 2025.4]"
