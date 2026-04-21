"""
Unit tests for Jira API functions in auto-backport-jira.py.

Tests:
  - jira_api_request
  - get_jira_issue
  - find_jira_user_by_email
  - get_jira_user_from_github_user
  - assign_jira_issue
  - extract_jira_key_from_pr_body
  - extract_all_jira_keys_from_pr_body
  - has_fixes_reference
  - extract_project_from_jira_key
  - find_existing_sub_issue
  - is_subtask_issue
  - get_parent_key_if_subtask
  - create_jira_sub_issue
  - add_jira_comment
"""

import pytest
from unittest.mock import patch, MagicMock


# ==========================================================================
# Pure string parsing functions (no mocking needed)
# ==========================================================================

class TestExtractJiraKeyFromPrBody:
    def test_simple_fixes(self, bp_module):
        assert bp_module.extract_jira_key_from_pr_body("Fixes: SCYLLADB-123") == "SCYLLADB-123"

    def test_fixes_no_space(self, bp_module):
        assert bp_module.extract_jira_key_from_pr_body("Fixes:SCYLLADB-456") == "SCYLLADB-456"

    def test_fixes_with_url(self, bp_module):
        body = "Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-789"
        assert bp_module.extract_jira_key_from_pr_body(body) == "SCYLLADB-789"

    def test_lowercase_fixes(self, bp_module):
        assert bp_module.extract_jira_key_from_pr_body("fixes: PROJ-42") == "PROJ-42"

    def test_no_fixes_reference(self, bp_module):
        assert bp_module.extract_jira_key_from_pr_body("Just a regular PR body") is None

    def test_empty_body(self, bp_module):
        assert bp_module.extract_jira_key_from_pr_body("") is None

    def test_none_body(self, bp_module):
        assert bp_module.extract_jira_key_from_pr_body(None) is None

    def test_multiple_fixes_returns_first(self, bp_module):
        body = "Fixes: SCYLLADB-100\nFixes: SCYLLADB-200"
        assert bp_module.extract_jira_key_from_pr_body(body) == "SCYLLADB-100"

    def test_fixes_with_markdown_link(self, bp_module):
        body = "Fixes: [RELENG-358](https://scylladb.atlassian.net/browse/RELENG-358)"
        assert bp_module.extract_jira_key_from_pr_body(body) == "RELENG-358"

    def test_multiple_markdown_links_returns_first(self, bp_module):
        body = "Fixes: [RELENG-358](https://scylladb.atlassian.net/browse/RELENG-358)\nFixes: [RELENG-121](https://scylladb.atlassian.net/browse/RELENG-121)"
        assert bp_module.extract_jira_key_from_pr_body(body) == "RELENG-358"


class TestExtractAllJiraKeysFromPrBody:
    def test_multiple_fixes(self, bp_module):
        body = "Fixes: SCYLLADB-100\nSome text\nFixes: SCYLLADB-200\nFixes: PROJ-300"
        result = bp_module.extract_all_jira_keys_from_pr_body(body)
        assert result == ["SCYLLADB-100", "SCYLLADB-200", "PROJ-300"]

    def test_single_fix(self, bp_module):
        result = bp_module.extract_all_jira_keys_from_pr_body("Fixes: SCYLLADB-123")
        assert result == ["SCYLLADB-123"]

    def test_no_fixes(self, bp_module):
        result = bp_module.extract_all_jira_keys_from_pr_body("No issues here")
        assert result == []

    def test_empty_body(self, bp_module):
        assert bp_module.extract_all_jira_keys_from_pr_body("") == []

    def test_none_body(self, bp_module):
        assert bp_module.extract_all_jira_keys_from_pr_body(None) == []

    def test_with_urls(self, bp_module):
        body = "Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-100\nFixes: PROJ-200"
        result = bp_module.extract_all_jira_keys_from_pr_body(body)
        assert result == ["SCYLLADB-100", "PROJ-200"]

    def test_with_markdown_links(self, bp_module):
        body = "Fixes: [RELENG-358](https://scylladb.atlassian.net/browse/RELENG-358)\nFixes: [RELENG-121](https://scylladb.atlassian.net/browse/RELENG-121)\nFixes: [RELENG-396](https://scylladb.atlassian.net/browse/RELENG-396)"
        result = bp_module.extract_all_jira_keys_from_pr_body(body)
        assert result == ["RELENG-358", "RELENG-121", "RELENG-396"]

    def test_mixed_formats(self, bp_module):
        body = "Fixes: [RELENG-358](https://scylladb.atlassian.net/browse/RELENG-358)\nFixes: SCYLLADB-123\nFixes: https://scylladb.atlassian.net/browse/PROJ-456"
        result = bp_module.extract_all_jira_keys_from_pr_body(body)
        assert result == ["RELENG-358", "SCYLLADB-123", "PROJ-456"]


class TestHasFixesReference:
    def test_jira_key(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: SCYLLADB-123") is True

    def test_jira_url(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-123") is True

    def test_jira_markdown_link(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: [SCYLLADB-123](https://scylladb.atlassian.net/browse/SCYLLADB-123)") is True

    def test_github_issue_number(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: #123") is True

    def test_github_cross_repo(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: scylladb/scylladb#123") is True

    def test_github_issue_url(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: https://github.com/scylladb/scylladb/issues/123") is True

    def test_github_pull_url(self, bp_module):
        assert bp_module.has_fixes_reference("Fixes: https://github.com/scylladb/scylladb/pull/123") is True

    def test_no_reference(self, bp_module):
        assert bp_module.has_fixes_reference("Just a PR body") is False

    def test_empty_body(self, bp_module):
        assert bp_module.has_fixes_reference("") is False

    def test_none_body(self, bp_module):
        assert bp_module.has_fixes_reference(None) is False


class TestExtractProjectFromJiraKey:
    def test_standard_key(self, bp_module):
        assert bp_module.extract_project_from_jira_key("SCYLLADB-123") == "SCYLLADB"

    def test_short_project(self, bp_module):
        assert bp_module.extract_project_from_jira_key("AB-1") == "AB"


class TestIsSubtaskIssue:
    def test_is_subtask(self, bp_module):
        issue = {"fields": {"issuetype": {"subtask": True}}}
        assert bp_module.is_subtask_issue(issue) is True

    def test_is_not_subtask(self, bp_module):
        issue = {"fields": {"issuetype": {"subtask": False}}}
        assert bp_module.is_subtask_issue(issue) is False

    def test_missing_fields(self, bp_module):
        assert bp_module.is_subtask_issue({}) is False

    def test_none_issue(self, bp_module):
        assert bp_module.is_subtask_issue(None) is False


class TestGetParentKeyIfSubtask:
    def test_subtask_with_parent(self, bp_module):
        issue = {
            "fields": {
                "issuetype": {"subtask": True},
                "parent": {"key": "SCYLLADB-100"}
            }
        }
        assert bp_module.get_parent_key_if_subtask(issue) == "SCYLLADB-100"

    def test_not_subtask(self, bp_module):
        issue = {"fields": {"issuetype": {"subtask": False}, "parent": {"key": "SCYLLADB-100"}}}
        assert bp_module.get_parent_key_if_subtask(issue) is None

    def test_missing_parent(self, bp_module):
        issue = {"fields": {"issuetype": {"subtask": True}}}
        assert bp_module.get_parent_key_if_subtask(issue) is None


# ==========================================================================
# Jira API functions (require mocking HTTP calls)
# ==========================================================================

class TestJiraApiRequest:
    def test_get_request(self, bp_module):
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "SCYLLADB-123"}
        mock_response.text = '{"key": "SCYLLADB-123"}'
        mock_response.raise_for_status = MagicMock()

        with patch.object(bp_module.requests, "get", return_value=mock_response):
            result = bp_module.jira_api_request("GET", "issue/SCYLLADB-123")
            assert result == {"key": "SCYLLADB-123"}

    def test_post_request(self, bp_module):
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "SCYLLADB-456"}
        mock_response.text = '{"key": "SCYLLADB-456"}'
        mock_response.raise_for_status = MagicMock()

        with patch.object(bp_module.requests, "post", return_value=mock_response):
            result = bp_module.jira_api_request("POST", "issue", {"fields": {}})
            assert result == {"key": "SCYLLADB-456"}

    def test_put_request(self, bp_module):
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.text = ""
        mock_response.raise_for_status = MagicMock()

        with patch.object(bp_module.requests, "put", return_value=mock_response):
            result = bp_module.jira_api_request("PUT", "issue/SCYLLADB-123/assignee", {"accountId": "abc"})
            assert result == {}

    def test_unsupported_method(self, bp_module):
        result = bp_module.jira_api_request("DELETE", "issue/SCYLLADB-123")
        assert result is None

    def test_network_error(self, bp_module):
        with patch.object(bp_module.requests, "get", side_effect=bp_module.requests.exceptions.RequestException("fail")):
            result = bp_module.jira_api_request("GET", "issue/SCYLLADB-123")
            assert result is None

    def test_no_jira_credentials(self, bp_module):
        original_user = bp_module.JIRA_USER
        original_token = bp_module.JIRA_API_TOKEN
        try:
            bp_module.JIRA_USER = None
            bp_module.JIRA_API_TOKEN = None
            result = bp_module.jira_api_request("GET", "issue/SCYLLADB-123")
            assert result is None
        finally:
            bp_module.JIRA_USER = original_user
            bp_module.JIRA_API_TOKEN = original_token


class TestGetJiraIssue:
    def test_success(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value={"key": "SCYLLADB-123"}):
            result = bp_module.get_jira_issue("SCYLLADB-123")
            assert result == {"key": "SCYLLADB-123"}


class TestFindJiraUserByEmail:
    def test_found(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value=[{"accountId": "abc123"}]):
            result = bp_module.find_jira_user_by_email("user@scylladb.com")
            assert result == "abc123"

    def test_not_found(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value=[]):
            result = bp_module.find_jira_user_by_email("unknown@example.com")
            assert result is None

    def test_empty_email(self, bp_module):
        result = bp_module.find_jira_user_by_email("")
        assert result is None

    def test_no_jira_credentials(self, bp_module):
        original_user = bp_module.JIRA_USER
        try:
            bp_module.JIRA_USER = None
            result = bp_module.find_jira_user_by_email("user@scylladb.com")
            assert result is None
        finally:
            bp_module.JIRA_USER = original_user


class TestGetJiraUserFromGithubUser:
    def test_found_by_email(self, bp_module):
        github_user = MagicMock()
        github_user.email = "user@scylladb.com"
        github_user.login = "testuser"
        github_user.name = "Test User"

        with patch.object(bp_module, "find_jira_user_by_email", return_value="abc123"):
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result == "abc123"

    def test_found_by_constructed_email(self, bp_module):
        github_user = MagicMock()
        github_user.email = None
        github_user.login = "testuser"
        github_user.name = "Test User"

        # First call (public email) returns None, second (constructed) returns the id
        with patch.object(bp_module, "find_jira_user_by_email", side_effect=[None, "abc123"]):
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result == "abc123"

    def test_found_by_name_email(self, bp_module):
        github_user = MagicMock()
        github_user.email = None
        github_user.login = "testuser"
        github_user.name = "Test User"

        # None for constructed email (login@scylladb.com), found by name-based (test.user@scylladb.com)
        # Note: email=None means the first attempt (public email) is skipped entirely
        with patch.object(bp_module, "find_jira_user_by_email", side_effect=[None, "abc123"]):
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result == "abc123"

    def test_not_found(self, bp_module):
        github_user = MagicMock()
        github_user.email = None
        github_user.login = "testuser"
        github_user.name = "Test User"

        with patch.object(bp_module, "find_jira_user_by_email", return_value=None):
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result is None

    def test_none_user(self, bp_module):
        result = bp_module.get_jira_user_from_github_user(None)
        assert result is None


class TestAssignJiraIssue:
    def test_success(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value={}):
            result = bp_module.assign_jira_issue("SCYLLADB-123", "abc123")
            assert result is True

    def test_empty_issue_key(self, bp_module):
        assert bp_module.assign_jira_issue("", "abc123") is False

    def test_empty_account_id(self, bp_module):
        assert bp_module.assign_jira_issue("SCYLLADB-123", "") is False

    def test_api_returns_none(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value=None):
            result = bp_module.assign_jira_issue("SCYLLADB-123", "abc123")
            assert result is False


class TestFindExistingSubIssue:
    def test_found_via_parent_subtasks(self, bp_module):
        """Primary path: finds existing sub-issue via parent's subtasks field."""
        parent_issue = {
            "fields": {
                "subtasks": [
                    {"key": "SCYLLADB-999", "fields": {"summary": "[Backport 2025.4] - Fix bug"}}
                ]
            }
        }
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue):
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result == "SCYLLADB-999"

    def test_found_via_jql_fallback(self, bp_module):
        """Fallback path: parent subtasks field doesn't have it, but JQL finds it."""
        parent_issue = {"fields": {"subtasks": []}}
        jql_result = {
            "issues": [
                {"key": "SCYLLADB-999", "fields": {"summary": "[Backport 2025.4] - Fix bug"}}
            ]
        }
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "jira_api_request", return_value=jql_result):
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result == "SCYLLADB-999"

    def test_found_exact_match(self, bp_module):
        parent_issue = {"fields": {"subtasks": []}}
        result_data = {
            "issues": [
                {"key": "SCYLLADB-999", "fields": {"summary": "[Backport 2025.4] - Fix bug"}}
            ]
        }
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "jira_api_request", return_value=result_data):
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result == "SCYLLADB-999"

    def test_not_found(self, bp_module):
        parent_issue = {"fields": {"subtasks": []}}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "jira_api_request", return_value={"issues": []}):
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result is None

    def test_no_jira_credentials(self, bp_module):
        original_user = bp_module.JIRA_USER
        original_token = bp_module.JIRA_API_TOKEN
        try:
            bp_module.JIRA_USER = None
            bp_module.JIRA_API_TOKEN = None
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result is None
        finally:
            bp_module.JIRA_USER = original_user
            bp_module.JIRA_API_TOKEN = original_token

    def test_avoids_partial_version_match(self, bp_module):
        """Searching for 2025.4 should NOT match 2025.40."""
        parent_issue = {
            "fields": {
                "subtasks": [
                    {"key": "SCYLLADB-999", "fields": {"summary": "[Backport 2025.40] - Fix bug"}}
                ]
            }
        }
        jql_result = {
            "issues": [
                {"key": "SCYLLADB-999", "fields": {"summary": "[Backport 2025.40] - Fix bug"}}
            ]
        }
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "jira_api_request", return_value=jql_result):
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result is None

    def test_parent_subtasks_takes_priority_over_jql(self, bp_module):
        """If found in parent subtasks, JQL should not be called."""
        parent_issue = {
            "fields": {
                "subtasks": [
                    {"key": "SCYLLADB-888", "fields": {"summary": "[Backport 2025.4] - Fix bug"}}
                ]
            }
        }
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "jira_api_request") as mock_jql:
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result == "SCYLLADB-888"
            mock_jql.assert_not_called()


class TestCreateJiraSubIssue:
    def test_create_new_subtask(self, bp_module):
        parent_issue = {"fields": {"issuetype": {"subtask": False}}}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "jira_api_request", return_value={"key": "SCYLLADB-999"}):
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug")
            assert result == "SCYLLADB-999"

    def test_existing_subtask_returned(self, bp_module):
        parent_issue = {"fields": {"issuetype": {"subtask": False}}}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value="SCYLLADB-888"):
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug")
            assert result == "SCYLLADB-888"

    def test_parent_is_subtask_uses_grandparent(self, bp_module):
        """When parent is already a sub-task, create under grandparent."""
        parent_issue = {
            "fields": {
                "issuetype": {"subtask": True},
                "parent": {"key": "SCYLLADB-50"}
            }
        }
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "jira_api_request", return_value={"key": "SCYLLADB-999"}) as mock_api:
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug")
            assert result == "SCYLLADB-999"
            # Verify it was created under the grandparent
            call_data = mock_api.call_args[0][2]
            assert call_data["fields"]["parent"]["key"] == "SCYLLADB-50"

    def test_parent_fetch_fails(self, bp_module):
        with patch.object(bp_module, "get_jira_issue", return_value=None):
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug")
            assert result is None

    def test_with_assignee(self, bp_module):
        parent_issue = {"fields": {"issuetype": {"subtask": False}}}
        created = {"key": "SCYLLADB-999"}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "jira_api_request", return_value=created) as mock_api:
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug", "user-account-id")
            assert result == "SCYLLADB-999"
            call_data = mock_api.call_args[0][2]
            assert call_data["fields"]["assignee"]["accountId"] == "user-account-id"

    def test_existing_subtask_gets_assigned(self, bp_module):
        """If subtask exists and assignee provided, should assign."""
        parent_issue = {"fields": {"issuetype": {"subtask": False}}}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value="SCYLLADB-888"), \
             patch.object(bp_module, "assign_jira_issue") as mock_assign:
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug", "user-account-id")
            assert result == "SCYLLADB-888"
            mock_assign.assert_called_once_with("SCYLLADB-888", "user-account-id")


class TestAddJiraComment:
    def test_simple_comment(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value={}) as mock_api:
            result = bp_module.add_jira_comment("SCYLLADB-123", "Test comment")
            assert result is True
            mock_api.assert_called_once()

    def test_comment_with_link(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value={}) as mock_api:
            result = bp_module.add_jira_comment("SCYLLADB-123", "Failed [View run|https://example.com]")
            assert result is True

    def test_api_failure(self, bp_module):
        with patch.object(bp_module, "jira_api_request", return_value=None):
            result = bp_module.add_jira_comment("SCYLLADB-123", "Test comment")
            assert result is False


class TestReportJiraFailure:
    def test_success(self, bp_module):
        """Line 745-751: report_jira_failure adds comment to Jira."""
        with patch.object(bp_module, "add_jira_comment", return_value=True) as mock_comment:
            bp_module.report_jira_failure("SCYLLADB-100", "2025.4")
            mock_comment.assert_called_once()
            comment_text = mock_comment.call_args[0][1]
            assert "2025.4" in comment_text
            assert "Failed" in comment_text

    def test_failure(self, bp_module):
        """Line 750-751: When add_jira_comment fails, logs error."""
        with patch.object(bp_module, "add_jira_comment", return_value=False):
            # Should not raise; just logs the error
            bp_module.report_jira_failure("SCYLLADB-100", "2025.4")


class TestFindJiraUserByEmailErrors:
    def test_exception_returns_none(self, bp_module):
        """Line 344-345: Exception in find_jira_user_by_email returns None."""
        with patch.object(bp_module, "jira_api_request", side_effect=Exception("boom")):
            result = bp_module.find_jira_user_by_email("user@scylladb.com")
            assert result is None

    def test_no_results_returns_none(self, bp_module):
        """Line 343: Warning logged when no Jira user found for valid email."""
        with patch.object(bp_module, "jira_api_request", return_value=[]):
            result = bp_module.find_jira_user_by_email("unknown@example.com")
            assert result is None


class TestGetJiraUserFromGithubUserErrors:
    def test_exception_returns_none(self, bp_module):
        """Line 393-394: Exception in get_jira_user_from_github_user returns None."""
        github_user = MagicMock()
        github_user.email = "user@scylladb.com"
        with patch.object(bp_module, "find_jira_user_by_email", side_effect=Exception("boom")):
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result is None

    def test_found_by_constructed_email_returns_early(self, bp_module):
        """Line 380: Returns early when constructed email succeeds (without trying name)."""
        github_user = MagicMock()
        github_user.email = None
        github_user.login = "testuser"
        github_user.name = "Test User"
        # Constructed email (testuser@scylladb.com) succeeds
        with patch.object(bp_module, "find_jira_user_by_email", return_value="acc123") as mock_find:
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result == "acc123"
            # Should only be called once (constructed email), not twice (no name fallback)
            mock_find.assert_called_once_with("testuser@scylladb.com")

    def test_all_lookups_fail_logs_warning(self, bp_module):
        """Line 392: Warning when all email lookups fail."""
        github_user = MagicMock()
        github_user.email = "user@test.com"
        github_user.login = "testuser"
        github_user.name = "Test User"
        with patch.object(bp_module, "find_jira_user_by_email", return_value=None):
            result = bp_module.get_jira_user_from_github_user(github_user)
            assert result is None


class TestAssignJiraIssueErrors:
    def test_exception_returns_false(self, bp_module):
        """Line 419-420: Exception in assign_jira_issue returns False."""
        with patch.object(bp_module, "jira_api_request", side_effect=Exception("boom")):
            result = bp_module.assign_jira_issue("SCYLLADB-123", "abc123")
            assert result is False


class TestFindExistingSubIssueErrors:
    def test_exception_returns_none(self, bp_module):
        """Exception in find_existing_sub_issue returns None."""
        with patch.object(bp_module, "get_jira_issue", side_effect=Exception("boom")):
            result = bp_module.find_existing_sub_issue("SCYLLADB-100", "2025.4")
            assert result is None


class TestGetParentKeyIfSubtaskErrors:
    def test_exception_returns_none(self, bp_module):
        """Line 529-530: Exception in get_parent_key_if_subtask returns None."""
        # Pass something that will cause an exception during processing
        result = bp_module.get_parent_key_if_subtask("not-a-dict")
        assert result is None


class TestCreateJiraSubIssueFailure:
    def test_api_returns_no_key(self, bp_module):
        """Line 682-683: jira_api_request returns result without 'key'."""
        parent_issue = {"fields": {"issuetype": {"subtask": False}}}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "jira_api_request", return_value={"error": "something"}):
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug")
            assert result is None

    def test_api_returns_none(self, bp_module):
        """Line 682-683: jira_api_request returns None."""
        parent_issue = {"fields": {"issuetype": {"subtask": False}}}
        with patch.object(bp_module, "get_jira_issue", return_value=parent_issue), \
             patch.object(bp_module, "find_existing_sub_issue", return_value=None), \
             patch.object(bp_module, "jira_api_request", return_value=None):
            result = bp_module.create_jira_sub_issue("SCYLLADB-100", "2025.4", "Fix bug")
            assert result is None
