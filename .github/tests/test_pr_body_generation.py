"""
Unit tests for PR body generation functions in auto-backport-jira.py.

Tests:
  - strip_cherry_pick_info
  - replace_fixes_in_body
  - generate_backport_pr_body
"""

import pytest


class TestStripCherryPickInfo:
    def test_removes_cherry_pick_marker(self, bp_module):
        body = "Fix bug\n\n(cherry picked from commit abc123)"
        result = bp_module.strip_cherry_pick_info(body)
        assert "cherry picked" not in result
        assert "Fix bug" in result

    def test_removes_cherry_pick_with_dash(self, bp_module):
        body = "Fix bug\n\n- (cherry picked from commit abc123)"
        result = bp_module.strip_cherry_pick_info(body)
        assert "cherry picked" not in result

    def test_removes_parent_pr_line(self, bp_module):
        body = "Fix bug\n\nParent PR: #1234"
        result = bp_module.strip_cherry_pick_info(body)
        assert "Parent PR" not in result
        assert "Fix bug" in result

    def test_removes_both_markers(self, bp_module):
        body = "Fix bug\n\n- (cherry picked from commit abc123)\n\nParent PR: #1234"
        result = bp_module.strip_cherry_pick_info(body)
        assert "cherry picked" not in result
        assert "Parent PR" not in result
        assert "Fix bug" in result

    def test_preserves_other_content(self, bp_module):
        body = "Fix bug\n\nSome details\n\nFixes: SCYLLADB-123"
        result = bp_module.strip_cherry_pick_info(body)
        assert result == body.rstrip()

    def test_removes_trailing_blank_lines(self, bp_module):
        body = "Fix bug\n\n(cherry picked from commit abc123)\n\n\n"
        result = bp_module.strip_cherry_pick_info(body)
        assert not result.endswith("\n\n")

    def test_empty_body(self, bp_module):
        assert bp_module.strip_cherry_pick_info("") == ""

    def test_none_body(self, bp_module):
        assert bp_module.strip_cherry_pick_info(None) is None


class TestReplaceFixesInBody:
    def test_single_replacement(self, bp_module):
        body = "Some text\nFixes: SCYLLADB-123\nMore text"
        mapping = {"SCYLLADB-123": "SCYLLADB-999"}
        result = bp_module.replace_fixes_in_body(body, mapping)
        assert "SCYLLADB-999" in result
        assert "SCYLLADB-123" not in result

    def test_multiple_replacements(self, bp_module):
        body = "Fixes: SCYLLADB-100\nFixes: SCYLLADB-200"
        mapping = {"SCYLLADB-100": "SCYLLADB-901", "SCYLLADB-200": "SCYLLADB-902"}
        result = bp_module.replace_fixes_in_body(body, mapping)
        assert "SCYLLADB-901" in result
        assert "SCYLLADB-902" in result
        assert "SCYLLADB-100" not in result
        assert "SCYLLADB-200" not in result

    def test_url_format_replacement(self, bp_module):
        body = "Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-123"
        mapping = {"SCYLLADB-123": "SCYLLADB-999"}
        result = bp_module.replace_fixes_in_body(body, mapping)
        assert "SCYLLADB-999" in result

    def test_no_mapping_preserves_body(self, bp_module):
        body = "Fixes: SCYLLADB-123"
        result = bp_module.replace_fixes_in_body(body, {})
        assert result == body

    def test_unmapped_key_preserved(self, bp_module):
        body = "Fixes: SCYLLADB-123"
        mapping = {"OTHER-456": "OTHER-789"}
        result = bp_module.replace_fixes_in_body(body, mapping)
        assert "SCYLLADB-123" in result

    def test_empty_body(self, bp_module):
        result = bp_module.replace_fixes_in_body("", {"SCYLLADB-123": "SCYLLADB-999"})
        assert result == ""

    def test_none_body(self, bp_module):
        result = bp_module.replace_fixes_in_body(None, {"SCYLLADB-123": "SCYLLADB-999"})
        assert result is None


class TestGenerateBackportPrBody:
    def test_basic_body(self, bp_module):
        result = bp_module.generate_backport_pr_body(
            original_pr_body="Fix bug\nFixes: SCYLLADB-123",
            main_pr_link="scylladb/scylladb#100",
            jira_mapping={"SCYLLADB-123": "SCYLLADB-999"},
            commits=["abc123"],
        )
        # Should contain modified Fixes reference
        assert "SCYLLADB-999" in result
        # Should contain cherry-pick info
        assert "(cherry picked from commit abc123)" in result
        # Should contain parent PR reference
        assert "Parent PR: #100" in result

    def test_multiple_commits(self, bp_module):
        result = bp_module.generate_backport_pr_body(
            original_pr_body="Fix bug",
            main_pr_link="#100",
            jira_mapping={},
            commits=["abc123", "def456"],
        )
        assert "(cherry picked from commit abc123)" in result
        assert "(cherry picked from commit def456)" in result

    def test_strips_existing_cherry_pick_markers(self, bp_module):
        original_body = "Fix bug\n\n- (cherry picked from commit abcdef123456)\n\nParent PR: #50"
        result = bp_module.generate_backport_pr_body(
            original_pr_body=original_body,
            main_pr_link="#100",
            jira_mapping={},
            commits=["aaa456bbb"],
        )
        # Old markers should be stripped
        assert "abcdef123456" not in result
        assert "Parent PR: #50" not in result
        # New markers should be present
        assert "(cherry picked from commit aaa456bbb)" in result
        assert "Parent PR: #100" in result

    def test_empty_original_body(self, bp_module):
        result = bp_module.generate_backport_pr_body(
            original_pr_body="",
            main_pr_link="#100",
            jira_mapping={},
            commits=["abc123"],
        )
        assert "(cherry picked from commit abc123)" in result
        assert "Parent PR: #100" in result

    def test_none_original_body(self, bp_module):
        result = bp_module.generate_backport_pr_body(
            original_pr_body=None,
            main_pr_link="#100",
            jira_mapping={},
            commits=["abc123"],
        )
        assert "(cherry picked from commit abc123)" in result
