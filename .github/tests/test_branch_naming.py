"""
Unit tests for branch naming helpers in auto-backport-jira.py.

Tests:
  - get_branch_prefix
  - is_manager_version
  - get_branch_name
  - parse_version
  - sort_versions_descending
"""

import pytest


class TestGetBranchPrefix:
    def test_scylladb_repo(self, bp_module):
        assert bp_module.get_branch_prefix("scylladb/scylladb") == "branch-"

    def test_other_repo(self, bp_module):
        assert bp_module.get_branch_prefix("scylladb/scylla-pkg") == "next-"

    def test_another_repo(self, bp_module):
        assert bp_module.get_branch_prefix("scylladb/scylla-tools") == "next-"


class TestIsManagerVersion:
    def test_manager_version(self, bp_module):
        assert bp_module.is_manager_version("manager-3.4") is True

    def test_regular_version(self, bp_module):
        assert bp_module.is_manager_version("2025.4") is False

    def test_empty_string(self, bp_module):
        assert bp_module.is_manager_version("") is False

    def test_manager_like_but_not_prefix(self, bp_module):
        assert bp_module.is_manager_version("some-manager-3.4") is False


class TestGetBranchName:
    def test_scylladb_regular_version(self, bp_module):
        assert bp_module.get_branch_name("scylladb/scylladb", "2025.4") == "branch-2025.4"

    def test_other_repo_regular_version(self, bp_module):
        assert bp_module.get_branch_name("scylladb/scylla-pkg", "2025.4") == "next-2025.4"

    def test_manager_version_any_repo(self, bp_module):
        # Manager versions always return the version as-is
        assert bp_module.get_branch_name("scylladb/scylladb", "manager-3.4") == "manager-3.4"
        assert bp_module.get_branch_name("scylladb/scylla-pkg", "manager-3.4") == "manager-3.4"


class TestParseVersion:
    def test_regular_version(self, bp_module):
        result = bp_module.parse_version("2025.4")
        assert result == (1, 2025, 4)

    def test_manager_version(self, bp_module):
        result = bp_module.parse_version("manager-3.4")
        assert result == (0, 3, 4)

    def test_sorting_regular_before_manager(self, bp_module):
        # Regular versions (prefix=1) should sort higher than manager (prefix=0)
        assert bp_module.parse_version("2025.4") > bp_module.parse_version("manager-3.4")


class TestSortVersionsDescending:
    def test_basic_sorting(self, bp_module):
        versions = ["2025.2", "2025.4", "2025.3"]
        result = bp_module.sort_versions_descending(versions)
        assert result == ["2025.4", "2025.3", "2025.2"]

    def test_mixed_years(self, bp_module):
        versions = ["2024.1", "2025.4", "2024.3"]
        result = bp_module.sort_versions_descending(versions)
        assert result == ["2025.4", "2024.3", "2024.1"]

    def test_manager_versions_sorted_after_regular(self, bp_module):
        versions = ["manager-3.4", "2025.4", "2025.3", "manager-3.5"]
        result = bp_module.sort_versions_descending(versions)
        # Regular versions first (descending), then manager versions (descending)
        assert result == ["2025.4", "2025.3", "manager-3.5", "manager-3.4"]

    def test_single_version(self, bp_module):
        assert bp_module.sort_versions_descending(["2025.4"]) == ["2025.4"]

    def test_empty_list(self, bp_module):
        assert bp_module.sort_versions_descending([]) == []
