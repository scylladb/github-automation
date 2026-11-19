"""
Closing keywords must be whole words: "hotfix: X-1" or "prefixes X-1" names no issue.

Covers the Jira sync (_extract_candidate_keys) and the backport Fixes parsers
in auto-backport-jira.py.
"""

import importlib
import os
import re
import sys

import pytest

_SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

jsm = importlib.import_module("jira_sync_modules")

NOT_A_KEYWORD = [
    "hotfix: SCT-123",
    "prefix SCT-9 bump",
    "suffix: SCT-1",
    "disclose SCT-7",
    "foreclosed: SCT-8",
    "prefixes SCT-12",
    "hotfixes: SCT-5",
    "fixesSCT-6",
    "hot-fix: SCT-10",
]

KEYWORD = [
    ("Fixes: SCT-1", "SCT-1"),
    ("fixes SCT-2", "SCT-2"),
    ("(Fixes: SCT-3)", "SCT-3"),
    ("text\nFixes:SCT-4", "SCT-4"),
    ("__Fixes: SCT-7__", "SCT-7"),
    ("_Fixes: SCT-8_", "SCT-8"),
    ("**Fixes: SCT-9**", "SCT-9"),
]


@pytest.mark.parametrize("text", NOT_A_KEYWORD)
def test_sync_ignores_keyword_inside_a_word(text):
    assert jsm._extract_candidate_keys(text) == []


@pytest.mark.parametrize("text,key", KEYWORD + [("Closes: SCT-5", "SCT-5"), ("resolved SCT-6", "SCT-6")])
def test_sync_matches_whole_keyword(text, key):
    assert jsm._extract_candidate_keys(text) == [key]


@pytest.mark.parametrize("text", NOT_A_KEYWORD)
def test_backport_ignores_keyword_inside_a_word(bp_module, text):
    assert bp_module.extract_all_jira_keys_from_pr_body(text) == []
    assert bp_module.extract_jira_key_from_pr_body(text) is None
    assert bp_module.has_fixes_reference(text) is False
    key = re.search(r"SCT-\d+", text).group()
    assert bp_module.replace_fixes_in_body(text, {key: "SCT-999"}) == text


@pytest.mark.parametrize("text", ["Fixesorg/repo#123", "Fixes#123"])
def test_backport_github_reference_needs_separator(bp_module, text):
    assert bp_module.has_fixes_reference(text) is False


@pytest.mark.parametrize("text,key", KEYWORD)
def test_backport_matches_whole_keyword(bp_module, text, key):
    assert bp_module.extract_all_jira_keys_from_pr_body(text) == [key]
    assert bp_module.has_fixes_reference(text) is True
