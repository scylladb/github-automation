"""
Unit tests for enforce_backport_fixes_reference (RELENG-175) in jira_sync_modules.

The function:
  - only acts on backport/<release> labels (not backport/none or other labels),
  - skips enforcement when the PR body links a valid issue,
  - otherwise comments once (mentioning author + assignees) and removes every
    backport label.

Network is fully mocked: extract_jira_keys decides validity and _gh_api stands
in for every GitHub REST call.
"""

import os
import sys
import importlib

import pytest

# jira_sync_modules lives in the top-level scripts/ dir (stdlib-only, safe to import).
_SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

jsm = importlib.import_module("jira_sync_modules")


# ---------------------------------------------------------------------------
# BACKPORT_LABEL_RE
# ---------------------------------------------------------------------------

class TestBackportLabelRegex:
    @pytest.mark.parametrize("label", [
        "backport/2025.4",
        "backport/2025.40",
        "backport/manager-3.4",
    ])
    def test_matches_release_labels(self, label):
        assert jsm.BACKPORT_LABEL_RE.match(label)

    @pytest.mark.parametrize("label", [
        "backport/none",
        "backport/",
        "backport/2025",
        "area/build",
        "P1",
    ])
    def test_rejects_non_release_labels(self, label):
        assert not jsm.BACKPORT_LABEL_RE.match(label)


# ---------------------------------------------------------------------------
# enforce_backport_fixes_reference
# ---------------------------------------------------------------------------

class _GhRecorder:
    """Records _gh_api calls and returns canned responses by (method, url-substring)."""

    def __init__(self, pr_json="{}", comments_json="[]", labels_json="[]"):
        self.calls = []
        self._pr_json = pr_json
        self._comments_json = comments_json
        self._labels_json = labels_json

    def __call__(self, method, url, gh_token, payload=None):
        self.calls.append((method, url, payload))
        if method == "GET" and "/pulls/" in url:
            return 200, self._pr_json
        if method == "GET" and "/comments" in url:
            return 200, self._comments_json
        if method == "GET" and url.endswith("/labels"):
            return 200, self._labels_json
        if method == "POST" and "/comments" in url:
            return 201, "{}"
        if method == "DELETE":
            return 200, "{}"
        return 200, "{}"

    def posted_comments(self):
        return [p["body"] for (m, u, p) in self.calls if m == "POST" and "/comments" in u]

    def deleted_labels(self):
        return [u.rsplit("/", 1)[-1] for (m, u, p) in self.calls if m == "DELETE"]


class TestBodyLinksExistingIssue:
    """_pr_body_links_existing_issue combines project validation with existence."""

    def test_no_keys_is_invalid(self, monkeypatch):
        monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])
        assert jsm._pr_body_links_existing_issue("t", "b", "u:p") is False

    def test_one_existing_key_is_valid(self, monkeypatch):
        monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["A-1", "A-2"])
        # First key is 404, second exists -> valid (any existing issue is enough).
        monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: key == "A-2")
        assert jsm._pr_body_links_existing_issue("t", "b", "u:p") is True

    def test_all_absent_is_invalid(self, monkeypatch):
        monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["A-1"])
        monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: False)
        assert jsm._pr_body_links_existing_issue("t", "b", "u:p") is False

    def test_unknown_existence_fails_open(self, monkeypatch):
        monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["A-1"])
        monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: None)
        assert jsm._pr_body_links_existing_issue("t", "b", "u:p") is True


def test_non_backport_label_is_a_noop(monkeypatch):
    called = []
    monkeypatch.setattr(jsm, "_gh_api", lambda *a, **k: called.append(a) or (200, "{}"))
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: pytest.fail("should not validate"))

    result = jsm.enforce_backport_fixes_reference(
        "title", "body", 1, "area/build", "scylladb/scylladb", "tok", "user:pass"
    )
    assert result is False
    assert called == []  # no GitHub calls at all


def test_valid_existing_reference_allows_backport(monkeypatch):
    rec = _GhRecorder()
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["SCYLLADB-123"])
    monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: True)

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes: SCYLLADB-123", 7, "backport/2025.4",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.calls == []  # validated via Jira, no comment / no label removal


def test_bogus_issue_number_is_enforced(monkeypatch):
    """A real project with a non-existent issue number (404) must be rejected."""
    rec = _GhRecorder(
        pr_json='{"user": {"login": "alice"}, "assignees": []}',
        labels_json='[{"name": "backport/2025.4"}]',
    )
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["SCYLLADB-9898758758"])
    monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: False)

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes: SCYLLADB-9898758758", 7, "backport/2025.4",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is True
    assert len(rec.posted_comments()) == 1
    assert "backport%2F2025.4" in rec.deleted_labels()


def test_existence_unknown_fails_open(monkeypatch):
    """If Jira can't confirm existence (None), fail open and allow the backport."""
    rec = _GhRecorder()
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["SCYLLADB-123"])
    monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: None)

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes: SCYLLADB-123", 7, "backport/2025.4",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.calls == []  # fail-open: no enforcement action taken


def test_missing_reference_comments_and_removes_labels(monkeypatch):
    rec = _GhRecorder(
        pr_json='{"user": {"login": "alice"}, "assignees": [{"login": "bob"}]}',
        comments_json="[]",
        labels_json='[{"name": "backport/2025.4"}, {"name": "backport/2025.3"}, {"name": "P1"}]',
    )
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_reference(
        "title", "no reference here", 7, "backport/2025.4",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is True

    comments = rec.posted_comments()
    assert len(comments) == 1
    assert "@alice" in comments[0]
    assert "@bob" in comments[0]
    assert jsm.REQUIRED_FIXES_COMMENT in comments[0]

    # Both backport labels removed (URL-encoded); the P1 label is left alone.
    deleted = rec.deleted_labels()
    assert "backport%2F2025.4" in deleted
    assert "backport%2F2025.3" in deleted
    assert all("P1" not in d for d in deleted)


def test_does_not_comment_twice(monkeypatch):
    rec = _GhRecorder(
        pr_json='{"user": {"login": "alice"}, "assignees": []}',
        comments_json='[{"body": "%s"}]' % jsm.REQUIRED_FIXES_COMMENT.replace("\n", "\\n"),
        labels_json='[{"name": "backport/2025.4"}]',
    )
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_reference(
        "title", "no reference", 7, "backport/2025.4",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is True
    assert rec.posted_comments() == []  # comment already present; not re-posted
    assert "backport%2F2025.4" in rec.deleted_labels()  # label still removed
