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
import json

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


def test_other_repo_is_a_noop(monkeypatch):
    """Only scylladb/scylladb requires a Fixes: reference; other repos backport as before."""
    rec = _GhRecorder(
        pr_json='{"user": {"login": "alice"}, "assignees": []}',
        labels_json='[{"name": "backport/2025.4"}]',
    )
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: pytest.fail("should not validate"))

    result = jsm.enforce_backport_fixes_reference(
        "title", "no reference here", 6543, "backport/2025.4",
        "scylladb/scylla-pkg", "tok", "user:pass",
    )
    assert result is False
    assert rec.calls == []  # no comment, no label removal


def test_other_repo_body_change_is_a_noop(monkeypatch):
    rec = _GhRecorder(labels_json='[{"name": "backport/2025.4"}]')
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: pytest.fail("should not validate"))

    result = jsm.enforce_backport_fixes_on_body_change(
        "title", "no reference here", 6543, "scylladb/scylla-dtest", "tok", "user:pass",
    )
    assert result is False
    assert rec.calls == []


def test_enforcement_repo_match():
    assert jsm._fixes_enforcement_enabled("scylladb/scylladb") is True
    assert jsm._fixes_enforcement_enabled("scylladb/scylla-pkg") is False
    assert jsm._fixes_enforcement_enabled("") is False


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


# ---------------------------------------------------------------------------
# enforce_backport_fixes_on_body_change (RELENG-81)
# ---------------------------------------------------------------------------

def test_body_change_without_backport_label_is_a_noop(monkeypatch):
    """A PR with no backport/<release> label is never asked for a Fixes: reference."""
    rec = _GhRecorder(labels_json='[{"name": "backport/none"}, {"name": "area/build"}]')
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: pytest.fail("should not validate"))

    result = jsm.enforce_backport_fixes_on_body_change(
        "title", "no reference", 7, "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.posted_comments() == []
    assert rec.deleted_labels() == []


def test_body_change_with_valid_reference_is_allowed(monkeypatch):
    rec = _GhRecorder(labels_json='[{"name": "backport/2025.4"}]')
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["SCYLLADB-123"])
    monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: True)

    result = jsm.enforce_backport_fixes_on_body_change(
        "title", "Fixes: SCYLLADB-123", 7, "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.deleted_labels() == []


def test_body_edited_to_drop_reference_is_enforced(monkeypatch):
    """The RELENG-81 gap: label added while valid, reference later edited away."""
    rec = _GhRecorder(
        pr_json='{"user": {"login": "alice"}, "assignees": []}',
        labels_json='[{"name": "backport/2025.4"}, {"name": "P1"}]',
    )
    monkeypatch.setattr(jsm, "_gh_api", rec)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_on_body_change(
        "title", "reference removed", 7, "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is True
    assert len(rec.posted_comments()) == 1
    assert "backport%2F2025.4" in rec.deleted_labels()
    assert all("P1" not in d for d in rec.deleted_labels())


def test_body_change_label_lookup_failure_fails_open(monkeypatch):
    """If the label list can't be read, do nothing rather than penalise the PR."""
    def failing_gh_api(method, url, gh_token, payload=None):
        if method == "GET" and url.endswith("/labels"):
            return 503, "service unavailable"
        pytest.fail("no further calls expected")

    monkeypatch.setattr(jsm, "_gh_api", failing_gh_api)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: pytest.fail("should not validate"))

    result = jsm.enforce_backport_fixes_on_body_change(
        "title", "no reference", 7, "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False


# ---------------------------------------------------------------------------
# GitHub issue references ("Fixes #123", scylladb/scylladb#123, issue URL)
# ---------------------------------------------------------------------------

class TestGithubFixesIssueNumbers:
    REPO = "scylladb/scylladb"

    @pytest.mark.parametrize("body,expected", [
        ("Fixes #30122", [30122]),
        ("Fixes: #30122", [30122]),
        ("Fixes scylladb/scylladb#30122", [30122]),
        ("Fixes https://github.com/scylladb/scylladb/issues/30122", [30122]),
        ("text\n\nFixes: https://github.com/scylladb/scylladb/issues/30122\n", [30122]),
        ("Fixes #1\nFixes #2\nFixes #1", [1, 2]),
    ])
    def test_matches_same_repo_references(self, body, expected):
        assert jsm._github_fixes_issue_numbers(body, self.REPO) == expected

    @pytest.mark.parametrize("body", [
        "",
        "no reference",
        "Fixes: SCYLLADB-123",
        "Fixes https://github.com/scylladb/scylla-pkg/issues/30122",
        "Fixes scylladb/scylla-pkg#30122",
        "Fixes https://github.com/scylladb/scylladb/pull/30122",
        "See #30122",
    ])
    def test_rejects_other_references(self, body):
        assert jsm._github_fixes_issue_numbers(body, self.REPO) == []

    def test_no_repo_means_no_match(self):
        assert jsm._github_fixes_issue_numbers("Fixes #1", "") == []


class TestGithubIssueExists:
    @pytest.mark.parametrize("code,expected", [
        (200, True), (404, False), (410, False), (403, None), (500, None),
    ])
    def test_http_codes(self, monkeypatch, code, expected):
        monkeypatch.setattr(jsm, "_gh_api", lambda *a, **k: (code, "{}"))
        assert jsm._github_issue_exists("scylladb/scylladb", 1, "tok") is expected

    def test_pull_request_is_not_an_issue(self, monkeypatch):
        monkeypatch.setattr(
            jsm, "_gh_api",
            lambda *a, **k: (200, '{"number": 1, "pull_request": {"url": "x"}}'),
        )
        assert jsm._github_issue_exists("scylladb/scylladb", 1, "tok") is False

    def test_no_token_is_unknown(self, monkeypatch):
        monkeypatch.setattr(jsm, "_gh_api", lambda *a, **k: pytest.fail("no API call"))
        assert jsm._github_issue_exists("scylladb/scylladb", 1, "") is None

    def test_network_error_is_unknown(self, monkeypatch):
        def boom(*a, **k):
            raise jsm.URLError("down")
        monkeypatch.setattr(jsm, "_gh_api", boom)
        assert jsm._github_issue_exists("scylladb/scylladb", 1, "tok") is None


def _github_issue_recorder(issue_code):
    """_GhRecorder that answers GET /issues/<n> with issue_code."""
    rec = _GhRecorder(
        pr_json='{"user": {"login": "alice"}, "assignees": []}',
        labels_json='[{"name": "backport/2026.1"}, {"name": "backport/2026.2"}]',
    )
    inner = rec.__call__

    def call(method, url, gh_token, payload=None):
        if method == "GET" and "/issues/" in url and url.rsplit("/", 1)[-1].isdigit():
            rec.calls.append((method, url, payload))
            return issue_code, "{}"
        return inner(method, url, gh_token, payload)
    return rec, call


def test_existing_github_issue_allows_backport(monkeypatch):
    """PR 32831: 'Fixes https://github.com/scylladb/scylladb/issues/30122' kept its labels."""
    rec, call = _github_issue_recorder(200)
    monkeypatch.setattr(jsm, "_gh_api", call)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes https://github.com/scylladb/scylladb/issues/30122", 32831,
        "backport/2026.1", "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.posted_comments() == []
    assert rec.deleted_labels() == []
    assert [u for (m, u, p) in rec.calls] == [
        "https://api.github.com/repos/scylladb/scylladb/issues/30122"
    ]


def test_missing_github_issue_is_enforced(monkeypatch):
    rec, call = _github_issue_recorder(404)
    monkeypatch.setattr(jsm, "_gh_api", call)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes #999999999", 7, "backport/2026.1",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is True
    assert len(rec.posted_comments()) == 1
    assert "backport%2F2026.1" in rec.deleted_labels()
    assert "backport%2F2026.2" in rec.deleted_labels()


def test_github_issue_lookup_failure_fails_open(monkeypatch):
    rec, call = _github_issue_recorder(500)
    monkeypatch.setattr(jsm, "_gh_api", call)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes #30122", 7, "backport/2026.1",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.deleted_labels() == []


def test_missing_github_issue_with_valid_jira_key_is_allowed(monkeypatch):
    rec, call = _github_issue_recorder(404)
    monkeypatch.setattr(jsm, "_gh_api", call)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["SCYLLADB-123"])
    monkeypatch.setattr(jsm, "_jira_issue_exists", lambda key, auth: True)

    result = jsm.enforce_backport_fixes_reference(
        "title", "Fixes #999999999\nFixes: SCYLLADB-123", 7, "backport/2026.1",
        "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.deleted_labels() == []


def test_body_edited_to_github_issue_keeps_labels(monkeypatch):
    rec, call = _github_issue_recorder(200)
    monkeypatch.setattr(jsm, "_gh_api", call)
    monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])

    result = jsm.enforce_backport_fixes_on_body_change(
        "title", "Fixes scylladb/scylladb#30122", 7, "scylladb/scylladb", "tok", "user:pass",
    )
    assert result is False
    assert rec.deleted_labels() == []


# ---------------------------------------------------------------------------
# Regression: real key parsing and validation against a fake Jira
# ---------------------------------------------------------------------------

class _FakeJiraResponse:
    def __init__(self, payload):
        self._data = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def getcode(self):
        return 200

    def read(self):
        return self._data


def _fake_jira_urlopen(existing_issues, projects):
    """urlopen stand-in for Jira: only existing_issues exist, only projects are known."""
    def fake_urlopen(req):
        url = req.full_url
        if "/rest/api/3/project/search" in url:
            return _FakeJiraResponse({"values": [{"key": p} for p in projects]})
        if "/rest/api/3/issue/" in url:
            key = url.split("/rest/api/3/issue/", 1)[1].split("?", 1)[0]
            if key in existing_issues:
                return _FakeJiraResponse({"key": key, "fields": {"issuetype": {"name": "Bug"}}})
            raise jsm.HTTPError(url, 404, "Not Found", {}, None)
        pytest.fail(f"unexpected Jira request: {url}")
    return fake_urlopen


class TestFixesReferenceRegression:
    """Only extract_jira_keys' HTTP layer is faked; parsing and validation are real."""

    @pytest.fixture(autouse=True)
    def fake_jira(self, monkeypatch):
        monkeypatch.setattr(
            jsm, "urlopen", _fake_jira_urlopen({"SCYLLADB-124"}, {"SCYLLADB", "RELENG"})
        )

    @pytest.mark.parametrize("body,valid", [
        ("Fixes: SCYLLADB-124", True),
        ("Fixes SCYLLADB-124", True),
        ("Fixes: SCYLLADB-10000000", False),  # known project, issue does not exist
        ("Fixes: ABDJFK-23", False),  # unknown project
        ("Fixes: ABDJFK-23\nFixes: SCYLLADB-10000000", False),
        ("Fixes: ABDJFK-23\nFixes: SCYLLADB-124", True),
    ])
    def test_body_links_existing_issue(self, body, valid):
        assert jsm._pr_body_links_existing_issue("title", body, "user:pass") is valid

    @pytest.mark.parametrize("body,enforced", [
        ("Fixes: SCYLLADB-124", False),
        ("Fixes: SCYLLADB-10000000", True),
        ("Fixes: ABDJFK-23", True),
    ])
    def test_backport_label_enforcement(self, monkeypatch, body, enforced):
        rec = _GhRecorder(
            pr_json='{"user": {"login": "alice"}, "assignees": []}',
            labels_json='[{"name": "backport/2026.1"}]',
        )
        monkeypatch.setattr(jsm, "_gh_api", rec)

        result = jsm.enforce_backport_fixes_reference(
            "title", body, 7, "backport/2026.1", "scylladb/scylladb", "tok", "user:pass",
        )
        assert result is enforced
        assert (rec.deleted_labels() == ["backport%2F2026.1"]) is enforced
        assert len(rec.posted_comments()) == (1 if enforced else 0)
