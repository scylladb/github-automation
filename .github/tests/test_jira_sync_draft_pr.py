"""
Unit tests for PM-335 in the PR-events Jira sync: a draft PR never moves the
linked Jira issue to "In Review".

  - manage_review_gh_event skips the "In Review" transition for a draft PR and
    still runs the other steps,
  - _run_manage_review_gh_event passes PR_DRAFT as the fallback and uses the
    draft state read from the API,
  - get_pr_is_draft reads the PR's current draft state and falls back to the
    event payload's value when the API call fails.

Network is fully mocked.
"""

import importlib
import json
import os
import sys
from urllib.error import URLError

import pytest

# The jira sync scripts live in the top-level scripts/ dir (stdlib-only, safe to import).
_SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

jsm = importlib.import_module("jira_sync_modules")
jsl = importlib.import_module("jira_sync_logic")


# ---------------------------------------------------------------------------
# manage_review_gh_event: no "In Review" transition for a draft PR
# ---------------------------------------------------------------------------

@pytest.fixture
def review_steps(monkeypatch):
    """Mock every step of manage_review_gh_event and record which ones ran."""
    calls = []
    monkeypatch.setattr(jsl, "extract_jira_keys", lambda *a: ["STAG-1"])
    monkeypatch.setattr(jsl, "add_pr_weblink_to_jira", lambda *a: calls.append("weblink"))
    monkeypatch.setattr(jsl, "extract_jira_issue_details",
                        lambda *a: (calls.append("details") or "csv", "", []))
    monkeypatch.setattr(jsl, "apply_jira_labels_to_pr", lambda *a: calls.append("labels"))
    monkeypatch.setattr(jsl, "jira_status_transition",
                        lambda csv, name, tid, auth: calls.append(("transition", name, tid)))
    return calls


def _review(pr_is_draft=None):
    kwargs = {} if pr_is_draft is None else {"pr_is_draft": pr_is_draft}
    jsl.manage_review_gh_event(
        "PR title", "Fixes: STAG-1", 7, "scylladb/staging", "gh-token", "someone", "u:t", **kwargs
    )


class TestManageReviewDraft:
    def test_draft_pr_skips_in_review_transition(self, review_steps):
        _review(pr_is_draft=True)
        assert not [c for c in review_steps if isinstance(c, tuple)]

    def test_draft_pr_still_runs_the_other_steps(self, review_steps):
        _review(pr_is_draft=True)
        assert review_steps == ["weblink", "details", "labels"]

    def test_ready_pr_moves_to_in_review(self, review_steps):
        _review(pr_is_draft=False)
        assert review_steps[-1] == ("transition", "In Review", "121")

    def test_default_is_not_a_draft(self, review_steps):
        _review()
        assert review_steps[-1] == ("transition", "In Review", "121")


# ---------------------------------------------------------------------------
# _run_manage_review_gh_event: PR_DRAFT parsing and the API lookup
# ---------------------------------------------------------------------------

@pytest.fixture
def review_env(monkeypatch):
    """Set the wrapper's env vars and capture what it passes on."""
    for name, value in {
        "PR_TITLE": "PR title", "PR_BODY": "Fixes: STAG-1", "PR_NUMBER": "7",
        "OWNER_REPO": "scylladb/staging", "GITHUB_TOKEN": "gh-token",
        "REQUESTED_REVIEWER": "someone", "JIRA_AUTH": "u:t",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("PR_DRAFT", raising=False)

    seen = {}

    def fake_get_pr_is_draft(owner_repo, pr_number, gh_token, fallback):
        seen["lookup"] = (owner_repo, pr_number, gh_token, fallback)
        return seen.get("api_answer", fallback)

    monkeypatch.setattr(jsl, "get_pr_is_draft", fake_get_pr_is_draft)
    monkeypatch.setattr(jsl, "manage_review_gh_event",
                        lambda *a, **kw: seen.__setitem__("pr_is_draft", kw.get("pr_is_draft")))
    return seen


class TestRunManageReviewDraft:
    @pytest.mark.parametrize("value, expected", [
        ("true", True), ("True", True), (" TRUE \n", True),
        ("false", False), ("", False), ("yes", False),
    ])
    def test_pr_draft_env_is_the_fallback(self, monkeypatch, review_env, value, expected):
        monkeypatch.setenv("PR_DRAFT", value)
        jsl._run_manage_review_gh_event()
        assert review_env["lookup"] == ("scylladb/staging", 7, "gh-token", expected)

    def test_unset_pr_draft_falls_back_to_not_a_draft(self, review_env):
        jsl._run_manage_review_gh_event()
        assert review_env["lookup"][3] is False

    def test_api_answer_wins_over_the_payload(self, monkeypatch, review_env):
        monkeypatch.setenv("PR_DRAFT", "false")
        review_env["api_answer"] = True  # converted to draft after the event fired
        jsl._run_manage_review_gh_event()
        assert review_env["pr_is_draft"] is True

    def test_api_ready_wins_over_a_draft_payload(self, monkeypatch, review_env):
        monkeypatch.setenv("PR_DRAFT", "true")
        review_env["api_answer"] = False  # marked ready after the event fired
        jsl._run_manage_review_gh_event()
        assert review_env["pr_is_draft"] is False


# ---------------------------------------------------------------------------
# get_pr_is_draft
# ---------------------------------------------------------------------------

class TestGetPrIsDraft:
    def _api(self, monkeypatch, result):
        calls = []

        def fake_gh_api(method, url, token, payload=None):
            calls.append((method, url, token))
            if isinstance(result, Exception):
                raise result
            return result

        monkeypatch.setattr(jsm, "_gh_api", fake_gh_api)
        return calls

    def test_reads_the_pull_request(self, monkeypatch):
        calls = self._api(monkeypatch, (200, json.dumps({"draft": True})))
        assert jsm.get_pr_is_draft("scylladb/staging", 7, "gh-token", fallback=False) is True
        assert calls == [("GET", "https://api.github.com/repos/scylladb/staging/pulls/7", "gh-token")]

    def test_current_state_overrides_the_fallback(self, monkeypatch):
        self._api(monkeypatch, (200, json.dumps({"draft": False})))
        assert jsm.get_pr_is_draft("o/r", 7, "t", fallback=True) is False

    @pytest.mark.parametrize("result", [
        (404, '{"message": "Not Found"}'),
        (500, "boom"),
        (200, "not json"),
        (200, json.dumps({"number": 7})),       # no 'draft' field
        (200, json.dumps({"draft": "true"})),   # not a bool
        URLError("connection reset"),
        TimeoutError("timed out"),
    ])
    @pytest.mark.parametrize("fallback", [True, False])
    def test_falls_back_to_the_payload_on_failure(self, monkeypatch, result, fallback):
        self._api(monkeypatch, result)
        assert jsm.get_pr_is_draft("o/r", 7, "t", fallback=fallback) is fallback
