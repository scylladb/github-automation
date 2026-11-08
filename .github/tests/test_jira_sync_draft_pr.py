"""
Unit tests for PM-335 in the PR-events Jira sync: a draft PR never moves the
linked Jira issue to "In Review".

  - manage_review_gh_event skips the "In Review" transition for a draft PR and
    still runs the other steps; a callable draft state is evaluated only at
    Step 4 (never for a PR with no Jira keys),
  - _run_manage_review_gh_event parses PR_DRAFT (None when unset) and passes a
    lazy API lookup with PR_DRAFT as its fallback,
  - get_pr_is_draft reads the PR's current draft state and falls back to the
    event payload's value (or "not a draft" without one) when the lookup fails,
  - _fetch_pr / _gh_api: shared PR fetch, non-object bodies, timeout plumbing.

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


def _lazy(value, calls):
    """A draft-state callable that records when it is evaluated."""
    def lookup():
        calls.append("lookup")
        return value
    return lookup


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

    def test_callable_draft_is_evaluated_just_before_step_4(self, review_steps):
        _review(pr_is_draft=_lazy(True, review_steps))
        assert review_steps == ["weblink", "details", "labels", "lookup"]

    def test_callable_ready_moves_to_in_review(self, review_steps):
        _review(pr_is_draft=_lazy(False, review_steps))
        assert review_steps[-2:] == ["lookup", ("transition", "In Review", "121")]

    def test_no_jira_keys_skips_the_lookup(self, monkeypatch, review_steps):
        monkeypatch.setattr(jsl, "extract_jira_keys", lambda *a: ["__NO_KEYS_FOUND__"])
        _review(pr_is_draft=_lazy(False, review_steps))
        assert review_steps == []

    def test_all_keys_not_found_skips_the_lookup(self, monkeypatch, review_steps):
        monkeypatch.setattr(jsl, "extract_jira_issue_details",
                            lambda *a: (review_steps.append("details") or "csv", "", ["STAG-1"]))
        _review(pr_is_draft=_lazy(False, review_steps))
        assert "lookup" not in review_steps


# ---------------------------------------------------------------------------
# _run_manage_review_gh_event: PR_DRAFT parsing and the lazy API lookup
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
    def test_passes_a_lazy_lookup(self, review_env):
        jsl._run_manage_review_gh_event()
        assert callable(review_env["pr_is_draft"])
        assert "lookup" not in review_env  # nothing fetched until it is called

    @pytest.mark.parametrize("value, expected", [
        ("true", True), ("True", True), (" TRUE \n", True),
        ("false", False), ("yes", False), ("", None), ("  ", None),
    ])
    def test_pr_draft_env_is_the_fallback(self, monkeypatch, review_env, value, expected):
        monkeypatch.setenv("PR_DRAFT", value)
        jsl._run_manage_review_gh_event()
        review_env["pr_is_draft"]()
        assert review_env["lookup"] == ("scylladb/staging", 7, "gh-token", expected)

    def test_unset_pr_draft_gives_no_fallback(self, review_env):
        jsl._run_manage_review_gh_event()
        review_env["pr_is_draft"]()
        assert review_env["lookup"][3] is None

    def test_api_answer_wins_over_the_payload(self, monkeypatch, review_env):
        monkeypatch.setenv("PR_DRAFT", "false")
        review_env["api_answer"] = True  # converted to draft after the event fired
        jsl._run_manage_review_gh_event()
        assert review_env["pr_is_draft"]() is True

    def test_api_ready_wins_over_a_draft_payload(self, monkeypatch, review_env):
        monkeypatch.setenv("PR_DRAFT", "true")
        review_env["api_answer"] = False  # marked ready after the event fired
        jsl._run_manage_review_gh_event()
        assert review_env["pr_is_draft"]() is False


# ---------------------------------------------------------------------------
# get_pr_is_draft / _fetch_pr
# ---------------------------------------------------------------------------

_FAILURES = [
    (404, '{"message": "Not Found"}'),
    (500, "boom"),
    (200, "not json"),
    (200, "null"),                          # JSON, but not an object
    (200, "[]"),
    (200, '"draft"'),
    URLError("connection reset"),
    TimeoutError("timed out"),
]


def _api(monkeypatch, result):
    """Replace _gh_api with a stub returning *result* (or raising it); record calls."""
    calls = []

    def fake_gh_api(method, url, token, payload=None, **kwargs):
        calls.append((method, url, token, kwargs))
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(jsm, "_gh_api", fake_gh_api)
    return calls


class TestGetPrIsDraft:
    def test_reads_the_pull_request_with_a_timeout(self, monkeypatch):
        calls = _api(monkeypatch, (200, json.dumps({"draft": True})))
        assert jsm.get_pr_is_draft("scylladb/staging", 7, "gh-token", fallback=False) is True
        assert calls == [("GET", "https://api.github.com/repos/scylladb/staging/pulls/7", "gh-token",
                          {"timeout": jsm._PR_DRAFT_LOOKUP_TIMEOUT})]

    def test_current_state_overrides_the_fallback(self, monkeypatch):
        _api(monkeypatch, (200, json.dumps({"draft": False})))
        assert jsm.get_pr_is_draft("o/r", 7, "t", fallback=True) is False

    @pytest.mark.parametrize("draft", [True, False])
    def test_current_state_without_a_fallback(self, monkeypatch, draft):
        _api(monkeypatch, (200, json.dumps({"draft": draft})))
        assert jsm.get_pr_is_draft("o/r", 7, "t", fallback=None) is draft

    @pytest.mark.parametrize("result", _FAILURES + [
        (200, json.dumps({"number": 7})),       # no 'draft' field
        (200, json.dumps({"draft": "true"})),   # not a bool
    ])
    @pytest.mark.parametrize("fallback", [True, False])
    def test_falls_back_to_the_payload_on_failure(self, monkeypatch, result, fallback):
        _api(monkeypatch, result)
        assert jsm.get_pr_is_draft("o/r", 7, "t", fallback=fallback) is fallback

    @pytest.mark.parametrize("result", _FAILURES)
    def test_no_fallback_means_not_a_draft(self, monkeypatch, capsys, result):
        _api(monkeypatch, result)
        assert jsm.get_pr_is_draft("o/r", 7, "t", fallback=None) is False
        assert "PR_DRAFT was not provided" in capsys.readouterr().out


class TestFetchPr:
    def test_returns_the_pr_object(self, monkeypatch):
        calls = _api(monkeypatch, (200, json.dumps({"number": 7, "draft": False})))
        assert jsm._fetch_pr("o/r", 7, "t") == {"number": 7, "draft": False}
        assert calls == [("GET", "https://api.github.com/repos/o/r/pulls/7", "t", {})]  # no timeout kwarg

    @pytest.mark.parametrize("result", _FAILURES)
    def test_returns_none_on_failure(self, monkeypatch, result):
        _api(monkeypatch, result)
        assert jsm._fetch_pr("o/r", 7, "t") is None

    def test_backport_enforcement_survives_a_non_object_body(self, monkeypatch):
        """enforce_backport_fixes_reference shares _fetch_pr: a 'null' PR body comments without mentions."""
        posted = []

        def fake_gh_api(method, url, token, payload=None, **kwargs):
            if method == "GET" and "/pulls/" in url:
                return 200, "null"
            if method == "GET" and "/comments" in url:
                return 200, "[]"
            if method == "GET" and url.endswith("/labels"):
                return 200, '[{"name": "backport/2025.4"}]'
            if method == "POST":
                posted.append(payload["body"])
                return 201, "{}"
            return 200, "{}"

        monkeypatch.setattr(jsm, "_gh_api", fake_gh_api)
        monkeypatch.setattr(jsm, "extract_jira_keys", lambda *a, **k: ["__NO_KEYS_FOUND__"])
        assert jsm.enforce_backport_fixes_reference(
            "title", "no reference", 7, "backport/2025.4", "scylladb/scylladb", "tok", "u:p",
        ) is True
        assert posted == [jsm.REQUIRED_FIXES_COMMENT]


class _Resp:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def getcode(self):
        return 200

    def read(self):
        return b"{}"


class TestGhApiTimeout:
    @pytest.mark.parametrize("timeout, expected", [(None, {}), (15, {"timeout": 15})])
    def test_timeout_is_passed_to_urlopen_only_when_set(self, monkeypatch, timeout, expected):
        seen = []
        monkeypatch.setattr(jsm, "urlopen", lambda req, **kw: seen.append(kw) or _Resp())
        assert jsm._gh_api("GET", "https://api.github.com/x", "t", timeout=timeout) == (200, "{}")
        assert seen == [expected]
