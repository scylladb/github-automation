"""
Unit tests for RELENG-808 in the PR-events Jira sync.

Opening a PR with reviewers set fires 'opened' and 'review_requested' at the
same time; when the 'opened' run writes last its "In Progress" overwrote the
"In Review" of the other runs. The 'opened' handler now reads the reviewers
requested on the PR and targets "In Review" when there are any (and the PR is
not a draft), so the final status is the same whichever run finishes last.

Network is fully mocked.
"""

import importlib
import json
import os
import sys

import pytest

_SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

jsm = importlib.import_module("jira_sync_modules")
jsl = importlib.import_module("jira_sync_logic")

DETAILS_CSV = jsm._CSV_HEADER + '\n"SCT-1","New","","","","","","","",""\n'


# ---------------------------------------------------------------------------
# get_pr_requested_reviewers
# ---------------------------------------------------------------------------

class TestGetPrRequestedReviewers:
    def _api(self, monkeypatch, code, body):
        calls = []

        def fake(method, url, token, payload=None):
            calls.append((method, url))
            return code, body

        monkeypatch.setattr(jsm, "_gh_api", fake)
        return calls

    def test_returns_user_logins_and_team_slugs(self, monkeypatch):
        body = json.dumps({
            "requested_reviewers": [{"login": "alice"}, {"login": "bob"}],
            "requested_teams": [{"slug": "core"}],
        })
        calls = self._api(monkeypatch, 200, body)
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") == ["alice", "bob", "core"]
        assert calls == [("GET", "https://api.github.com/repos/o/r/pulls/7")]

    def test_no_reviewers_is_an_empty_list(self, monkeypatch):
        self._api(monkeypatch, 200, json.dumps({"requested_reviewers": [], "requested_teams": None}))
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") == []

    def test_missing_fields_is_an_empty_list(self, monkeypatch):
        self._api(monkeypatch, 200, json.dumps({"number": 7}))
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") == []

    @pytest.mark.parametrize("code", [403, 404, 500, 0])
    def test_http_error_is_none(self, monkeypatch, code):
        self._api(monkeypatch, code, "nope")
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") is None

    def test_bad_json_is_none(self, monkeypatch):
        self._api(monkeypatch, 200, "<html>")
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") is None

    def test_network_error_is_none(self, monkeypatch):
        # A DNS/connection failure must not abort the sync: the lookup reports
        # it as unreadable and the caller falls back to "In Progress".
        from urllib.error import URLError

        def boom(req):
            raise URLError("name resolution failed")

        monkeypatch.setattr(jsm, "urlopen", boom)
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") is None

    def test_socket_error_is_none(self, monkeypatch):
        # A reset or timeout during the read is an OSError, not a URLError;
        # it must take the same fallback path.
        def boom(req):
            raise ConnectionResetError("connection reset by peer")

        monkeypatch.setattr(jsm, "urlopen", boom)
        assert jsm.get_pr_requested_reviewers("o/r", 7, "t") is None


# ---------------------------------------------------------------------------
# manage_opened_gh_event picks the transition target
# ---------------------------------------------------------------------------

@pytest.fixture
def opened_run(monkeypatch):
    """Stub every external step of manage_opened_gh_event and record the transition."""
    state = {"transitions": [], "reviewer_reads": 0, "reviewers": [], "api_draft": False, "draft_fallbacks": []}

    monkeypatch.setattr(jsl, "enforce_backport_fixes_on_body_change", lambda *a: False)
    monkeypatch.setattr(jsl, "extract_jira_keys", lambda *a: ["SCT-1"])
    monkeypatch.setattr(jsl, "add_pr_weblink_to_jira", lambda *a, **k: None)
    monkeypatch.setattr(jsl, "extract_jira_issue_details", lambda *a: (DETAILS_CSV, "", []))
    monkeypatch.setattr(jsl, "apply_jira_labels_to_pr", lambda *a: None)
    monkeypatch.setattr(jsl, "add_comment_to_jira", lambda *a, **k: None)
    monkeypatch.setattr(
        jsl, "jira_status_transition",
        lambda csv, name, tid, auth: state["transitions"].append((name, tid)) or [],
    )

    def fake_reviewers(owner_repo, pr_number, gh_token):
        state["reviewer_reads"] += 1
        return state["reviewers"]

    monkeypatch.setattr(jsl, "get_pr_requested_reviewers", fake_reviewers)

    def fake_is_draft(owner_repo, pr_number, gh_token, fallback):
        # Stands in for the API read (PM-335): returns the PR's *current* draft
        # state and records the payload value handed over as the fallback.
        state["draft_fallbacks"].append(fallback)
        return state["api_draft"]

    monkeypatch.setattr(jsl, "get_pr_is_draft", fake_is_draft)

    def run(action, reviewers, draft=False, api_draft=None):
        state["reviewers"] = reviewers
        state["api_draft"] = draft if api_draft is None else api_draft
        monkeypatch.setenv("CALLER_ACTION", action)
        jsl.manage_opened_gh_event("title", "Fixes: SCT-1", 7, "o/r", "t", "u:t", pr_is_draft=draft)
        return state["transitions"]

    run.state = state
    return run


class TestOpenedTransitionTarget:
    def test_opened_with_reviewers_targets_in_review(self, opened_run):
        assert opened_run("opened", ["alice"]) == [("In Review", "121")]

    def test_opened_with_team_reviewer_targets_in_review(self, opened_run):
        assert opened_run("opened", ["core-team"]) == [("In Review", "121")]

    def test_opened_without_reviewers_targets_in_progress(self, opened_run):
        assert opened_run("opened", []) == [("In Progress", "111")]

    def test_opened_draft_with_reviewers_stays_in_progress(self, opened_run):
        # PM-335: a draft is never moved to "In Review", so the reviewers are not even read.
        assert opened_run("opened", ["alice"], draft=True) == [("In Progress", "111")]
        assert opened_run.state["reviewer_reads"] == 0

    def test_opened_reads_the_current_draft_state(self, opened_run):
        # PM-335: the payload said draft, but the PR was marked ready since; the
        # API's answer wins and the payload value is only passed as the fallback.
        assert opened_run("opened", ["alice"], draft=True, api_draft=False) == [("In Review", "121")]
        assert opened_run.state["draft_fallbacks"] == [True]

    def test_opened_converted_to_draft_stays_in_progress(self, opened_run):
        # The reverse: payload said ready, the PR is a draft now.
        assert opened_run("opened", ["alice"], draft=False, api_draft=True) == [("In Progress", "111")]
        assert opened_run.state["reviewer_reads"] == 0

    def test_edited_with_reviewers_keeps_in_progress(self, opened_run):
        # 'edited' never targets "In Review", so neither the draft state nor the reviewers are read.
        assert opened_run("edited", ["alice"]) == [("In Progress", "111")]
        assert opened_run.state["reviewer_reads"] == 0
        assert opened_run.state["draft_fallbacks"] == []

    def test_unreadable_reviewers_falls_back_to_in_progress(self, opened_run, capsys):
        assert opened_run("opened", None) == [("In Progress", "111")]
        assert "::warning::Could not read the requested reviewers" in capsys.readouterr().out


class TestOpenedCliWrapper:
    @pytest.mark.parametrize("draft_env,expected", [("true", True), ("false", False), ("", None)])
    def test_pr_draft_env_is_passed_through(self, monkeypatch, draft_env, expected):
        # PR_DRAFT is the fallback for the API read; unset means "not provided" (None).
        seen = {}

        def fake(*args, **kwargs):
            seen.update(kwargs)

        monkeypatch.setattr(jsl, "manage_opened_gh_event", fake)
        for name, value in {
            "PR_TITLE": "t", "PR_BODY": "b", "PR_NUMBER": "7", "OWNER_REPO": "o/r",
            "GITHUB_TOKEN": "t", "JIRA_AUTH": "u:t", "PR_DRAFT": draft_env,
        }.items():
            monkeypatch.setenv(name, value)
        jsl._run_manage_opened_gh_event()
        assert seen == {"pr_is_draft": expected}
