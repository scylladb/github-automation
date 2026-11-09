"""
Unit tests for PM-365 in the PR-events Jira sync: in scylladb/scylladb, a
change that lands on master moves the linked Jira issues to "Done" instead of
"Merged". Backports and every other repository keep going to "Merged".

  - merged_transition_target: the (repo, branch) -> target rule,
  - manage_closed_gh_event: a PR merged into master vs. other base branches,
  - manage_labeled_gh_event: promoted-to-master vs. promoted-to-branch-*,
  - _run_manage_closed_gh_event: the base branch comes from GITHUB_BASE_REF,
    with a GitHub API fallback only where it matters,
  - get_pr_base_ref: reads base.ref and returns "" on any failure,
  - the real jira_status_transition POSTs transition 141 (Done), not 7.

The scylladb.git master branch can't be exercised end to end from the staging
repo, so these tests drive the real handlers with the network mocked.
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

SCYLLADB = "scylladb/scylladb"
DONE = ("Done", "141")
MERGED = ("Merged", "7")


@pytest.fixture(autouse=True)
def _no_sleep_and_clean_failures(monkeypatch):
    monkeypatch.setattr(jsm.time, "sleep", lambda _s: None)
    jsm._SYNC_FAILURES.clear()
    yield
    jsm._SYNC_FAILURES.clear()


def _details_csv(*rows):
    lines = [jsm._CSV_HEADER]
    for key, status in rows:
        lines.append(f'"{key}","{status}","","","","","","","",""')
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# merged_transition_target
# ---------------------------------------------------------------------------

class TestMergedTransitionTarget:
    def test_constants_match_the_jira_workflow(self):
        assert (jsm.DONE_STATUS_NAME, jsm.DONE_TRANSITION_ID) == DONE
        assert (jsm.MERGED_STATUS_NAME, jsm.MERGED_TRANSITION_ID) == MERGED
        assert jsm.DONE_ON_MASTER_REPOS == {SCYLLADB}

    @pytest.mark.parametrize("repo, branch", [
        (SCYLLADB, "master"),
        ("ScyllaDB/ScyllaDB", "master"),        # repo names are case-insensitive
        (" scylladb/scylladb ", " master "),
    ])
    def test_scylladb_master_goes_to_done(self, repo, branch):
        assert jsm.merged_transition_target(repo, branch) == DONE

    @pytest.mark.parametrize("repo, branch", [
        (SCYLLADB, "branch-2026.4"),            # backport
        (SCYLLADB, "next"),
        (SCYLLADB, "next-2026.4"),
        (SCYLLADB, "Master"),                   # branch names are case-sensitive
        (SCYLLADB, "master-old"),
        (SCYLLADB, ""),                         # base branch unknown
        (SCYLLADB, None),
        ("scylladb/staging", "master"),
        ("scylladb/scylla-manager", "master"),
        ("other/scylladb", "master"),
        ("", "master"),
    ])
    def test_everything_else_goes_to_merged(self, repo, branch):
        assert jsm.merged_transition_target(repo, branch) == MERGED


# ---------------------------------------------------------------------------
# Handler fixtures: mock every step and record the transition that is made
# ---------------------------------------------------------------------------

@pytest.fixture
def sync_steps(monkeypatch):
    """Mock the Jira/GitHub steps of the closed and labeled handlers."""
    calls = {"transitions": [], "comments": []}
    csv = _details_csv(("SCYLLADB-1", "In Review"))
    monkeypatch.setattr(jsl, "extract_closed_pr_jira_keys", lambda *a, **k: ["SCYLLADB-1"])
    monkeypatch.setattr(jsl, "extract_jira_keys", lambda *a, **k: ["SCYLLADB-1"])
    monkeypatch.setattr(jsl, "add_pr_weblink_to_jira", lambda *a, **k: None)
    monkeypatch.setattr(jsl, "add_label_to_jira_issue", lambda *a, **k: [])
    monkeypatch.setattr(jsl, "extract_jira_issue_details", lambda *a, **k: (csv, "", []))
    monkeypatch.setattr(jsl, "apply_jira_labels_to_pr", lambda *a, **k: None)
    monkeypatch.setattr(jsl, "add_comment_to_jira",
                        lambda keys, text, auth, **k: calls["comments"].append(text))
    monkeypatch.setattr(jsl, "jira_status_transition",
                        lambda csv_, name, tid, auth: calls["transitions"].append((name, tid)))
    return calls


def _closed(repo, merged=True, **kwargs):
    jsl.manage_closed_gh_event("PR title", "Fixes: SCYLLADB-1", 7, merged, repo, "gh", "u:t", **kwargs)


def _labeled(repo, label):
    jsl.manage_labeled_gh_event("PR title", "Fixes: SCYLLADB-1", 7, label, repo, "gh", "u:t")


class TestClosedEvent:
    def test_merged_into_scylladb_master_goes_to_done(self, sync_steps):
        _closed(SCYLLADB, base_ref="master")
        assert sync_steps["transitions"] == [DONE]

    def test_merge_comment_is_unchanged(self, sync_steps):
        _closed(SCYLLADB, base_ref="master")
        assert sync_steps["comments"] == ["Closed via PR merge "]

    @pytest.mark.parametrize("base_ref", ["branch-2026.4", "next", ""])
    def test_other_scylladb_branches_go_to_merged(self, sync_steps, base_ref):
        _closed(SCYLLADB, base_ref=base_ref)
        assert sync_steps["transitions"] == [MERGED]

    def test_default_base_ref_keeps_merged(self, sync_steps):
        _closed(SCYLLADB)  # callers that don't pass base_ref
        assert sync_steps["transitions"] == [MERGED]

    @pytest.mark.parametrize("repo", ["scylladb/staging", "scylladb/scylla-manager"])
    def test_other_repos_merged_into_master_go_to_merged(self, sync_steps, repo):
        _closed(repo, base_ref="master")
        assert sync_steps["transitions"] == [MERGED]

    def test_closed_without_merge_makes_no_transition(self, sync_steps):
        _closed(SCYLLADB, merged=False, base_ref="master")
        assert sync_steps["transitions"] == []
        assert sync_steps["comments"] == ["PR closed without merge "]


class TestLabeledEvent:
    def test_promoted_to_master_in_scylladb_goes_to_done(self, sync_steps):
        _labeled(SCYLLADB, "promoted-to-master")
        assert sync_steps["transitions"] == [DONE]
        assert sync_steps["comments"] == ["Closed via promoted-to-master label on PR "]

    @pytest.mark.parametrize("label", ["promoted-to-branch-2026.4", "promoted-to-next"])
    def test_other_promotions_in_scylladb_go_to_merged(self, sync_steps, label):
        _labeled(SCYLLADB, label)
        assert sync_steps["transitions"] == [MERGED]

    @pytest.mark.parametrize("repo", ["scylladb/staging", "scylladb/scylla-manager"])
    def test_promoted_to_master_elsewhere_goes_to_merged(self, sync_steps, repo):
        _labeled(repo, "promoted-to-master")
        assert sync_steps["transitions"] == [MERGED]

    def test_non_promoted_label_makes_no_transition(self, sync_steps):
        _labeled(SCYLLADB, "area/build")
        assert sync_steps["transitions"] == []


# ---------------------------------------------------------------------------
# _run_manage_closed_gh_event: where the base branch comes from
# ---------------------------------------------------------------------------

@pytest.fixture
def closed_env(monkeypatch):
    """Set the wrapper's env vars; capture the base_ref it passes and any API lookup."""
    for name, value in {
        "PR_TITLE": "PR title", "PR_BODY": "Fixes: SCYLLADB-1", "PR_NUMBER": "7",
        "PR_MERGED": "true", "OWNER_REPO": SCYLLADB,
        "GITHUB_TOKEN": "gh", "JIRA_AUTH": "u:t",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("GITHUB_BASE_REF", raising=False)

    seen = {"lookups": []}

    def fake_get_pr_base_ref(owner_repo, pr_number, gh_token):
        seen["lookups"].append((owner_repo, pr_number, gh_token))
        return seen.get("api_base_ref", "")

    monkeypatch.setattr(jsl, "get_pr_base_ref", fake_get_pr_base_ref)
    monkeypatch.setattr(jsl, "manage_closed_gh_event",
                        lambda *a, **kw: seen.__setitem__("base_ref", kw.get("base_ref")))
    return seen


class TestRunManageClosedBaseRef:
    def test_github_base_ref_is_used_without_an_api_call(self, monkeypatch, closed_env):
        monkeypatch.setenv("GITHUB_BASE_REF", "master")
        jsl._run_manage_closed_gh_event()
        assert closed_env["base_ref"] == "master"
        assert closed_env["lookups"] == []

    def test_missing_base_ref_is_read_from_the_api(self, closed_env):
        closed_env["api_base_ref"] = "master"
        jsl._run_manage_closed_gh_event()
        assert closed_env["lookups"] == [(SCYLLADB, 7, "gh")]
        assert closed_env["base_ref"] == "master"

    def test_api_failure_keeps_merged(self, closed_env, capsys):
        jsl._run_manage_closed_gh_event()
        assert closed_env["base_ref"] == ""
        assert "linked issues will move to 'Merged'" in capsys.readouterr().out

    def test_other_repos_never_call_the_api(self, monkeypatch, closed_env):
        monkeypatch.setenv("OWNER_REPO", "scylladb/scylla-manager")
        jsl._run_manage_closed_gh_event()
        assert closed_env["lookups"] == []
        assert closed_env["base_ref"] == ""

    def test_unmerged_pr_never_calls_the_api(self, monkeypatch, closed_env):
        monkeypatch.setenv("PR_MERGED", "false")
        jsl._run_manage_closed_gh_event()
        assert closed_env["lookups"] == []


# ---------------------------------------------------------------------------
# get_pr_base_ref
# ---------------------------------------------------------------------------

def _api(monkeypatch, result):
    calls = []

    def fake_gh_api(method, url, token, payload=None, **kwargs):
        calls.append((method, url, kwargs))
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(jsm, "_gh_api", fake_gh_api)
    return calls


class TestGetPrBaseRef:
    def test_reads_base_ref_with_a_timeout(self, monkeypatch):
        calls = _api(monkeypatch, (200, json.dumps({"base": {"ref": "master"}})))
        assert jsm.get_pr_base_ref(SCYLLADB, 7, "gh") == "master"
        assert calls == [("GET", "https://api.github.com/repos/scylladb/scylladb/pulls/7",
                          {"timeout": jsm._PR_DRAFT_LOOKUP_TIMEOUT})]

    @pytest.mark.parametrize("result", [
        (404, '{"message": "Not Found"}'),
        (500, "boom"),
        (200, "not json"),
        (200, "null"),
        (200, json.dumps({"number": 7})),            # no base
        (200, json.dumps({"base": None})),
        (200, json.dumps({"base": "master"})),       # base is not an object
        (200, json.dumps({"base": {"ref": 5}})),     # ref is not a string
        URLError("connection reset"),
        TimeoutError("timed out"),
    ])
    def test_returns_empty_on_failure(self, monkeypatch, result):
        _api(monkeypatch, result)
        assert jsm.get_pr_base_ref(SCYLLADB, 7, "gh") == ""


# ---------------------------------------------------------------------------
# Through the real jira_status_transition: which transition id is POSTed
# ---------------------------------------------------------------------------

@pytest.fixture
def jira_posts(monkeypatch):
    """Run the closed handler with the real jira_status_transition; record POSTed ids."""
    posts = []
    monkeypatch.setattr(jsl, "extract_closed_pr_jira_keys", lambda *a, **k: ["SCYLLADB-1"])
    monkeypatch.setattr(jsl, "add_pr_weblink_to_jira", lambda *a, **k: None)
    monkeypatch.setattr(jsl, "apply_jira_labels_to_pr", lambda *a, **k: None)
    monkeypatch.setattr(jsl, "add_comment_to_jira", lambda *a, **k: None)
    monkeypatch.setattr(jsm, "_jira_put", lambda *a: (204, ""))
    monkeypatch.setattr(jsm, "_post_transition",
                        lambda key, tid, auth: posts.append((key, tid)) or (204, ""))

    def run(repo, base_ref, status="In Review"):
        csv = _details_csv(("SCYLLADB-1", status))
        monkeypatch.setattr(jsl, "extract_jira_issue_details", lambda *a, **k: (csv, "", []))
        jsl.manage_closed_gh_event("PR title", "Fixes: SCYLLADB-1", 7, True, repo, "gh", "u:t",
                                   base_ref=base_ref)
        return posts

    return run


class TestRealTransition:
    def test_scylladb_master_posts_done(self, jira_posts):
        assert jira_posts(SCYLLADB, "master") == [("SCYLLADB-1", "141")]
        assert jsm.get_sync_failures() == []

    def test_scylladb_backport_posts_merged(self, jira_posts):
        assert jira_posts(SCYLLADB, "branch-2026.4") == [("SCYLLADB-1", "7")]

    def test_already_closed_issue_is_left_alone(self, jira_posts):
        assert jira_posts(SCYLLADB, "master", status="Merged") == []
