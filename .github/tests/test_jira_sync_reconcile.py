"""
Unit tests for RELENG-1033 in the PR-events Jira sync:

  - a failed Jira transition or comment makes the run exit non-zero,
  - status_changed_since reads the issue changelog,
  - reconcile_merged_prs closes only the issues the sync missed.

Network is fully mocked.
"""

import importlib
import json
import os
import sys
from datetime import datetime, timezone

import pytest

# The jira sync scripts live in the top-level scripts/ dir (stdlib-only, safe to import).
_SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

jsm = importlib.import_module("jira_sync_modules")
jsl = importlib.import_module("jira_sync_logic")
rmp = importlib.import_module("reconcile_merged_prs")

MERGED_AT = datetime(2026, 10, 7, 15, 4, tzinfo=timezone.utc)
DETAILS_HEADER = jsm._CSV_HEADER


@pytest.fixture(autouse=True)
def _no_sleep_and_clean_failures(monkeypatch):
    monkeypatch.setattr(jsm.time, "sleep", lambda _s: None)
    jsm._SYNC_FAILURES.clear()
    yield
    jsm._SYNC_FAILURES.clear()


def _details_csv(*rows):
    lines = [DETAILS_HEADER]
    for key, status in rows:
        lines.append(f'"{key}","{status}","","","","","","","",""')
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Failed Jira writes fail the run
# ---------------------------------------------------------------------------

class TestWriteFailuresFailTheRun:
    def test_failed_transition_is_recorded(self, monkeypatch):
        monkeypatch.setattr(jsm, "_jira_put", lambda *a: (204, ""))
        monkeypatch.setattr(jsm, "_post_transition", lambda *a: (500, "boom"))
        jsm.jira_status_transition(_details_csv(("SCT-1", "In Progress")), "Merged", "7", "u:t")
        assert jsm.get_sync_failures() == ["1 transition(s) to 'Merged' failed"]

    def test_rejected_merged_falls_back_to_done_without_failure(self, monkeypatch):
        monkeypatch.setattr(jsm, "_jira_put", lambda *a: (204, ""))
        codes = {"7": (400, "not valid"), "141": (204, "")}
        monkeypatch.setattr(jsm, "_post_transition", lambda key, tid, auth: codes[tid])
        closed = jsm.jira_status_transition(_details_csv(("SCT-1", "In Progress")), "Merged", "7", "u:t")
        assert closed == ["SCT-1"]
        assert jsm.get_sync_failures() == []

    def test_missing_issue_is_not_a_failure(self, monkeypatch):
        monkeypatch.setattr(jsm, "_jira_put", lambda *a: (204, ""))
        monkeypatch.setattr(jsm, "_post_transition", lambda *a: (404, ""))
        jsm.jira_status_transition(_details_csv(("SCT-1", "In Progress")), "Merged", "7", "u:t")
        assert jsm.get_sync_failures() == []

    def test_failed_comment_is_recorded(self, monkeypatch):
        monkeypatch.setattr(jsm, "_jira_post", lambda *a: (0, "network"))
        jsm.add_comment_to_jira('["SCT-1"]', "hello ", "u:t")
        assert jsm.get_sync_failures() == ["1 comment(s) failed"]

    def test_main_exits_non_zero_after_running_the_handler(self, monkeypatch):
        ran = []

        def handler():
            ran.append(True)
            jsm.record_sync_failure("1 comment(s) failed")

        monkeypatch.setitem(jsl.ACTION_DISPATCH, "manage_closed_gh_event", handler)
        monkeypatch.setattr(sys, "argv", ["jira_sync_logic.py", "--action", "closed"])
        monkeypatch.setenv("CALLER_ACTION", "")  # main() sets it; restore it afterwards
        assert jsl.main() == 1
        assert ran == [True]

    def test_main_exits_zero_without_failures(self, monkeypatch):
        monkeypatch.setitem(jsl.ACTION_DISPATCH, "manage_closed_gh_event", lambda: None)
        monkeypatch.setattr(sys, "argv", ["jira_sync_logic.py", "--action", "closed"])
        monkeypatch.setenv("CALLER_ACTION", "")
        assert jsl.main() == 0


# ---------------------------------------------------------------------------
# status_changed_since
# ---------------------------------------------------------------------------

def _entry(created, field="status", author="someone"):
    return {"created": created, "author": {"accountId": author}, "items": [{"field": field}]}


class TestStatusChangedSince:
    def _pages(self, monkeypatch, pages):
        calls = []

        def fake_get(url, auth):
            calls.append(url)
            return pages[len(calls) - 1]

        monkeypatch.setattr(jsm, "_jira_get", fake_get)
        return calls

    def test_status_change_after_merge(self, monkeypatch):
        self._pages(monkeypatch, [{"values": [_entry("2026-10-08T10:00:00.000+0000")], "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is True

    def test_only_status_changes_before_merge(self, monkeypatch):
        self._pages(monkeypatch, [{"values": [_entry("2026-10-01T10:00:00.000+0000")], "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is False

    def test_non_status_change_after_merge_is_ignored(self, monkeypatch):
        self._pages(monkeypatch, [{"values": [_entry("2026-10-08T10:00:00.000+0300", field="labels")],
                                   "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is False

    def test_timezone_offset_is_honoured(self, monkeypatch):
        # 17:30+0300 is 14:30 UTC, before the 15:04 UTC merge.
        self._pages(monkeypatch, [{"values": [_entry("2026-10-07T17:30:00.000+0300")], "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is False

    def test_changes_by_the_sync_account_are_ignored(self, monkeypatch):
        self._pages(monkeypatch, [{"values": [_entry("2026-10-08T10:00:00.000+0000", author="sync-bot")],
                                   "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t", ignore_account_id="sync-bot") is False

    def test_a_person_changing_it_after_the_sync_counts(self, monkeypatch):
        self._pages(monkeypatch, [{"values": [
            _entry("2026-10-07T15:05:00.000+0000", author="sync-bot"),
            _entry("2026-10-08T10:00:00.000+0000", author="someone"),
        ], "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t", ignore_account_id="sync-bot") is True

    def test_reads_every_page(self, monkeypatch):
        calls = self._pages(monkeypatch, [
            {"values": [_entry("2026-10-01T10:00:00.000+0000")], "isLast": False},
            {"values": [_entry("2026-10-09T10:00:00.000+0000")], "isLast": True},
        ])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is True
        assert "startAt=1" in calls[1]

    def test_unreadable_changelog(self, monkeypatch):
        self._pages(monkeypatch, [None])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is None

    def test_unparsable_timestamp(self, monkeypatch):
        self._pages(monkeypatch, [{"values": [_entry("yesterday")], "isLast": True}])
        assert jsm.status_changed_since("SCT-1", MERGED_AT, "u:t") is None


# ---------------------------------------------------------------------------
# get_issue_status_category
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class TestGetIssueStatusCategory:
    def test_reads_the_category(self, monkeypatch):
        payload = {"fields": {"status": {"name": "Closed", "statusCategory": {"key": "done"}}}}
        monkeypatch.setattr(jsm, "urlopen", lambda req: _Resp(payload))
        assert jsm.get_issue_status_category("SCT-1", "u:t") == (200, "done")

    def test_http_error_returns_its_code(self, monkeypatch):
        def fail(req):
            raise jsm.HTTPError(req.full_url, 404, "Not Found", {}, None)

        monkeypatch.setattr(jsm, "urlopen", fail)
        assert jsm.get_issue_status_category("SCT-1", "u:t") == (404, "")

    def test_network_error_returns_zero(self, monkeypatch):
        def fail(req):
            raise jsm.URLError("down")

        monkeypatch.setattr(jsm, "urlopen", fail)
        assert jsm.get_issue_status_category("SCT-1", "u:t") == (0, "")


# ---------------------------------------------------------------------------
# reconcile_pr
# ---------------------------------------------------------------------------

PR = {
    "number": 16391,
    "title": "fix(sct): something",
    "body": "Fixes: SCT-1186",
    "html_url": "https://github.com/scylladb/scylla-cluster-tests/pull/16391",
    "pull_request": {"merged_at": "2026-10-07T15:04:00Z"},
}


class _Jira:
    """Stands in for the Jira helpers reconcile_pr uses."""

    # Status category of each status name used below; other names are "indeterminate".
    _DONE_STATUSES = {"Merged", "Done", "Won't Fix", "Duplicate", "Closed", "Resolved"}

    def __init__(self, monkeypatch, keys, statuses, changed=None, failing=None, status_errors=None,
                 edited=False):
        self.comments = []
        self.transitions = []
        changed = changed or {}
        status_errors = status_errors or {}
        monkeypatch.setattr(rmp, "extract_closed_pr_jira_keys", lambda *a: keys)
        monkeypatch.setattr(rmp, "_edited_after", lambda *a: edited)

        def status_category(key, auth):
            if key in status_errors:
                return status_errors[key], ""
            if key not in statuses:
                return 404, ""
            return 200, "done" if statuses[key] in self._DONE_STATUSES else "indeterminate"

        monkeypatch.setattr(rmp, "get_issue_status_category", status_category)

        def details(keys_json, auth):
            wanted = jsm._parse_jira_keys_json(keys_json)
            rows = [(k, statuses[k]) for k in wanted if k in statuses]
            return _details_csv(*rows), "", [k for k in wanted if k not in statuses]

        monkeypatch.setattr(rmp, "extract_jira_issue_details", details)
        self.ignored_accounts = []

        def status_changed(key, since, auth, ignore_account_id=""):
            self.ignored_accounts.append(ignore_account_id)
            return changed.get(key, False)

        monkeypatch.setattr(rmp, "status_changed_since", status_changed)
        monkeypatch.setattr(rmp, "add_comment_to_jira",
                            lambda keys_json, *a, **kw: self.comments.append(keys_json))
        failing = set(failing or ())

        def transition(csv_text, name, tid, auth):
            self.transitions.append((csv_text, name, tid))
            planned = [row[0] for row in jsm._plan_transitions(csv_text, name)[0]]
            return [k for k in planned if k not in failing]

        monkeypatch.setattr(rmp, "jira_status_transition", transition)


class TestReconcilePr:
    def test_missed_issue_is_closed(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == ["SCT-1186"]
        assert jira.comments == ['["SCT-1186"]']
        assert len(jira.transitions) == 1
        csv_text, name, tid = jira.transitions[0]
        assert '"SCT-1186"' in csv_text and name == "Merged" and tid == "7"

    def test_sync_account_is_passed_to_the_changelog_check(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"})
        rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", True, sync_account_id="sync-bot")
        assert jira.ignored_accounts == ["sync-bot"]

    def test_dry_run_reports_without_writing(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", True) == ["SCT-1186"]
        assert jira.comments == [] and jira.transitions == []

    @pytest.mark.parametrize("status", ["Merged", "Done", "Won't Fix", "Duplicate", "Closed", "Resolved"])
    def test_closed_issue_is_left_alone(self, monkeypatch, status):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": status})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == [] and jira.transitions == []

    def test_closed_status_name_outside_done_category_is_not_reported(self, monkeypatch):
        # A "Merged" status whose category is not "done": the transition would
        # leave it alone, so a dry run must not report it as missed.
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "Merged"})
        monkeypatch.setattr(rmp, "get_issue_status_category", lambda key, auth: (200, "indeterminate"))
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", True) == []
        assert jira.comments == [] and jira.transitions == []

    def test_issue_changed_after_merge_is_left_alone(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"}, changed={"SCT-1186": True})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == [] and jira.transitions == []
        assert jsm.get_sync_failures() == []

    def test_unreadable_changelog_is_a_failure_and_not_closed(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"}, changed={"SCT-1186": None})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == [] and jira.transitions == []
        assert len(jsm.get_sync_failures()) == 1

    def test_missing_issue_is_skipped(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-9999"], {})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == []
        assert jsm.get_sync_failures() == []

    @pytest.mark.parametrize("code", [0, 401, 403, 500])
    def test_unreadable_status_is_a_failure_and_not_closed(self, monkeypatch, code):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"}, status_errors={"SCT-1186": code})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == [] and jira.transitions == []
        assert len(jsm.get_sync_failures()) == 1

    def test_no_keys(self, monkeypatch):
        jira = _Jira(monkeypatch, ["__NO_KEYS_FOUND__"], {})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == []

    def test_only_the_missed_keys_are_written(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1", "SCT-2", "SCT-3"],
                     {"SCT-1": "In Progress", "SCT-2": "Done", "SCT-3": "In Review"},
                     changed={"SCT-3": True})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == ["SCT-1"]
        assert jira.comments == ['["SCT-1"]']
        assert '"SCT-2"' not in jira.transitions[0][0] and '"SCT-3"' not in jira.transitions[0][0]

    def test_failed_transition_is_not_commented(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1", "SCT-2"],
                     {"SCT-1": "In Progress", "SCT-2": "In Progress"}, failing={"SCT-2"})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == ["SCT-1"]
        assert jira.comments == ['["SCT-1"]']

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_pr_edited_after_merge_is_left_alone(self, monkeypatch, dry_run):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"}, edited=True)
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", dry_run) == []
        assert jira.comments == [] and jira.transitions == []
        assert jsm.get_sync_failures() == []

    def test_unreadable_edit_history_is_a_failure_and_not_closed(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"}, edited=None)
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []
        assert jira.comments == [] and jira.transitions == []
        assert len(jsm.get_sync_failures()) == 1

    def test_edit_history_is_not_read_without_missed_issues(self, monkeypatch):
        _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "Done"})
        monkeypatch.setattr(rmp, "_edited_after", lambda *a: pytest.fail("edit history read"))
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", PR, "gh", "u:t", False) == []

    def test_bad_merge_time_is_a_failure(self, monkeypatch):
        jira = _Jira(monkeypatch, ["SCT-1186"], {"SCT-1186": "In Progress"})
        pr = dict(PR, pull_request={"merged_at": None})
        assert rmp.reconcile_pr("scylladb/scylla-cluster-tests", pr, "gh", "u:t", False) == []
        assert jira.comments == []
        assert len(jsm.get_sync_failures()) == 1


def _graphql(last_edited=None, renamed=None):
    nodes = [{"createdAt": renamed}] if renamed else []
    pull = {"lastEditedAt": last_edited, "timelineItems": {"nodes": nodes}}
    return json.dumps({"data": {"repository": {"pullRequest": pull}}})


class TestEditedAfter:
    @pytest.mark.parametrize("last_edited, renamed, expected", [
        (None, None, False),
        ("2026-10-07T15:00:00Z", "2026-10-06T10:00:00Z", False),
        ("2026-10-08T09:00:00Z", None, True),
        (None, "2026-10-08T09:00:00Z", True),
    ])
    def test_edits_are_compared_with_the_merge_time(self, monkeypatch, last_edited, renamed, expected):
        calls = []
        monkeypatch.setattr(rmp, "_gh_api",
                            lambda *a: calls.append(a) or (200, _graphql(last_edited, renamed)))
        assert rmp._edited_after("scylladb/scylla-cluster-tests", 16391, MERGED_AT, "gh") is expected
        assert calls[0][3]["variables"] == {"owner": "scylladb", "name": "scylla-cluster-tests", "number": 16391}

    @pytest.mark.parametrize("response", [
        (500, "boom"),
        (200, "not json"),
        (200, json.dumps({"data": None, "errors": [{"message": "bad"}]})),
        (200, json.dumps({"data": {"repository": None}})),
        (200, _graphql(last_edited="2026-10-08T09:00:00.123Z")),
    ])
    def test_unreadable_history_is_none(self, monkeypatch, response):
        monkeypatch.setattr(rmp, "_gh_api", lambda *a: response)
        assert rmp._edited_after("scylladb/scylla-cluster-tests", 16391, MERGED_AT, "gh") is None


class TestSearchMergedPrs:
    SINCE = datetime(2026, 10, 1, tzinfo=timezone.utc)

    def _run(self, monkeypatch, pages):
        pages = iter(pages)
        monkeypatch.setattr(rmp, "_throttle_search", lambda: None)
        monkeypatch.setattr(rmp, "_gh_api", lambda *a: (200, json.dumps(next(pages))))
        return rmp._search_merged_prs("a/b", self.SINCE, "gh")

    def test_incomplete_page_is_retried(self, monkeypatch):
        prs = self._run(monkeypatch, [
            {"incomplete_results": True, "items": [{"number": 1}]},
            {"incomplete_results": False, "items": [{"number": 1}, {"number": 2}]},
        ])
        assert [pr["number"] for pr in prs] == [1, 2]
        assert jsm.get_sync_failures() == []

    def test_still_incomplete_page_is_a_failure(self, monkeypatch):
        page = {"incomplete_results": True, "items": [{"number": 1}]}
        prs = self._run(monkeypatch, [page] * (rmp._SEARCH_INCOMPLETE_RETRIES + 1))
        assert [pr["number"] for pr in prs] == [1]
        assert len(jsm.get_sync_failures()) == 1


class TestBackportTitle:
    @pytest.mark.parametrize("title", [
        "[Backport 2026.1] fix", "[Backport manager-3.4] fix", "[Backport perf-v15] fix",
    ])
    def test_backport_pr_keys_come_from_the_body_only(self, monkeypatch, title):
        calls = []
        monkeypatch.setattr(jsl, "extract_jira_keys", lambda *a, **kw: calls.append(kw) or ["SCT-2"])
        assert jsl.extract_closed_pr_jira_keys(title, "Fixes: SCT-2", 1, "a/b", "gh", "u:t") == ["SCT-2"]
        assert calls == [{}]

    def test_regular_pr_keys_include_the_commits(self, monkeypatch):
        calls = []
        monkeypatch.setattr(jsl, "extract_jira_keys", lambda *a, **kw: calls.append(kw) or ["SCT-1"])
        jsl.extract_closed_pr_jira_keys("fix(sct): perf-v15 thing", "", 1, "a/b", "gh", "u:t")
        assert calls == [{"owner_repo": "a/b", "pr_number": 1, "gh_token": "gh"}]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

class TestReconcileMain:
    def _run(self, monkeypatch, argv, search):
        monkeypatch.setenv("GITHUB_TOKEN", "gh")
        monkeypatch.setenv("JIRA_AUTH", "u:t")
        monkeypatch.setattr(sys, "argv", ["reconcile_merged_prs.py"] + argv)
        monkeypatch.setattr(rmp, "_search_merged_prs", search)
        monkeypatch.setattr(rmp, "get_jira_account_id", lambda auth: "sync-bot")
        seen = []
        monkeypatch.setattr(rmp, "reconcile_pr",
                            lambda repo, pr, gh, auth, dry, sync_account_id="":
                            seen.append((repo, pr["number"], dry, sync_account_id)) or [])
        return rmp.main(), seen

    def test_recent_merges_are_skipped(self, monkeypatch):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        prs = [dict(PR), dict(PR, number=2, pull_request={"merged_at": now})]
        code, seen = self._run(monkeypatch, ["--repositories", "a/b"], lambda *a: prs)
        assert code == 0
        assert seen == [("a/b", 16391, False, "sync-bot")]

    def test_search_failure_fails_the_run_but_other_repos_still_run(self, monkeypatch):
        code, seen = self._run(monkeypatch, ["--repositories", "a/b, c/d", "--dry-run"],
                               lambda repo, *a: None if repo == "a/b" else [dict(PR)])
        assert code == 1
        assert seen == [("c/d", 16391, True, "sync-bot")]
