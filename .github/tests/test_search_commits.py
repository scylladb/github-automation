"""
Unit tests for search_commits.py (promoted-to-* labeling) and
reconcile_promoted_labels.py (the RELENG-824 reconciliation safety net).
"""

import importlib
import os
import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session")
def sc_module(_set_env_vars):
    scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
    scripts_dir = os.path.abspath(scripts_dir)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    spec = importlib.util.spec_from_file_location(
        "search_commits",
        os.path.join(scripts_dir, "search_commits.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["search_commits"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def reconcile_module(sc_module):
    scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
    spec = importlib.util.spec_from_file_location(
        "reconcile_promoted_labels",
        os.path.join(scripts_dir, "reconcile_promoted_labels.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["reconcile_promoted_labels"] = mod
    spec.loader.exec_module(mod)
    return mod


def _commit(sha, message):
    commit = MagicMock()
    commit.sha = sha
    commit.commit = MagicMock()
    commit.commit.message = message
    return commit


def _response(json_data, ok=True, status_code=None):
    resp = MagicMock()
    resp.ok = ok
    resp.status_code = status_code if status_code is not None else (200 if ok else 500)
    resp.text = str(json_data)
    resp.json.return_value = json_data
    return resp


class TestGetPromotedLabelForRef:
    def test_master(self, sc_module):
        assert sc_module.get_promoted_label_for_ref("master") == "promoted-to-master"

    def test_next(self, sc_module):
        assert sc_module.get_promoted_label_for_ref("next") == "promoted-to-master"

    def test_branch(self, sc_module):
        assert sc_module.get_promoted_label_for_ref("refs/heads/branch-2026.3") == "promoted-to-branch-2026.3"

    def test_next_gating_branch(self, sc_module):
        assert sc_module.get_promoted_label_for_ref("next-2026.3") == "promoted-to-branch-2026.3"


class TestLabelPromotedCommits:
    def test_skips_gating_branch(self, sc_module):
        result = sc_module.label_promoted_commits(
            "scylladb/scylladb", [_commit("a", "msg")], "refs/heads/next-2026.3",
            "promoted-to-master", "tok",
        )
        assert result == set()

    def test_labels_pr_found_via_search_api(self, sc_module):
        commit = _commit("sha1", "Fix bug\n\nCloses scylladb/scylladb#123")
        search_resp = _response({"items": [{"number": 123, "body": "Fixes: SCYLLADB-1"}]})
        add_label_resp = _response({})

        with patch.object(sc_module.http, "get", return_value=search_resp) as mock_get, \
             patch.object(sc_module.http, "post", return_value=add_label_resp) as mock_post:
            processed = sc_module.label_promoted_commits(
                "scylladb/scylladb", [commit], "refs/heads/master",
                "promoted-to-master", "tok",
            )

        assert processed == {123}
        mock_post.assert_called_once()
        assert mock_post.call_args[0][0] == (
            "https://api.github.com/repos/scylladb/scylladb/issues/123/labels"
        )
        assert mock_post.call_args[1]["json"] == {"labels": ["promoted-to-master"]}
        mock_get.assert_called_once()  # only the search call, no 'Closes' fallback needed

    def test_labels_pr_via_closes_fallback_when_search_misses(self, sc_module):
        """Reproduces scylladb/scylladb#30992 (RELENG-824): PR closed by pushing a
        rebased commit directly, so the search-by-sha API finds nothing and the
        'Closes #NNN' fallback is what must catch it."""
        commit = _commit("rebased_sha", "s3_client: fix bug\n\nCloses scylladb/scylladb#30992")
        search_resp = _response({"items": []})
        pr_resp = _response({
            "number": 30992,
            "body": "Fixes: SCYLLADB-569",
            "state": "closed",
            "base": {"ref": "master"},
        })
        add_label_resp = _response({})

        with patch.object(sc_module.http, "get", side_effect=[search_resp, pr_resp]), \
             patch.object(sc_module.http, "post", return_value=add_label_resp) as mock_post:
            processed = sc_module.label_promoted_commits(
                "scylladb/scylladb", [commit], "refs/heads/master",
                "promoted-to-master", "tok",
            )

        assert processed == {30992}
        assert mock_post.call_args[0][0] == (
            "https://api.github.com/repos/scylladb/scylladb/issues/30992/labels"
        )

    def test_closes_fallback_skips_pr_targeting_other_branch(self, sc_module):
        commit = _commit("sha1", "Closes scylladb/scylladb#99")
        search_resp = _response({"items": []})
        pr_resp = _response({
            "number": 99,
            "body": "",
            "state": "closed",
            "base": {"ref": "branch-2025.4"},
        })

        with patch.object(sc_module.http, "get", side_effect=[search_resp, pr_resp]), \
             patch.object(sc_module.http, "post") as mock_post:
            processed = sc_module.label_promoted_commits(
                "scylladb/scylladb", [commit], "refs/heads/master",
                "promoted-to-master", "tok",
            )

        assert processed == set()
        mock_post.assert_not_called()

    def test_dedupes_pr_across_commits(self, sc_module):
        commits = [
            _commit("sha1", "Closes scylladb/scylladb#7"),
            _commit("sha2", "Closes scylladb/scylladb#7"),
        ]
        add_label_resp = _response({})

        with patch.object(sc_module.http, "get", side_effect=[
                 _response({"items": []}),
                 _response({"number": 7, "body": "", "state": "closed", "base": {"ref": "master"}}),
                 _response({"items": []}),
             ]), \
             patch.object(sc_module.http, "post", return_value=add_label_resp) as mock_post:
            processed = sc_module.label_promoted_commits(
                "scylladb/scylladb", commits, "refs/heads/master",
                "promoted-to-master", "tok",
            )

        assert processed == {7}
        mock_post.assert_called_once()

    def test_null_pr_body_does_not_crash(self, sc_module):
        """A PR with no description (body=None) must not raise and abort
        every later commit/PR - it just isn't a backport PR, so it gets the
        plain promoted label."""
        commit = _commit("sha1", "Fix bug\n\nCloses scylladb/scylladb#42")
        search_resp = _response({"items": [{"number": 42, "body": None}]})
        add_label_resp = _response({})

        with patch.object(sc_module.http, "get", return_value=search_resp), \
             patch.object(sc_module.http, "post", return_value=add_label_resp) as mock_post:
            processed = sc_module.label_promoted_commits(
                "scylladb/scylladb", [commit], "refs/heads/master",
                "promoted-to-master", "tok",
            )

        assert processed == {42}
        assert mock_post.call_args[1]["json"] == {"labels": ["promoted-to-master"]}

    def test_raises_on_failed_search_request(self, sc_module):
        commit = _commit("sha1", "msg")
        failed_resp = _response({"message": "rate limit exceeded"}, ok=False, status_code=403)

        with patch.object(sc_module.http, "get", return_value=failed_resp), \
             patch.object(sc_module.http, "post") as mock_post:
            with pytest.raises(RuntimeError, match="Searching for PRs"):
                sc_module.label_promoted_commits(
                    "scylladb/scylladb", [commit], "refs/heads/master",
                    "promoted-to-master", "tok",
                )

        mock_post.assert_not_called()

    def test_raises_on_failed_label_add_and_leaves_pr_unprocessed(self, sc_module):
        commit = _commit("sha1", "msg")
        search_resp = _response({"items": [{"number": 55, "body": ""}]})
        failed_post = _response({"message": "Internal Server Error"}, ok=False, status_code=500)

        with patch.object(sc_module.http, "get", return_value=search_resp), \
             patch.object(sc_module.http, "post", return_value=failed_post):
            with pytest.raises(RuntimeError, match="Adding label"):
                sc_module.label_promoted_commits(
                    "scylladb/scylladb", [commit], "refs/heads/master",
                    "promoted-to-master", "tok",
                )
        # A failure must not let the PR silently count as done.

    def test_404_on_closes_fallback_pr_fetch_is_not_an_error(self, sc_module):
        """A 'Closes #NNN' reference to a PR number that no longer exists
        (stale/typo) should be skipped quietly, not treated as an API failure."""
        commit = _commit("sha1", "Closes scylladb/scylladb#404")
        search_resp = _response({"items": []})
        not_found_resp = _response({"message": "Not Found"}, ok=False, status_code=404)

        with patch.object(sc_module.http, "get", side_effect=[search_resp, not_found_resp]), \
             patch.object(sc_module.http, "post") as mock_post:
            processed = sc_module.label_promoted_commits(
                "scylladb/scylladb", [commit], "refs/heads/master",
                "promoted-to-master", "tok",
            )

        assert processed == set()
        mock_post.assert_not_called()


class TestReconcilePromotedLabels:
    def test_reconciles_each_branch_with_lookback_window(self, reconcile_module, sc_module):
        commits = [_commit(f"sha{i}", "msg") for i in range(5)]

        repo = MagicMock()
        repo.get_commits.return_value = commits
        gh_instance = MagicMock()
        gh_instance.get_repo.return_value = repo

        with patch.object(reconcile_module, "Github", return_value=gh_instance), \
             patch.object(reconcile_module, "label_promoted_commits") as mock_label, \
             patch.object(reconcile_module, "parser") as mock_parser:
            mock_parser.return_value = MagicMock(
                repository="scylladb/scylladb",
                branches="master,branch-2026.3",
                lookback=3,
            )
            reconcile_module.main()

        assert mock_label.call_count == 2
        refs_used = {call.kwargs["ref"] for call in mock_label.call_args_list}
        assert refs_used == {"refs/heads/master", "refs/heads/branch-2026.3"}
        for call in mock_label.call_args_list:
            assert call.kwargs["repository"] == "scylladb/scylladb"
            assert len(call.kwargs["commits"]) == 3  # sliced to the lookback window

    def test_no_branches_is_a_noop(self, reconcile_module):
        with patch.object(reconcile_module, "Github") as mock_gh, \
             patch.object(reconcile_module, "parser") as mock_parser:
            mock_parser.return_value = MagicMock(
                repository="scylladb/scylladb", branches="  , ", lookback=500,
            )
            reconcile_module.main()

        mock_gh.assert_not_called()
