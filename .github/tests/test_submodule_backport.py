"""
Unit tests for submodule update handling in auto-backport-jira.py.

Tests:
  - get_submodule_updates
  - resolve_submodule_repo
  - find_submodule_fix_commits
  - find_missing_commits
  - get_bot_fork
  - backport_submodule_commits
  - create_submodule_bump_pr
  - backport_submodule_update
  - backport() handing a submodule update to backport_submodule_update
  - create_pull_request with draft_reason
"""

import pytest
from unittest.mock import patch, MagicMock

OLD = "f47769f69cb840347b3e15bae36df3006d007ba3"
NEW = "9b7c1aa2bc64f754e420989fafd5aee97ce14d8b"

GITMODULES = """[submodule "seastar"]
\tpath = seastar
\turl = ../scylla-seastar
\tignore = dirty
[submodule "tools/cqlsh"]
\tpath = tools/cqlsh
\turl = https://github.com/scylladb/scylla-cqlsh.git
"""


def _make_file(filename, patch_text):
    f = MagicMock()
    f.filename = filename
    f.patch = patch_text
    return f


def _sub_commit(make_commit, sha, message, merge=False):
    c = make_commit(sha=sha, message=message, parents=[MagicMock(), MagicMock()] if merge else [MagicMock()])
    c.html_url = f"https://github.com/scylladb/seastar/commit/{sha}"
    return c


class TestGetSubmoduleUpdates:
    def test_detects_gitlink_change(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit()
        commit.files = [
            _make_file("seastar", f"@@ -1 +1 @@\n-Subproject commit {OLD}\n+Subproject commit {NEW}"),
            _make_file("main.cc", "@@ -1 +1 @@\n-a\n+b"),
        ]
        repo.get_commit.return_value = commit
        assert bp_module.get_submodule_updates(repo, "abc") == [("seastar", OLD, NEW)]

    def test_regular_commit_has_no_updates(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit()
        commit.files = [_make_file("main.cc", "@@ -1 +1 @@\n-a\n+b"), _make_file("image.png", None)]
        repo.get_commit.return_value = commit
        assert bp_module.get_submodule_updates(repo, "abc") == []


class TestResolveSubmoduleRepo:
    def _repo(self, make_repo):
        repo = make_repo()
        repo.get_contents.return_value.decoded_content = GITMODULES.encode()
        return repo

    def test_relative_url(self, bp_module, make_repo):
        assert bp_module.resolve_submodule_repo(self._repo(make_repo), "seastar", "branch-2026.3") == "scylladb/scylla-seastar"

    def test_absolute_url(self, bp_module, make_repo):
        assert bp_module.resolve_submodule_repo(self._repo(make_repo), "tools/cqlsh", "master") == "scylladb/scylla-cqlsh"

    def test_unknown_path(self, bp_module, make_repo):
        assert bp_module.resolve_submodule_repo(self._repo(make_repo), "abseil", "master") is None

    def test_missing_gitmodules(self, bp_module, make_repo):
        repo = make_repo()
        repo.get_contents.side_effect = Exception("404")
        assert bp_module.resolve_submodule_repo(repo, "seastar", "master") is None


class TestFindSubmoduleFixCommits:
    def test_matches_jira_key_and_skips_merges(self, bp_module, make_commit):
        fix = _sub_commit(make_commit, "a1", "resource: fix NUMA\n\nFixes: https://scylladb.atlassian.net/browse/SCYLLADB-5118")
        other = _sub_commit(make_commit, "a2", "build: remove valgrind support")
        merge = _sub_commit(make_commit, "a3", "Merge 'x'\n\nFixes: SCYLLADB-5118", merge=True)
        similar = _sub_commit(make_commit, "a4", "io: fix\n\nFixes: SCYLLADB-51180")
        sub_repo = MagicMock()
        sub_repo.compare.return_value.commits = [fix, other, merge, similar]

        assert bp_module.find_submodule_fix_commits(sub_repo, OLD, NEW, ["SCYLLADB-5118"]) == [fix]
        sub_repo.compare.assert_called_once_with(OLD, NEW)

    def test_no_jira_keys(self, bp_module):
        sub_repo = MagicMock()
        assert bp_module.find_submodule_fix_commits(sub_repo, OLD, NEW, []) == []
        sub_repo.compare.assert_not_called()


class TestFindMissingCommits:
    def test_matches_by_title(self, bp_module, make_commit):
        backported = _sub_commit(make_commit, "a1", "resource: fix NUMA\n\nbody")
        missing = _sub_commit(make_commit, "a2", "shared_ptr: fix use-after-free")
        target_repo = MagicMock()
        target_repo.get_commits.return_value = [make_commit(sha="b1", message="resource: fix NUMA\n\n(cherry picked from commit a1)")]
        assert bp_module.find_missing_commits(target_repo, "branch-2026.3", [backported, missing]) == [missing]

    def test_error_reports_all_missing(self, bp_module, make_commit):
        commits = [_sub_commit(make_commit, "a1", "resource: fix NUMA")]
        target_repo = MagicMock()
        target_repo.get_commits.side_effect = Exception("boom")
        assert bp_module.find_missing_commits(target_repo, "branch-2026.3", commits) == commits


class TestGetBotFork:
    def test_existing_fork(self, bp_module):
        g = MagicMock()
        target = MagicMock()
        target.name = "scylla-seastar"
        assert bp_module.get_bot_fork(g, target) is g.get_repo.return_value
        g.get_repo.assert_called_once_with("scylladbbot/scylla-seastar")
        target.create_fork.assert_not_called()

    def test_creates_missing_fork(self, bp_module):
        from github import GithubException
        g = MagicMock()
        g.get_repo.side_effect = GithubException(404, "Not Found")
        target = MagicMock()
        assert bp_module.get_bot_fork(g, target) is target.create_fork.return_value


class TestBackportSubmoduleCommits:
    def _setup(self, make_commit):
        target = MagicMock()
        target.full_name = "scylladb/scylla-seastar"
        target.get_pulls.return_value = []
        fix = _sub_commit(make_commit, NEW, "resource: fix NUMA\n\nFixes: SCYLLADB-5118")
        return target, fix

    def _run(self, bp_module, target, fix, pr, local):
        fork = MagicMock()
        fork.full_name = "scylladbbot/scylla-seastar"
        with patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=local) as mock_clone, \
             patch.object(bp_module, "get_bot_fork", return_value=fork):
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
            result = bp_module.backport_submodule_commits(MagicMock(), target, "branch-2026.3", "2026.3",
                                                          [fix], "scylladb/seastar", pr)
            mock_clone.assert_called_once()
            assert mock_clone.call_args[1]["branch"] == "branch-2026.3"
            return result

    def test_cherry_picks_from_source_and_opens_pr(self, bp_module, make_pr, make_commit):
        target, fix = self._setup(make_commit)
        pr = make_pr(number=32182, html_url="https://github.com/scylladb/scylladb/pull/32182")
        local = MagicMock()

        result = self._run(bp_module, target, fix, pr, local)

        assert result is target.create_pull.return_value
        local.git.fetch.assert_called_once_with("https://github.com/scylladb/seastar.git", NEW)
        local.git.cherry_pick.assert_called_once_with(NEW, "-x")
        assert "scylladbbot/scylla-seastar.git" in local.git.push.call_args[0][0]
        kwargs = target.create_pull.call_args[1]
        assert kwargs["title"] == "[Backport 2026.3] resource: fix NUMA"
        assert kwargs["head"] == "scylladbbot:backport/32182/to-2026.3"
        assert kwargs["base"] == "branch-2026.3"
        assert kwargs["draft"] is False
        assert f"(cherry picked from commit {NEW})" in kwargs["body"]
        assert "Needed for the backport of https://github.com/scylladb/scylladb/pull/32182" in kwargs["body"]
        result.add_to_assignees.assert_called_once()

    def test_conflict_opens_draft(self, bp_module, make_pr, make_commit):
        from git import GitCommandError
        target, fix = self._setup(make_commit)
        local = MagicMock()
        local.git.cherry_pick.side_effect = [GitCommandError("cherry-pick", "conflict"), None]

        result = self._run(bp_module, target, fix, make_pr(), local)

        local.git.add.assert_called_with(A=True)
        assert target.create_pull.call_args[1]["draft"] is True
        result.create_issue_comment.assert_called_once()

    def test_reuses_existing_pr(self, bp_module, make_pr, make_commit):
        target, fix = self._setup(make_commit)
        existing = MagicMock()
        target.get_pulls.return_value = [existing]
        with patch.object(bp_module.Repo, "clone_from") as mock_clone:
            assert bp_module.backport_submodule_commits(MagicMock(), target, "branch-2026.3", "2026.3",
                                                        [fix], "scylladb/seastar", make_pr()) is existing
            mock_clone.assert_not_called()

    def test_git_failure_returns_none(self, bp_module, make_pr, make_commit):
        from git import GitCommandError
        target, fix = self._setup(make_commit)
        local = MagicMock()
        local.git.push.side_effect = GitCommandError("push", "denied")

        assert self._run(bp_module, target, fix, make_pr(), local) is None
        target.create_pull.assert_not_called()


class TestCreateSubmoduleBumpPr:
    def test_writes_gitlink_and_opens_pr(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        pr = make_pr(number=32182, title="Update seastar submodule")
        fix = _sub_commit(make_commit, NEW, "resource: fix NUMA")
        local = MagicMock()

        with patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=local), \
             patch.object(bp_module, "create_pull_request") as mock_create:
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
            result = bp_module.create_submodule_bump_pr(
                repo, pr, "2026.3", "branch-2026.3", [("seastar", OLD, NEW)], [fix], ["SCYLLADB-5118"],
                pr_body="BODY", draft_reason="wait", jira_mapping={"SCYLLADB-5118": "SCYLLADB-5312"},
                remaining_backport_labels=["backport/2026.2"])

            assert result is mock_create.return_value
            local.git.update_index.assert_called_once_with("--cacheinfo", f"160000,{NEW},seastar")
            message = local.git.commit.call_args[0][1]
            assert message.startswith("Update seastar submodule\n\n")
            assert f"* seastar {OLD[:10]}...{NEW[:10]}:" in message
            assert "  > resource: fix NUMA" in message
            assert "Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-5312" in message
            args, kwargs = mock_create.call_args
            assert args[1:5] == ("backport/32182/to-2026.3", "branch-2026.3", pr,
                                 "[Backport 2026.3] Update seastar submodule")
            assert kwargs["draft_reason"] == "wait"
            assert kwargs["remaining_backport_labels"] == ["backport/2026.2"]
            assert "jira_mapping" not in kwargs

    def test_git_failure_returns_none(self, bp_module, make_pr, make_repo):
        from git import GitCommandError
        local = MagicMock()
        local.git.push.side_effect = GitCommandError("push", "denied")
        with patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=local), \
             patch.object(bp_module, "create_pull_request") as mock_create:
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
            assert bp_module.create_submodule_bump_pr(make_repo(), make_pr(), "2026.3", "branch-2026.3",
                                                      [("seastar", OLD, NEW)], [], [], "BODY", None) is None
            mock_create.assert_not_called()


class TestBackportSubmoduleUpdate:
    def _patches(self, bp_module, fix_commits, missing, sub_pr=None, bump_pr=None):
        def resolve(_repo, _path, ref):
            return "scylladb/seastar" if ref == "sha1" else "scylladb/scylla-seastar"
        return [patch.object(bp_module, "Github"),
                patch.object(bp_module, "resolve_submodule_repo", side_effect=resolve),
                patch.object(bp_module, "find_submodule_fix_commits", return_value=fix_commits),
                patch.object(bp_module, "find_missing_commits", return_value=missing),
                patch.object(bp_module, "backport_submodule_commits", return_value=sub_pr),
                patch.object(bp_module, "create_submodule_bump_pr", return_value=bump_pr),
                patch.object(bp_module, "add_jira_comment"),
                patch.object(bp_module, "replace_backport_label_with_done")]

    def _run(self, bp_module, patches, repo, pr, **kwargs):
        from contextlib import ExitStack
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            result = bp_module.backport_submodule_update(repo, pr, "sha1", [("seastar", OLD, NEW)], "2026.3",
                                                         "branch-2026.3", **kwargs)
            return result, mocks

    def _pr(self, make_pr):
        pr = make_pr(number=32182, body="Fixes: https://scylladb.atlassian.net/browse/SCYLLADB-5118",
                     user_login="avikivity")
        pr.get_issue_comments = MagicMock(return_value=[])
        return pr

    def test_opens_submodule_pr_and_draft_bump_for_one_version(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        repo.get_contents.return_value.sha = OLD
        pr = self._pr(make_pr)
        fix = _sub_commit(make_commit, NEW, "resource: build IO groups from the booted shards' NUMA nodes")
        sub_pr = MagicMock(html_url="https://github.com/scylladb/scylla-seastar/pull/21", draft=False)
        sub_pr.head.sha = "c" * 40
        bump_pr = MagicMock(html_url="https://github.com/scylladb/scylladb/pull/99")

        result, mocks = self._run(bp_module, self._patches(bp_module, [fix], [fix], sub_pr, bump_pr), repo, pr,
                                  jira_mapping={"SCYLLADB-5118": "SCYLLADB-5312"},
                                  remaining_backport_labels=["backport/2026.2"], pr_body="BODY")

        assert result is bump_pr
        mock_sub, mock_bump, mock_jira = mocks[4], mocks[5], mocks[6]
        assert mock_sub.call_count == 1
        assert mock_sub.call_args[0][2:6] == ("branch-2026.3", "2026.3", [fix], "scylladb/seastar")
        args, kwargs = mock_bump.call_args
        assert args[2:5] == ("2026.3", "branch-2026.3", [("seastar", OLD, "c" * 40)])
        assert "scylla-seastar/pull/21" in kwargs["draft_reason"]
        assert kwargs["remaining_backport_labels"] == ["backport/2026.2"]
        assert kwargs["jira_mapping"] == {"SCYLLADB-5118": "SCYLLADB-5312"}
        body = pr.create_issue_comment.call_args[0][0]
        assert "<!-- submodule-backport:sha1:branch-2026.3 -->" in body
        assert "- https://github.com/scylladb/scylla-seastar/pull/21\n" in body
        assert "- https://github.com/scylladb/scylladb/pull/99 (submodule update" in body
        mock_jira.assert_called_once()

    def test_already_backported_bumps_to_branch_head(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        repo.get_contents.return_value.sha = OLD
        fix = _sub_commit(make_commit, NEW, "resource: fix NUMA")
        bump_pr = MagicMock()
        patches = self._patches(bp_module, [fix], [], bump_pr=bump_pr)

        from contextlib import ExitStack
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            mocks[0].return_value.get_repo.return_value.get_branch.return_value.commit.sha = "d" * 40
            result = bp_module.backport_submodule_update(repo, self._pr(make_pr), "sha1", [("seastar", OLD, NEW)],
                                                         "2026.3", "branch-2026.3")
        assert result is bump_pr
        mocks[4].assert_not_called()
        args, kwargs = mocks[5].call_args
        assert args[4] == [("seastar", OLD, "d" * 40)]
        assert kwargs["draft_reason"] is None

    def test_submodule_already_up_to_date_marks_done(self, bp_module, make_pr, make_repo, make_commit):
        repo = make_repo()
        repo.get_contents.return_value.sha = "d" * 40
        pr = self._pr(make_pr)
        fix = _sub_commit(make_commit, NEW, "resource: fix NUMA")
        patches = self._patches(bp_module, [fix], [])

        from contextlib import ExitStack
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in patches]
            mocks[0].return_value.get_repo.return_value.get_branch.return_value.commit.sha = "d" * 40
            result = bp_module.backport_submodule_update(repo, pr, "sha1", [("seastar", OLD, NEW)],
                                                         "2026.3", "branch-2026.3")
        assert result is None
        mocks[5].assert_not_called()
        mocks[7].assert_called_once_with(repo, pr, "2026.3")

    def test_failed_submodule_backport(self, bp_module, make_pr, make_repo, make_commit):
        pr = self._pr(make_pr)
        fix = _sub_commit(make_commit, NEW, "resource: fix NUMA")
        result, mocks = self._run(bp_module, self._patches(bp_module, [fix], [fix], sub_pr=None), make_repo(), pr)
        assert result is None
        mocks[5].assert_not_called()
        assert "Failed to open a `scylladb/scylla-seastar` `branch-2026.3` backport PR for 9b7c1aa2bc" in \
            pr.create_issue_comment.call_args[0][0]

    def test_no_matching_commits_links_range(self, bp_module, make_pr, make_repo):
        pr = self._pr(make_pr)
        result, mocks = self._run(bp_module, self._patches(bp_module, [], []), make_repo(), pr)
        assert result is None
        mocks[4].assert_not_called()
        body = pr.create_issue_comment.call_args[0][0]
        assert "No `seastar` commit in" in body
        assert f"https://github.com/scylladb/seastar/compare/{OLD[:10]}...{NEW[:10]}" in body

    def test_comment_not_repeated(self, bp_module, make_pr, make_repo):
        pr = self._pr(make_pr)
        existing = MagicMock()
        existing.body = "<!-- submodule-backport:sha1:branch-2026.3 -->\nold"
        pr.get_issue_comments.return_value = [existing]
        self._run(bp_module, self._patches(bp_module, [], []), make_repo(), pr)
        pr.create_issue_comment.assert_not_called()


class TestBackportWithSubmoduleUpdate:
    def test_backports_submodule_instead_of_cherry_picking(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(number=32182, title="Update seastar submodule")
        bump_pr = MagicMock()

        with patch.object(bp_module, "is_commit_in_branch", return_value=True) as mock_in_branch, \
             patch.object(bp_module, "get_submodule_updates", return_value=[("seastar", OLD, NEW)]), \
             patch.object(bp_module, "backport_submodule_update", return_value=bump_pr) as mock_sub, \
             patch.object(bp_module.Repo, "clone_from") as mock_clone:
            result = bp_module.backport(repo, pr, "2026.3", ["sha1"], "branch-2026.3", pr_body="BODY", original_pr=pr,
                                        remaining_backport_labels=["backport/2026.2", "backport/2025.1"],
                                        jira_mapping={"SCYLLADB-5118": "SCYLLADB-5312"})

            assert result is bump_pr
            # "Update seastar submodule" titles repeat on every branch, so the
            # title-based check must not run before the submodule detection.
            mock_in_branch.assert_not_called()
            mock_clone.assert_not_called()
            mock_sub.assert_called_once_with(
                repo, pr, "sha1", [("seastar", OLD, NEW)], "2026.3", "branch-2026.3",
                original_pr=pr, jira_mapping={"SCYLLADB-5118": "SCYLLADB-5312"}, pr_body="BODY",
                jira_failed=False, remaining_backport_labels=["backport/2026.2", "backport/2025.1"],
                warn_missing_fixes=False)


class TestCreatePullRequestDraftReason:
    def test_draft_without_conflicts_label(self, bp_module, make_pr, make_repo):
        repo = make_repo()
        pr = make_pr(user_login="avikivity")
        created = repo.create_pull.return_value
        with patch.object(bp_module, "set_pr_milestone"):
            bp_module.create_pull_request(repo, "b", "branch-2026.3", pr, "t", [], pr_body="BODY",
                                          draft_reason="wait for seastar",
                                          remaining_backport_labels=["backport/2026.2"])
        assert repo.create_pull.call_args[1]["draft"] is True
        created.create_issue_comment.assert_called_once_with("@avikivity wait for seastar")
        created.add_to_labels.assert_called_once_with("backport/2026.2")
