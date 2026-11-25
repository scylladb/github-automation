"""
Unit tests for where backport branches are pushed (RELENG-1173).

By default backport branches go to the scylladbbot fork. With
BACKPORT_SAME_REPO=true they go to the target repo itself, and the PR head
is '<repo owner>:backport/<pr>/to-<version>'.
"""

from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture
def same_repo(bp_module):
    with patch.object(bp_module, "SAME_REPO_BRANCHES", True):
        yield


def _repo(make_repo):
    repo = make_repo(full_name="scylladb/scylla-pkg", name="scylla-pkg")
    repo.owner = MagicMock()
    repo.owner.login = "scylladb"
    return repo


class TestForkModeUnchanged:
    def test_head_owner(self, bp_module, make_repo):
        assert bp_module.backport_head_owner(_repo(make_repo)) == "scylladbbot"

    def test_urls(self, bp_module, make_repo):
        token = bp_module.github_token
        repo = _repo(make_repo)
        assert bp_module.backport_clone_url(repo) == \
            f"https://scylladbbot:{token}@github.com/scylladb/scylla-pkg.git"
        assert bp_module.backport_push_url(repo) == \
            f"https://scylladbbot:{token}@github.com/scylladbbot/scylla-pkg.git"

    def test_heads(self, bp_module, make_repo):
        assert bp_module.backport_heads(_repo(make_repo), "backport/10/to-2026.4") == \
            ["scylladbbot:backport/10/to-2026.4"]

    def test_default_bot_logins(self, bp_module):
        assert "scylladbbot" in bp_module.BOT_LOGINS


class TestSameRepoMode:
    def test_head_owner(self, bp_module, make_repo, same_repo):
        assert bp_module.backport_head_owner(_repo(make_repo)) == "scylladb"

    def test_urls_point_at_target_repo(self, bp_module, make_repo, same_repo):
        token = bp_module.github_token
        repo = _repo(make_repo)
        expected = f"https://x-access-token:{token}@github.com/scylladb/scylla-pkg.git"
        assert bp_module.backport_clone_url(repo) == expected
        assert bp_module.backport_push_url(repo) == expected

    def test_heads_include_old_fork_head(self, bp_module, make_repo, same_repo):
        assert bp_module.backport_heads(_repo(make_repo), "backport/10/to-2026.4") == \
            ["scylladb:backport/10/to-2026.4", "scylladbbot:backport/10/to-2026.4"]

    def test_create_pull_request_uses_repo_head(self, bp_module, make_pr, make_repo, same_repo):
        repo = _repo(make_repo)
        repo.create_pull.return_value = make_pr(number=42)
        pr = make_pr(number=10, labels=[])

        bp_module.create_pull_request(repo, "backport/10/to-2026.4", "branch-2026.4", pr,
                                      "[Backport 2026.4] Fix", ["abc123"])

        assert repo.create_pull.call_args.kwargs["head"] == "scylladb:backport/10/to-2026.4"

    def test_find_existing_checks_new_then_old_head(self, bp_module, make_pr, make_repo, same_repo):
        repo = _repo(make_repo)
        old_pr = make_pr(number=7)
        # open: new head, old head (found)
        repo.get_pulls.side_effect = [[], [old_pr]]

        result = bp_module.find_existing_backport_pr(repo, 10, "2026.4")

        assert result.number == 7
        heads = [c.kwargs["head"] for c in repo.get_pulls.call_args_list]
        assert heads == ["scylladb:backport/10/to-2026.4", "scylladbbot:backport/10/to-2026.4"]

    def test_find_existing_checks_all_states_for_both_heads(self, bp_module, make_pr, make_repo, same_repo):
        repo = _repo(make_repo)
        merged_pr = make_pr(number=8, state="closed")
        # open: new, old; all: new (found)
        repo.get_pulls.side_effect = [[], [], [merged_pr]]

        result = bp_module.find_existing_backport_pr(repo, 10, "2026.4")

        assert result.number == 8
        assert repo.get_pulls.call_args_list[2].kwargs["state"] == "all"


class TestAppBotLogin:
    def test_dedup_accepts_app_bot(self, bp_module, make_pr, make_repo):
        repo = _repo(make_repo)
        other_pr = make_pr(number=99)
        other_pr.head = MagicMock()
        other_pr.head.ref = "backport/20/to-2026.4"
        other_pr.title = "[Backport 2026.4] Fix bug"
        other_pr.user = MagicMock()
        other_pr.user.login = "scylladb-backport[bot]"
        repo.get_pulls.side_effect = [[], [], [other_pr]]

        with patch.object(bp_module, "BOT_LOGINS", {"scylladbbot", "scylladb-backport[bot]"}):
            result = bp_module.find_existing_backport_pr(repo, 10, "2026.4", "Fix bug")

        assert result.number == 99

    def test_dedup_ignores_app_bot_when_not_configured(self, bp_module, make_pr, make_repo):
        repo = _repo(make_repo)
        other_pr = make_pr(number=99)
        other_pr.head = MagicMock()
        other_pr.head.ref = "backport/20/to-2026.4"
        other_pr.title = "[Backport 2026.4] Fix bug"
        other_pr.user = MagicMock()
        other_pr.user.login = "scylladb-backport[bot]"
        repo.get_pulls.side_effect = [[], [], [other_pr]]

        with patch.object(bp_module, "BOT_LOGINS", {"scylladbbot"}):
            result = bp_module.find_existing_backport_pr(repo, 10, "2026.4", "Fix bug")

        assert result is None


class TestBackportPush:
    def _run_backport(self, bp_module, repo, make_pr, make_commit):
        pr = make_pr(number=10, title="Fix bug")
        repo.get_commit.return_value = make_commit(sha="abc123", parents=[MagicMock()])
        local = MagicMock()
        with patch.object(bp_module, "is_commit_in_branch", return_value=False), \
             patch("tempfile.TemporaryDirectory") as mock_tmp, \
             patch.object(bp_module.Repo, "clone_from", return_value=local) as mock_clone, \
             patch.object(bp_module, "create_pull_request", return_value=make_pr(number=42)):
            mock_tmp.return_value.__enter__ = MagicMock(return_value="/tmp/test")
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
            bp_module.backport(repo, pr, "2026.4", ["abc123"], "branch-2026.4")
        return mock_clone.call_args[0][0], local.git.push.call_args[0]

    def test_fork_mode_pushes_to_fork(self, bp_module, make_pr, make_repo, make_commit):
        clone_url, push_args = self._run_backport(bp_module, _repo(make_repo), make_pr, make_commit)
        assert clone_url.endswith("@github.com/scylladb/scylla-pkg.git")
        assert push_args[0].endswith("@github.com/scylladbbot/scylla-pkg.git")
        assert push_args[1] == "backport/10/to-2026.4"

    def test_same_repo_mode_pushes_to_target_repo(self, bp_module, make_pr, make_repo, make_commit, same_repo):
        clone_url, push_args = self._run_backport(bp_module, _repo(make_repo), make_pr, make_commit)
        assert clone_url.startswith("https://x-access-token:")
        assert push_args[0] == clone_url
        assert push_args[1] == "backport/10/to-2026.4"
