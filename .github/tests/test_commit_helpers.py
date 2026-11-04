"""
Unit tests for get_pr_commits and is_commit_in_branch in auto-backport-jira.py.

Tests:
  - get_pr_commits (merged PR, squash merge, merge commit, closed PR, direct push)
  - is_commit_in_branch
"""

import pytest
from unittest.mock import patch, MagicMock


class TestGetPrCommits:
    def test_merged_pr_with_merge_commit(self, bp_module, make_pr, make_repo, make_commit):
        """Merged PR with a merge commit (multiple parents) returns individual commit SHAs."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="merge123")

        merge_commit = make_commit(sha="merge123", parents=[MagicMock(), MagicMock()])
        repo.get_commit.return_value = merge_commit

        # PR has individual commits
        c1 = MagicMock()
        c1.sha = "commit1"
        c2 = MagicMock()
        c2.sha = "commit2"
        pr.get_commits.return_value = [c1, c2]

        result = bp_module.get_pr_commits(repo, pr, "master")
        assert result == ["commit1", "commit2"]

    def test_merged_pr_squash_with_start_commit(self, bp_module, make_pr, make_repo, make_commit):
        """Squash-merged PR with start_commit uses compare to find promoted commits."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")

        # Single parent -> not a merge commit; a rebased one-commit PR keeps its commit message
        squash_commit = make_commit(sha="squash123", message="Fix bug\n\nSome details", parents=[MagicMock()])
        repo.get_commit.return_value = squash_commit

        # PR has one commit
        pr_commit = MagicMock()
        pr_commit.commit.message = "Fix bug\n\nSome details"
        pr.get_commits.return_value = [pr_commit]

        # Compare returns promoted commits
        promoted_commit = make_commit(sha="promoted123", message="Fix bug")
        promoted_commit.commit.message = "Fix bug"
        comparison = MagicMock()
        comparison.commits = [promoted_commit]
        repo.compare.return_value = comparison

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert "promoted123" in result

    def test_merged_pr_squash_multi_commit(self, bp_module, make_pr, make_repo, make_commit):
        """Squash-merged multi-commit PR resolves to the squash commit (RELENG-165).

        The PR commits never reach the branch, so matching their titles found nothing,
        or an unrelated commit that shares a title like Copilot's 'Initial plan'.
        """
        repo = make_repo()
        pr = make_pr(number=12993, merged=True, merge_commit_sha="squash123")
        repo.get_commit.return_value = make_commit(
            sha="squash123", message="fix(args): keep repeated args (#12993)\n\n* Initial plan\n* Fix")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix pre-commit failures"),
        ]

        repo.compare.return_value = MagicMock(behind_by=0)

        with patch.object(bp_module, '_find_commit_on_stable_branch') as find:
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit=None)
        assert result == ["squash123"]
        repo.compare.assert_called_once_with("squash123", "master")
        find.assert_not_called()

    def test_merged_pr_squash_api_error_is_not_read_as_missing(self, bp_module, make_pr, make_repo, make_commit):
        """An API error while checking the squash commit fails the run instead of skipping the backport."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")
        repo.get_commit.return_value = make_commit(sha="squash123", message="Fix bug (#10)")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]
        repo.compare.side_effect = RuntimeError("API rate limit exceeded")

        with pytest.raises(RuntimeError):
            bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")

    def test_merged_pr_squash_without_start_commit_skips_history_scan(self, bp_module, make_pr, make_repo,
                                                                       make_commit):
        """A squash is resolved on the branch directly, without listing the whole branch history."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")
        repo.get_commit.return_value = make_commit(sha="squash123", message="Fix bug (#10)")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="squash123"):
            result = bp_module.get_pr_commits(repo, pr, "branch-2025.4", start_commit=None)
        assert result == ["squash123"]
        repo.get_commits.assert_not_called()

    def test_merged_pr_squash_rewritten_on_stable_branch(self, bp_module, make_pr, make_repo, make_commit):
        """A squash commit a promoter rewrote is backported from its copy on the stable branch."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash_on_next")
        repo.get_commit.return_value = make_commit(sha="squash_on_next", message="Fix bug (#10)")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="squash_on_master"):
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit=None)
        assert result == ["squash_on_master"]

    def test_merged_pr_squash_not_on_stable_branch(self, bp_module, make_pr, make_repo, make_commit, caplog):
        """A squash commit not on the stable branch is not backported, and no other commit is picked.

        With no close event pointing at it either, nothing is found."""
        import logging
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")
        repo.get_commit.return_value = make_commit(sha="squash123", message="Fix bug (#10)")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]

        repo.compare.return_value = MagicMock(behind_by=3)
        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value=None), \
             caplog.at_level(logging.WARNING):
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit=None)
        assert result == []
        assert any("not found on master" in msg for msg in caplog.messages)

    def test_merged_pr_squash_titled_like_a_pr_commit(self, bp_module, make_pr, make_repo, make_commit):
        """A squash whose title equals one PR commit's title is still a squash: its body differs."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")
        repo.get_commit.return_value = make_commit(sha="squash123", message="Fix bug\n\n* Initial plan\n* Fix bug")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="squash123"):
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit=None)
        assert result == ["squash123"]

    def test_rebase_with_edited_last_message_is_not_a_squash(self, bp_module, make_repo, make_commit):
        """A rebase merge whose last commit message was edited (e.g. a Closes line) keeps every PR commit."""
        repo = make_repo()
        merge_commit = make_commit(sha="rebased2", message="Second change\n\nCloses #10",
                                   parents=[MagicMock(sha="rebased1")])
        repo.get_commit.return_value = make_commit(sha="rebased1", message="First change")
        pr_commits = [
            make_commit(sha="head1", message="First change"),
            make_commit(sha="head2", message="Second change"),
        ]

        assert bp_module.is_squash_merge(repo, merge_commit, pr_commits) is False
        assert bp_module.get_rebase_merged_commits(repo, merge_commit, pr_commits) == ["rebased1", "rebased2"]

    def test_single_commit_squash_is_a_squash(self, bp_module, make_repo, make_commit):
        """A one-commit PR squashed to 'Title (#N)' is a squash."""
        repo = make_repo()
        merge_commit = make_commit(sha="squash", message="Fix bug (#10)\n\nbody")
        pr_commits = [make_commit(sha="head1", message="Fix bug")]

        assert bp_module.is_squash_merge(repo, merge_commit, pr_commits) is True

    def test_empty_title_pr_commit_is_never_a_squash(self, bp_module, make_repo, make_commit):
        """An empty title can't be told apart from another empty title, so it is not called a squash."""
        repo = make_repo()
        merge_commit = make_commit(sha="m", message="Other (#10)")
        pr_commits = [make_commit(sha="head1", message="")]

        assert bp_module.is_squash_merge(repo, merge_commit, pr_commits) is False

    def test_rebase_merged_commits_empty_titles_return_nothing(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        merge_commit = make_commit(sha="m2", message="", parents=[MagicMock(sha="m1")])
        repo.get_commit.return_value = make_commit(sha="m1", message="")
        pr_commits = [make_commit(sha="h1", message=""), make_commit(sha="h2", message="")]

        assert bp_module.get_rebase_merged_commits(repo, merge_commit, pr_commits) == []

    def test_find_commit_on_stable_branch_empty_title_does_not_match(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        repo.get_commit.return_value = make_commit(sha="orig", message="")
        repo.compare.return_value = MagicMock(behind_by=2)
        repo.get_commits.return_value = [make_commit(sha="unrelated", message="")]

        assert bp_module._find_commit_on_stable_branch(repo, "orig", "master") is None

    def test_merged_pr_squash_with_start_commit_searches_promoted_range(self, bp_module, make_pr, make_repo,
                                                                         make_commit):
        """start_commit still bounds the search: the squash (or its rewritten copy) must be in the promoted range."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash_on_next")
        repo.get_commit.return_value = make_commit(sha="squash_on_next", message="Fix bug (#10)")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]
        repo.compare.return_value = MagicMock(commits=[
            make_commit(sha="other", message="Other change"),
            make_commit(sha="squash_on_master", message="Fix bug (#10)"),
        ])

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert result == ["squash_on_master"]
        repo.compare.assert_called_once_with("before123", "master")

    def test_merged_pr_squash_is_start_commit(self, bp_module, make_pr, make_repo, make_commit):
        """When start_commit is the squash commit itself, compare() excludes it, so it is returned directly."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash_sha")
        repo.get_commit.return_value = make_commit(sha="squash_sha", message="Fix bug (#10)")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]
        repo.compare.return_value = MagicMock(commits=[])

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="squash_sha")
        assert result == ["squash_sha"]

    def test_merged_pr_squash_not_found_uses_close_event_fallback(self, bp_module, make_pr, make_repo, make_commit):
        """A squash not found on the branch falls through to the closed-PR fallback instead of returning nothing."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash_on_next", state="closed")
        squash = make_commit(sha="squash_on_next", message="Fix bug (#10)", parents=[MagicMock(sha="p1")])
        promoted = make_commit(sha="squash_on_master", message="Fix bug (#10)")
        other = make_commit(sha="p1", message="Other PR change")
        repo.get_commit.side_effect = lambda sha: {"squash_on_next": squash, "ref1": promoted, "p1": other}[sha]
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]
        repo.compare.return_value = MagicMock(commits=[])
        event = MagicMock(event="referenced", commit_id="ref1")
        pr.get_issue_events.return_value = [event]

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="squash_on_master"):
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert result == ["squash_on_master"]

    def test_merged_pr_squash_close_event_ignores_commits_titled_like_pr_commits(self, bp_module, make_pr,
                                                                                 make_repo, make_commit):
        """For a squash, a referenced commit of another PR titled like a PR commit is not picked."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash_on_next", state="closed")
        squash = make_commit(sha="squash_on_next", message="Fix bug (#10)", parents=[MagicMock(sha="p1")])
        foreign = make_commit(sha="ref1", message="Initial plan")
        other = make_commit(sha="p1", message="Other PR change")
        repo.get_commit.side_effect = lambda sha: {"squash_on_next": squash, "ref1": foreign, "p1": other}[sha]
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]
        repo.compare.return_value = MagicMock(commits=[])
        pr.get_issue_events.return_value = [MagicMock(event="referenced", commit_id="ref1")]

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="foreign_on_master") as find:
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert result == []
        find.assert_not_called()

    def test_merged_pr_rebase_multi_commit_matches_pr_commits(self, bp_module, make_pr, make_repo, make_commit):
        """Rebase-merged multi-commit PR still resolves every PR commit by title."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="rebased2")
        repo.get_commit.return_value = make_commit(sha="rebased2", message="Second change")
        pr.get_commits.return_value = [
            make_commit(sha="head1", message="First change"),
            make_commit(sha="head2", message="Second change"),
        ]
        comparison = MagicMock()
        comparison.commits = [
            make_commit(sha="rebased1", message="First change"),
            make_commit(sha="rebased2", message="Second change"),
        ]
        repo.compare.return_value = comparison

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert result == ["rebased1", "rebased2"]

    def test_rebase_merged_commits_listed_out_of_order_keeps_all(self, bp_module, make_repo, make_commit):
        """A real rebase merge whose PR commits the API lists out of order still returns every commit."""
        repo = make_repo()
        merge_commit = make_commit(sha="rebased2", message="Second change",
                                   parents=[MagicMock(sha="rebased1")])
        repo.get_commit.return_value = make_commit(sha="rebased1", message="First change")
        pr_commits = [
            make_commit(sha="head2", message="Second change"),
            make_commit(sha="head1", message="First change"),
        ]

        assert bp_module.get_rebase_merged_commits(repo, merge_commit, pr_commits) == ["rebased1", "rebased2"]

    def test_rebase_merged_commits_title_mismatch_returns_nothing(self, bp_module, make_repo, make_commit):
        """A first-parent chain that doesn't match the PR titles is not backported at all.

        Backporting the chain would pick another PR's commit, and backporting only
        merge_commit_sha would silently drop the rest of the PR.
        """
        repo = make_repo()
        merge_commit = make_commit(sha="squash_sha", message="Fix bug",
                                   parents=[MagicMock(sha="other_pr_sha")])
        repo.get_commit.return_value = make_commit(sha="other_pr_sha", message="Unrelated change")
        pr_commits = [
            make_commit(sha="head1", message="Initial plan"),
            make_commit(sha="head2", message="Fix bug"),
        ]

        assert bp_module.get_rebase_merged_commits(repo, merge_commit, pr_commits) == []

    def test_merged_pr_rebase_head_commit_not_listed_last(self, bp_module, make_pr, make_repo, make_commit):
        """Rebase merge is not taken for a squash when the API lists the head commit out of order."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="rebased2")
        repo.get_commit.return_value = make_commit(sha="rebased2", message="Second change")
        pr.get_commits.return_value = [
            make_commit(sha="head2", message="Second change"),
            make_commit(sha="head1", message="First change"),
        ]
        comparison = MagicMock()
        comparison.commits = [
            make_commit(sha="rebased1", message="First change"),
            make_commit(sha="rebased2", message="Second change"),
        ]
        repo.compare.return_value = comparison

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert sorted(result) == ["rebased1", "rebased2"]

    def test_merged_pr_squash_no_promoted_commits(self, bp_module, make_pr, make_repo, make_commit):
        """When compare returns no commits, fall back to merge_commit_sha."""
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")

        squash_commit = make_commit(sha="squash123", parents=[MagicMock()])
        repo.get_commit.return_value = squash_commit

        comparison = MagicMock()
        comparison.commits = []
        repo.compare.return_value = comparison

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")
        assert result == ["squash123"]

    def test_closed_pr_not_merged(self, bp_module, make_pr, make_repo, make_commit):
        """Closed (not merged) PR finds commit from referenced event matching PR commit title."""
        repo = make_repo()
        pr = make_pr(number=10, state="closed", merged=False, merge_commit_sha=None)

        # PR has a commit with this title
        pr_commit = MagicMock()
        pr_commit.commit.message = "Fix the view builder bug\n\nDetails"
        pr.get_commits.return_value = [pr_commit]

        event = MagicMock()
        event.event = "referenced"
        event.commit_id = "direct123"
        pr.get_issue_events.return_value = [event]

        # The referenced commit has matching title
        ref_commit = make_commit(sha="direct123", message="Fix the view builder bug")
        repo.get_commit.return_value = ref_commit

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="direct123"):
            result = bp_module.get_pr_commits(repo, pr, "master")
        assert result == ["direct123"]

    def test_closed_pr_referenced_commit_not_on_stable_branch(self, bp_module, make_pr, make_repo, make_commit):
        """Closed PR where referenced commit is on gating branch, finds match by title on stable branch."""
        repo = make_repo()
        pr = make_pr(number=10, state="closed", merged=False, merge_commit_sha=None)

        pr_commit = MagicMock()
        pr_commit.commit.message = "Fix the view builder bug\n\nDetails"
        pr.get_commits.return_value = [pr_commit]

        event = MagicMock()
        event.event = "referenced"
        event.commit_id = "gating_branch_commit"
        pr.get_issue_events.return_value = [event]

        ref_commit = make_commit(sha="gating_branch_commit", message="Fix the view builder bug")
        repo.get_commit.return_value = ref_commit

        # Simulate: referenced commit not on stable branch, but matching commit found by title
        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="stable_branch_commit"):
            result = bp_module.get_pr_commits(repo, pr, "master")
        assert result == ["stable_branch_commit"]

    def test_closed_pr_merge_commit_resolves_from_promoted_range(self, bp_module, make_pr, make_repo, make_commit):
        """Closed (not merged) PR promoted via a merge commit resolves commits from the promoted range.

        Regression test for scylladb/scylladb#30139: the PR was promoted by pushing a
        2-parent "Merge '...' from <author>" commit directly to master, so the PR is
        closed-but-not-merged and the only referenced event points at the merge commit.
        Its title matches none of the PR's commit titles, so the title-equality path
        finds nothing. The merge-commit path must instead resolve the PR's commits from
        the first-parent..stable_branch range and match them by title.
        """
        repo = make_repo()
        pr = make_pr(number=30139, state="closed", merged=False, merge_commit_sha=None)

        # PR's individual commits.
        pr_commit1 = MagicMock()
        pr_commit1.commit.message = "sstables: introduce sstable format version mt\n\nDetails"
        pr_commit2 = MagicMock()
        pr_commit2.commit.message = "conf: use mt instead of ms as the default format"
        pr.get_commits.return_value = [pr_commit1, pr_commit2]

        # Only referenced event is the merge commit that closed the PR.
        event = MagicMock()
        event.event = "referenced"
        event.commit_id = "merge_sha"
        pr.get_issue_events.return_value = [event]

        # The referenced commit is a 2-parent merge whose title matches no PR commit.
        merge_commit = make_commit(
            sha="merge_sha",
            message="Merge 'sstables: supersede ms with mt' from Some Author",
            parents=[MagicMock(), MagicMock()],
        )
        repo.get_commit.return_value = merge_commit

        # The promoted range (first-parent..master) contains the rebased PR commits,
        # which have different SHAs than pr.get_commits() but the same titles.
        promoted1 = make_commit(sha="promoted_sha1", message="sstables: introduce sstable format version mt")
        promoted2 = make_commit(sha="promoted_sha2", message="conf: use mt instead of ms as the default format")
        comparison = MagicMock()
        comparison.commits = [promoted1, promoted2]
        repo.compare.return_value = comparison

        result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before_sha")
        assert result == ["promoted_sha1", "promoted_sha2"]
        # The promoted range must be compared from the given start_commit, not the merge title.
        repo.compare.assert_called_once_with("before_sha", "master")

    def test_closed_pr_merge_commit_falls_back_to_first_parent(self, bp_module, make_pr, make_repo, make_commit):
        """Merge-commit resolution uses the merge's first parent as base when no start_commit is given."""
        repo = make_repo()
        pr = make_pr(number=30139, state="closed", merged=False, merge_commit_sha=None)

        pr_commit = MagicMock()
        pr_commit.commit.message = "sstables: introduce sstable format version mt\n\nDetails"
        pr.get_commits.return_value = [pr_commit]

        event = MagicMock()
        event.event = "referenced"
        event.commit_id = "merge_sha"
        pr.get_issue_events.return_value = [event]

        first_parent = MagicMock()
        first_parent.sha = "first_parent_sha"
        merge_commit = make_commit(
            sha="merge_sha",
            message="Merge '...' from Some Author",
            parents=[first_parent, MagicMock()],
        )
        repo.get_commit.return_value = merge_commit

        promoted = make_commit(sha="promoted_sha", message="sstables: introduce sstable format version mt")
        comparison = MagicMock()
        comparison.commits = [promoted]
        repo.compare.return_value = comparison

        result = bp_module.get_pr_commits(repo, pr, "master")
        assert result == ["promoted_sha"]
        repo.compare.assert_called_once_with("first_parent_sha", "master")

    def test_closed_pr_skips_unrelated_referenced_commits(self, bp_module, make_pr, make_repo, make_commit):
        """Closed PR skips referenced commits whose titles don't match PR commits."""
        repo = make_repo()
        pr = make_pr(number=10, state="closed", merged=False, merge_commit_sha=None)

        # PR has a commit with this title
        pr_commit = MagicMock()
        pr_commit.commit.message = "db: view: Serialize creation and dropping of views on shard0\n\nDetails"
        pr.get_commits.return_value = [pr_commit]

        # Two referenced events: first is the real one, second is from an unrelated PR
        event1 = MagicMock()
        event1.event = "referenced"
        event1.commit_id = "correct_commit"
        event2 = MagicMock()
        event2.event = "referenced"
        event2.commit_id = "unrelated_commit"
        pr.get_issue_events.return_value = [event1, event2]

        # The unrelated commit (last) has a different title
        unrelated = make_commit(sha="unrelated_commit", message="db: view: refactor semaphore usage")
        correct = make_commit(sha="correct_commit", message="db: view: Serialize creation and dropping of views on shard0")

        def get_commit_side_effect(sha):
            if sha == "unrelated_commit":
                return unrelated
            return correct

        repo.get_commit.side_effect = get_commit_side_effect

        with patch.object(bp_module, '_find_commit_on_stable_branch', return_value="correct_commit"):
            result = bp_module.get_pr_commits(repo, pr, "master")
        # Should pick the correct commit (walking backwards, skips unrelated, finds correct)
        assert result == ["correct_commit"]

    def test_closed_pr_referenced_commit_not_found_anywhere(self, bp_module, make_pr, make_repo, make_commit, caplog):
        """Closed PR where no referenced commit matches PR titles returns empty list."""
        import logging
        repo = make_repo()
        pr = make_pr(number=10, state="closed", merged=False, merge_commit_sha=None)

        pr_commit = MagicMock()
        pr_commit.commit.message = "Fix the bug\n\nDetails"
        pr.get_commits.return_value = [pr_commit]

        event = MagicMock()
        event.event = "referenced"
        event.commit_id = "orphan_commit"
        pr.get_issue_events.return_value = [event]

        # Referenced commit has a completely different title
        orphan = make_commit(sha="orphan_commit", message="Completely unrelated change")
        repo.get_commit.return_value = orphan

        with caplog.at_level(logging.WARNING):
            result = bp_module.get_pr_commits(repo, pr, "master")
        assert result == []
        assert "No valid referenced commit found" in caplog.text

    def test_no_commits_found(self, bp_module, make_pr, make_repo, make_commit):
        """When no commits can be found, returns empty list."""
        repo = make_repo()
        pr = make_pr(number=10, state="open", merged=False)
        pr.get_issue_events.return_value = []

        result = bp_module.get_pr_commits(repo, pr, "master")
        assert result == []

    def test_warns_on_partial_commit_match(self, bp_module, make_pr, make_repo, make_commit, caplog):
        """When title matching finds fewer commits than PR has, a warning should be logged."""
        import logging
        repo = make_repo()
        pr = make_pr(number=10, merged=True, merge_commit_sha="squash123")

        # Single parent -> not a merge commit (rebase merge path: keeps the last PR commit message)
        squash_commit = make_commit(sha="squash123", message="Add comment\n\nMore details", parents=[MagicMock()])
        repo.get_commit.return_value = squash_commit

        # PR has 2 commits
        pr_commit1 = MagicMock()
        pr_commit1.commit.message = "Fix the bug\n\nDetails"
        pr_commit2 = MagicMock()
        pr_commit2.commit.message = "Add comment\n\nMore details"
        pr.get_commits.return_value = [pr_commit1, pr_commit2]

        # Only one promoted commit matches (the first one)
        promoted_commit = make_commit(sha="promoted1", message="Fix the bug")
        promoted_commit.commit.message = "Fix the bug"
        comparison = MagicMock()
        comparison.commits = [promoted_commit]
        repo.compare.return_value = comparison

        with caplog.at_level(logging.WARNING):
            result = bp_module.get_pr_commits(repo, pr, "master", start_commit="before123")

        assert len(result) == 1
        assert "promoted1" in result
        assert any("has 2 commits but only 1 were matched" in msg for msg in caplog.messages)


class TestIsCommitInBranch:
    def test_commit_found_by_title(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit(sha="abc123", message="Fix bug in component")
        repo.get_commit.return_value = commit

        branch_commit = make_commit(sha="def456", message="Fix bug in component")
        branch_commit.commit.message = "Fix bug in component"
        repo.get_commits.return_value = [branch_commit]

        result = bp_module.is_commit_in_branch(repo, "abc123", "branch-2025.4")
        assert result is True

    def test_substring_title_does_not_match(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit(sha="abc123", message="session: add logging")
        repo.get_commit.return_value = commit

        branch_commit = make_commit(sha="zzz999", message="session: add logging to drain_closing_sessions")
        branch_commit.commit.message = "session: add logging to drain_closing_sessions"
        repo.get_commits.return_value = [branch_commit]

        result = bp_module.is_commit_in_branch(repo, "abc123", "branch-2025.1")
        assert result is False

    def test_commit_found_by_sha(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit(sha="abc123", message="Fix bug")
        repo.get_commit.return_value = commit

        branch_commit = make_commit(sha="abc123", message="Different message")
        branch_commit.commit.message = "Different message"
        repo.get_commits.return_value = [branch_commit]

        result = bp_module.is_commit_in_branch(repo, "abc123", "branch-2025.4")
        assert result is True

    def test_commit_not_found(self, bp_module, make_repo, make_commit):
        repo = make_repo()
        commit = make_commit(sha="abc123", message="Fix bug in component X")
        repo.get_commit.return_value = commit

        branch_commit = make_commit(sha="zzz999", message="Completely different commit")
        branch_commit.commit.message = "Completely different commit"
        repo.get_commits.return_value = [branch_commit]

        result = bp_module.is_commit_in_branch(repo, "abc123", "branch-2025.4")
        assert result is False

    def test_error_returns_false(self, bp_module, make_repo):
        repo = make_repo()
        repo.get_commit.side_effect = Exception("API error")

        result = bp_module.is_commit_in_branch(repo, "abc123", "branch-2025.4")
        assert result is False


class TestFindCommitOnStableBranch:
    def test_commit_directly_on_stable_branch(self, bp_module, make_repo, make_commit):
        """Commit is already on stable branch (behind_by == 0)."""
        repo = make_repo()
        commit = make_commit(sha="abc123", message="Fix something")
        repo.get_commit.return_value = commit

        comparison = MagicMock()
        comparison.behind_by = 0
        repo.compare.return_value = comparison

        result = bp_module._find_commit_on_stable_branch(repo, "abc123", "master")
        assert result == "abc123"

    def test_commit_not_on_branch_finds_by_title(self, bp_module, make_repo, make_commit):
        """Commit not on stable branch, but matching commit found by title."""
        repo = make_repo()
        commit = make_commit(sha="gating123", message="Fix something")
        repo.get_commit.return_value = commit

        comparison = MagicMock()
        comparison.behind_by = 5
        repo.compare.return_value = comparison

        stable_commit = make_commit(sha="master456", message="Fix something")
        stable_commit.commit.message = "Fix something"
        repo.get_commits.return_value = [stable_commit]

        result = bp_module._find_commit_on_stable_branch(repo, "gating123", "master")
        assert result == "master456"

    def test_commit_not_found_anywhere(self, bp_module, make_repo, make_commit):
        """Commit not on stable branch and no title match found."""
        repo = make_repo()
        commit = make_commit(sha="orphan123", message="Unique title")
        repo.get_commit.return_value = commit

        comparison = MagicMock()
        comparison.behind_by = 5
        repo.compare.return_value = comparison

        other_commit = make_commit(sha="other456", message="Different title")
        other_commit.commit.message = "Different title"
        repo.get_commits.return_value = [other_commit]

        result = bp_module._find_commit_on_stable_branch(repo, "orphan123", "master")
        assert result is None
