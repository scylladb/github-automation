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

        # Single parent -> not a merge commit
        squash_commit = make_commit(sha="squash123", parents=[MagicMock()])
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

        # Single parent -> not a merge commit (rebase merge path)
        squash_commit = make_commit(sha="squash123", parents=[MagicMock()])
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
