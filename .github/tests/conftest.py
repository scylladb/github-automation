"""
conftest.py - Shared fixtures for backport process unit tests.

The auto-backport-jira.py script reads environment variables at module level
(GITHUB_TOKEN, JIRA_AUTH, etc.) and will sys.exit(1) if GITHUB_TOKEN is missing.
We must set these env vars BEFORE importing the module.

Additionally, the script imports from 'github' (PyGithub) and 'git' (GitPython)
at module level. These may not be installed in the test environment, so we inject
mock modules into sys.modules before loading the script.
"""

import importlib
import os
import sys
import types
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Create mock modules for github (PyGithub) and git (GitPython) BEFORE
# anything tries to import them. This allows tests to run without these
# packages actually installed.
# ---------------------------------------------------------------------------

def _setup_mock_github_module():
    """Inject a mock 'github' package into sys.modules if the real one is broken/missing."""
    try:
        from github import Github, GithubException
        # Real package works, nothing to do
        return
    except (ImportError, ModuleNotFoundError):
        pass

    # Build a mock 'github' package
    mock_github_pkg = types.ModuleType("github")
    mock_github_pkg.__path__ = []  # mark as package

    # Create realistic GithubException
    class _GithubException(Exception):
        def __init__(self, status=None, data=None, headers=None):
            self.status = status
            self.data = data
            self.headers = headers
            super().__init__(status, data)

    mock_github_pkg.GithubException = _GithubException
    mock_github_pkg.Github = MagicMock

    sys.modules["github"] = mock_github_pkg
    # Also add sub-modules that PyGithub has, in case the script ever imports them
    sys.modules.setdefault("github.GithubException", mock_github_pkg)


def _setup_mock_git_module():
    """Inject a mock 'git' package into sys.modules if the real one is broken/missing."""
    try:
        from git import Repo, GitCommandError
        # Real package works, nothing to do
        return
    except (ImportError, ModuleNotFoundError):
        pass

    mock_git_pkg = types.ModuleType("git")
    mock_git_pkg.__path__ = []  # mark as package

    class _GitCommandError(Exception):
        def __init__(self, command=None, status=None, stderr=None):
            self.command = command
            self.status = status
            self.stderr = stderr
            super().__init__(command, status)

    mock_git_pkg.GitCommandError = _GitCommandError
    mock_git_pkg.Repo = MagicMock

    sys.modules["git"] = mock_git_pkg
    sys.modules.setdefault("git.exc", mock_git_pkg)


# Run these immediately at conftest load time (before any fixture)
_setup_mock_github_module()
_setup_mock_git_module()


# ---------------------------------------------------------------------------
# Module-level setup: ensure env vars are set before any test imports the script
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def _set_env_vars():
    """Set required environment variables before the backport module is imported."""
    os.environ.setdefault("GITHUB_TOKEN", "fake-gh-token-for-tests")
    os.environ.setdefault("JIRA_AUTH", "testuser:testtoken")
    os.environ.setdefault("GITHUB_SERVER_URL", "https://github.com")
    os.environ.setdefault("GITHUB_REPOSITORY", "scylladb/scylladb")
    os.environ.setdefault("GITHUB_RUN_ID", "12345")


@pytest.fixture(scope="session")
def bp_module(_set_env_vars):
    """
    Import and return the auto-backport-jira module.

    Because the script filename contains hyphens, we use importlib.
    The session scope ensures the module is loaded once per test session.
    """
    scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
    scripts_dir = os.path.abspath(scripts_dir)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    spec = importlib.util.spec_from_file_location(
        "auto_backport_jira",
        os.path.join(scripts_dir, "auto-backport-jira.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["auto_backport_jira"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Reusable mock factories
# ---------------------------------------------------------------------------

def _make_label(name):
    """Create a mock GitHub label."""
    label = MagicMock()
    label.name = name
    return label


def _make_pr(
    number=1,
    title="Test PR",
    body="Test body\nFixes: SCYLLADB-123",
    state="closed",
    merged=True,
    merge_commit_sha="abc123",
    labels=None,
    user_login="testuser",
    user_email="testuser@scylladb.com",
    user_name="Test User",
    base_ref="master",
    html_url="https://github.com/scylladb/scylladb/pull/1",
):
    """Create a mock GitHub PullRequest object."""
    pr = MagicMock()
    pr.number = number
    pr.title = title
    pr.body = body
    pr.state = state
    pr.merged = merged
    pr.merge_commit_sha = merge_commit_sha
    pr.html_url = html_url

    # User
    pr.user = MagicMock()
    pr.user.login = user_login
    pr.user.email = user_email
    pr.user.name = user_name

    # Base branch
    pr.base = MagicMock()
    pr.base.ref = base_ref
    pr.base.repo = MagicMock()
    pr.base.repo.full_name = "scylladb/scylladb"

    # Labels
    if labels is None:
        labels = []
    mock_labels = [_make_label(l) for l in labels]
    pr.labels = mock_labels
    pr.get_labels.return_value = mock_labels

    # Milestone
    pr.milestone = None

    # Methods
    pr.add_to_labels = MagicMock()
    pr.remove_from_labels = MagicMock()
    pr.add_to_assignees = MagicMock()
    pr.create_issue_comment = MagicMock()
    pr.edit = MagicMock()
    pr.get_commits = MagicMock(return_value=[])
    pr.get_issue_events = MagicMock(return_value=[])
    pr.as_issue = MagicMock(return_value=MagicMock())

    return pr


def _make_repo(full_name="scylladb/scylladb", name="scylladb"):
    """Create a mock GitHub Repository object."""
    repo = MagicMock()
    repo.full_name = full_name
    repo.name = name
    repo.get_pull = MagicMock()
    repo.get_pulls = MagicMock(return_value=[])
    repo.get_commit = MagicMock()
    repo.get_commits = MagicMock(return_value=[])
    repo.get_tags = MagicMock(return_value=[])
    repo.get_milestones = MagicMock(return_value=[])
    repo.create_milestone = MagicMock()
    repo.create_pull = MagicMock()
    repo.compare = MagicMock()
    return repo


def _make_commit(sha="abc123", message="Test commit message", parents=None):
    """Create a mock GitHub Commit object."""
    commit = MagicMock()
    commit.sha = sha
    commit.commit = MagicMock()
    commit.commit.message = message
    if parents is None:
        parents = [MagicMock()]
    commit.parents = parents
    commit.get_pulls = MagicMock(return_value=[])
    return commit


def _make_tag(name):
    """Create a mock GitHub Tag object."""
    tag = MagicMock()
    tag.name = name
    return tag


def _make_milestone(title):
    """Create a mock GitHub Milestone object."""
    milestone = MagicMock()
    milestone.title = title
    return milestone


# Export helper factories as fixtures
@pytest.fixture
def make_label():
    return _make_label


@pytest.fixture
def make_pr():
    return _make_pr


@pytest.fixture
def make_repo():
    return _make_repo


@pytest.fixture
def make_commit():
    return _make_commit


@pytest.fixture
def make_tag():
    return _make_tag


@pytest.fixture
def make_milestone():
    return _make_milestone
