import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
import jira_sync_modules  # noqa: E402


class AddPrWeblinkToJiraTests(unittest.TestCase):
    @patch("jira_sync_modules.time.sleep")
    @patch("jira_sync_modules._jira_post")
    @patch("jira_sync_modules._jira_get")
    def test_adds_missing_pr_weblink(self, mock_get, mock_post, _mock_sleep):
        mock_get.return_value = []
        mock_post.return_value = (201, "")

        jira_sync_modules.add_pr_weblink_to_jira(
            '["PM-327"]',
            "Fix race in scheduler",
            "https://github.com/scylladb/github-automation/pull/123",
            "user:token",
        )

        mock_get.assert_called_once_with(
            "https://scylladb.atlassian.net/rest/api/3/issue/PM-327/remotelink",
            "user:token",
        )
        mock_post.assert_called_once()
        post_url, payload, auth = mock_post.call_args.args
        self.assertEqual(
            post_url,
            "https://scylladb.atlassian.net/rest/api/3/issue/PM-327/remotelink",
        )
        self.assertEqual(auth, "user:token")
        self.assertEqual(
            payload["object"]["url"],
            "https://github.com/scylladb/github-automation/pull/123",
        )
        self.assertEqual(payload["object"]["title"], "Fix race in scheduler")

    @patch("jira_sync_modules.time.sleep")
    @patch("jira_sync_modules._jira_post")
    @patch("jira_sync_modules._jira_get")
    def test_skips_when_pr_weblink_already_exists(self, mock_get, mock_post, _mock_sleep):
        mock_get.return_value = [
            {
                "object": {
                    "url": "https://github.com/scylladb/github-automation/pull/123/",
                    "title": "Existing link",
                }
            }
        ]

        jira_sync_modules.add_pr_weblink_to_jira(
            '["PM-327"]',
            "Fix race in scheduler",
            "https://github.com/scylladb/github-automation/pull/123",
            "user:token",
        )

        mock_post.assert_not_called()

    @patch("jira_sync_modules.time.sleep")
    @patch("jira_sync_modules._jira_post")
    @patch("jira_sync_modules._jira_get")
    def test_skips_when_incoming_url_has_trailing_slash(self, mock_get, mock_post, _mock_sleep):
        mock_get.return_value = [
            {
                "object": {
                    "url": "https://github.com/scylladb/github-automation/pull/123",
                    "title": "Existing link",
                }
            }
        ]

        jira_sync_modules.add_pr_weblink_to_jira(
            '["PM-327"]',
            "Fix race in scheduler",
            "https://github.com/scylladb/github-automation/pull/123/",
            "user:token",
        )

        mock_post.assert_not_called()

    @patch("jira_sync_modules.time.sleep")
    @patch("jira_sync_modules._jira_post")
    @patch("jira_sync_modules._jira_get")
    def test_uses_pr_url_as_title_when_pr_title_empty(self, mock_get, mock_post, _mock_sleep):
        mock_get.return_value = []
        mock_post.return_value = (201, "")

        pr_url = "https://github.com/scylladb/github-automation/pull/123"
        jira_sync_modules.add_pr_weblink_to_jira(
            '["PM-327"]',
            "",
            pr_url,
            "user:token",
        )

        self.assertEqual(mock_post.call_args.args[1]["object"]["title"], pr_url)


if __name__ == "__main__":
    unittest.main()
