#!/usr/bin/env python3

import argparse
from datetime import datetime, timedelta
import json
import logging
from operator import itemgetter
import pytz
import re
import requests
import sys

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


class Event:
    def __init__(self, issue_type, repo, issue_id, title, event, by, assignees):
        self.issue_type = issue_type
        self.repo = repo
        self.issue_id = issue_id
        self.title = title
        self.events = [(event, by)]
        self.assignees = []
        self.assignees.append(assignees)
        self.assignees.sort()

    def append_event(self, event, by):
        self.events.append((event, by))

    def __lt__(self, other):
        return self.assignees[0] < other.assignees[0]

    def __hash__(self):
        return hash((self.issue_type, self.repo, self.issue_id, hash(str(self.events)), hash(str(self.assignees))))

    def __eq__(self, other):
        return self.issue_type == other.issue_type and self.repo == other.repo and self.issue_id == other.issue_id and \
            self.title == other.title and self.events == other.events and self.assignees == other.assignees


class CollapsedEvents:
    events = dict()

    def insert(self, issue_id, event):
        if event.issue_id in self.events:
            self.events[issue_id].append_event(event.events[0][0], event.events[0][1])
        else:
            self.events[issue_id] = event

    def __str__(self):
        ret = ""
        list_of_events = list(self.events.values())
        list_of_events.sort()
        for value in list_of_events:
            issue_url_path = "issues" if value.issue_type == "issue" else "pull"
            ret += f"Assignees: {value.assignees}, \"{value.title}\", URL: https://github.com/scylladb/{value.repo}/{issue_url_path}/{value.issue_id}, "
            ret += "events:" + str([f"event: {pair[0]} by: {pair[1]}" for pair in value.events])
            ret += "\n"
        return ret


class GithubAPI:
    API_ENDPOINT = 'https://api.github.com/graphql'

    def __init__(self, token) -> None:
        self.token = token
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"bearer {token}"
        self.organization = "scylladb"

    def check_rate_limits(self):
        """
        https://docs.github.com/en/rest/overview/resources-in-the-rest-api?apiVersion=2022-11-28#checking-your-rate-limit-status
        Header Name	            Description
        x-ratelimit-limit	    The maximum number of requests you're permitted to make per hour.
        x-ratelimit-remaining	The number of requests remaining in the current rate limit window.
        x-ratelimit-used	    The number of requests you've made in the current rate limit window.
        x-ratelimit-reset	    The time at which the current rate limit window resets in UTC epoch seconds.
        """
        response = self.session.get(self.API_ENDPOINT)
        if response.status_code == 502:
            sys.exit("! GitHub is overloaded and will drop queries, aborting !")

        rate_limits = {key: value for (key, value) in response.headers.items() if 'x-ratelimit' in key.lower()}
        logging.info(f"GitHub API's rate limits: {rate_limits}")
        if float(rate_limits['X-RateLimit-Remaining']) / float(rate_limits['X-RateLimit-Limit']) < 0.20:
            logging.warning("Reaching GitHub API's rate limit!")

    def do_query(self, query):
        # logging.debug(query)
        response = self.session.post(self.API_ENDPOINT, data=json.dumps({"query": query}))
        # logging.debug(response.json())
        if response.status_code == 502:
            sys.exit("! GitHub is overloaded and will drop queries, aborting !")
        if 'errors' in response.json():
            logging.error(response.json())
            sys.exit("Error when processing request, most probably due to malformed GraphQL, exiting...")
        return response

    def get_project_views_filters(self, organization, project_number, page_size=100):
        query = """
        query {{
            organization(login :  "{}") {{
                projectV2(number : {}) {{
                    views(first: {}, after: "{}") {{
                        nodes {{
                            name
                            filter
                        }}
                        edges {{
                            cursor
                        }}
                    }}
                }}
            }}
        }}
        """
        after = ""
        views_to_ret = []
        while True:
            q = query.format(organization, project_number, page_size, after)
            ret = self.do_query(q).json()
            new_views = ret["data"]["organization"]["projectV2"]["views"]["nodes"]
            if len(new_views) > 0:
                views_to_ret = views_to_ret + ret["data"]["organization"]["projectV2"]["views"]["nodes"]
                after = ret["data"]["organization"]["projectV2"]["views"]["edges"][-1]["cursor"]

            if len(new_views) < page_size:
                break

        return views_to_ret

    def get_issues_ids(self, issues_filter):
        # TODO: do not add issues that had been added to the project and later have been removed from it.
        #       This could be achieved by searching for AddedToProjectEvent in issue's events,
        #       but currently it's not working, per doc: https://docs.github.com/en/webhooks-and-events/events/issue-event-types#added_to_project
        #       In order for it to work, we'd have to extend our query and skip issues that have current project id:
        #       ... on Issue {
        #           id
        #           number
        #           timelineItems(first:100) {
        #               edges {
        #                   node {
        #                       ... on AddedToProjectEvent {
        #                           project_id
        #                       }
        #                   }
        #               }
        #           }
        #       }
        query = """
        query {{
            search(type: ISSUE, first: 100, query:"is:issue, {}", {}) {{
                issueCount
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    ... on Issue {{
                        id
                        number
                        repository {{
                            name
                        }}
                    }}
                }}
            }}
        }}
        """
        cursor_filter = "after:{}"
        last_cursor = "null"
        ids = []
        while True:
            q = query.format(issues_filter, cursor_filter.format(last_cursor))
            ret = self.do_query(q).json()
            ids += [[issue.get('id'), issue.get('number'), issue.get('repository')["name"]] for issue in
                    ret["data"]["search"]["nodes"]]
            if ret["data"]["search"]["pageInfo"]["hasNextPage"]:
                last_cursor = f'"{ret["data"]["search"]["pageInfo"]["endCursor"]}"'
            else:
                break
        logging.debug(f"Found {len(ids)} issues for given filter \"{issues_filter}\": {ids}")
        return ids

    def get_issues_last_events(self, issues_filter, author, author_name, since):
        # TODO: New Projects (non-classic) DO NOT capture MOVED_COLUMNS_IN_PROJECT_EVENT
        #       https://github.com/orgs/community/discussions/30979
        query = """
        query {{
            search(type: ISSUE, first: 100, query:"is:{} {} {}", {}) {{
                issueCount
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    ... on PullRequest {{
                        id
                        number
                        title
                        author {{
                            login
                        }}
                        repository {{
                            name
                        }}
                        timelineItems(first:100, since:"{}") {{
                            edges {{
                                node {{
                                    ... on AssignedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on ClosedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on ConvertedToDiscussionEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on MarkedAsDuplicateEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on MentionedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on IssueComment {{
                                        __typename
                                        author {{
                                            login
                                        }}
                                    }}
                                    ... on MergedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on PullRequestCommit {{
                                        __typename
                                        pullRequest {{
                                            author {{
                                                login
                                            }}
                                        }}
                                    }}
                                    ... on PullRequestReview {{
                                        __typename
                                        author {{
                                            login
                                        }}
                                    }}
                                    ... on ReadyForReviewEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on ReopenedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on ReviewRequestedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on UserBlockedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                    ... on Issue {{
                        id
                        number
                        title
                        assignees(first:10) {{
                            edges {{
                                node {{
                                    name
                                }}
                            }}
                        }}
                        repository {{
                            name
                        }}
                        timelineItems(first:100, since:"{}") {{
                            edges {{
                                node {{
                                    ... on ClosedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on AssignedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on ReopenedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on UserBlockedEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on MarkedAsDuplicateEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                    ... on ConvertedNoteToIssueEvent {{
                                        __typename
                                        actor {{
                                            login
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """

        cursor_filter = "after:{}"
        last_cursor = "null"
        all_events = []
        issue_owner_tag = {"issue": "assignee", "pr": "author"}
        for issue_type, owner in issue_owner_tag.items():
            while True:
                q = query.format(issue_type, f"{owner}:{author}", issues_filter, cursor_filter.format(last_cursor),
                                 since, since)
                ret = self.do_query(q).json()
                for issue in ret["data"]["search"]["nodes"]:
                    for event in issue["timelineItems"]["edges"]:
                        if "__typename" in event["node"]:
                            assignees = []
                            if owner == "assignee":
                                assignees = [assignee['node']['name'] for assignee in issue.get('assignees')['edges']]
                            else:
                                assignees.append(author_name)
                            repo_name = issue.get('repository')["name"]
                            issue_id = issue.get('number')
                            title = issue.get('title')
                            event_type = event["node"]["__typename"]
                            by = None
                            if "actor" in event["node"]:
                                by = event["node"]["actor"]["login"]
                            elif "author" in event["node"]:
                                by = event["node"]["author"]["login"]
                            elif "pullRequest" in event["node"]:
                                by = event["node"]["pullRequest"]["author"]["login"]
                            all_events.append(Event(issue_type, repo_name, issue_id, title, event_type, by, assignees))
                if ret["data"]["search"]["pageInfo"]["hasNextPage"]:
                    last_cursor = f'"{ret["data"]["search"]["pageInfo"]["endCursor"]}"'
                else:
                    break
        return all_events

    def get_project_id(self, project_filter):
        # Note that we want to find exactly 1 project, but are asking for more projects matching the criteria
        # If search returns more than 1 project, we error out, but if there's only 1 found, this is our precise match
        query = """
            query {{
                organization(login: "scylladb") {{
                    projectsV2(first: 10, query:"{}") {{
                        nodes {{
                            id
                            title
                            number
                        }}
                    }}
                }}
            }}
        """
        q = query.format(project_filter)
        ret = self.do_query(q).json()
        matching_project_ids = ret["data"]["organization"]["projectsV2"]["nodes"]
        if len(matching_project_ids) == 1:
            return matching_project_ids[0]
        elif len(matching_project_ids) > 1:
            logging.error(f"Found more than one matching projects: {matching_project_ids}")
            return None
        else:
            logging.error(f"Didn't find any project matching criteria: {project_filter}")
            return None

    def add_issue_to_project(self, project_id, issue_id):
        if not args.update_project:
            return
        query = """
        mutation {{
            addProjectV2ItemById( input: {{ projectId: "{}" contentId: "{}" }} ) {{
                item {{
                    id
                }}
            }}
        }}
        """
        q = query.format(project_id, issue_id)
        ret = self.do_query(q).json()
        project_issue_id = ret["data"]["addProjectV2ItemById"]["item"]["id"]
        logging.info(f"Added issue with ID: {project_issue_id} to the project with ID: {project_id}")

    def get_team_members(self, team_name):
        query = """
            query {{
                organization(login: "scylladb") {{
                    team(slug:"{}") {{
                        members(first:100) {{
                            nodes {{
                                login
                                name
                            }}
                        }}
                    }}
                }}
            }}
        """
        q = query.format(team_name)
        ret = self.do_query(q).json()
        members = ret["data"]["organization"]["team"]
        if members is None:
            logging.error(f"Couldn't find any team members for team: {team_name}")
            return None
        return [[member.get('login'), member.get('name')] for member in
                members["members"]["nodes"]]


def run():
    gh_api.check_rate_limits()

    views = gh_api.get_project_views_filters("scylladb", project_number)
    filters = [x["filter"] for x in views]

    filter_cats = re.compile(r"\S+:")

    accumulated_label_filters = []
    for f in filters:
        pos = 0
        broken_filter = {}
        prev_filter = None
        if f is None:
            continue
        while m := filter_cats.search(f):
            if prev_filter is not None:
                broken_filter[prev_filter] = f[:m.span(0)[0]]
            prev_filter = m.group(0)[:-1]
            f = f[m.span(0)[1]:]
        if prev_filter is not None:
            broken_filter[prev_filter] = f
        if "label" in broken_filter:
            accumulated_label_filters.append(broken_filter["label"])

    labels = set()
    for label in accumulated_label_filters:
        broken_labels = [x.strip() for x in label.split(",")]
        broken_labels = map(lambda x: x.replace('"', '').strip(), broken_labels)
        labels = labels.union(set(broken_labels))

    def matches_blacklisted_labels(label):
        blacklisted_labels_regexp = ["^P[0-9]$", "^customer*", "^top10$"]
        for regexp in blacklisted_labels_regexp:
            if re.search(regexp, label):
                return True
        return False

    labels = [x for x in labels if not matches_blacklisted_labels(x)]

    logging.info(f"Found {len(labels)} labels in the project: {labels}")
    labels_query = "label:" + ",".join(['"' + x + '"' for x in labels])
    logging.debug(labels_query)

    issues_ids = []
    for label in labels:
        found_ids = gh_api.get_issues_ids(
            f"is:open org:scylladb is:issue -project:scylladb/{project_number} label:{label}")
        if len(found_ids) > 0:
            logging.info(f"'{label}' label's not added issues count: " + str(len(found_ids)))
            issues_ids += found_ids

    team_members = gh_api.get_team_members(args.team)
    # if a team member doesn't have a name, copy nick into name, so it's not None
    for team_member in team_members:
        if team_member[1] is None:
            team_member[1] = team_member[0]

    if team_members is not None:
        team_members.sort(key=itemgetter(1))
        logging.debug("Team size: " + str(len(team_members)) + ", members: " + str(team_members))
        for nick, name in team_members:
            found_ids = gh_api.get_issues_ids(
                f"is:open org:scylladb is:issue -project:scylladb/{project_number} assignee:{nick}")
            if len(found_ids) > 0:
                logging.info(f"{name}'s not added issues count: " + str(len(found_ids)))
                issues_ids += found_ids
    logging.info(f"Total number of found issues: {len(issues_ids)}")

    if args.update_project:
        for issue_id, issue_number, issue_repo in issues_ids:
            gh_api.add_issue_to_project(project_id, issue_id)
            logging.info(f"Added issue with ID: {issue_id} and number: {issue_number} from: {issue_repo} repository to "
                         f"the project with ID: {project_name}")

    if args.weekly_reports:
        events = []
        previous_week_date = (datetime.now() - timedelta(days=7)).isoformat()
        if team_members is not None:
            team_members.sort(key=itemgetter(1))
            for nick, name in team_members:
                events += gh_api.get_issues_last_events(issues_filter=f"is:open org:scylladb", author=nick, author_name=name,
                                                        since=previous_week_date)

        events = list(set(events))  # remove duplicates
        collapsed_events = CollapsedEvents()
        for event in events:
            collapsed_events.insert(event.issue_id, event)
        logging.info("Found events: ")
        logging.info("\n" + str(collapsed_events))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automate github project operations")
    parser.add_argument("gh_token", type=str, help="github's PAT to use for manipulating the project")
    parser.add_argument('-c', '--cron-job', action='store_true',
                        help="schedule a cron job to run updates periodically")
    parser.add_argument("-l", "--log-level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help="set the logging level (default: %(default)s)")
    parser.add_argument('--project-name', type=str,
                        help='Name of project for assigning issues, can be substring')
    parser.add_argument('--team', type=str,
                        help='Name of the team members to search for un-assigned issues to project')
    parser.add_argument('--query', metavar="'query'", type=str, help="execute a raw GraphQL query")
    parser.add_argument('--update-project', action='store_true',
                        help='Use to update projects.Default will run without actually updating projects')
    parser.add_argument('--weekly-reports', action='store_true',
                        help="Generate a report of what events occurred in team's Issues and PRs")
    args = parser.parse_args()

    gh_api = GithubAPI(args.gh_token)

    if args.query:
        response = gh_api.do_query(args.query).json()
        pretty_response = json.dumps(response, indent=4)
        print(pretty_response)
        sys.exit(0)

    if args.team is None or args.project_name is None:
        print("Both --project-name and --team are required when using the script automation")
        sys.exit(1)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("gh_project_automation.log"),
            logging.StreamHandler()
        ]
    )

    project_data = gh_api.get_project_id(args.project_name)
    project_id = project_data["id"]
    project_name = project_data["title"]
    project_number = project_data["number"]
    if project_id is not None:
        logging.info(f"Found a project with name: {project_name}, number: {project_number}, ID: {project_id}")
    else:
        sys.exit("Couldn't find project ID, exiting...")

    run()

    if args.cron_job:
        scheduler = BlockingScheduler(logger=logging.getLogger())
        our_timezone = pytz.timezone("CET")
        trigger = CronTrigger(year="*", month="*", day="*", hour="0", minute="*", second="*", timezone=our_timezone)
        scheduler.add_job(func=run, trigger=trigger)
        try:
            scheduler.start()
        except Exception as e:
            logging.exception(f"An error occurred: {e}")
        except KeyboardInterrupt:
            logging.info("Scheduler received a keyboard interruption")
        finally:
            scheduler.shutdown()
