import requests
import json
import base64
import configparser
import argparse
import sys

# --------------------------
# LOAD CONFIG
# --------------------------

CONFIG_FILE = "config.ini"

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

try:
    EMAIL = config["auth"]["JIRA_EMAIL"].strip()
    API_TOKEN = config["auth"]["JIRA_API_TOKEN"].strip()
except KeyError:
    print("ERROR: Missing [auth] section or keys in config.ini", file=sys.stderr)
    sys.exit(1)

if not EMAIL or not API_TOKEN:
    print("ERROR: JIRA_EMAIL or JIRA_API_TOKEN is empty in config.ini", file=sys.stderr)
    sys.exit(1)

# --------------------------
# PARSE CLI ARGUMENTS
# --------------------------

parser = argparse.ArgumentParser(description="Create a Jira subtask in scylladb Jira")

parser.add_argument(
    "--parent",
    required=True,
    help="Parent Jira issue key (e.g., STAG-123)"
)
parser.add_argument(
    "--summary",
    required=True,
    help="Subtask summary/title"
)
parser.add_argument(
    "--description",
    required=True,
    help="Subtask description (plain text)"
)
parser.add_argument(
    "--fixversion",
    required=False,
    help="Fix Version name to assign (e.g., '2025.1'). Will be created if missing."
)

args = parser.parse_args()

PARENT_ISSUE_KEY = args.parent
SUBTASK_SUMMARY = args.summary
SUBTASK_DESCRIPTION = args.description
FIX_VERSION = args.fixversion  # may be None

# Extract project key from parent issue key
if "-" not in PARENT_ISSUE_KEY:
    print(f"ERROR: parent issue key '{PARENT_ISSUE_KEY}' does not look valid (expected PROJECT-123)", file=sys.stderr)
    sys.exit(1)

PROJECT_KEY = PARENT_ISSUE_KEY.split("-", 1)[0]

# --------------------------
# JIRA CONSTANTS
# --------------------------

JIRA_BASE_URL = "https://scylladb.atlassian.net"

# --------------------------
# AUTH
# --------------------------

auth_string = f"{EMAIL}:{API_TOKEN}".encode("utf-8")
auth_header = base64.b64encode(auth_string).decode("utf-8")

headers = {
    "Authorization": f"Basic {auth_header}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# --------------------------
# ENSURE FIX VERSION (POST-ONLY)
# --------------------------

def ensure_fix_version(project_key: str, version_name: str) -> str | None:
    """
    Ensure a fix version with this name exists in the project.
    We *always* try to create it:
      - If creation succeeds → OK.
      - If Jira says it already exists → also OK.
      - Any other error → fail.
    Returns the version name on success, or None if version_name is falsy.
    """
    if not version_name:
        return None

    create_payload = {
        "name": version_name,
        "project": project_key,
    }

    url = f"{JIRA_BASE_URL}/rest/api/3/version"
    resp = requests.post(url, headers=headers, data=json.dumps(create_payload))

    if resp.status_code in (200, 201):
        data = resp.json()
        # return canonical name if present; fallback to given name
        return data.get("name", version_name)

    # If the version already exists, Jira often returns 400 with a message like:
    # "A version with this name already exists."
    if resp.status_code == 400:
        text_lower = resp.text.lower()
        if "already exists" in text_lower:
            # Treat as success: version is already in the project
            return version_name

    # Any other error is fatal
    print(
        f"ERROR creating version '{version_name}' in project '{project_key}': "
        f"{resp.status_code} {resp.text}",
        file=sys.stderr,
    )
    sys.exit(1)

# Ensure fix version exists (if provided)
if FIX_VERSION:
    FIX_VERSION = ensure_fix_version(PROJECT_KEY, FIX_VERSION)

# --------------------------
# DESCRIPTION AS ADF
# --------------------------

description_adf = {
    "type": "doc",
    "version": 1,
    "content": [
        {
            "type": "paragraph",
            "content": [
                {
                    "type": "text",
                    "text": SUBTASK_DESCRIPTION
                }
            ]
        }
    ]
}

# --------------------------
# PAYLOAD
# --------------------------

fields = {
    "project": {"key": PROJECT_KEY},
    "parent": {"key": PARENT_ISSUE_KEY},
    "summary": SUBTASK_SUMMARY,
    "description": description_adf,
    "issuetype": {"name": "Sub-task"},
}

if FIX_VERSION:
    fields["fixVersions"] = [{"name": FIX_VERSION}]

payload = {"fields": fields}

# --------------------------
# CREATE SUBTASK
# --------------------------

url = f"{JIRA_BASE_URL}/rest/api/3/issue"
resp = requests.post(url, headers=headers, data=json.dumps(payload))

if resp.status_code in (200, 201):
    data = resp.json()
    # Print only the created issue key
    print(data["key"])
else:
    print(f"ERROR {resp.status_code}: {resp.text}", file=sys.stderr)
    sys.exit(1)

