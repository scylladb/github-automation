#!/usr/bin/env python3
"""
create_jira_issue_from_gh_issue.py - Create a Jira issue from a newly opened GitHub issue.

Reads environment variables set by the GitHub Actions workflow and creates
a corresponding Jira issue in the configured project, including:
  - Title, description, labels, issue type mapping
  - Assignee / reporter lookup (best-effort by display name)
  - Clickable cross-links in both the Jira description footer and
    the GitHub issue body footer
  - Markdown-to-ADF conversion for the issue description
  - Inline image migration (download from GitHub, upload as Jira attachments,
    embed as mediaSingle ADF nodes)

Reuses HTTP helpers from jira_sync_modules to avoid code duplication.
"""

import base64
import hashlib
import json
import os
import re
import sys
import tempfile
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from jira_sync_modules import (
    _jira_get,
    _jira_post,
    _jira_put,
    _gh_api,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JIRA_BASE_URL = "https://scylladb.atlassian.net"

GITHUB_DOMAINS = [
    "github.com",
    "user-images.githubusercontent.com",
    "raw.githubusercontent.com",
    "avatars.githubusercontent.com",
    "camo.githubusercontent.com",
    "media.githubusercontent.com",
]

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
JIRA_AUTH = os.environ.get("JIRA_AUTH", "")          # email:api_token
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
JIRA_PROJECT_KEY = os.environ.get("JIRA_PROJECT_KEY", "")

# GitHub issue payload fields (set by the workflow)
ISSUE_TITLE = os.environ.get("ISSUE_TITLE", "")
ISSUE_BODY = os.environ.get("ISSUE_BODY", "")
ISSUE_NUMBER = os.environ.get("ISSUE_NUMBER", "")
ISSUE_HTML_URL = os.environ.get("ISSUE_HTML_URL", "")
ISSUE_LABELS = os.environ.get("ISSUE_LABELS", "")     # comma-separated
ISSUE_TYPE = os.environ.get("ISSUE_TYPE", "")          # GitHub issue type name
ISSUE_MILESTONE = os.environ.get("ISSUE_MILESTONE", "")
OWNER_REPO = os.environ.get("OWNER_REPO", "")

# Assignee / reporter (GitHub login + display name)
GH_ASSIGNEE_NAME = os.environ.get("GH_ASSIGNEE_NAME", "")
GH_REPORTER_NAME = os.environ.get("GH_REPORTER_NAME", "")


# ---------------------------------------------------------------------------
# Image helpers (download, upload, embed)
# ---------------------------------------------------------------------------

def _download_image(url: str) -> tuple[str | None, str | None]:
    """Download an image from *url* to a temp file.

    Uses GITHUB_TOKEN for authenticated access to private GitHub URLs.
    Returns (local_path, filename) or (None, None) on failure.
    """
    parsed = urlparse(url)
    req = Request(url, method="GET")
    if any(d in parsed.netloc for d in GITHUB_DOMAINS) and GITHUB_TOKEN:
        req.add_header("Authorization", f"Bearer {GITHUB_TOKEN}")
    try:
        with urlopen(req) as resp:
            data = resp.read()
        filename = os.path.basename(parsed.path) or "image.png"
        ext = os.path.splitext(filename)[1] or ".png"
        fd, temp_path = tempfile.mkstemp(suffix=ext)
        with os.fdopen(fd, "wb") as out:
            out.write(data)
        return temp_path, filename
    except (HTTPError, URLError, OSError) as exc:
        print(f"WARNING: Failed to download image {url}: {exc}")
        return None, None


def _upload_attachment(issue_key: str, local_path: str, filename: str) -> dict | None:
    """Upload a file to a Jira issue as an attachment (multipart/form-data).

    Returns the first attachment metadata dict from Jira, or None on failure.
    """
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/attachments"
    encoded_auth = base64.b64encode(JIRA_AUTH.encode()).decode()

    boundary = "----JiraAttachmentBoundary"
    with open(local_path, "rb") as fh:
        file_data = fh.read()

    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + file_data + f"\r\n--{boundary}--\r\n".encode()

    req = Request(url, data=body, method="POST")
    req.add_header("Authorization", f"Basic {encoded_auth}")
    req.add_header("X-Atlassian-Token", "no-check")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")

    try:
        with urlopen(req) as resp:
            attachments = json.loads(resp.read().decode())
            if attachments and isinstance(attachments, list):
                return attachments[0]
    except (HTTPError, URLError) as exc:
        body_text = exc.read().decode() if hasattr(exc, "read") and exc.fp else str(exc)
        print(f"WARNING: Attachment upload failed for {filename}: {body_text}")
    return None


def _get_media_uuid(attachment_id: str) -> str | None:
    """Follow the Jira attachment content redirect to extract the media UUID."""
    url = f"{JIRA_BASE_URL}/rest/api/3/attachment/content/{attachment_id}"
    encoded_auth = base64.b64encode(JIRA_AUTH.encode()).decode()

    req = Request(url, method="GET")
    req.add_header("Authorization", f"Basic {encoded_auth}")
    req.add_header("Accept", "application/json")

    try:
        # We need the redirect URL, not the content itself.
        # urllib follows redirects by default, so we catch the redirect manually.
        import http.client
        http.client.HTTPConnection.debuglevel = 0

        class NoRedirectHandler:
            pass

        import urllib.request
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                self.redirect_url = newurl
                return None

        handler = NoRedirect()
        opener = urllib.request.build_opener(handler)
        try:
            opener.open(req)
        except urllib.error.HTTPError as redir_exc:
            if redir_exc.code in (301, 302, 303, 307, 308):
                location = redir_exc.headers.get("Location", "")
            else:
                raise
        else:
            location = getattr(handler, "redirect_url", "")

        if location:
            # Try to extract UUID from the redirect location
            uuid_match = re.search(r"/file/([a-f0-9-]{36})/", location)
            if uuid_match:
                return uuid_match.group(1)
            uuid_match = re.search(
                r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
                location,
            )
            if uuid_match:
                return uuid_match.group(1)
    except Exception as exc:
        print(f"WARNING: Failed to get media UUID for attachment {attachment_id}: {exc}")
    return None


def _replace_images_with_placeholders(text: str) -> tuple[str, list[dict]]:
    """Replace ``![alt](url)`` and ``<img>`` tags with unique placeholders.

    Returns (new_text, images_list) where each image entry contains:
      num, alt, url, placeholder, unique_id
    """
    md_pattern = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
    html_pattern = re.compile(
        r'<img [^>]*src=["\']([^"\']+)["\'][^>]*?(?:alt=["\']([^"\']*)["\'])?[^>]*?>',
        re.IGNORECASE,
    )
    matches: list[tuple[int, int, str, str]] = []  # (start, end, alt, url)
    for m in md_pattern.finditer(text):
        matches.append((m.start(), m.end(), m.group(1), m.group(2)))
    for m in html_pattern.finditer(text):
        matches.append((m.start(), m.end(), m.group(2) or "image", m.group(1)))
    matches.sort()

    new_text = ""
    last_idx = 0
    images: list[dict] = []
    for img_num, (start, end, alt, url) in enumerate(matches, 1):
        new_text += text[last_idx:start]
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        unique_id = f"{img_num}-{url_hash}"
        placeholder = f"GitHub-Image-{unique_id}"
        new_text += placeholder
        images.append(
            {"num": img_num, "alt": alt, "url": url,
             "placeholder": placeholder, "unique_id": unique_id}
        )
        last_idx = end
    new_text += text[last_idx:]
    return new_text, images


def _download_and_upload_images(
    images: list[dict], issue_key: str
) -> list[dict]:
    """Download images from GitHub, upload to Jira, resolve media UUIDs.

    Returns a list of dicts with keys:
      placeholder, unique_id, attachment_id, media_uuid, attachment_filename, alt
    """
    uploaded: list[dict] = []
    for img in images:
        local_path, orig_filename = _download_image(img["url"])
        if not local_path:
            continue
        name, ext = os.path.splitext(orig_filename or "image.png")
        filename = f"GitHub-Image-{img['unique_id']}-{name}{ext}"
        try:
            att = _upload_attachment(issue_key, local_path, filename)
            if att:
                att_id = att.get("id")
                media_uuid = _get_media_uuid(str(att_id)) if att_id else None
                print(f"  Uploaded {filename} -> attachment {att_id}, media UUID {media_uuid}")
                uploaded.append({
                    "placeholder": img["placeholder"],
                    "unique_id": img["unique_id"],
                    "attachment_id": att_id,
                    "media_uuid": media_uuid,
                    "attachment_filename": att.get("filename", filename),
                    "alt": img["alt"],
                })
        finally:
            try:
                os.remove(local_path)
            except OSError:
                pass
    return uploaded


def _embed_images_in_adf(adf: dict, uploaded: list[dict]) -> dict:
    """Walk the ADF tree and replace placeholder text nodes with mediaSingle nodes."""
    if not uploaded:
        return adf

    placeholder_map = {att["placeholder"]: att for att in uploaded}
    placeholder_re = re.compile(r"GitHub-Image-\d+-[a-f0-9]{8}")

    def _process(node: dict) -> list[dict]:
        if not isinstance(node, dict):
            return [node]

        if node.get("type") == "paragraph":
            full_text = "".join(
                c.get("text", "") for c in node.get("content", []) if c.get("type") == "text"
            )
            m = placeholder_re.fullmatch(full_text.strip())
            if m and m.group(0) in placeholder_map:
                att = placeholder_map[m.group(0)]
                if att.get("media_uuid"):
                    return [{
                        "type": "mediaSingle",
                        "attrs": {"layout": "center"},
                        "content": [{
                            "type": "media",
                            "attrs": {
                                "id": att["media_uuid"],
                                "type": "file",
                                "collection": "",
                            },
                        }],
                    }]
                if att.get("attachment_id"):
                    fn = att.get("attachment_filename", "")
                    return [{
                        "type": "paragraph",
                        "content": [{
                            "type": "text",
                            "text": f"Image: {att.get('alt', 'image')}",
                            "marks": [{"type": "link", "attrs": {
                                "href": f"/secure/attachment/{att['attachment_id']}/{fn}"
                            }}],
                        }],
                    }]

        if "content" in node:
            new_children: list[dict] = []
            for child in node.get("content", []):
                new_children.extend(_process(child))
            return [{**node, "content": new_children}]

        return [node]

    new_content: list[dict] = []
    for n in adf.get("content", []):
        new_content.extend(_process(n))
    return {**adf, "content": new_content}


# ---------------------------------------------------------------------------
# Markdown -> ADF conversion
# ---------------------------------------------------------------------------

def _inline_markdown(text: str) -> list[dict]:
    """Convert inline markdown (bold, italic, code, links) to ADF inline nodes."""
    if not text:
        return [{"type": "text", "text": ""}]

    nodes: list[dict] = []
    pattern = re.compile(
        r"\*\*(.+?)\*\*"                       # group 1: bold
        r"|`([^`]+)`"                           # group 2: inline code
        r"|\[([^\]]+)\]\(([^)]+)\)"             # group 3,4: link text + url
        r"|(?<!\*)\*([^*\n]+?)\*(?!\*)"         # group 5: italic
    )

    last_end = 0
    for m in pattern.finditer(text):
        if m.start() > last_end:
            nodes.append({"type": "text", "text": text[last_end:m.start()]})

        if m.group(1) is not None:
            nodes.append({"type": "text", "text": m.group(1),
                          "marks": [{"type": "strong"}]})
        elif m.group(2) is not None:
            nodes.append({"type": "text", "text": m.group(2),
                          "marks": [{"type": "code"}]})
        elif m.group(3) is not None:
            nodes.append({"type": "text", "text": m.group(3),
                          "marks": [{"type": "link", "attrs": {"href": m.group(4)}}]})
        elif m.group(5) is not None:
            nodes.append({"type": "text", "text": m.group(5),
                          "marks": [{"type": "em"}]})

        last_end = m.end()

    if last_end < len(text):
        remaining = text[last_end:]
        if remaining:
            nodes.append({"type": "text", "text": remaining})

    return nodes if nodes else [{"type": "text", "text": text}]


def _markdown_to_adf_nodes(text: str) -> list[dict]:
    """Convert markdown text to a list of ADF block-level nodes."""
    nodes: list[dict] = []
    lines = text.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Blank line
        if not stripped:
            i += 1
            continue

        # Fenced code block
        if stripped.startswith("```"):
            lang = stripped[3:].strip()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            if i < len(lines):
                i += 1
            node: dict = {
                "type": "codeBlock",
                "content": [{"type": "text", "text": "\n".join(code_lines)}],
            }
            if lang:
                node["attrs"] = {"language": lang}
            nodes.append(node)
            continue

        # Heading
        hm = re.match(r"^(#{1,6})\s+(.*)", line)
        if hm:
            nodes.append({
                "type": "heading",
                "attrs": {"level": len(hm.group(1))},
                "content": _inline_markdown(hm.group(2).strip()),
            })
            i += 1
            continue

        # Horizontal rule
        if re.match(r"^[-*_]{3,}\s*$", stripped):
            nodes.append({"type": "rule"})
            i += 1
            continue

        # Blockquote
        if stripped.startswith(">"):
            quote_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_lines.append(re.sub(r"^>\s?", "", lines[i]))
                i += 1
            inner = _markdown_to_adf_nodes("\n".join(quote_lines))
            if not inner:
                inner = [{"type": "paragraph",
                          "content": [{"type": "text",
                                       "text": "\n".join(quote_lines)}]}]
            nodes.append({"type": "blockquote", "content": inner})
            continue

        # Bullet list
        if re.match(r"^[\s]*[-*+]\s", line):
            items: list[dict] = []
            while i < len(lines) and re.match(r"^[\s]*[-*+]\s", lines[i]):
                item_text = re.sub(r"^[\s]*[-*+]\s+", "", lines[i])
                items.append({
                    "type": "listItem",
                    "content": [{"type": "paragraph",
                                 "content": _inline_markdown(item_text)}],
                })
                i += 1
            nodes.append({"type": "bulletList", "content": items})
            continue

        # Ordered list
        if re.match(r"^[\s]*\d+[.)]\s", line):
            items = []
            while i < len(lines) and re.match(r"^[\s]*\d+[.)]\s", lines[i]):
                item_text = re.sub(r"^[\s]*\d+[.)]\s+", "", lines[i])
                items.append({
                    "type": "listItem",
                    "content": [{"type": "paragraph",
                                 "content": _inline_markdown(item_text)}],
                })
                i += 1
            nodes.append({"type": "orderedList", "content": items})
            continue

        # Regular paragraph
        para_lines: list[str] = []
        while (i < len(lines)
               and lines[i].strip()
               and not lines[i].strip().startswith("#")
               and not lines[i].strip().startswith("```")
               and not lines[i].strip().startswith(">")
               and not re.match(r"^[\s]*[-*+]\s", lines[i])
               and not re.match(r"^[\s]*\d+[.)]\s", lines[i])
               and not re.match(r"^[-*_]{3,}\s*$", lines[i].strip())):
            para_lines.append(lines[i])
            i += 1
        if para_lines:
            nodes.append({
                "type": "paragraph",
                "content": _inline_markdown(" ".join(para_lines)),
            })

    return nodes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _map_issue_type(gh_type_name: str, labels: list[str]) -> str:
    """Map a GitHub issue type / labels to a Jira issue type name."""
    if gh_type_name:
        low = gh_type_name.lower()
        if low == "bug":
            return "Bug"
        if low in ("enhancement", "feature"):
            return "Enhancement"
        if low == "epic":
            return "Epic"
    label_set = {l.lower() for l in labels}
    if "bug" in label_set:
        return "Bug"
    if "enhancement" in label_set:
        return "Enhancement"
    return "Task"


def _find_jira_account_id(display_name: str) -> str | None:
    """Best-effort lookup of a Jira accountId by display name."""
    if not display_name:
        return None
    url = f"{JIRA_BASE_URL}/rest/api/3/user/search?query={quote(display_name)}&maxResults=50"
    result = _jira_get(url, JIRA_AUTH)
    if result and isinstance(result, list):
        for user in result:
            if user.get("displayName", "").lower() == display_name.lower():
                return user["accountId"]
    return None


def _build_description_adf(body_text: str, gh_url: str) -> dict:
    """Build an ADF document from the GH issue markdown body + a footer link.

    Images are left as placeholder text nodes (replaced later after the Jira
    issue exists so that attachments can be uploaded).
    """
    content_nodes = _markdown_to_adf_nodes(body_text) if body_text else []

    # Divider before footer
    content_nodes.append({"type": "rule"})

    # Footer with clickable link to original GitHub issue
    content_nodes.append({
        "type": "paragraph",
        "content": [
            {"type": "text", "text": "Original GitHub issue: "},
            {
                "type": "text",
                "text": gh_url,
                "marks": [{"type": "link", "attrs": {"href": gh_url}}],
            },
        ],
    })

    return {"version": 1, "type": "doc", "content": content_nodes}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def create_jira_issue(body_for_adf: str) -> tuple[str | None, str]:
    """Create a Jira issue and return (jira_key, body_with_placeholders).

    *body_for_adf* is the issue body **after** images have been replaced with
    placeholders.  The caller is responsible for replacing the placeholders
    with embedded images after the issue is created.
    """
    labels = [l.strip() for l in ISSUE_LABELS.split(",") if l.strip()]
    issue_type = _map_issue_type(ISSUE_TYPE, labels)

    # Determine priority from labels
    priority = None
    priority_labels = {"P0", "P1", "P2", "P3", "P4"}
    for label in labels:
        if label in priority_labels:
            priority = label
            break

    description_adf = _build_description_adf(body_for_adf, ISSUE_HTML_URL)

    payload: dict = {
        "fields": {
            "project": {"key": JIRA_PROJECT_KEY},
            "summary": ISSUE_TITLE,
            "description": description_adf,
            "issuetype": {"name": issue_type},
            "labels": labels if labels else [],
        }
    }

    if ISSUE_MILESTONE:
        payload["fields"]["fixVersions"] = [{"name": ISSUE_MILESTONE}]

    if priority:
        payload["fields"]["priority"] = {"name": priority}

    # Reporter
    reporter_id = _find_jira_account_id(GH_REPORTER_NAME)
    if reporter_id:
        payload["fields"]["reporter"] = {"accountId": reporter_id}

    print(f"Creating Jira issue in project {JIRA_PROJECT_KEY}")
    print(f"  type     = {issue_type}")
    print(f"  summary  = {ISSUE_TITLE!r}")
    print(f"  labels   = {labels}")
    print(f"  priority = {priority}")
    print(f"  reporter = {GH_REPORTER_NAME!r} -> {reporter_id}")

    url = f"{JIRA_BASE_URL}/rest/api/3/issue"
    status, resp_body = _jira_post(url, payload, JIRA_AUTH)

    if status not in (200, 201):
        print(f"ERROR: Jira issue creation failed (HTTP {status})")
        print(resp_body)
        return None, body_for_adf

    jira_key = json.loads(resp_body)["key"]
    print(f"Created Jira issue: {jira_key}")

    # Assign (separate call)
    assignee_id = _find_jira_account_id(GH_ASSIGNEE_NAME)
    if assignee_id:
        print(f"  assignee = {GH_ASSIGNEE_NAME!r} -> {assignee_id}")
        assign_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{jira_key}/assignee"
        a_status, a_body = _jira_put(assign_url, {"accountId": assignee_id}, JIRA_AUTH)
        if a_status not in (200, 204):
            print(f"WARNING: Could not assign {jira_key} (HTTP {a_status}): {a_body}")

    return jira_key, body_for_adf


def _migrate_images(jira_key: str, images: list[dict], body_with_placeholders: str) -> None:
    """Download images from GitHub, upload to Jira, and update the description
    to embed them as inline mediaSingle nodes.
    """
    if not images:
        return

    print(f"Migrating {len(images)} image(s) to Jira issue {jira_key} ...")
    uploaded = _download_and_upload_images(images, jira_key)
    if not uploaded:
        print("  No images were successfully uploaded.")
        return

    import time
    time.sleep(3)  # Give Jira time to index attachments

    # Rebuild the ADF from the placeholder text and embed the uploaded images
    adf = _build_description_adf(body_with_placeholders, ISSUE_HTML_URL)
    final_adf = _embed_images_in_adf(adf, uploaded)

    # Update the issue description
    update_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{jira_key}"
    status, resp = _jira_put(
        update_url,
        {"fields": {"description": final_adf}},
        JIRA_AUTH,
    )
    if status in (200, 204):
        print(f"  Updated {jira_key} description with {len(uploaded)} embedded image(s).")
    else:
        print(f"  WARNING: Failed to update description with images (HTTP {status}): {resp}")


def append_jira_link_to_gh_issue(jira_key: str) -> None:
    """Append a Jira link footer to the GitHub issue body."""
    jira_url = f"{JIRA_BASE_URL}/browse/{jira_key}"
    footer = f"\n\n---\nJira issue: [{jira_key}]({jira_url})"

    current_body = ISSUE_BODY or ""
    new_body = current_body + footer

    owner, repo = OWNER_REPO.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/issues/{ISSUE_NUMBER}"

    status, body = _gh_api("PATCH", url, GITHUB_TOKEN, {"body": new_body})

    if status == 200:
        print(f"Updated GitHub issue #{ISSUE_NUMBER} with Jira link to {jira_key}")
    else:
        print(f"WARNING: Failed to update GitHub issue #{ISSUE_NUMBER} (HTTP {status})")
        print(body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print(" create_jira_issue_from_gh_issue")
    print("=" * 60)

    if not JIRA_AUTH:
        print("ERROR: JIRA_AUTH env var is not set.")
        sys.exit(1)
    if not JIRA_PROJECT_KEY:
        print("ERROR: JIRA_PROJECT_KEY env var is not set.")
        sys.exit(1)
    if not ISSUE_TITLE:
        print("ERROR: ISSUE_TITLE env var is not set.")
        sys.exit(1)
    if not GITHUB_TOKEN:
        print("ERROR: GITHUB_TOKEN env var is not set.")
        sys.exit(1)

    # --- Step 1: Replace images with placeholders ---
    body_with_placeholders, images = _replace_images_with_placeholders(ISSUE_BODY or "")
    if images:
        print(f"Found {len(images)} inline image(s) in the issue body.")

    # --- Step 2: Create Jira issue (with placeholders, no images yet) ---
    jira_key, _ = create_jira_issue(body_with_placeholders)
    if not jira_key:
        print("ERROR: Failed to create Jira issue. Exiting.")
        sys.exit(1)

    # --- Step 3: Upload images and embed in the description ---
    _migrate_images(jira_key, images, body_with_placeholders)

    # --- Step 4: Append Jira link to the GitHub issue ---
    append_jira_link_to_gh_issue(jira_key)

    print("=" * 60)
    print(f" Done. GitHub #{ISSUE_NUMBER} -> {jira_key}")
    print("=" * 60)


if __name__ == "__main__":
    main()