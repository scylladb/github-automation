#!/usr/bin/env python3
"""Snapshot of every open PR in a GitHub org, for Tentacle.

Tentacle (the RelEng action board) used to read each PR with ~6 REST calls
per person per refresh, which kept people's personal GitHub rate limit
exhausted. This job reads the org once, with one bot token, and writes a
JSON snapshot to a private repo. Every Tentacle reads that snapshot with a
couple of calls and filters it for the viewer.

Two passes keep the GraphQL cost low:
  * light pass, every run: all open PRs with the fields that change without
    the PR's updatedAt moving (CI rollup, mergeable, review decision, review
    requests, labels, assignees);
  * heavy pass, only for PRs that are new, changed (updatedAt moved) or whose
    heavy part is older than HEAVY_MAX_AGE: body, reviews, last comments and
    unresolved review threads. Everything else is reused from the previous
    snapshot.

Output (pushed as a single commit, force-pushed so history stays tiny):
  index.json            one compact entry per PR, used for filtering
  shards/<repo>-<k>.json  the full records, a few hundred KB each

The repo this runs in may be public: nothing about PRs is printed to the
log, only counts.
"""
import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

import requests

API = "https://api.github.com/graphql"
SEARCH_CAP = 1000            # GitHub search never returns more than this
LIGHT_PAGE = 100
HEAVY_BATCH = 20
HEAVY_MAX_AGE = 60 * 60      # re-read heavy fields at least hourly
SHARD_MAX_BYTES = 300_000
BODY_MAX = 3000
COMMENT_MAX = 1000
REVIEW_MAX = 500
MENTION_RE = re.compile(r"(?<![A-Za-z0-9-])@([A-Za-z0-9](?:[A-Za-z0-9-]{0,38}))")

REQ = """
  requestedReviewer {
    __typename
    ... on User { login }
    ... on Bot { login }
    ... on Mannequin { login }
    ... on Team { slug organization { login } }
  }
"""

LIGHT_Q = """
query($q: String!, $after: String) {
  rateLimit { cost remaining }
  search(query: $q, type: ISSUE, first: %d, after: $after) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        id number title url isDraft createdAt updatedAt
        baseRefName headRefName mergeable reviewDecision
        repository { name }
        author { __typename login }
        labels(first: 30) { nodes { name } }
        assignees(first: 10) { nodes { login } }
        reviewRequests(first: 20) { nodes { %s } }
        commits(last: 1) { nodes { commit { oid statusCheckRollup { state } } } }
        comments { totalCount }
      }
    }
  }
}
""" % (LIGHT_PAGE, REQ)

HEAVY_Q = """
query($ids: [ID!]!) {
  rateLimit { cost remaining }
  nodes(ids: $ids) {
    ... on PullRequest {
      id updatedAt body
      reviews(last: 40) { nodes { author { __typename login } state submittedAt bodyText commit { oid } } }
      comments(last: 10) { nodes { author { __typename login } body createdAt url } }
      reviewThreads(first: 50) {
        nodes {
          id isResolved isOutdated path line originalLine
          comments(last: 3) { totalCount nodes { author { __typename login } body createdAt url path line originalLine } }
        }
      }
    }
  }
}
"""


def log(msg):
    print(msg, flush=True)


class GraphQL:
    def __init__(self, token):
        self.s = requests.Session()
        self.s.headers.update({"Authorization": "bearer " + token, "Accept": "application/vnd.github+json"})
        self.cost = 0
        self.remaining = None

    def run(self, query, variables, tries=4):
        for attempt in range(tries):
            r = self.s.post(API, json={"query": query, "variables": variables}, timeout=90)
            if r.status_code in (502, 503, 504) or (r.status_code == 403 and "rate" in r.text.lower()):
                wait = int(r.headers.get("Retry-After") or 0) or 10 * (attempt + 1)
                log("GitHub answered %d, retrying in %ds" % (r.status_code, wait))
                time.sleep(min(wait, 120))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("errors") and not data.get("data"):
                raise RuntimeError("GraphQL error: %s" % json.dumps(data["errors"])[:500])
            rl = (data.get("data") or {}).get("rateLimit") or {}
            self.cost += rl.get("cost") or 0
            self.remaining = rl.get("remaining", self.remaining)
            return data["data"]
        raise RuntimeError("GitHub kept failing")


def login_of(actor):
    """REST-like login: bots get the [bot] suffix the REST API shows."""
    if not actor:
        return None
    login = actor.get("login")
    if login and actor.get("__typename") == "Bot" and not login.endswith("[bot]"):
        login += "[bot]"
    return login


def iso_day(d):
    return d.strftime("%Y-%m-%d")


def search_all(gql, base_query, since=None, until=None):
    """Every PR for base_query. Splits by creation date when a range holds
    more than the search cap."""
    rng = ""
    if since or until:
        rng = " created:%s..%s" % (iso_day(since) if since else "*", iso_day(until) if until else "*")
    q = base_query + rng
    out, after = [], None
    first = gql.run(LIGHT_Q, {"q": q, "after": None})["search"]
    if first["issueCount"] > SEARCH_CAP:
        lo = since or dt.date(2010, 1, 1)
        hi = until or dt.date.today() + dt.timedelta(days=1)
        if (hi - lo).days <= 1:
            log("warning: more than %d PRs created on one day, some are left out" % SEARCH_CAP)
        else:
            mid = lo + (hi - lo) / 2
            return search_all(gql, base_query, lo, mid) + search_all(gql, base_query, mid + dt.timedelta(days=1), hi)
    page = first
    while True:
        out.extend(n for n in page["nodes"] if n)
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
        page = gql.run(LIGHT_Q, {"q": q, "after": after})["search"]
    return out


def light_record(n):
    reqs, teams = [], []
    for rr in (n.get("reviewRequests") or {}).get("nodes") or []:
        rv = (rr or {}).get("requestedReviewer") or {}
        if rv.get("__typename") == "Team":
            teams.append("%s/%s" % ((rv.get("organization") or {}).get("login", ""), rv.get("slug", "")))
        elif rv.get("login"):
            reqs.append(login_of(rv))
    commit = (((n.get("commits") or {}).get("nodes") or [{}])[0] or {}).get("commit") or {}
    rollup = (commit.get("statusCheckRollup") or {}).get("state")
    return {
        "id": n["id"], "r": n["repository"]["name"], "n": n["number"], "t": n["title"], "u": n["url"],
        "d": bool(n.get("isDraft")), "c": n.get("createdAt"), "up": n.get("updatedAt"),
        "a": login_of(n.get("author")), "as": [x["login"] for x in (n.get("assignees") or {}).get("nodes") or [] if x],
        "rq": reqs, "rt": teams, "b": n.get("baseRefName"), "h": n.get("headRefName"),
        "m": n.get("mergeable"), "dec": n.get("reviewDecision"), "ci": rollup, "sha": commit.get("oid"),
        "lb": [x["name"] for x in (n.get("labels") or {}).get("nodes") or [] if x],
        "cc": (n.get("comments") or {}).get("totalCount", 0),
    }


def cut(s, n):
    s = s or ""
    return s if len(s) <= n else s[:n] + "…"


def heavy_record(n, now):
    reviews = [{"who": login_of(x.get("author")), "st": x.get("state"), "at": x.get("submittedAt"),
                "body": cut(x.get("bodyText"), REVIEW_MAX), "sha": (x.get("commit") or {}).get("oid")}
               for x in (n.get("reviews") or {}).get("nodes") or [] if x]
    comments = [{"who": login_of(x.get("author")), "body": cut(x.get("body"), COMMENT_MAX), "at": x.get("createdAt"), "url": x.get("url")}
                for x in (n.get("comments") or {}).get("nodes") or [] if x]
    threads = []
    for t in (n.get("reviewThreads") or {}).get("nodes") or []:
        if not t or t.get("isResolved"):
            continue
        cs = (t.get("comments") or {}).get("nodes") or []
        threads.append({"id": t.get("id"), "out": bool(t.get("isOutdated")), "path": t.get("path"),
                        "line": t.get("line"), "oline": t.get("originalLine"),
                        "n": (t.get("comments") or {}).get("totalCount", len(cs)),
                        "cs": [{"who": login_of(c.get("author")), "body": cut(c.get("body"), COMMENT_MAX), "at": c.get("createdAt"),
                                "url": c.get("url"), "path": c.get("path"), "line": c.get("line"), "oline": c.get("originalLine")}
                               for c in cs if c]})
    body = cut(n.get("body"), BODY_MAX)
    mentioned = set()
    for text in [body] + [c["body"] for c in comments] + [c["body"] for t in threads for c in t["cs"]] + [r["body"] for r in reviews]:
        mentioned.update(m.lower() for m in MENTION_RE.findall(text or ""))
    return {"body": body, "reviews": reviews, "comments": comments, "threads": threads,
            "mn": sorted(mentioned), "heavy_at": now, "heavy_up": n.get("updatedAt")}


HEAVY_KEYS = ("body", "reviews", "comments", "threads", "mn", "heavy_at", "heavy_up")


def load_previous(workdir):
    prev = {}
    shard_dir = os.path.join(workdir, "shards")
    if not os.path.isdir(shard_dir):
        return prev
    for f in os.listdir(shard_dir):
        try:
            with open(os.path.join(shard_dir, f)) as fh:
                for rec in json.load(fh).get("prs", {}).values():
                    if rec.get("id"):
                        prev[rec["id"]] = rec
        except (OSError, ValueError):
            continue
    return prev


def build(gql, org, prev, now_ts):
    light = search_all(gql, "org:%s is:pr is:open archived:false" % org)
    recs, need = {}, []
    for n in light:
        rec = light_record(n)
        old = prev.get(rec["id"])
        fresh = old and old.get("heavy_up") == rec["up"] and now_ts - (old.get("heavy_at") or 0) < HEAVY_MAX_AGE
        if fresh:
            rec.update({k: old[k] for k in HEAVY_KEYS if k in old})
        else:
            need.append(rec["id"])
        recs[rec["id"]] = rec
    for i in range(0, len(need), HEAVY_BATCH):
        ids = need[i:i + HEAVY_BATCH]
        for n in gql.run(HEAVY_Q, {"ids": ids})["nodes"]:
            if n and n.get("id") in recs:
                recs[n["id"]].update(heavy_record(n, now_ts))
    return list(recs.values()), len(light), len(need)


INDEX_KEYS = ("r", "n", "t", "u", "d", "c", "up", "a", "as", "rq", "rt", "b", "m", "dec", "ci", "lb", "cc", "mn")


def write(workdir, org, recs, now_iso):
    shard_dir = os.path.join(workdir, "shards")
    if os.path.isdir(shard_dir):
        shutil.rmtree(shard_dir)
    os.makedirs(shard_dir)
    recs.sort(key=lambda x: (x["r"], x["n"]))
    shards, cur, cur_repo, size, k = [], {}, None, 0, 0
    def flush():
        nonlocal cur, size
        if cur:
            shards.append((cur_name, cur))
        cur, size = {}, 0
    cur_name = None
    for rec in recs:
        blob = len(json.dumps(rec, separators=(",", ":")))
        if rec["r"] != cur_repo or size + blob > SHARD_MAX_BYTES:
            flush()
            k = 0 if rec["r"] != cur_repo else k + 1
            cur_repo = rec["r"]
            cur_name = "%s-%d.json" % (re.sub(r"[^A-Za-z0-9_.-]", "_", cur_repo), k)
        cur[str(rec["n"])] = rec
        rec["f"] = "shards/" + cur_name
        size += blob
    flush()
    for name, prs in shards:
        with open(os.path.join(shard_dir, name), "w") as fh:
            json.dump({"v": 1, "generated_at": now_iso, "prs": prs}, fh, separators=(",", ":"))
    index = {"v": 1, "generated_at": now_iso, "org": org, "count": len(recs),
             "prs": [dict({k: r.get(k) for k in INDEX_KEYS}, f=r["f"]) for r in recs]}
    with open(os.path.join(workdir, "index.json"), "w") as fh:
        json.dump(index, fh, separators=(",", ":"))
    with open(os.path.join(workdir, "README.md"), "w") as fh:
        fh.write("Snapshot of open %s PRs for Tentacle, written by scylladb/github-automation "
                 "(tentacle-snapshot workflow). Rewritten on every run, do not edit.\n" % org)
    return len(shards)


def git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--org", default="scylladb")
    ap.add_argument("--target", required=True, help="owner/repo that receives the snapshot (should be private)")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--dry-run", action="store_true", help="build into a temp dir, don't push")
    args = ap.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        sys.exit("GITHUB_TOKEN is not set")
    now = dt.datetime.now(dt.timezone.utc)
    now_ts, now_iso = int(now.timestamp()), now.strftime("%Y-%m-%dT%H:%M:%SZ")

    workdir = tempfile.mkdtemp(prefix="tentacle-snap-")
    url = "https://x-access-token:%s@github.com/%s.git" % (token, args.target)
    cloned = subprocess.run(["git", "clone", "--quiet", "--depth", "1", "--branch", args.branch, url, workdir],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if not cloned:
        log("no previous snapshot on %s, starting fresh" % args.branch)
        os.makedirs(workdir, exist_ok=True)
    prev = load_previous(workdir)

    gql = GraphQL(token)
    recs, n_open, n_heavy = build(gql, args.org, prev, now_ts)
    n_shards = write(workdir, args.org, recs, now_iso)
    log("open PRs: %d, re-read in full: %d, reused: %d, shards: %d, GraphQL cost: %d, remaining: %s"
        % (n_open, n_heavy, n_open - n_heavy, n_shards, gql.cost, gql.remaining))
    if args.dry_run:
        log("dry run, snapshot left in " + workdir)
        return

    # one commit, force-pushed: the repo never grows past one snapshot
    shutil.rmtree(os.path.join(workdir, ".git"), ignore_errors=True)
    git("init", "--quiet", "-b", args.branch, cwd=workdir)
    git("config", "user.name", "scylladbbot", cwd=workdir)
    git("config", "user.email", "scylladbbot@users.noreply.github.com", cwd=workdir)
    git("add", "-A", cwd=workdir)
    git("commit", "--quiet", "-m", "Snapshot %s (%d open PRs)" % (now_iso, n_open), cwd=workdir)
    git("push", "--quiet", "--force", url, "HEAD:" + args.branch, cwd=workdir)
    log("pushed snapshot to %s@%s" % (args.target, args.branch))


if __name__ == "__main__":
    main()
