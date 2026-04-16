#!/usr/bin/env python3
"""
jira_sync_modules.
py - Shared constants, helpers, and action fu
nctions
for the Jira/GitHub sync workflows.


Contains:
  - All imports and shared constant
s
  - Private HTTP/utility helpers
  - Public
 action functions:
      extract_jira_keys
  
    add_label_to_jira_issue
      remove_labe
l_from_jira_issue
      extract_jira_issue_de
tails
      apply_jira_labels_to_pr
      jir
a_status_transition
      add_comment_to_jira


The orchestrator functions and CLI dispatch
er live in jira_sync_logic.py
which imports f
rom this module.
"""

import base64
import cs
v
import io
import json
import os
import re
i
mport sys
import time
from datetime import da
te
from urllib.request import Request, urlope
n
from urllib.error import URLError, HTTPErro
r



KNOWN_PROJECT_PREFIXES = {
    "ANSROLES
", "ARGUS", "CE", "CLOUD", "CLOUDEVOPS", "COR
EPROD",
    "CUSTOMER", "CXTOOLS", "DOCTOR", 
"DRIVER", "DTEST",
    "FIELDAUTO", "FIELDCLO
UD", "FIELDCLUS", "FIELDENG", "ILIAD",
    "O
PERATOR", "PKG", "PKGDASH", "PM", "PT", "PUB"
,
    "QAINFRA", "QATOOLS", "RELENG", "SCT", 
"SCYLLADB", "SMI",
    "STAG", "TOOLS", "UX",
 "VECTOR", "WEBINSTALL",
}

JIRA_BASE_URL = "
https://scylladb.atlassian.net"
SCYLLA_COMPON
ENTS_FIELD = "customfield_10321"
SYMPTOM_FIEL
D = "customfield_11120"

# Regex: any JIRA-st
yle key (PROJECT-123) in any text
_JIRA_KEY_R
E = re.compile(r'[A-Za-z]+-[0-9]+')

# Regex:
 closing keywords followed by a Jira key (opt
ionally as a browse URL)
_CLOSING_KEYWORD_RE 
= re.compile(
    r'(?:close|closes|closed|fi
x|fixes|fixed|resolve|resolves|resolved)'
   
 r'\s*[: ]\s*\[?\s*(?:https?://\S*/browse/)?(
[A-Za-z]+-[0-9]+)',
    re.IGNORECASE,
)

# P
riority names recognised as Jira priority val
ues
_PRIORITY_NAMES = {"P0", "P1", "P2", "P3"
, "P4"}


def _sanitize(text: str) -> str:
  
  """Remove carriage returns and backticks (m
atches the shell workflow)."""
    return tex
t.replace('\r', '').replace('`', ' ')


def _
extract_candidate_keys(text: str) -> list[str
]:
    """
    Extract candidate Jira keys fr
om arbitrary text (PR body, commit message).


    Only keys preceded by a closing keyword 
(Fixes, Closes, Resolves, etc.)
    are accep
ted. Bare key mentions are ignored.
    Retur
ns a sorted, deduplicated list.
    """
    c
andidates: set[str] = set()

    body = _sani
tize(text)

    # Closing-keyword keys (Fixes
, Closes, Resolves, etc.)
    candidates.upda
te(k.upper() for k in _CLOSING_KEYWORD_RE.fin
dall(body))

    return sorted(candidates)



def _fetch_jira_project_keys(jira_auth: str) 
-> set[str]:
    """
    Query the Jira REST 
API for all project keys.

    jira_auth is e
xpected as "email:api_token" (Basic auth).
  
  """
    url = f"{JIRA_BASE_URL}/rest/api/3/
project/search?maxResults=1000"

    encoded 
= base64.b64encode(jira_auth.encode()).decode
()

    req = Request(url)
    req.add_header
("Accept", "application/json")
    req.add_he
ader("Authorization", f"Basic {encoded}")

  
  try:
        with urlopen(req) as resp:
   
         data = json.loads(resp.read().decode
())
            return {project["key"] for pr
oject in data.get("values", [])}
    except (
HTTPError, URLError) as exc:
        print(f"
Warning: Jira project lookup failed: {exc}")

        return set()


def _fetch_commits(
  
  owner_repo: str, pr_number: int, gh_token: 
str,
) -> list[tuple[str, str]]:
    """
    
Fetch all commits for a PR via the GitHub RES
T API.

    Returns a list of (short_sha, mes
sage) tuples.
    Paginates automatically (up
 to 250 commits per PR).
    """
    results:
 list[tuple[str, str]] = []
    page = 1
    
per_page = 100

    while True:
        url =
 (f"https://api.github.com/repos/{owner_repo}
"
               f"/pulls/{pr_number}/commits
"
               f"?per_page={per_page}&page=
{page}")

        req = Request(url)
        
req.add_header("Accept", "application/vnd.git
hub+json")
        req.add_header("Authorizat
ion", f"Bearer {gh_token}")

        try:
   
         with urlopen(req) as resp:
         
       commits = json.loads(resp.read().decod
e())
        except (HTTPError, URLError) as 
exc:
            print(f"Warning: failed to f
etch PR commits (page {page}): {exc}")
      
      break

        if not commits:
        
    break

        for commit in commits:
   
         sha = commit.get("sha", "")[:10]
   
         msg = commit.get("commit", {}).get("
message", "")
            if msg:
           
     results.append((sha, msg))

        if l
en(commits) < per_page:
            break
   
     page += 1

    print(f"Fetched {len(resu
lts)} commit(s) from {owner_repo}#{pr_number}
")
    return results


def extract_jira_keys
(
    pr_title: str,
    pr_body: str,
    ji
ra_auth: str,
    owner_repo: str = "",
    p
r_number: int = 0,
    gh_token: str = "",
) 
-> list[str]:
    """
    Replicate the extra
ct_jira_keys.yml logic in pure Python.

    1
. Extract candidate JIRA keys from the PR bod
y and commit messages.
    2. Accept keys who
se project prefix is in the hard-coded set.
 
   3. For remaining keys, query the Jira API 
and accept valid prefixes.
    4. Return a so
rted, deduplicated list (or ["__NO_KEYS_FOUND
__"]).

    When owner_repo, pr_number, and g
h_token are provided the function
    also fe
tches the PR's commit messages from the GitHu
b API and scans
    them for closing-keyword 
patterns (Fixes, Closes, Resolves, etc.).
   
 """
    print(f"PR body: {pr_body}")

    if
 not jira_auth:
        print("Warning: jira_
auth is not set. "
              "Jira API fa
llback for unknown prefixes will be skipped."
)

    candidates = _extract_candidate_keys(p
r_body)

    # Track where each key was found
 for logging.
    key_origins: dict[str, list
[str]] = {}
    for key in candidates:
      
  key_origins.setdefault(key, []).append("PR 
body")

    # Also scan commit messages for c
losing-keyword Jira keys (PM-279).
    if own
er_repo and pr_number and gh_token:
        c
ommits = _fetch_commits(owner_repo, pr_number
, gh_token)
        for sha, msg in commits:

            commit_keys = _extract_candidate_
keys(msg)
            for key in commit_keys:

                key_origins.setdefault(key, 
[]).append(f"commit {sha}")
            candi
dates.extend(commit_keys)
        # Re-dedupl
icate after merging body + commit keys.
     
   candidates = sorted(set(candidates))
    e
lse:
        print("Skipping commit-message s
can "
              "(owner_repo/pr_number/gh
_token not provided)")

    if not candidates
:
        print("No Jira-like keys found in P
R body or commit messages")
        return ["
__NO_KEYS_FOUND__"]

    print("~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    print("Candidate keys:")
    for key in 
candidates:
        origins = ", ".join(key_o
rigins.get(key, ["unknown"]))
        print(f
"  {key}  (found in: {origins})")
    print("
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~")

    accepted: list[str] = []
 
   unknown: list[str] = []

    # --- Pass 1:
 hard-coded prefixes ---
    print(f"Known pr
oject prefixes (hard-coded): {' '.join(sorted
(KNOWN_PROJECT_PREFIXES))}")
    for key in c
andidates:
        prefix = key.split('-', 1)
[0]
        if prefix in KNOWN_PROJECT_PREFIX
ES:
            print(f"Accepting {key} via h
ard-coded prefix '{prefix}'.")
            ac
cepted.append(key)
        else:
            
print(f"Deferring {key} - prefix '{prefix}' n
ot in hard-coded list.")
            unknown.
append(key)

    # --- Pass 2: Jira API for u
nknown prefixes ---
    if unknown:
        p
rint("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~")
        print("Some prefi
xes not in hard-coded list; querying Jira for
 project keys...")
        print("Unknown-pre
fix candidates:")
        for key in unknown:

            print(f"  {key}")

        api_k
eys = _fetch_jira_project_keys(jira_auth)

  
      if api_keys:
            print(f"Valid 
Jira project keys from API (first 20): {' '.j
oin(sorted(api_keys)[:20])}")

        for ke
y in unknown:
            prefix = key.split(
'-', 1)[0]
            if prefix in api_keys:

                print(f"Accepting {key} via 
Jira API (valid project prefix '{prefix}').")

                accepted.append(key)
       
     else:
                print(f"Skipping {
key} - unknown project prefix '{prefix}' (not
 in Jira).")
    else:
        print("All pre
fixes resolved via hard-coded list; no Jira p
roject lookup needed.")

    print("~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~")

    if not accepted:
        print("No 
valid Jira keys found after validation")
    
    return ["__NO_KEYS_FOUND__"]

    result 
= sorted(set(accepted))
    print("Final Jira
 keys:")
    for key in result:
        print
(f"  {key}")

    return result


# ---------
---------------------------------------------
---------------------
# add_label_to_jira_iss
ue
# ----------------------------------------
-----------------------------------

def _par
se_jira_keys_json(raw: str) -> list[str]:
   
 """Parse and deduplicate a JSON array of Jir
a keys.

    Returns an empty list when the i
nput is empty, sentinel, or invalid.
    """

    raw = raw.strip()
    print(f"Incoming ji
ra_keys_json: {raw}")

    if not raw or raw 
in ('[]', '[""]', '["__NO_KEYS_FOUND__"]'):
 
       print("No usable Jira keys in jira_key
s_json; nothing to update.")
        return [
]

    try:
        data = json.loads(raw)
  
  except json.JSONDecodeError as exc:
       
 print(f"ERROR: jira_keys_json is not valid J
SON: {exc}", file=sys.stderr)
        sys.exi
t(1)

    if not isinstance(data, list):
    
    print(f"ERROR: jira_keys_json must be a J
SON array; got: {type(data)}", file=sys.stder
r)
        sys.exit(1)

    keys: list[str] =
 []
    seen: set[str] = set()
    for item i
n data:
        if not isinstance(item, str):

            continue
        k = item.strip(
)
        if k and k != "__NO_KEYS_FOUND__" a
nd k not in seen:
            seen.add(k)
   
         keys.append(k)

    print(f"Found {l
en(keys)} issue(s).")
    return keys


def _
determine_mode(label: str) -> tuple[str, str 
| None, dict | None]:
    """Decide the updat
e mode and build the appropriate JSON payload
.

    Returns (mode, priority_name_or_none, 
payload_dict).
    Modes: "priority", "scylla
_component", "symptom", "label".
    """
    
label_upper = label.upper()
    print(f"Incom
ing label: '{label}'")

    # P0..P4 -> set p
riority field
    if label_upper in _PRIORITY
_NAMES:
        payload = {"fields": {"priori
ty": {"name": label_upper}}}
        return "
priority", label_upper, payload

    # area/*
 -> add Scylla component
    if label.startsw
ith("area/"):
        component_value = label
[len("area/"):].replace("_", " ")
        pri
nt(f"Derived Scylla component value: '{compon
ent_value}' from label '{label}'")
        pa
yload = {
            "update": {
           
     SCYLLA_COMPONENTS_FIELD: [{"add": {"valu
e": component_value}}]
            }
        
}
        return "scylla_component", None, pa
yload

    # symptom/* -> add symptom custom 
field
    if label.startswith("symptom/"):
  
      symptom_value = label[len("symptom/"):]
.replace("_", " ")
        print(f"Derived sy
mptom value: '{symptom_value}' from label '{l
abel}'")
        payload = {
            "upd
ate": {
                SYMPTOM_FIELD: [{"add
": {"value": symptom_value}}]
            }
 
       }
        return "symptom", None, payl
oad

    # Fallback: normal Jira label
    pa
yload = {"update": {"labels": [{"add": label}
]}}
    return "label", None, payload


def _
jira_put(url: str, payload: dict, jira_auth: 
str) -> tuple[int, str]:
    """PUT JSON to a
 Jira REST endpoint. Returns (http_code, resp
onse_body)."""
    encoded_auth = base64.b64e
ncode(jira_auth.encode()).decode()
    body =
 json.dumps(payload).encode()

    req = Requ
est(url, data=body, method="PUT")
    req.add
_header("Accept", "application/json")
    req
.add_header("Content-Type", "application/json
")
    req.add_header("Authorization", f"Basi
c {encoded_auth}")

    try:
        with url
open(req) as resp:
            return resp.ge
tcode(), resp.read().decode()
    except HTTP
Error as exc:
        return exc.code, exc.re
ad().decode() if exc.fp else str(exc)
    exc
ept URLError as exc:
        print(f"Warning:
 network error - {exc}")
        return 0, st
r(exc)


def add_label_to_jira_issue(jira_key
s_json: str, label: str, jira_auth: str) -> l
ist[str]:
    """Add a label, priority, or Sc
ylla component to every Jira issue in *jira_k
eys_json*.

    Replicates the logic of add_l
abel_to_jira_issue.yml in pure Python.

    M
odes:
      - P0..P4           -> sets the is
sue priority field
      - area/<component>  
 -> adds a Scylla component (customfield_1032
1)
      - symptom/<symptom>  -> adds a sympt
om (customfield_11120)
      - anything else 
     -> adds a plain Jira label
    """
    p
rint(f"jira_keys_json={jira_keys_json}")

   
 if not label:
        print("Error: label is
 not set or empty.")
        sys.exit(1)

   
 if not jira_auth:
        print("Error: jira
_auth is not set or empty.")
        sys.exit
(1)

    keys = _parse_jira_keys_json(jira_ke
ys_json)
    if not keys:
        return []


    mode, priority_name, payload = _determine
_mode(label)

    if mode == "priority":
    
    action_desc = "Set priority"
        prin
t(f"Will set priority to: {priority_name}")
 
   elif mode == "scylla_component":
        a
ction_desc = "Add Scylla component"
        p
rint(f"Will add Scylla component derived from
 label: {label}")
    elif mode == "symptom":

        action_desc = "Add symptom"
        
print(f"Will add symptom derived from label: 
{label}")
    else:
        action_desc = "Ad
d label"
        print(f"Will add label: {lab
el}")

    ok = 0
    skipped = 0
    failed 
= 0
    not_found_keys: list[str] = []

    f
or key in keys:
        issue_url = f"{JIRA_B
ASE_URL}/rest/api/3/issue/{key}"
        prin
t(f"{action_desc} on {key} ...")

        cod
e, body_text = _jira_put(issue_url, payload, 
jira_auth)

        if code in (200, 204):
  
          print(f"OK {key} ({code})")
       
     ok += 1

        elif code == 400 and mo
de == "label":
            print(f"SKIP {key}
 ({code}) likely already has the label. First
 200 chars:")
            print(body_text[:20
0])
            skipped += 1

        elif mo
de in ("scylla_component", "symptom") and cod
e not in (200, 204):
            print(f"WARN
 {key} ({code}) custom field update failed. F
irst 200 chars:")
            print(body_text
[:200])
            print(f"Falling back to a
dding '{label}' as a plain Jira label on {key
} ...")
            fallback_payload = {"upda
te": {"labels": [{"add": label}]}}
          
  fb_code, fb_body = _jira_put(issue_url, fal
lback_payload, jira_auth)
            if fb_c
ode in (200, 204):
                print(f"OK
 {key} (fallback label, {fb_code})")
        
        ok += 1
            elif fb_code == 4
00:
                print(f"SKIP {key} (fallb
ack label, {fb_code}) likely already has the 
label.")
                skipped += 1
       
     elif fb_code == 404:
                pri
nt(f"SKIP {key} (fallback label, {fb_code}) i
ssue not found or no permission. Removing fro
m further processing.")
                skipp
ed += 1
                not_found_keys.append
(key)
            else:
                print
(f"FAIL {key} (fallback label, {fb_code}) Fir
st 400 chars:")
                print(fb_body
[:400])
                failed += 1

        
elif code == 404:
            print(f"SKIP {k
ey} ({code}) issue not found or no permission
. Removing from further processing.")
       
     skipped += 1
            not_found_keys.
append(key)

        else:
            print(
f"FAIL {key} ({code}) First 400 chars:")
    
        print(body_text[:400])
            fa
iled += 1

        time.sleep(0.2)

    print
(f"Summary: ok={ok} skipped={skipped} failed=
{failed}")
    if not_found_keys:
        pri
nt(f"Not-found keys (will be removed from fur
ther processing): {not_found_keys}")
    if f
ailed > 0:
        sys.exit(1)
    return not
_found_keys


def remove_label_from_jira_issu
e(jira_keys_json: str, label: str, jira_auth:
 str) -> list[str]:
    """Remove a label or 
Scylla component from every Jira issue in *ji
ra_keys_json*.

    Replicates the logic of r
emove_label_from_jira_issue.yml in pure Pytho
n.

    Modes:
      - area/<component>  -> r
emoves a Scylla component (customfield_10321)

      - symptom/<symptom> -> removes a Probl
em Symptom (customfield_11120)
      - anythi
ng else     -> removes a plain Jira label
   
 """
    print(f"jira_keys_json={jira_keys_js
on}")

    if not label:
        print("Error
: label is not set or empty.")
        sys.ex
it(1)

    if not jira_auth:
        print("E
rror: jira_auth is not set or empty.")
      
  sys.exit(1)

    keys = _parse_jira_keys_js
on(jira_keys_json)
    if not keys:
        r
eturn []

    print(f"Incoming removed label:
 '{label}'")

    if label.startswith("area/"
):
        mode = "scylla_component"
        
component_value = label[len("area/"):].replac
e("_", " ")
        payload = {
            "
update": {
                SCYLLA_COMPONENTS_
FIELD: [{"remove": {"value": component_value}
}]
            }
        }
        action_des
c = "Remove Scylla component"
        print(f
"Will remove Scylla component: '{component_va
lue}'")
    elif label.startswith("symptom/")
:
        mode = "symptom"
        symptom_va
lue = label[len("symptom/"):].replace("_", " 
")
        payload = {
            "update": 
{
                SYMPTOM_FIELD: [{"remove": 
{"value": symptom_value}}]
            }
    
    }
        action_desc = "Remove symptom"

        print(f"Will remove symptom: '{sympto
m_value}'")
    else:
        mode = "label"

        payload = {"update": {"labels": [{"re
move": label}]}}
        action_desc = "Remov
e label"
        print(f"Will remove label: '
{label}'")

    ok = 0
    skipped = 0
    fa
iled = 0
    not_found_keys: list[str] = []


    for key in keys:
        issue_url = f"{J
IRA_BASE_URL}/rest/api/3/issue/{key}"
       
 print(f"{action_desc} on {key} ...")

      
  code, body_text = _jira_put(issue_url, payl
oad, jira_auth)

        if code in (200, 204
):
            print(f"OK {key} ({code})")
  
          ok += 1

        elif code == 400 a
nd mode == "label":
            print(f"SKIP 
{key} ({code}) value may not exist in Jira. F
irst 200 chars:")
            print(body_text
[:200])
            skipped += 1

        eli
f mode in ("scylla_component", "symptom") and
 code not in (200, 204):
            print(f"
WARN {key} ({code}) custom field update faile
d. First 200 chars:")
            print(body_
text[:200])
            print(f"Falling back 
to removing '{label}' as a plain Jira label o
n {key} ...")
            fallback_payload = 
{"update": {"labels": [{"remove": label}]}}
 
           fb_code, fb_body = _jira_put(issue
_url, fallback_payload, jira_auth)
          
  if fb_code in (200, 204):
                p
rint(f"OK {key} (fallback label, {fb_code})")

                ok += 1
            elif fb_
code == 400:
                print(f"SKIP {ke
y} (fallback label, {fb_code}) label may not 
exist.")
                skipped += 1
       
     elif fb_code == 404:
                pri
nt(f"SKIP {key} (fallback label, {fb_code}) i
ssue not found or no permission. Removing fro
m further processing.")
                skipp
ed += 1
                not_found_keys.append
(key)
            else:
                print
(f"FAIL {key} (fallback label, {fb_code}) Fir
st 400 chars:")
                print(fb_body
[:400])
                failed += 1

        
elif code == 404:
            print(f"SKIP {k
ey} ({code}) issue not found or no permission
. Removing from further processing.")
       
     skipped += 1
            not_found_keys.
append(key)

        else:
            print(
f"FAIL {key} ({code}) First 400 chars:")
    
        print(body_text[:400])
            fa
iled += 1

        time.sleep(0.2)

    print
(f"Summary: ok={ok} skipped={skipped} failed=
{failed}")
    if not_found_keys:
        pri
nt(f"Not-found keys (will be removed from fur
ther processing): {not_found_keys}")
    if f
ailed > 0:
        sys.exit(1)
    return not
_found_keys


# -----------------------------
---------------------------------------------
-
# extract_jira_issue_details
# ------------
---------------------------------------------
------------------

# CSV columns produced by
 this action
_CSV_HEADER = "key,status,labels
,assignee,priority,fixVersions,scylla_compone
nts,symptoms,startDate,dueDate"
START_DATE_FI
ELD = "customfield_10015"
DUE_DATE_FIELD = "d
uedate"
_DETAIL_DELIM = ";"


def _jira_get(u
rl: str, jira_auth: str) -> dict | None:
    
"""GET JSON from a Jira REST endpoint. Return
s parsed JSON or None on failure."""
    enco
ded_auth = base64.b64encode(jira_auth.encode(
)).decode()

    req = Request(url)
    req.a
dd_header("Accept", "application/json")
    r
eq.add_header("Authorization", f"Basic {encod
ed_auth}")

    try:
        with urlopen(req
) as resp:
            return json.loads(resp
.read().decode())
    except (HTTPError, URLE
rror) as exc:
        print(f"Warning: GET {u
rl} failed: {exc}")
        return None


def
 _csv_escape(value: str) -> str:
    """Wrap 
a value in double-quotes for CSV, escaping in
ternal quotes."""
    return '"' + value.repl
ace('"', '""') + '"'


def extract_jira_issue
_details(jira_keys_json: str, jira_auth: str)
 -> tuple[str, str, list[str]]:
    """Fetch 
Jira issue details and produce a CSV plus a d
eduplicated labels string.

    Replicates th
e logic of extract_jira_issue_details.yml in 
pure Python.

    Returns (csv_content, label
s_csv, not_found_keys).
    not_found_keys li
sts issue keys that returned 404 or other fet
ch errors.
    """
    print(f"jira_keys_json
={jira_keys_json}")

    if not jira_auth:
  
      print("Error: jira_auth is not set or e
mpty.")
        sys.exit(1)

    keys = _pars
e_jira_keys_json(jira_keys_json)

    if not 
keys:
        print("------------------------
---------------------------")
        print("
Generated CSV (empty-keys short-circuit):")
 
       print("-------------------------------
--------------------")
        print(_CSV_HEA
DER)
        print("-------------------------
--------------------------")
        return _
CSV_HEADER + "\n", "", []

    fields_param =
 ",".join([
        "status", "labels", "assi
gnee", "priority", "fixVersions",
        SCY
LLA_COMPONENTS_FIELD, SYMPTOM_FIELD, START_DA
TE_FIELD, DUE_DATE_FIELD,
    ])

    csv_lin
es: list[str] = [_CSV_HEADER]
    all_labels:
 list[str] = []
    not_found_keys: list[str]
 = []

    for key in keys:
        url = f"{
JIRA_BASE_URL}/rest/api/3/issue/{key}?fields=
{fields_param}"
        print(f"Fetching Jira
 issue: {key}")

        resp = _jira_get(url
, jira_auth)
        if resp is None:
       
     print(f"Skipping {key} - fetch failed")

            not_found_keys.append(key)
      
      continue

        fields = resp.get("fi
elds", {})

        status = (fields.get("sta
tus") or {}).get("name", "")
        assignee
 = (fields.get("assignee") or {}).get("displa
yName", "")
        priority = (fields.get("p
riority") or {}).get("name", "")

        lab
els_list = fields.get("labels") or []
       
 labels_str = _DETAIL_DELIM.join(labels_list)

        all_labels.extend(labels_list)

    
    fix_versions_raw = fields.get("fixVersion
s") or []
        fix_versions = _DETAIL_DELI
M.join(
            v.get("name", "") for v i
n fix_versions_raw
        )

        compone
nts_raw = fields.get(SCYLLA_COMPONENTS_FIELD)

        if isinstance(components_raw, list):

            components = _DETAIL_DELIM.join(

                c.get("value", "") if isinst
ance(c, dict) else str(c)
                for
 c in components_raw
            )
        el
if components_raw is not None:
            co
mponents = str(components_raw)
        else:

            components = ""

        symptoms
_raw = fields.get(SYMPTOM_FIELD)
        if i
sinstance(symptoms_raw, list):
            sy
mptoms = _DETAIL_DELIM.join(
                
s.get("value", "") if isinstance(s, dict) els
e str(s)
                for s in symptoms_ra
w
            )
        elif symptoms_raw is 
not None:
            symptoms = str(symptoms
_raw)
        else:
            symptoms = ""


        start_date = fields.get(START_DATE_
FIELD) or ""
        due_date = fields.get(DU
E_DATE_FIELD) or ""

        row = ",".join([

            _csv_escape(key),
            _c
sv_escape(status),
            _csv_escape(la
bels_str),
            _csv_escape(assignee),

            _csv_escape(priority),
         
   _csv_escape(fix_versions),
            _cs
v_escape(components),
            _csv_escape
(symptoms),
            _csv_escape(start_dat
e),
            _csv_escape(due_date),
      
  ])
        csv_lines.append(row)

    # Ded
uplicate labels
    if all_labels:
        la
bels_csv = _DETAIL_DELIM.join(sorted(set(all_
labels)))
    else:
        labels_csv = ""


    csv_content = "\n".join(csv_lines) + "\n"


    print("--------------------------------
-------------------")
    print("Generated CS
V (after fetching issues):")
    print("Showi
ng first 20 lines:")
    print("-------------
--------------------------------------")
    
for line in csv_lines[:20]:
        print(lin
e)
    print("-------------------------------
--------------------")

    return csv_conten
t, labels_csv, not_found_keys


# -----------
---------------------------------------------
-------------------
# apply_jira_labels_to_pr

# ------------------------------------------
---------------------------------

# Jira pri
ority name -> P* rank mapping
_PRIORITY_RANK_
MAP = {
    "p0": 0, "highest": 0, "blocker":
 0,
    "p1": 1, "critical": 1, "high": 1,
  
  "p2": 2, "medium": 2, "major": 2,
    "p3":
 3, "low": 3, "minor": 3,
    "p4": 4, "lowes
t": 4, "trivial": 4,
}

_GH_API_VERSION = "20
22-11-28"


def _gh_api(method: str, url: str
, gh_token: str, payload: dict | None = None)
 -> tuple[int, str]:
    """Make a GitHub RES
T API request. Returns (http_code, response_b
ody)."""
    body = json.dumps(payload).encod
e() if payload else None

    req = Request(u
rl, data=body, method=method)
    req.add_hea
der("Accept", "application/vnd.github+json")

    req.add_header("Authorization", f"Bearer 
{gh_token}")
    req.add_header("X-GitHub-Api
-Version", _GH_API_VERSION)
    if body:
    
    req.add_header("Content-Type", "applicati
on/json; charset=utf-8")

    try:
        wi
th urlopen(req) as resp:
            return r
esp.getcode(), resp.read().decode()
    excep
t HTTPError as exc:
        return exc.code, 
exc.read().decode() if exc.fp else str(exc)



def _compute_labels(labels_csv: str, details
_csv: str, new_priority_label: str) -> list[s
tr]:
    """Compute the final list of labels 
to apply to a PR.

    1. Parse labels_csv in
to a deduped list.
    2. Strip any existing 
P0..P4 from that list.
    3. Parse details_c
sv to derive the best Jira priority (P*) and 
area/* labels.
    4. If the triggering event
 label is P0..P4 it overrides the Jira priori
ty.
    5. Append area/* labels.
    6. Appen
d symptom/* labels.
    """
    # 1) Parse la
bels_csv
    raw_labels = [s.strip() for s in
 labels_csv.split(_DETAIL_DELIM)]
    seen: s
et[str] = set()
    labels: list[str] = []
  
  for s in raw_labels:
        if s and s not
 in seen:
            seen.add(s)
           
 labels.append(s)

    # 2) Remove P0..P4 fro
m base list
    priority_names = {"P0", "P1",
 "P2", "P3", "P4"}
    labels = [lb for lb in
 labels if lb not in priority_names]

    # 3
) Parse details CSV for priority + scylla_com
ponents
    best_rank = None
    area_labels:
 list[str] = []
    area_seen: set[str] = set
()
    symptom_labels: list[str] = []
    sym
ptom_seen: set[str] = set()

    stripped_csv
 = details_csv.strip()
    if stripped_csv:
 
       reader = csv.DictReader(io.StringIO(st
ripped_csv))
        for row in reader:
     
       prio = (row.get("priority") or "").str
ip()
            if prio:
                ran
k = _PRIORITY_RANK_MAP.get(prio.lower())
    
            if rank is not None:
            
        if best_rank is None or rank < best_r
ank:
                        best_rank = rank


            comp_raw = (row.get("scylla_com
ponents") or "").strip()
            if comp_
raw:
                for part in comp_raw.spl
it(";"):
                    comp = part.stri
p()
                    if not comp:
        
                continue
                    
safe = re.sub(r"\s+", "_", comp)
            
        label = f"area/{safe}"
              
      if label not in area_seen:
            
            area_seen.add(label)
            
            area_labels.append(label)

      
      symp_raw = (row.get("symptoms") or "").
strip()
            if symp_raw:
            
    for part in symp_raw.split(";"):
        
            symp = part.strip()
             
       if not symp:
                        c
ontinue
                    safe = re.sub(r"\
s+", "_", symp)
                    label = f
"symptom/{safe}"
                    if label
 not in symptom_seen:
                       
 symptom_seen.add(label)
                    
    symptom_labels.append(label)

    # 4) De
cide P* label
    priority_label = None
    i
f best_rank is not None:
        priority_lab
el = f"P{best_rank}"

    new_p = (new_priori
ty_label or "").strip()
    if new_p and re.m
atch(r"(?i)^P[0-4]$", new_p):
        print(f
"Overriding Jira priority with PR-added label
: {new_p}")
        priority_label = new_p.up
per()

    if priority_label:
        print(f
"Effective priority label: {priority_label}")

        if priority_label not in labels:
   
         labels.insert(0, priority_label)
   
 else:
        print("No effective P* priorit
y (from Jira or event); not adding P* label."
)

    # 5) Append area/* labels
    for area
 in area_labels:
        if area not in label
s:
            labels.append(area)

    # 6) 
Append symptom/* labels
    for symp in sympt
om_labels:
        if symp not in labels:
   
         labels.append(symp)

    print(f"Fin
al labels to apply: {labels}")
    return lab
els


def _remove_stale_priority_labels(
    
owner_repo: str,
    pr_number: int,
    desi
red_labels: list[str],
    gh_token: str,
) -
> None:
    """Remove P0-P4 labels from a PR 
that are not in the desired set."""
    issue
_api = f"https://api.github.com/repos/{owner_
repo}/issues/{pr_number}"

    desired_p = {l
b for lb in desired_labels if re.match(r"^P[0
-4]$", lb)}
    if not desired_p:
        pri
nt("No desired P* labels computed; keeping ex
isting P* labels unchanged.")
        return


    print(f"Fetching existing labels for PR 
#{pr_number}...")
    code, body = _gh_api("G
ET", f"{issue_api}/labels", gh_token)
    if 
code != 200:
        print(f"Warning: failed 
to fetch existing labels (HTTP {code})")
    
    return

    existing = {item["name"] for 
item in json.loads(body)}
    existing_p = {l
b for lb in existing if re.match(r"^P[0-4]$",
 lb)}

    for p in sorted(existing_p):
     
   if p in desired_p:
            print(f"Kee
ping existing priority label: {p} (also desir
ed)")
        else:
            print(f"Remov
ing existing priority label not desired anymo
re: {p}")
            del_code, _ = _gh_api("
DELETE", f"{issue_api}/labels/{p}", gh_token)

            print(f"Delete {p} -> HTTP {del_
code}")


def apply_jira_labels_to_pr(
    pr
_number: int,
    labels_csv: str,
    detail
s_csv: str,
    new_priority_label: str,
    
owner_repo: str,
    gh_token: str,
) -> None
:
    """Apply Jira-derived labels to a GitHu
b PR.

    Replicates the logic of apply_labe
ls_to_pr.yml in pure Python.

    1. Compute 
the final label set (priority, area/*, plain 
labels).
    2. Remove stale P0-P4 labels fro
m the PR.
    3. Add each computed label to t
he PR via the GitHub API.
    """
    if not 
owner_repo:
        print("Error: owner_repo 
is not set or empty.")
        sys.exit(1)

 
   if not gh_token:
        print("Error: gh_
token is not set or empty.")
        sys.exit
(1)

    print("=============================
=========================")
    print(" Apply
 Labels to PR -- Input Parameters")
    print
("===========================================
===========")
    print(f"PR Number:       {p
r_number}")
    print(f"labels_csv:      {lab
els_csv}")
    print(f"NEW_PRIORITY_LABEL (fr
om event): {new_priority_label or '<none>'}")

    print("details_csv (first 5 lines):")
  
  print("------------------------------------
------------------")
    for i, line in enume
rate(details_csv.splitlines()):
        print
(line)
        if i >= 4:
            break
 
   print("-----------------------------------
-------------------\n")

    labels = _comput
e_labels(labels_csv, details_csv, new_priorit
y_label)

    if not labels:
        print("N
o labels to apply. Skipping.")
        return


    _remove_stale_priority_labels(owner_rep
o, pr_number, labels, gh_token)

    issue_ap
i = f"https://api.github.com/repos/{owner_rep
o}/issues/{pr_number}/labels"

    ok = 0
   
 failed = 0
    for lb in labels:
        lb 
= lb.strip()
        if not lb:
            c
ontinue

        print("---------------------
-------------------")
        print(f"Applyin
g label: '{lb}'")

        code, body_text = 
_gh_api("POST", issue_api, gh_token, {"labels
": [lb]})

        if code in (200, 201):
   
         print(f"Result: success (HTTP {code}
).")
            ok += 1
        else:
      
      print(f"Result: failed (HTTP {code}). B
ody (first 200 chars):")
            print(bo
dy_text[:200])
            failed += 1

    p
rint(f"Summary: ok={ok} failed={failed}")


#
 --------------------------------------------
-------------------------------
# jira_status
_transition
# -------------------------------
--------------------------------------------


_WORKING_STATES = {"in progress", "in review
", "ready for merge"}
_CLOSED_STATES = {"done
", "won't fix", "duplicate"}


def _jira_post
(url: str, payload: dict, jira_auth: str) -> 
tuple[int, str]:
    """POST JSON to a Jira R
EST endpoint. Returns (http_code, response_bo
dy)."""
    encoded_auth = base64.b64encode(j
ira_auth.encode()).decode()
    body = json.d
umps(payload).encode()

    req = Request(url
, data=body, method="POST")
    req.add_heade
r("Accept", "application/json")
    req.add_h
eader("Content-Type", "application/json")
   
 req.add_header("Authorization", f"Basic {enc
oded_auth}")

    try:
        with urlopen(r
eq) as resp:
            return resp.getcode(
), resp.read().decode()
    except HTTPError 
as exc:
        return exc.code, exc.read().d
ecode() if exc.fp else str(exc)
    except UR
LError as exc:
        print(f"Warning: netwo
rk error - {exc}")
        return 0, str(exc)



def _plan_transitions(
    details_csv: st
r, transition_name: str,
) -> tuple[list[tupl
e[str, str, str, str]], list[tuple[str, str]]
, list[tuple[str, str]]]:
    """Parse the de
tails CSV and categorize issues.

    Returns
 (to_transition, already_ok, done_issues).
  
  Each to_transition item is (key, current_st
atus, start_date, due_date).
    Each already
_ok / done_issues item is (key, current_statu
s).
    """
    to_transition: list[tuple[str
, str, str, str]] = []
    already_ok: list[t
uple[str, str]] = []
    done_issues: list[tu
ple[str, str]] = []

    stripped = details_c
sv.strip()
    if not stripped:
        retur
n to_transition, already_ok, done_issues

   
 reader = csv.DictReader(io.StringIO(stripped
))
    fieldmap = {(h or "").strip().lower():
 h for h in reader.fieldnames or []}

    def
 get(row: dict, name: str) -> str:
        h 
= fieldmap.get(name.lower())
        return (
row.get(h) or "").strip() if h else ""

    f
or row in reader:
        key = get(row, "key
")
        status = get(row, "status")
      
  start_dt = get(row, "startdate") or get(row
, "startDate")
        due_dt = get(row, "due
date") or get(row, "dueDate")
        if not 
key:
            continue

        if status.
lower() == transition_name.lower():
         
   already_ok.append((key, status))
        e
lif status.lower() in _CLOSED_STATES:
       
     done_issues.append((key, status))
      
  else:
            to_transition.append((key
, status, start_dt, due_dt))

    return to_t
ransition, already_ok, done_issues

def _set_
date_field(key: str, field_id: str, field_lab
el: str, jira_auth: str) -> None:
    """Set 
a date field on a Jira issue to today's date 
(UTC)."""
    today = date.today().isoformat(
)
    print(f"Setting {field_label} to {today
} for {key} (field: {field_id})")
    payload
 = {"fields": {field_id: today}}
    code, bo
dy_text = _jira_put(f"{JIRA_BASE_URL}/rest/ap
i/3/issue/{key}", payload, jira_auth)
    if 
code not in (200, 204):
        print(f"Warni
ng: Failed to set {field_label} for {key} (HT
TP {code})")
        print(body_text[:200])
 
   time.sleep(0.2)


def jira_status_transiti
on(
    details_csv: str,
    transition_name
: str,
    transition_id: str,
    jira_auth:
 str,
) -> None:
    """Transition Jira issue
s to a target status.

    Replicates the log
ic of jira_transition.yml in pure Python.

  
  1. Parse the details CSV and categorize iss
ues.
    2. For issues moving to a working st
ate, set start date if empty.
    3. For issu
es moving to a closed state, set due date if 
empty.
    4. POST the transition for each is
sue that needs it.
    """
    if not details
_csv:
        print("Error: details_csv is no
t set or empty.")
        sys.exit(1)

    if
 not transition_name:
        print("Error: t
ransition_name is not set or empty.")
       
 sys.exit(1)

    if not transition_id:
     
   print("Error: transition_id is not set or 
empty.")
        sys.exit(1)

    if not jira
_auth:
        print("Error: jira_auth is not
 set or empty.")
        sys.exit(1)

    pri
nt("=========================================
=============")
    print(" Jira Status Trans
ition -- Input Parameters")
    print("======
=============================================
===")
    print(f"Transition name: {transitio
n_name}")
    print(f"Transition ID:   {trans
ition_id}")
    print("details_csv (first 5 l
ines):")
    print("-------------------------
-----------------------------")
    for i, li
ne in enumerate(details_csv.splitlines()):
  
      print(line)
        if i >= 4:
        
    break
    print("------------------------
------------------------------\n")

    to_tr
ansition, already_ok, done_issues = _plan_tra
nsitions(details_csv, transition_name)

    p
rint(f"Target status:           {transition_n
ame}")
    print(f"Issues already at target: 
{len(already_ok)}")
    print(f"Issues in clo
sed state:   {len(done_issues)}")
    print(f
"Issues to transition:     {len(to_transition
)}")

    if already_ok:
        print("-----
 Already OK (up to 10) -----")
        for ke
y, status in already_ok[:10]:
            pri
nt(f"  {key} ({status})")

    if done_issues
:
        print("----- Done / closed issues (
up to 10) -----")
        for key, status in 
done_issues[:10]:
            print(f"  {key}
 ({status})")

    if not to_transition:
    
    print("No issues require transition. Done
.")
        return

    target_lower = transi
tion_name.lower()
    is_working = target_low
er in _WORKING_STATES
    is_closed = target_
lower in _CLOSED_STATES

    ok = 0
    faile
d = 0
    skipped = 0

    for key, current_s
tatus, start_dt, due_dt in to_transition:
   
     # Guard: do not regress issues that are 
further along in the workflow
        _FORBID
DEN_TRANSITIONS = {
            ('in review',
 'in progress'),
            ('ready for merg
e', 'in progress'),
            ('ready for m
erge', 'in review'),
            ('done', 'do
ne'),
        }
        if (current_status.lo
wer(), target_lower) in _FORBIDDEN_TRANSITION
S:
            print(f"SKIP {key}: refusing t
o move from '{current_status}' to '{transitio
n_name}'")
            skipped += 1
         
   continue
        print(f"Transitioning {ke
y} from '{current_status}' -> '{transition_na
me}' (id={transition_id})")

        # Set st
art date for working states if empty
        
if is_working and (not start_dt or start_dt =
= "null"):
            _set_date_field(key, S
TART_DATE_FIELD, "start date", jira_auth)

  
      # Set due date for closed states if emp
ty
        if is_closed and (not due_dt or du
e_dt == "null"):
            _set_date_field(
key, DUE_DATE_FIELD, "due date", jira_auth)


        # POST the transition
        url = f
"{JIRA_BASE_URL}/rest/api/3/issue/{key}/trans
itions"
        payload = {"transition": {"id
": transition_id}}
        code, body_text = 
_jira_post(url, payload, jira_auth)

        
if code in (200, 204):
            print(f"OK
 {key} ({code})")
            ok += 1
       
 elif code == 404:
            print(f"SKIP {
key} ({code}) issue not found or no permissio
n. Continuing.")
            skipped += 1
   
     else:
            print(f"FAIL {key} ({c
ode}) First 400 chars:")
            print(bo
dy_text[:400])
            failed += 1

     
   time.sleep(0.2)

    print(f"Summary: ok={
ok} skipped={skipped} failed={failed}")
    i
f failed > 0:
        print(f"WARNING: {faile
d} comment(s) failed. Continuing.")


# -----
---------------------------------------------
-------------------------
# add_comment_to_ji
ra
# ----------------------------------------
-----------------------------------


def _bu
ild_adf_comment(comment: str, link_text: str,
 link_url: str) -> dict:
    """Build an Atla
ssian Document Format (ADF) comment payload.


    If *link_text* and *link_url* are provid
ed the comment text is followed
    by a clic
kable link.  Otherwise the comment is rendere
d as plain text.
    """
    if link_text and
 link_url:
        content = [
            {"
type": "text", "text": comment},
            
{
                "type": "text",
           
     "text": link_text,
                "mark
s": [
                    {"type": "link", "a
ttrs": {"href": link_url}}
                ],

            },
        ]
    else:
        c
ontent = [{"type": "text", "text": comment}]


    return {
        "body": {
            "
type": "doc",
            "version": 1,
     
       "content": [
                {"type": 
"paragraph", "content": content}
            
],
        }
    }


def add_comment_to_jira(

    jira_keys_json: str,
    comment: str,
 
   jira_auth: str,
    link_text: str = "",
 
   link_url: str = "",
) -> None:
    """Add 
a comment to one or more Jira issues.

    Re
plicates the logic of add_comment_to_jira.yml
 in pure Python.

    Parameters
    --------
--
    jira_keys_json : str
        JSON arra
y of Jira issue keys, e.g. '["STAG-1","STAG-2
"]'.
    comment : str
        The comment te
xt (prefix before an optional link).
    jira
_auth : str
        Jira auth credential "ema
il:api_token".
    link_text : str, optional

        Display text for a clickable link app
ended to the comment.
    link_url : str, opt
ional
        URL for the clickable link.
   
 """
    print(f"jira_keys_json={jira_keys_js
on}")

    if not jira_auth:
        print("E
rror: jira_auth is not set or empty.")
      
  sys.exit(1)

    keys = _parse_jira_keys_js
on(jira_keys_json)
    if not keys:
        p
rint("No Jira keys to comment on.")
        r
eturn

    if not comment:
        print("Com
ment text is empty; nothing to do.")
        
return

    payload = _build_adf_comment(comm
ent, link_text, link_url)

    print(f"Adding
 comment to {len(keys)} issue(s)")
    print(
f"Comment: {comment}")
    if link_text:
    
    print(f"Link text: {link_text}")
    if l
ink_url:
        print(f"Link URL: {link_url}
")

    ok = 0
    skipped = 0
    failed = 0


    for key in keys:
        url = f"{JIRA_
BASE_URL}/rest/api/3/issue/{key}/comment"
   
     print(f"Posting comment on {key} ...")


        code, body_text = _jira_post(url, pay
load, jira_auth)

        if code in (200, 20
1):
            print(f"OK {key} ({code})")
 
           ok += 1
        elif code == 404:

            print(f"SKIP {key} ({code}) issue
 not found or no permission. Continuing.")
  
          skipped += 1
        else:
        
    print(f"FAIL {key} ({code}) First 400 cha
rs:")
            print(body_text[:400])
    
        failed += 1

        time.sleep(0.2)


    print(f"Summary: ok={ok} skipped={skippe
d} failed={failed}")
    if failed > 0:
     
   print(f"WARNING: {failed} comment(s) faile
d. Continuing.")




