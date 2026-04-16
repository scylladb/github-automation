#!/usr/bin/env python3
"""
jira_sync_logic.py
 - Top-level orchestrator and CLI dispatcher 
for Jira sync.

Contains manage_labeled_gh_ev
ent, manage_unlabeled_gh_event, manage_review
_gh_event (orchestration functions),
debug_sy
nc_context, the ACTION_DISPATCH table, and ma
in().

All helpers, constants, and individual
 action implementations live in
jira_sync_mod
ules.py.
"""

import json
import os
import sy
s
import argparse

from jira_sync_modules imp
ort (
    extract_jira_keys,
    add_label_to
_jira_issue,
    extract_jira_issue_details,

    apply_jira_labels_to_pr,
    jira_status_
transition,
    add_comment_to_jira,
    remo
ve_label_from_jira_issue,
)

# Sentinel value
 returned by extract_jira_keys when no keys a
re found.
_NO_KEYS = '["__NO_KEYS_FOUND__"]'


# Labels that should be ignored by the label
ed/unlabeled event handlers.
# When a PR labe
l event fires for one of these labels the aut
omation
# skips all Jira sync steps and exits
 early.
_EXCLUDED_LABELS: set[str] = {
    "s
tatus/ci_in_progress",
}

def manage_labeled_
gh_event(
    pr_title: str,
    pr_body: str
,
    pr_number: int,
    triggering_label: s
tr,
    owner_repo: str,
    gh_token: str,
 
   jira_auth: str,
) -> None:
    """Orchestr
ate every label-sync step in a single invocat
ion.

    Replicates the full job graph of ma
in_jira_sync_add_label.yml:

      1.  extrac
t_jira_keys
      2.  add_label_to_jira_issue
  (triggering label)
      3.  if label is st
atus/release_blocker  -> also add P0
      4.
  extract_jira_issue_details
      5.  apply_
jira_labels_to_pr
      6.  if label is statu
s/merge_candidate  -> transition to Ready for
 Merge
      7.  if label starts with promote
d-to-:
            a. add comment  b. transit
ion to Done
    """
    print("=" * 60)
    p
rint(" manage_labeled_gh_event  input paramet
ers")
    print("=" * 60)
    print(f"  pr_ti
tle         = {pr_title!r}")
    print(f"  pr
_body          = {pr_body!r}")
    print(f"  
pr_event         = {os.environ.get('CALLER_AC
TION', 'N/A')!r}")
    print(f"  pr_number   
     = {pr_number!r}")
    print(f"  triggeri
ng_label = {triggering_label!r}")
    print(f
"  owner_repo       = {owner_repo!r}")
    
 
   # --- Early exit: excluded labels ---
    
if triggering_label in _EXCLUDED_LABELS:
    
    print(f"SKIPPED: triggering_label '{trigg
ering_label}' is in the exclusion list. "
   
           "No Jira sync will be performed.")

        return

    # --- Step 1: extract ji
ra keys ---
    print("=" * 60)
    print(" S
tep 1 / extract_jira_keys")
    print("=" * 6
0)
    keys = extract_jira_keys(pr_title, pr_
body, jira_auth,
                            
 owner_repo=owner_repo,
                     
        pr_number=pr_number,
                
             gh_token=gh_token)
    jira_keys
_json = json.dumps(keys)
    print(f"jira-key
s-json={jira_keys_json}")

    if jira_keys_j
son == _NO_KEYS:
        print("No Jira keys 
found. Nothing to do.")
        return

    #
 --- Step 2: add the triggering label ---
   
 print("\n" + "=" * 60)
    print(" Step 2 / 
add_label_to_jira_issue")
    print("=" * 60)

    not_found = add_label_to_jira_issue(jira
_keys_json, triggering_label, jira_auth)

   
 # Remove issues that were not found (404) fr
om all subsequent steps
    if not_found:
   
     keys = [k for k in keys if k not in not_
found]
        jira_keys_json = json.dumps(ke
ys)
        print(f"Filtered jira-keys-json (
removed {len(not_found)} not-found): {jira_ke
ys_json}")
        if not keys:
            p
rint("All Jira keys were not found. Nothing m
ore to do.")
            return

    # --- St
ep 3: add P0 when status/release_blocker ---

    print("\n" + "=" * 60)
    print(" Step 3
 / add P0 (release_blocker)")
    print("=" *
 60)
    if triggering_label == "status/relea
se_blocker":
        add_label_to_jira_issue(
jira_keys_json, "P0", jira_auth)
    else:
  
      print(f"SKIPPED: triggering_label is '{
triggering_label}', not 'status/release_block
er'")

    # --- Step 4: extract issue detail
s ---
    print("\n" + "=" * 60)
    print(" 
Step 4 / extract_jira_issue_details")
    pri
nt("=" * 60)
    csv_content, labels_csv, det
ails_not_found = extract_jira_issue_details(j
ira_keys_json, jira_auth)

    if details_not
_found:
        keys = [k for k in keys if k 
not in details_not_found]
        jira_keys_j
son = json.dumps(keys)
        print(f"Filter
ed jira-keys-json after details (removed {len
(details_not_found)} not-found): {jira_keys_j
son}")
        if not keys:
            print
("All Jira keys were not found. Nothing more 
to do.")
            return

    # --- Step 5
: apply labels to PR ---
    print("\n" + "="
 * 60)
    print(" Step 5 / apply_jira_labels
_to_pr")
    print("=" * 60)
    apply_jira_l
abels_to_pr(
        pr_number, labels_csv, c
sv_content, triggering_label, owner_repo, gh_
token,
    )

    # --- Step 6: status/merge_
candidate -> Ready for Merge ---
    print("\
n" + "=" * 60)
    print(" Step 6 / jira_stat
us_transition -> Ready for Merge")
    print(
"=" * 60)
    if triggering_label == "status/
merge_candidate":
        jira_status_transit
ion(csv_content, "Ready for Merge", "131", ji
ra_auth)
    else:
        print(f"SKIPPED: t
riggering_label is '{triggering_label}', not 
'status/merge_candidate'")

    # --- Step 7:
 promoted-to-* label ---
    print("\n" + "="
 * 60)
    print(" Step 7a / add_comment_to_j
ira (promoted-to-*)")
    print("=" * 60)
   
 if triggering_label.startswith("promoted-to-
"):
        pr_url = f"https://github.com/{ow
ner_repo}/pull/{pr_number}"
        add_comme
nt_to_jira(
            jira_keys_json,
     
       f"Closed via {triggering_label} label 
on PR ",
            jira_auth,
            l
ink_text=pr_title,
            link_url=pr_ur
l,
        )
    else:
        print(f"SKIPPE
D: triggering_label is '{triggering_label}'"

              f" (requires label starting wit
h 'promoted-to-')")

    print("\n" + "=" * 6
0)
    print(" Step 7b / jira_status_transiti
on -> Done")
    print("=" * 60)
    if trigg
ering_label.startswith("promoted-to-"):
     
   jira_status_transition(csv_content, "Done"
, "141", jira_auth)
    else:
        print(f
"SKIPPED: triggering_label is '{triggering_la
bel}'"
              f" (requires label start
ing with 'promoted-to-')")

    print("\n" + 
"=" * 60)
    print(" manage_labeled_gh_event
 completed successfully")
    print("=" * 60)



def _run_manage_labeled_gh_event() -> None
:
    """CLI entry-point wrapper for manage_l
abeled_gh_event.

    Reads PR_TITLE, PR_BODY
, PR_NUMBER, TRIGGERING_LABEL,
    OWNER_REPO
, GITHUB_TOKEN, and JIRA_AUTH from environmen
t variables.
    """
    pr_title = os.enviro
n.get("PR_TITLE", "")
    pr_body = os.enviro
n.get("PR_BODY", "")
    pr_number_str = os.e
nviron.get("PR_NUMBER", "")
    triggering_la
bel = os.environ.get("TRIGGERING_LABEL", "")

    owner_repo = os.environ.get("OWNER_REPO",
 "")
    gh_token = os.environ.get("GITHUB_TO
KEN", "")
    jira_auth = os.environ.get("JIR
A_AUTH", "")

    if not pr_number_str:
     
   print("Error: PR_NUMBER env var is not set
 or empty.")
        sys.exit(1)

    try:
  
      pr_number = int(pr_number_str)
    exce
pt ValueError:
        print(f"Error: PR_NUMB
ER '{pr_number_str}' is not a valid integer."
)
        sys.exit(1)

    if not triggering_
label:
        print("Error: TRIGGERING_LABEL
 env var is not set or empty.")
        sys.e
xit(1)

    if not owner_repo:
        print(
"Error: OWNER_REPO env var is not set or empt
y.")
        sys.exit(1)

    if not gh_token
:
        print("Error: GITHUB_TOKEN env var 
is not set or empty.")
        sys.exit(1)

 
   if not jira_auth:
        print("Error: JI
RA_AUTH env var is not set or empty.")
      
  sys.exit(1)

    manage_labeled_gh_event(
 
       pr_title, pr_body, pr_number, triggeri
ng_label,
        owner_repo, gh_token, jira_
auth,
    )



def manage_review_gh_event(
  
  pr_title: str,
    pr_body: str,
    pr_num
ber: int,
    owner_repo: str,
    gh_token: 
str,
    requested_reviewer: str,
    jira_au
th: str,
) -> None:
    """Orchestrate the "I
n Review" sync in a single invocation.

    R
eplicates the full job graph of main_jira_syn
c_in_review.yml:

      1.  extract_jira_keys

      2.  extract_jira_issue_details
      3
.  apply_jira_labels_to_pr
      4.  jira_sta
tus_transition -> "In Review" (id 121)
    ""
"
    print("=" * 60)
    print(" manage_revi
ew_gh_event  input parameters")
    print("="
 * 60)
    print(f"  pr_title   = {pr_title!r
}")
    print(f"  pr_body    = {pr_body!r}")

    print(f"  pr_event   = {os.environ.get('C
ALLER_ACTION', 'N/A')!r}")
    print(f"  pr_n
umber  = {pr_number!r}")
    print(f"  owner_
repo = {owner_repo!r}")
    print(f"  request
ed_reviewer = {requested_reviewer!r}")

    #
 --- Step 1: extract jira keys ---
    print(
"\n" + "=" * 60)
    print(" Step 1 / extract
_jira_keys")
    print("=" * 60)
    keys = e
xtract_jira_keys(pr_title, pr_body, jira_auth
,
                             owner_repo=own
er_repo,
                             pr_numb
er=pr_number,
                             gh
_token=gh_token)
    jira_keys_json = json.du
mps(keys)
    print(f"jira-keys-json={jira_ke
ys_json}")

    if jira_keys_json == _NO_KEYS
:
        print("No Jira keys found. Nothing 
to do.")
        return

    # --- Step 2: ex
tract issue details ---
    print("\n" + "=" 
* 60)
    print(" Step 2 / extract_jira_issue
_details")
    print("=" * 60)
    csv_conten
t, labels_csv, details_not_found = extract_ji
ra_issue_details(jira_keys_json, jira_auth)


    if details_not_found:
        keys = [k f
or k in keys if k not in details_not_found]
 
       jira_keys_json = json.dumps(keys)
    
    print(f"Filtered jira-keys-json after det
ails (removed {len(details_not_found)} not-fo
und): {jira_keys_json}")
        if not keys:

            print("All Jira keys were not fo
und. Nothing more to do.")
            return


    # --- Step 3: apply labels to PR ---
  
  print("\n" + "=" * 60)
    print(" Step 3 /
 apply_jira_labels_to_pr")
    print("=" * 60
)
    apply_jira_labels_to_pr(
        pr_num
ber, labels_csv, csv_content, "", owner_repo,
 gh_token,
    )

    # --- Step 4: transitio
n to In Review ---
    print("\n" + "=" * 60)

    print(" Step 4 / jira_status_transition 
-> In Review")
    print("=" * 60)
    jira_s
tatus_transition(csv_content, "In Review", "1
21", jira_auth)

    print("\n" + "=" * 60)
 
   print(" manage_review_gh_event completed s
uccessfully")
    print("=" * 60)


def _run_
manage_review_gh_event() -> None:
    """CLI 
entry-point wrapper for manage_review_gh_even
t.

    Reads PR_TITLE, PR_BODY, PR_NUMBER, O
WNER_REPO,
    GITHUB_TOKEN, and JIRA_AUTH fr
om environment variables.
    """
    pr_titl
e = os.environ.get("PR_TITLE", "")
    pr_bod
y = os.environ.get("PR_BODY", "")
    pr_numb
er_str = os.environ.get("PR_NUMBER", "")
    
owner_repo = os.environ.get("OWNER_REPO", "")

    gh_token = os.environ.get("GITHUB_TOKEN"
, "")
    requested_reviewer = os.environ.get
("REQUESTED_REVIEWER", "")
    jira_auth = os
.environ.get("JIRA_AUTH", "")

    if not pr_
number_str:
        print("Error: PR_NUMBER e
nv var is not set or empty.")
        sys.exi
t(1)

    try:
        pr_number = int(pr_num
ber_str)
    except ValueError:
        print
(f"Error: PR_NUMBER '{pr_number_str}' is not 
a valid integer.")
        sys.exit(1)

    i
f not owner_repo:
        print("Error: OWNER
_REPO env var is not set or empty.")
        
sys.exit(1)

    if not gh_token:
        pri
nt("Error: GITHUB_TOKEN env var is not set or
 empty.")
        sys.exit(1)

    if not jir
a_auth:
        print("Error: JIRA_AUTH env v
ar is not set or empty.")
        sys.exit(1)


    manage_review_gh_event(
        pr_titl
e, pr_body, pr_number,
        owner_repo, gh
_token, requested_reviewer, jira_auth,
    )




def manage_closed_gh_event(
    pr_title: 
str,
    pr_body: str,
    pr_number: int,
  
  pr_merged: bool,
    owner_repo: str,
    g
h_token: str,
    jira_auth: str,
) -> None:

    """Orchestrate the "PR Closed" sync in a 
single invocation.

    Replicates the full j
ob graph of main_jira_sync_pr_closed.yml:

  
    1.  extract_jira_keys
      2.  extract_j
ira_issue_details
      3.  apply_jira_labels
_to_pr
      4.  add_comment_to_jira (merged:
 "Closed via PR merge"; not merged: "PR close
d without merge")
      5.  if merged: jira_s
tatus_transition -> "Done" (id 141)
    """
 
   print("=" * 60)
    print(" manage_closed_
gh_event  input parameters")
    print("=" * 
60)
    print(f"  pr_title   = {pr_title!r}")

    print(f"  pr_body    = {pr_body!r}")
   
 print(f"  pr_event   = {os.environ.get('CALL
ER_ACTION', 'N/A')!r}")
    print(f"  pr_numb
er  = {pr_number!r}")
    print(f"  pr_merged
  = {pr_merged!r}")
    print(f"  owner_repo 
= {owner_repo!r}")

    # --- Step 1: extract
 jira keys ---
    print("\n" + "=" * 60)
   
 print(" Step 1 / extract_jira_keys")
    pri
nt("=" * 60)
    keys = extract_jira_keys(pr_
title, pr_body, jira_auth,
                  
           owner_repo=owner_repo,
           
                  pr_number=pr_number,
      
                       gh_token=gh_token)
   
 jira_keys_json = json.dumps(keys)
    print(
f"jira-keys-json={jira_keys_json}")

    if j
ira_keys_json == _NO_KEYS:
        print("No 
Jira keys found. Nothing to do.")
        ret
urn

    # --- Step 2: extract issue details 
---
    print("\n" + "=" * 60)
    print(" St
ep 2 / extract_jira_issue_details")
    print
("=" * 60)
    csv_content, labels_csv, detai
ls_not_found = extract_jira_issue_details(jir
a_keys_json, jira_auth)

    if details_not_f
ound:
        keys = [k for k in keys if k no
t in details_not_found]
        jira_keys_jso
n = json.dumps(keys)
        print(f"Filtered
 jira-keys-json after details (removed {len(d
etails_not_found)} not-found): {jira_keys_jso
n}")
        if not keys:
            print("
All Jira keys were not found. Nothing more to
 do.")
            return

    # --- Step 3: 
apply labels to PR ---
    print("\n" + "=" *
 60)
    print(" Step 3 / apply_jira_labels_t
o_pr")
    print("=" * 60)
    apply_jira_lab
els_to_pr(
        pr_number, labels_csv, csv
_content, "", owner_repo, gh_token,
    )

  
  # --- Step 4: add "PR closed" comment ---
 
   print("\n" + "=" * 60)
    print(" Step 4 
/ add_comment_to_jira (PR closed)")
    print
("=" * 60)
    pr_url = f"https://github.com/
{owner_repo}/pull/{pr_number}"
    if pr_merg
ed:
        add_comment_to_jira(
            
jira_keys_json,
            "Closed via PR me
rge ",
            jira_auth,
            lin
k_text=pr_title,
            link_url=pr_url,

        )
    else:
        add_comment_to_j
ira(
            jira_keys_json,
            
"PR closed without merge ",
            jira_
auth,
            link_text=pr_title,
       
     link_url=pr_url,
        )

    # --- St
ep 5: transition to Done (merged PRs only) --
-
    print("\n" + "=" * 60)
    print(" Step
 5 / jira_status_transition -> Done")
    pri
nt("=" * 60)
    if pr_merged:
        jira_s
tatus_transition(csv_content, "Done", "141", 
jira_auth)
    else:
        print("SKIPPED: 
PR was closed without merge")

    print("\n"
 + "=" * 60)
    print(" manage_closed_gh_eve
nt completed successfully")
    print("=" * 6
0)


def _run_manage_closed_gh_event() -> Non
e:
    """CLI entry-point wrapper for manage_
closed_gh_event.

    Reads PR_TITLE, PR_BODY
, PR_NUMBER, PR_MERGED, OWNER_REPO,
    GITHU
B_TOKEN, and JIRA_AUTH from environment varia
bles.
    """
    pr_title = os.environ.get("
PR_TITLE", "")
    pr_body = os.environ.get("
PR_BODY", "")
    pr_number_str = os.environ.
get("PR_NUMBER", "")
    pr_merged_str = os.e
nviron.get("PR_MERGED", "false")
    owner_re
po = os.environ.get("OWNER_REPO", "")
    gh_
token = os.environ.get("GITHUB_TOKEN", "")
  
  jira_auth = os.environ.get("JIRA_AUTH", "")


    if not pr_number_str:
        print("Er
ror: PR_NUMBER env var is not set or empty.")

        sys.exit(1)

    try:
        pr_num
ber = int(pr_number_str)
    except ValueErro
r:
        print(f"Error: PR_NUMBER '{pr_numb
er_str}' is not a valid integer.")
        sy
s.exit(1)

    pr_merged = pr_merged_str.lowe
r() == "true"

    if not owner_repo:
       
 print("Error: OWNER_REPO env var is not set 
or empty.")
        sys.exit(1)

    if not g
h_token:
        print("Error: GITHUB_TOKEN e
nv var is not set or empty.")
        sys.exi
t(1)

    if not jira_auth:
        print("Er
ror: JIRA_AUTH env var is not set or empty.")

        sys.exit(1)

    manage_closed_gh_ev
ent(
        pr_title, pr_body, pr_number, pr
_merged,
        owner_repo, gh_token, jira_a
uth,
    )


def manage_opened_gh_event(
    
pr_title: str,
    pr_body: str,
    pr_numbe
r: int,
    owner_repo: str,
    gh_token: st
r,
    jira_auth: str,
) -> None:
    """Orch
estrate the "PR Opened" sync in a single invo
cation.

    Replicates the full job graph of
 main_jira_sync_pr_opened.yml:

      1.  ext
ract_jira_keys
      2.  extract_jira_issue_d
etails
      3.  apply_jira_labels_to_pr
    
  4.  jira_status_transition -> "In Progress"
 (id 111)
    """
    print("=" * 60)
    pri
nt(" manage_opened_gh_event  input parameters
")
    print("=" * 60)
    print(f"  pr_title
   = {pr_title!r}")
    print(f"  pr_body    
= {pr_body!r}")
    print(f"  pr_event   = {o
s.environ.get('CALLER_ACTION', 'N/A')!r}")
  
  print(f"  pr_number  = {pr_number!r}")
    
print(f"  owner_repo = {owner_repo!r}")

    
# --- Step 1: extract jira keys ---
    print
("\n" + "=" * 60)
    print(" Step 1 / extrac
t_jira_keys")
    print("=" * 60)
    keys = 
extract_jira_keys(pr_title, pr_body, jira_aut
h,
                             owner_repo=ow
ner_repo,
                             pr_num
ber=pr_number,
                             g
h_token=gh_token)
    jira_keys_json = json.d
umps(keys)
    print(f"jira-keys-json={jira_k
eys_json}")

    if jira_keys_json == _NO_KEY
S:
        print("No Jira keys found. Nothing
 to do.")
        return

    # --- Step 2: e
xtract issue details ---
    print("\n" + "="
 * 60)
    print(" Step 2 / extract_jira_issu
e_details")
    print("=" * 60)
    csv_conte
nt, labels_csv, details_not_found = extract_j
ira_issue_details(jira_keys_json, jira_auth)


    if details_not_found:
        keys = [k 
for k in keys if k not in details_not_found]

        jira_keys_json = json.dumps(keys)
   
     print(f"Filtered jira-keys-json after de
tails (removed {len(details_not_found)} not-f
ound): {jira_keys_json}")
        if not keys
:
            print("All Jira keys were not f
ound. Nothing more to do.")
            retur
n

    # --- Step 3: apply labels to PR ---
 
   print("\n" + "=" * 60)
    print(" Step 3 
/ apply_jira_labels_to_pr")
    print("=" * 6
0)
    apply_jira_labels_to_pr(
        pr_nu
mber, labels_csv, csv_content, "", owner_repo
, gh_token,
    )

    # --- Step 4: transiti
on to In Progress ---
    print("\n" + "=" * 
60)
    print(" Step 4 / jira_status_transiti
on -> In Progress")
    print("=" * 60)
    j
ira_status_transition(csv_content, "In Progre
ss", "111", jira_auth)

    print("\n" + "=" 
* 60)
    print(" manage_opened_gh_event comp
leted successfully")
    print("=" * 60)


de
f _run_manage_opened_gh_event() -> None:
    
"""CLI entry-point wrapper for manage_opened_
gh_event.

    Reads PR_TITLE, PR_BODY, PR_NU
MBER, OWNER_REPO,
    GITHUB_TOKEN, and JIRA_
AUTH from environment variables.
    """
    
pr_title = os.environ.get("PR_TITLE", "")
   
 pr_body = os.environ.get("PR_BODY", "")
    
pr_number_str = os.environ.get("PR_NUMBER", "
")
    owner_repo = os.environ.get("OWNER_REP
O", "")
    gh_token = os.environ.get("GITHUB
_TOKEN", "")
    jira_auth = os.environ.get("
JIRA_AUTH", "")

    if not pr_number_str:
  
      print("Error: PR_NUMBER env var is not 
set or empty.")
        sys.exit(1)

    try:

        pr_number = int(pr_number_str)
    e
xcept ValueError:
        print(f"Error: PR_N
UMBER '{pr_number_str}' is not a valid intege
r.")
        sys.exit(1)

    if not owner_re
po:
        print("Error: OWNER_REPO env var 
is not set or empty.")
        sys.exit(1)

 
   if not gh_token:
        print("Error: GIT
HUB_TOKEN env var is not set or empty.")
    
    sys.exit(1)

    if not jira_auth:
      
  print("Error: JIRA_AUTH env var is not set 
or empty.")
        sys.exit(1)

    manage_o
pened_gh_event(
        pr_title, pr_body, pr
_number,
        owner_repo, gh_token, jira_a
uth,
    )

def manage_unlabeled_gh_event(
  
  pr_title: str,
    pr_body: str,
    pr_num
ber: int,
    removed_label: str,
    owner_r
epo: str,
    gh_token: str,
    jira_auth: s
tr,
) -> None:
    """Orchestrate the "PR Unl
abeled" sync in a single invocation.

    Rep
licates the full job graph of main_jira_sync_
remove_label.yml:

      1.  extract_jira_key
s
      2.  remove_label_from_jira_issue  (sk
ip P0-P4 labels)
      3.  extract_jira_issue
_details
      4.  apply_jira_labels_to_pr
  
  """
    print("=" * 60)
    print(" manage_
unlabeled_gh_event  input parameters")
    pr
int("=" * 60)
    print(f"  pr_title       = 
{pr_title!r}")
    print(f"  pr_body        =
 {pr_body!r}")
    print(f"  pr_event       =
 {os.environ.get('CALLER_ACTION', 'N/A')!r}")

    print(f"  pr_number      = {pr_number!r}
")
    print(f"  removed_label  = {removed_la
bel!r}")
    print(f"  owner_repo     = {owne
r_repo!r}")

    # --- Early exit: excluded l
abels ---
    if removed_label in _EXCLUDED_L
ABELS:
        print(f"SKIPPED: removed_label
 '{removed_label}' is in the exclusion list. 
"
              "No Jira sync will be perform
ed.")
        return

    # --- Step 1: extra
ct jira keys ---
    print("\n" + "=" * 60)
 
   print(" Step 1 / extract_jira_keys")
    p
rint("=" * 60)
    keys = extract_jira_keys(p
r_title, pr_body, jira_auth,
                
             owner_repo=owner_repo,
         
                    pr_number=pr_number,
    
                         gh_token=gh_token)
 
   jira_keys_json = json.dumps(keys)
    prin
t(f"jira-keys-json={jira_keys_json}")

    if
 jira_keys_json == _NO_KEYS:
        print("N
o Jira keys found. Nothing to do.")
        r
eturn

    # --- Step 2: remove label from Ji
ra (skip priority labels) ---
    print("\n" 
+ "=" * 60)
    print(" Step 2 / remove_label
_from_jira_issue")
    print("=" * 60)
    _P
RIORITY_LABELS = {"P0", "P1", "P2", "P3", "P4
"}
    if removed_label in _PRIORITY_LABELS:

        print(f"SKIPPED: removed_label '{remo
ved_label}' is a priority label (P0-P4)")
   
 else:
        not_found = remove_label_from_
jira_issue(jira_keys_json, removed_label, jir
a_auth)

        # Remove issues that were no
t found (404) from subsequent steps
        i
f not_found:
            keys = [k for k in k
eys if k not in not_found]
            jira_k
eys_json = json.dumps(keys)
            print
(f"Filtered jira-keys-json (removed {len(not_
found)} not-found): {jira_keys_json}")
      
      if not keys:
                print("All
 Jira keys were not found. Nothing more to do
.")
                return

    # --- Step 3:
 extract issue details ---
    print("\n" + "
=" * 60)
    print(" Step 3 / extract_jira_is
sue_details")
    print("=" * 60)
    csv_con
tent, labels_csv, details_not_found = extract
_jira_issue_details(jira_keys_json, jira_auth
)

    if details_not_found:
        keys = [
k for k in keys if k not in details_not_found
]
        jira_keys_json = json.dumps(keys)
 
       print(f"Filtered jira-keys-json after 
details (removed {len(details_not_found)} not
-found): {jira_keys_json}")
        if not ke
ys:
            print("All Jira keys were not
 found. Nothing more to do.")
            ret
urn

    # --- Step 4: apply labels to PR ---

    print("\n" + "=" * 60)
    print(" Step 
4 / apply_jira_labels_to_pr")
    print("=" *
 60)
    apply_jira_labels_to_pr(
        pr_
number, labels_csv, csv_content, "", owner_re
po, gh_token,
    )

    print("\n" + "=" * 6
0)
    print(" manage_unlabeled_gh_event comp
leted successfully")
    print("=" * 60)


de
f _run_manage_unlabeled_gh_event() -> None:
 
   """CLI entry-point wrapper for manage_unla
beled_gh_event.

    Reads PR_TITLE, PR_BODY,
 PR_NUMBER, REMOVED_LABEL,
    OWNER_REPO, GI
THUB_TOKEN, and JIRA_AUTH from environment va
riables.
    """
    pr_title = os.environ.ge
t("PR_TITLE", "")
    pr_body = os.environ.ge
t("PR_BODY", "")
    pr_number_str = os.envir
on.get("PR_NUMBER", "")
    removed_label = o
s.environ.get("REMOVED_LABEL", "")
    owner_
repo = os.environ.get("OWNER_REPO", "")
    g
h_token = os.environ.get("GITHUB_TOKEN", "")

    jira_auth = os.environ.get("JIRA_AUTH", "
")

    if not pr_number_str:
        print("
Error: PR_NUMBER env var is not set or empty.
")
        sys.exit(1)

    try:
        pr_n
umber = int(pr_number_str)
    except ValueEr
ror:
        print(f"Error: PR_NUMBER '{pr_nu
mber_str}' is not a valid integer.")
        
sys.exit(1)

    if not removed_label:
      
  print("Error: REMOVED_LABEL env var is not 
set or empty.")
        sys.exit(1)

    if n
ot owner_repo:
        print("Error: OWNER_RE
PO env var is not set or empty.")
        sys
.exit(1)

    if not gh_token:
        print(
"Error: GITHUB_TOKEN env var is not set or em
pty.")
        sys.exit(1)

    if not jira_a
uth:
        print("Error: JIRA_AUTH env var 
is not set or empty.")
        sys.exit(1)

 
   manage_unlabeled_gh_event(
        pr_titl
e, pr_body, pr_number, removed_label,
       
 owner_repo, gh_token, jira_auth,
    )


def
 debug_sync_context():
    """Log GitHub even
t context and label-specific transition hints
."""
    event_name = os.environ.get('GITHUB_
EVENT_NAME', '')
    action = os.environ.get(
'GITHUB_EVENT_ACTION', '')
    jira_keys_json
 = os.environ.get('JIRA_KEYS_JSON', '')
    l
abel = os.environ.get('TRIGGERING_LABEL', '')

    repository = os.environ.get('GITHUB_REPO
SITORY', '')
    github_context = os.environ.
get('GITHUB_CONTEXT', '')

    print(f"event_
name='{event_name}'")
    print(f"action='{ac
tion}'")
    print(f"jira-keys-json='{jira_ke
ys_json}'")
    print(f"triggering-label='{la
bel}'")
    print(f"repository='{repository}'
")

    if label == 'status/merge_candidate':

        print("Try to transition Jira issue 
to 'Ready For Merge'")

    if label.startswi
th('promoted-to-'):
        print(f"Try to cl
ose Jira issue ({label} label added)")

    p
rint("~~~~~~~~~~~ GitHub Context ~~~~~~~~~~~"
)
    if github_context:
        try:
       
     parsed = json.loads(github_context)
    
        print(json.dumps(parsed, indent=2))
 
       except json.JSONDecodeError:
         
   print(github_context)
    else:
        pr
int("(GITHUB_CONTEXT not set)")


ACTION_DISP
ATCH = {
    'debug': debug_sync_context,
   
 'manage_labeled_gh_event': _run_manage_label
ed_gh_event,
    'manage_review_gh_event': _r
un_manage_review_gh_event,
    'manage_closed
_gh_event': _run_manage_closed_gh_event,
    
'manage_opened_gh_event': _run_manage_opened_
gh_event,
    'manage_unlabeled_gh_event': _r
un_manage_unlabeled_gh_event,
}

# Map GitHub
 event actions to orchestrator function names
.
# This allows the consolidated workflow to 
pass the raw github.event.action
# and have t
he script resolve the correct handler automat
ically.
EVENT_ACTION_MAP = {
    'opened': 'm
anage_opened_gh_event',
    'edited': 'manage
_opened_gh_event',
    'ready_for_review': 'm
anage_review_gh_event',
    'review_requested
': 'manage_review_gh_event',
    'labeled': '
manage_labeled_gh_event',
    'unlabeled': 'm
anage_unlabeled_gh_event',
    'closed': 'man
age_closed_gh_event',
}


def _resolve_action
(raw_action: str) -> str:
    """Resolve a ra
w --action value to an ACTION_DISPATCH key.


    Accepts either a direct ACTION_DISPATCH k
ey (e.g. manage_labeled_gh_event)
    or a Gi
tHub event action (e.g. labeled, closed, open
ed).
    """
    if raw_action in ACTION_DISP
ATCH:
        return raw_action
    resolved 
= EVENT_ACTION_MAP.get(raw_action)
    if res
olved:
        return resolved
    return raw
_action


def main():
    parser = argparse.A
rgumentParser(
        description='Jira sync
 logic for GitHub Actions workflows'
    )
  
  parser.add_argument(
        '--action',
  
      required=True,
        help='The action
 to execute (orchestrator name or GitHub even
t action)'
    )
    args = parser.parse_args
()

    action = _resolve_action(args.action)

    print(f"=== Jira Sync: {action} (raw inp
ut: {args.action}) ===")
    os.environ['CALL
ER_ACTION'] = args.action

    handler = ACTI
ON_DISPATCH.get(action)
    if not handler:
 
       valid = ', '.join(list(ACTION_DISPATCH
.keys()) + list(EVENT_ACTION_MAP.keys()))
   
     print(f"Error: Unknown action '{args.act
ion}'. Valid values: {valid}")
        sys.ex
it(1)

    handler()
    return 0


if __name
__ == '__main__':
    sys.exit(main())


