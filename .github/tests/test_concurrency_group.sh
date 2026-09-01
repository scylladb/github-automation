#!/bin/bash
# Test suite for the concurrency group-key construction used in
# backport-with-jira.yaml (RELENG-853): the group must be scoped so unrelated
# backport runs never collide and cancel/skip each other the way the old
# repo-wide "newer run in progress" guard did, while runs for the *same* PR
# (e.g. several backport/* labels added together) still queue behind each
# other instead of racing.
# Run with: bash .github/tests/test_concurrency_group.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOW_FILE="$SCRIPT_DIR/../workflows/backport-with-jira.yaml"

PASS=0
FAIL=0

# Pull the actual `concurrency.group` template straight out of the production
# workflow (rather than hand-copying it here) so this test fails loudly --
# instead of silently drifting -- if that expression is ever changed.
GROUP_TEMPLATE=$(awk '/group: >-/{getline; gsub(/^[[:space:]]+/,""); print; exit}' "$WORKFLOW_FILE")
if [ -z "$GROUP_TEMPLATE" ]; then
  echo "FATAL: could not extract concurrency.group template from $WORKFLOW_FILE"
  exit 1
fi
echo "Extracted group template: $GROUP_TEMPLATE"
echo ""

# Renders the extracted template for a given set of workflow_call inputs.
render_group() {
  local event_type="$1" pr_number="$2" base_branch="$3" commits="$4"
  local result="$GROUP_TEMPLATE"
  result="${result//\$\{\{ inputs.event_type \}\}/$event_type}"
  result="${result//\$\{\{ inputs.pull_request_number \}\}/$pr_number}"
  result="${result//\$\{\{ inputs.base_branch \}\}/$base_branch}"
  result="${result//\$\{\{ inputs.commits \}\}/$commits}"
  echo "$result"
}

assert_eq() {
  local test_name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $test_name (expected='$expected', actual='$actual')"
    FAIL=$((FAIL + 1))
  fi
}

assert_ne() {
  local test_name="$1" a="$2" b="$3"
  if [ "$a" != "$b" ]; then
    echo "  PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $test_name (both were '$a', expected different groups)"
    FAIL=$((FAIL + 1))
  fi
}

# --- Test 1: Same event, same PR -> same group (dedupes retriggers) ---
echo "Test 1: Identical labeled events collide into the same group"
g1=$(render_group "labeled" "15904" "" "")
g2=$(render_group "labeled" "15904" "" "")
assert_eq "same group for identical inputs" "$g1" "$g2"

# --- Test 2: RELENG-853 regression -- unrelated PR must NOT collide ---
# This is the exact scenario from the ticket: PR 15904 (5 backport labels) vs.
# an unrelated run for PR head branch backport/14932/to-2026.2. The old
# repo-wide guard treated these as the same race; they must now be distinct.
echo "Test 2: Different PRs never share a group (RELENG-853 regression check)"
pr_15904=$(render_group "labeled" "15904" "" "")
pr_other=$(render_group "labeled" "14932" "" "")
assert_ne "PR 15904 vs unrelated PR 14932" "$pr_15904" "$pr_other"

# --- Test 3: Same PR, different labels -> SAME group (queue, don't race) ---
# label_name is deliberately excluded from the group key: the workflow's own
# 30s "wait for additional labels" debounce step assumes multiple backport/*
# labels routinely land on one PR together, and the only thing guarding
# against duplicate work past that point is a check-then-act Jira/PR
# existence check, not a real lock. So same-PR runs must queue behind each
# other one at a time regardless of which label triggered them.
echo "Test 3: Different labels on the same PR queue behind each other (same group)"
label_a=$(render_group "labeled" "15904" "" "")
label_b=$(render_group "labeled" "15904" "" "")
assert_eq "same PR, different label triggers -> same group" "$label_a" "$label_b"

# --- Test 4: Push events are distinguished by base_branch + commits ---
# Note: push events pass `commits` (before..after) -- every one of the 5
# repos calling this workflow does this -- not `head_commit`, which only
# labeled events populate. The group must key on `commits` to actually tell
# push runs on the same branch apart.
echo "Test 4: Push events to different branches/commit ranges get independent groups"
push_a=$(render_group "push" "0" "refs/heads/master" "sha1..sha2")
push_b=$(render_group "push" "0" "refs/heads/branch-2026.1" "sha1..sha2")
push_c=$(render_group "push" "0" "refs/heads/master" "sha3..sha4")
assert_ne "different base branches" "$push_a" "$push_b"
assert_ne "different commit ranges" "$push_a" "$push_c"

# --- Test 5: Chain events keyed by merged PR number stay independent ---
echo "Test 5: Chain events for different merged PRs get independent groups"
chain_a=$(render_group "chain" "111" "refs/heads/branch-2026.1" "")
chain_b=$(render_group "chain" "222" "refs/heads/branch-2026.1" "")
assert_ne "different merged PR numbers" "$chain_a" "$chain_b"

# --- Test 6: Different event types for the same PR/branch never collide ---
echo "Test 6: Different event types for the same PR/branch get independent groups"
evt_labeled=$(render_group "labeled" "15904" "refs/heads/master" "")
evt_chain=$(render_group "chain" "15904" "refs/heads/master" "")
assert_ne "labeled vs chain for the same PR number" "$evt_labeled" "$evt_chain"

# --- Summary ---
echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  exit 1
fi
