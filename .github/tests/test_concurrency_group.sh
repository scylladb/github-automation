#!/bin/bash
# Test suite for the concurrency group-key construction used in
# backport-with-jira.yaml (RELENG-853): the group must be scoped per PR/label/
# branch/commit so unrelated backport runs never collide and cancel/skip each
# other the way the old repo-wide "newer run in progress" guard did.
# Run with: bash .github/tests/test_concurrency_group.sh

PASS=0
FAIL=0

# Mirrors the `concurrency.group` expression in backport-with-jira.yaml:
#   backport-${{ inputs.event_type }}-${{ inputs.pull_request_number }}-${{ inputs.label_name }}-${{ inputs.base_branch }}-${{ inputs.head_commit }}
build_group() {
  local event_type="$1" pr_number="$2" label_name="$3" base_branch="$4" head_commit="$5"
  echo "backport-${event_type}-${pr_number}-${label_name}-${base_branch}-${head_commit}"
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

# --- Test 1: Same event, same PR, same label -> same group (dedupes retriggers) ---
echo "Test 1: Identical labeled events collide into the same group"
g1=$(build_group "labeled" "15904" "backport/2026.2" "" "")
g2=$(build_group "labeled" "15904" "backport/2026.2" "" "")
assert_eq "same group for identical inputs" "$g1" "$g2"

# --- Test 2: RELENG-853 regression -- unrelated PR must NOT collide ---
# This is the exact scenario from the ticket: PR 15904 (5 backport labels) vs.
# an unrelated run for PR head branch backport/14932/to-2026.2. The old
# repo-wide guard treated these as the same race; they must now be distinct.
echo "Test 2: Different PRs never share a group (RELENG-853 regression check)"
pr_15904=$(build_group "labeled" "15904" "backport/2026.2" "" "")
pr_other=$(build_group "labeled" "14932" "backport/2026.2" "" "")
assert_ne "PR 15904 vs unrelated PR 14932" "$pr_15904" "$pr_other"

# --- Test 3: Same PR, different labels -> different groups (no cross-label blocking) ---
echo "Test 3: Different labels on the same PR get independent groups"
label_a=$(build_group "labeled" "15904" "backport/2025.1" "" "")
label_b=$(build_group "labeled" "15904" "backport/perf-v17" "" "")
label_c=$(build_group "labeled" "15904" "backport/2026.1" "" "")
assert_ne "2025.1 vs perf-v17" "$label_a" "$label_b"
assert_ne "perf-v17 vs 2026.1" "$label_b" "$label_c"
assert_ne "2025.1 vs 2026.1" "$label_a" "$label_c"

# --- Test 4: Push events are distinguished by base_branch + head_commit ---
echo "Test 4: Push events to different branches/commits get independent groups"
push_a=$(build_group "push" "0" "" "refs/heads/master" "abc123")
push_b=$(build_group "push" "0" "" "refs/heads/branch-2026.1" "abc123")
push_c=$(build_group "push" "0" "" "refs/heads/master" "def456")
assert_ne "different base branches" "$push_a" "$push_b"
assert_ne "different head commits" "$push_a" "$push_c"

# --- Test 5: Chain events keyed by merged PR number stay independent ---
echo "Test 5: Chain events for different merged PRs get independent groups"
chain_a=$(build_group "chain" "111" "" "refs/heads/branch-2026.1" "")
chain_b=$(build_group "chain" "222" "" "refs/heads/branch-2026.1" "")
assert_ne "different merged PR numbers" "$chain_a" "$chain_b"

# --- Summary ---
echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  exit 1
fi
