#!/bin/bash
# Test suite for the gh_retry() function used in backport-with-jira.yaml
# Run with: bash .github/tests/test_gh_retry.sh
set -uo pipefail

PASS=0
FAIL=0
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# The gh_retry function under test (with 0 sleep for fast tests)
gh_retry() {
  local max_attempts=3
  local delay=0
  local attempt=1
  local output
  local status
  while [ $attempt -le $max_attempts ]; do
    output=$("$GH_MOCK" "$@" 2>&1)
    status=$?
    if [ $status -eq 0 ]; then
      echo "$output"
      return 0
    fi
    if echo "$output" | grep -qi "error connecting\|connection refused\|timeout\|ETIMEDOUT\|network\|502\|503"; then
      echo "::warning::gh command failed (attempt ${attempt}/${max_attempts}): $output" >&2
      if [ $attempt -lt $max_attempts ]; then
        sleep $delay
        delay=$((delay * 2))
      fi
    else
      echo "$output"
      return $status
    fi
    attempt=$((attempt + 1))
  done
  echo "::error::gh command failed after ${max_attempts} attempts: $output" >&2
  echo "$output"
  return $status
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

# --- Test 1: Successful command on first attempt ---
echo "Test 1: Success on first attempt"
cat > "$TMPDIR/mock_gh" << 'EOF'
#!/bin/bash
echo "success output"
exit 0
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
assert_eq "returns output" "success output" "$output"

# --- Test 2: Non-transient error fails immediately ---
echo "Test 2: Non-transient error fails immediately"
cat > "$TMPDIR/mock_gh" << 'EOF'
#!/bin/bash
echo "permission denied"
exit 1
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
assert_eq "returns error output" "permission denied" "$output"

# --- Test 3: Transient error retries then succeeds ---
echo "Test 3: Transient error retries then succeeds"
COUNTER="$TMPDIR/counter3"
echo "0" > "$COUNTER"
cat > "$TMPDIR/mock_gh" << EOF
#!/bin/bash
count=\$(cat "$COUNTER")
count=\$((count + 1))
echo "\$count" > "$COUNTER"
if [ \$count -lt 3 ]; then
  echo "error connecting to api.github.com"
  exit 1
fi
echo "success after retry"
exit 0
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
status=$?
assert_eq "succeeds after retries" "success after retry" "$output"
assert_eq "exit code is 0" "0" "$status"
assert_eq "called 3 times" "3" "$(cat $COUNTER)"

# --- Test 4: Transient error exhausts all retries ---
echo "Test 4: Transient error exhausts retries"
cat > "$TMPDIR/mock_gh" << 'EOF'
#!/bin/bash
echo "connection refused"
exit 1
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
status=$?
assert_eq "returns last error" "connection refused" "$output"
assert_eq "exit code is non-zero" "1" "$status"

# --- Test 5: 502 error is treated as transient ---
echo "Test 5: 502 error is transient"
COUNTER="$TMPDIR/counter5"
echo "0" > "$COUNTER"
cat > "$TMPDIR/mock_gh" << EOF
#!/bin/bash
count=\$(cat "$COUNTER")
count=\$((count + 1))
echo "\$count" > "$COUNTER"
if [ \$count -lt 2 ]; then
  echo "HTTP 502 Bad Gateway"
  exit 1
fi
echo "ok"
exit 0
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
assert_eq "recovers from 502" "ok" "$output"

# --- Test 6: ETIMEDOUT is treated as transient ---
echo "Test 6: ETIMEDOUT is transient"
COUNTER="$TMPDIR/counter6"
echo "0" > "$COUNTER"
cat > "$TMPDIR/mock_gh" << EOF
#!/bin/bash
count=\$(cat "$COUNTER")
count=\$((count + 1))
echo "\$count" > "$COUNTER"
if [ \$count -lt 2 ]; then
  echo "dial tcp: ETIMEDOUT"
  exit 1
fi
echo "recovered"
exit 0
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
assert_eq "recovers from ETIMEDOUT" "recovered" "$output"

# --- Test 7: 503 error is treated as transient ---
echo "Test 7: 503 error is transient"
COUNTER="$TMPDIR/counter7"
echo "0" > "$COUNTER"
cat > "$TMPDIR/mock_gh" << EOF
#!/bin/bash
count=\$(cat "$COUNTER")
count=\$((count + 1))
echo "\$count" > "$COUNTER"
if [ \$count -lt 2 ]; then
  echo "HTTP 503 Service Unavailable"
  exit 1
fi
echo "back online"
exit 0
EOF
chmod +x "$TMPDIR/mock_gh"
GH_MOCK="$TMPDIR/mock_gh"
output=$(gh_retry api /repos 2>/dev/null)
assert_eq "recovers from 503" "back online" "$output"

# --- Summary ---
echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  exit 1
fi
