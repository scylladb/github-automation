# AI Code Review (`copilot-review`)

AI-powered code review for pull requests using **GitHub Copilot CLI** or **OpenCode CLI**.
Posts findings as PR comments (summary or inline on specific lines).

## Features

- **Self-contained** — clones the repo and checks out the PR automatically; works from any directory
- **Multi-tool** — supports both GitHub Copilot CLI and OpenCode CLI via `--tool`
- **Inline review** — posts findings directly on the relevant source lines (`--inline-review`)
- **Deduplication** — skips lines that already have review comments, so re-runs don't nag
- **Line correction** — cross-references AI-reported line numbers against the actual file to fix off-by-few errors
- **Guideline-aware** — the AI tool automatically discovers `.github/copilot-instructions.md` and `.github/instructions/*.instructions.md` from the target repo
- **Reusable workflow** — `copilot-review.yaml` can be called from any repository via `workflow_call`
- **Security** — treats all PR content as untrusted; validates model names; caps user instructions at 1000 chars

## Quick Start

### Manual (local)

```bash
# Dry run — review but don't post anything
python3 .github/scripts/copilot-review.py \
  --repo scylladb/scylladb --pr-number 12345 --dry-run

# Post review as a PR comment
python3 .github/scripts/copilot-review.py \
  --repo scylladb/scylladb --pr-number 12345

# Post inline comments on specific lines
python3 .github/scripts/copilot-review.py \
  --repo scylladb/scylladb --pr-number 12345 --inline-review

# Use OpenCode instead of Copilot CLI
python3 .github/scripts/copilot-review.py \
  --repo scylladb/scylladb --pr-number 12345 --tool opencode
```

### CI (reusable workflow)

Add a caller workflow in your repository (e.g. `.github/workflows/ai-review.yml`)
and set the `COPILOT_TOKEN` repository secret (fine-grained PAT with **Copilot
Requests** permission).

The example below supports two trigger modes:

1. **PR comment** — type `/ai-review` on any pull request
2. **Manual dispatch** — run from the Actions tab for any PR number

Comment flags:

```
/ai-review                           — default review
/ai-review --tool opencode           — use OpenCode CLI
/ai-review --model gpt-4o            — override the AI model
/ai-review --inline                  — post inline review comments
/ai-review --inline --tool opencode  — combine flags
```

Full caller example:

```yaml
name: AI Code Review
on:
  issue_comment:
    types: [created]
  workflow_dispatch:
    inputs:
      pr_number:
        description: 'Pull request number to review'
        required: true
        type: number
      tool:
        description: 'AI CLI tool: copilot or opencode'
        required: false
        type: choice
        options: [copilot, opencode]
        default: copilot
      model:
        description: 'AI model override (leave empty for default)'
        required: false
        type: string
        default: ''
      inline_review:
        description: 'Post findings as inline PR review comments'
        required: false
        type: boolean
        default: false

permissions:
  contents: read
  pull-requests: write
  issues: write

jobs:
  prepare:
    if: >-
      (github.event_name == 'workflow_dispatch') ||
      (github.event_name == 'issue_comment' &&
       github.event.issue.pull_request &&
       startsWith(github.event.comment.body, '/ai-review'))
    runs-on: ubuntu-latest
    outputs:
      pr_number: ${{ steps.resolve.outputs.pr_number }}
      tool: ${{ steps.resolve.outputs.tool }}
      model: ${{ steps.resolve.outputs.model }}
      inline_review: ${{ steps.resolve.outputs.inline_review }}
      comment_id: ${{ steps.resolve.outputs.comment_id }}
    steps:
      - name: Verify commenter has write access
        if: github.event_name == 'issue_comment'
        env:
          GH_TOKEN: ${{ github.token }}
          COMMENTER: ${{ github.event.comment.user.login }}
          REPO: ${{ github.repository }}
        run: |
          perm=$(gh api "repos/${REPO}/collaborators/${COMMENTER}/permission" \
                   --jq '.permission')
          case "$perm" in
            admin|maintain|write) echo "Authorized (${perm})" ;;
            *) echo "::error::Unauthorized"; exit 1 ;;
          esac

      - name: Resolve inputs
        id: resolve
        env:
          COMMENT_BODY: ${{ github.event.comment.body || '' }}
          EVENT_NAME: ${{ github.event_name }}
          INPUT_PR_NUMBER: ${{ inputs.pr_number }}
          INPUT_TOOL: ${{ inputs.tool }}
          INPUT_MODEL: ${{ inputs.model }}
          INPUT_INLINE_REVIEW: ${{ inputs.inline_review }}
          ISSUE_NUMBER: ${{ github.event.issue.number }}
          ISSUE_COMMENT_ID: ${{ github.event.comment.id }}
        run: |
          if [ "$EVENT_NAME" = "workflow_dispatch" ]; then
            echo "pr_number=$INPUT_PR_NUMBER" >> "$GITHUB_OUTPUT"
            echo "tool=$INPUT_TOOL" >> "$GITHUB_OUTPUT"
            echo "model=$INPUT_MODEL" >> "$GITHUB_OUTPUT"
            echo "inline_review=$INPUT_INLINE_REVIEW" >> "$GITHUB_OUTPUT"
            echo "comment_id=0" >> "$GITHUB_OUTPUT"
            exit 0
          fi
          pr_number="$ISSUE_NUMBER"
          comment_id="$ISSUE_COMMENT_ID"
          tool="copilot"; model=""; inline_review="false"
          if echo "$COMMENT_BODY" | grep -qoP -- '--tool\s+\K\S+'; then
            tool=$(echo "$COMMENT_BODY" | grep -oP -- '--tool\s+\K\S+')
          fi
          if echo "$COMMENT_BODY" | grep -qoP -- '--model\s+\K\S+'; then
            model=$(echo "$COMMENT_BODY" | grep -oP -- '--model\s+\K\S+')
          fi
          if echo "$COMMENT_BODY" | grep -q -- '--inline'; then
            inline_review="true"
          fi
          if [ "$tool" != "copilot" ] && [ "$tool" != "opencode" ]; then
            echo "::error::Invalid tool '$tool'"; exit 1
          fi
          echo "pr_number=$pr_number" >> "$GITHUB_OUTPUT"
          echo "tool=$tool" >> "$GITHUB_OUTPUT"
          echo "model=$model" >> "$GITHUB_OUTPUT"
          echo "inline_review=$inline_review" >> "$GITHUB_OUTPUT"
          echo "comment_id=$comment_id" >> "$GITHUB_OUTPUT"

  review:
    needs: prepare
    uses: scylladb/github-automation/.github/workflows/copilot-review.yaml@main
    with:
      pr_number: ${{ fromJSON(needs.prepare.outputs.pr_number) }}
      tool: ${{ needs.prepare.outputs.tool }}
      model: ${{ needs.prepare.outputs.model }}
      inline_review: ${{ fromJSON(needs.prepare.outputs.inline_review) }}
      comment_id: ${{ fromJSON(needs.prepare.outputs.comment_id) }}
    secrets:
      COPILOT_TOKEN: ${{ secrets.COPILOT_TOKEN }}
```

## Prerequisites

| Tool | Requirement |
|------|-------------|
| **copilot** (default) | `npm install -g @github/copilot` + fine-grained PAT with **Copilot Requests** permission set as `COPILOT_GITHUB_TOKEN` env var |
| **opencode** | `opencode` CLI installed + provider credentials (e.g. `GITHUB_TOKEN` for `github-copilot` provider) |
| **both** | `gh` CLI authenticated (for cloning repos and posting comments) |

## CLI Reference

```
python3 copilot-review.py --repo OWNER/REPO --pr-number N [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--repo` | `$GITHUB_REPOSITORY` | Repository in `owner/name` format |
| `--pr-number` | *(required)* | Pull request number |
| `--base-ref` | *(auto-detected)* | Base branch (fetched from PR metadata if omitted) |
| `--tool` | `copilot` | AI CLI tool: `copilot` or `opencode` |
| `--model` | tool-dependent | AI model override |
| `--inline-review` | off | Post findings as inline comments on specific lines |
| `--dry-run` | off | Run review but don't post anything |
| `--prompt-only` | off | Only generate and display the prompt |
| `--timeout` | `600` | Timeout in seconds for the AI tool |
| `--additional-instructions` | *(none)* | Extra review instructions (max 1000 chars) |
| `--max-continues` | `50` | Max autopilot continuation steps (copilot only) |

## Output Format

The review produces a findings table:

| # | Severity | File | Line(s) | Category | Description | Risk | Suggested Fix | Fix Complexity |
|---|----------|------|---------|----------|-------------|------|---------------|----------------|
| 1 | 🔴 Critical | path/file.py | 42 | Bug | Description... | What breaks if not fixed | `<pre>- old<br>+ new</pre>` | Easy |

Severity levels: 🔴 Critical, 🟠 High, 🟡 Medium, 🔵 Low

## How It Works

1. **Fetch PR metadata** — title, base branch, head SHA via `gh api`
2. **Clone & checkout** — blobless clone, fetch PR head + base branch
3. **Build prompt** — structured review instructions with security guardrails; the AI agent discovers changed files, diffs, PR description, and project guidelines on its own
4. **Run AI tool** — copilot or opencode with the prompt, captures output
5. **Parse findings** — extract table rows, resolve file paths, correct line numbers
6. **Dedup** — skip lines that already have review comments
7. **Post** — as a PR comment (with commit SHA) or inline review comments

## Files

| File | Description |
|------|-------------|
| `.github/scripts/copilot-review.py` | Standalone review script (all logic) |
| `.github/workflows/copilot-review.yaml` | Reusable `workflow_call` workflow |
| `.github/scripts/README-copilot-review.md` | This file |
