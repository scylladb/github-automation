#!/bin/bash

show_help() {
    echo "Usage: clone_pr.sh [OPTIONS]"
    echo ""
    echo "Clone a pull request to a origin remote branch and create a new PR"
    echo "it copy the title, body, labels and base branch of the original PR"
    echo "it assumes github cli is installed and authenticated,"
    echo "and run from within clone repository"
    echo
    echo "Options:"
    echo "  -d, --dry-run       Perform a dry run without creating new PR"
    echo "  -p, --pr <number>   Specify the pull request number to clone"
    echo "  -r, --remote <name> Specify the remote name to push the branch (default: origin)"
    echo "  -h, --help          Display this help message and exit"
    echo ""
    echo "Example:"
    echo "  clone_pr.sh -p 123"
    echo "  clone_pr.sh --dry-run --pr 123"
    echo "  clone_pr.sh --pr 123 --remote upstream"
}

clone_pr() {
    local dry_run=""
    local source_remote="origin"
    VARS=$(getopt -o dhp:r --long dry-run,help,pr:,remote: -- "$@")
    eval set -- "$VARS"

    while true; do
        case "$1" in
            -d|--dry-run)
                dry_run="--dry-run"
                shift
                ;;
            -p|--pr)
                pr_num=$2
                shift 2
                ;;
            -r|--remote)
                source_remote=$2
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            --)
                shift
                break
                ;;
        esac
    done

    if [[ -z $pr_num ]]; then
        echo "Error: need to supply PR number with -p/--pr"
        show_help
        exit 1
    fi

    pr_json="pr_${pr_num}.json"

    gh pr checkout "${pr_num}"

    current_branch=$(git branch --show-current)
    git push "${source_remote}" "${current_branch}"

    gh pr view "${pr_num}" --json title,body,labels,baseRefName > "${pr_json}"

    title=$(jq -r '.title' < "${pr_json}")
    body=$(jq -r '.body' < "${pr_json}")
    labels=$(jq -r '[.labels[].name] | join(",")' < "${pr_json}")
    base_branch=$(jq -r '.baseRefName' < "${pr_json}")

    gh pr create ${dry_run} --title "${title}" --body "${body}" \
      --label "${labels}" --base "${base_branch}" --head "${source_remote}:${current_branch}"

    rm "${pr_json}"
}

clone_pr "$@"
