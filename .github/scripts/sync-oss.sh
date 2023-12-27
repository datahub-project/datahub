#!/bin/bash

set -euxo pipefail

# This script does a few things:
# 1. Mirror datahub/master to the datahub-fork/master branch.
# 2. Tries to create or update a PR to merge changes from datahub-fork/master into datahub-fork/acryl-main.
#    - If the PR already exists, it will update it unless someone has manually added commits to the PR.
#    - If the PR doesn't exist, it will create it.
#
# The sync branch is based on the fork target branch, and merges in commits from the oss master branch.
# Once the PR is manually approved, it should automatically get merged in once CI passes.

OSS_MASTER_BRANCH=master
FORK_TARGET_BRANCH=acryl-main
SYNC_BRANCH=hs--merge-oss-into-acryl-main

# Assumes git clone is run and we're in the right directory.
git status
git checkout $FORK_TARGET_BRANCH

# Add the oss remote.
if ! git remote | grep -q oss; then
    git remote add oss https://github.com/datahub-project/datahub
fi
git remote -v
git fetch oss $OSS_MASTER_BRANCH

# Sync fork master with oss/master.
if git show-ref --verify --quiet refs/heads/master; then
    git branch -d master
fi
git checkout -b master origin/master
git reset --hard oss/$OSS_MASTER_BRANCH
git push origin master  # Needs admin to bypass branch protections.

# Helper for adding comments to the PR.
function pr_number() {
    gh pr list --state open --base $FORK_TARGET_BRANCH --head $SYNC_BRANCH --json number | jq -r '.[0].number'
}
function add_comment() {
    PR_NUMBER=$(pr_number)
    echo "Adding comment to PR $PR_NUMBER: $1"
    gh pr comment "$PR_NUMBER" --body "$1"
}

# Check if the sync branch already exists in the remote.
if git ls-remote --heads origin $SYNC_BRANCH | grep -q $SYNC_BRANCH; then
    # If it already exists, we need to check if it has extra commits that don't appear on oss/master nor datahub-fork/acryl-main.
    # If so, there was a conflict or other changes that needed to be resolved manually, so we don't want to overwrite it.
    # For details on the ^ syntax, see https://stackoverflow.com/a/4207176
    if git log --no-merges --oneline origin/$SYNC_BRANCH ^master ^$FORK_TARGET_BRANCH | grep -q .; then
        add_comment "The sync branch $SYNC_BRANCH already exists and has manual commits. Skipping sync on $(date)."
        exit 0
    fi
fi

# Create or update the sync branch.
if git show-ref --verify --quiet refs/heads/$SYNC_BRANCH; then
    # Delete the local sync branch if it exists.
    git branch -D $SYNC_BRANCH
fi
if git ls-remote --heads origin $SYNC_BRANCH | grep -q $SYNC_BRANCH; then
    git checkout -b $SYNC_BRANCH origin/$SYNC_BRANCH
    git merge $FORK_TARGET_BRANCH --no-edit
else
    git checkout $FORK_TARGET_BRANCH
    git checkout -b $SYNC_BRANCH
fi
git merge master --no-edit
# TODO detect conflicts and abort if there are any.
# We'd also want to add a comment to the PR if this happens

git push origin $SYNC_BRANCH

# Create or update the PR.
gh repo set-default acryldata/datahub-fork
if gh pr list --state open --base $FORK_TARGET_BRANCH --head $SYNC_BRANCH | grep -q .; then
    PR_NUMBER=$(pr_number)

    # Update the PR title and add a comment.
    gh pr edit "$PR_NUMBER" --title "Merge oss $(date)"
    add_comment "Synced with oss/master at $(date)."
else
    gh pr create \
        --title "Merge oss $(date)" \
        --body "This automatic PR merges changes from oss/master into $FORK_TARGET_BRANCH." \
        --base $FORK_TARGET_BRANCH \
        --head $SYNC_BRANCH

    PR_NUMBER=$(pr_number)
    echo "Created PR $PR_NUMBER"

    # Note that it's important to use --merge here, since we don't want to squash the commits.
    gh pr merge --auto --merge --delete-branch "$PR_NUMBER"
fi
