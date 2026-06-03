#!/bin/bash

REF="${GITHUB_REF:-${GITHUB_REF_FALLBACK:-}}"
if [ -z "$REF" ]; then
    echo "Error: No ref available from GITHUB_REF or fallback"
    exit 1
fi

echo "GITHUB_REF: $REF"
echo "GITHUB_SHA: $GITHUB_SHA"

export MAIN_BRANCH="master"
# Floating tag for compose/quickstart; applied only after full smoke pass (see publish_images).
export QUICKSTART_TAG="quickstart"
export SHA_TAG_PREFIX="sha-"

function get_short_sha {
    # On pull_request events GITHUB_SHA is the ephemeral merge commit, which isn't a useful
    # reference. Prefer the PR branch HEAD sha (passed explicitly by the workflow) so tags
    # pin to the actual reviewed commit.
    if [ -n "${GITHUB_PR_HEAD_SHA:-}" ]; then
        echo "${GITHUB_PR_HEAD_SHA}" | head -c7
    else
        echo $(git rev-parse --short "$GITHUB_SHA"|head -c7)
    fi
}

export SHORT_SHA=$(get_short_sha)
export SHA_TAG="${SHA_TAG_PREFIX}${SHORT_SHA}"
echo "SHORT_SHA: $SHORT_SHA"
echo "SHA_TAG: $SHA_TAG"

function get_tag {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHA_TAG},g" -e 's,refs/tags/,,g' -e 's,refs/heads/,,g' -e "s,refs/pull/\([0-9]*\).*,pr\1-${SHORT_SHA},g"  -e 's,/,-,g')
}

function get_tag_slim {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHA_TAG}-slim,g" -e 's,refs/tags/\(.*\),\1-slim,g' -e 's,refs/heads/\(.*\),\1-slim,g' -e "s,refs/pull/\([0-9]*\).*,pr\1-${SHORT_SHA}-slim,g"  -e 's,/,-,g')
}

function get_tag_full {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHA_TAG}-full,g" -e 's,refs/tags/\(.*\),\1-full,g' -e 's,refs/heads/\(.*\),\1-full,g' -e "s,refs/pull/\([0-9]*\).*,pr\1-${SHORT_SHA}-full,g"  -e 's,/,-,g')
}

function get_python_docker_release_v() {
    echo "$(echo "${REF}" | \
        sed -e "s,refs/heads/${MAIN_BRANCH},1\!0.0.0+docker.${SHORT_SHA},g" \
            -e 's,refs/heads/\(.*\),1!0.0.0+docker.\1,g' \
            -e 's,refs/tags/v\([0-9a-zA-Z.]*\).*,\1+docker,g' \
            -e 's,refs/pull/\([0-9]*\).*,1!0.0.0+docker.pr\1,g' \
            -e 's,/,-,g'
            )"
}
# To run these, set TEST_DOCKER_HELPERS=1 and then copy the function + test cases into a bash shell.
if [ ${TEST_DOCKER_HELPERS:-0} -eq 1 ]; then
    REF="refs/pull/4788/merge"     get_python_docker_release_v # '1!0.0.0+docker.pr4788'
    REF="refs/tags/v0.1.2-test"    get_python_docker_release_v # '0.1.2'
    REF="refs/tags/v0.1.2.1-test"  get_python_docker_release_v # '0.1.2.1'
    REF="refs/tags/v0.1.2rc1-test" get_python_docker_release_v # '0.1.2rc1'
    REF="refs/heads/branch-name"   get_python_docker_release_v # '1!0.0.0+docker.branch-name'
    REF="refs/heads/releases/branch-name" get_python_docker_release_v # 1!0.0.0+docker.releases-branch-name'

    REF="refs/tags/v0.1.2rc1"    get_tag # 'v0.1.2rc1'
    REF="refs/tags/v0.1.2rc1"    get_tag_slim # 'v0.1.2rc1-slim'
    REF="refs/tags/v0.1.2rc1"    get_tag_full # 'v0.1.2rc1-full'

    REF="refs/pull/4788/merge"    get_tag # 'pr4788-<short_sha>'
    REF="refs/pull/4788/merge"    get_tag_slim # 'pr4788-<short_sha>-slim'
    REF="refs/pull/4788/merge"    get_tag_full # 'pr4788-<short_sha>-full'

    REF="refs/heads/branch-name" get_tag # 'branch-name'
    REF="refs/heads/branch-name" get_tag_slim # 'branch-name-slim'
    REF="refs/heads/branch-name" get_tag_full # 'branch-name-full'

    REF="refs/heads/releases/branch-name" get_tag # 'releases-branch-name'
    REF="refs/heads/releases/branch-name" get_tag_slim # 'releases-branch-name-slim'
    REF="refs/heads/releases/branch-name" get_tag_full # 'releases-branch-name-full'

    REF="refs/heads/master" get_tag # 'sha-<short_sha>'
fi

function get_unique_tag {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHA_TAG},g" -e 's,refs/tags/,,g' -e "s,refs/heads/.*,${SHA_TAG},g"  -e "s,refs/pull/\([0-9]*\).*,pr\1-${SHORT_SHA},g")
}

function get_unique_tag_slim {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHA_TAG}-slim,g" -e 's,refs/tags/\(.*\),\1-slim,g' -e "s,refs/heads/.*,${SHA_TAG}-slim,g" -e "s,refs/pull/\([0-9]*\).*,pr\1-${SHORT_SHA}-slim,g")
}

function get_unique_tag_full {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHA_TAG}-full,g" -e 's,refs/tags/\(.*\),\1-full,g' -e "s,refs/heads/.*,${SHA_TAG}-full,g" -e "s,refs/pull/\([0-9]*\).*,pr\1-${SHORT_SHA}-full,g")
}

function get_platforms_based_on_branch {
    if [ "${GITHUB_EVENT_NAME}" == "push" ] && [ "${REF}" == "refs/heads/${MAIN_BRANCH}" ]; then
        echo "linux/amd64,linux/arm64"
    else
        echo "linux/amd64"
    fi
}

function echo_tags {
    echo "short_sha=${SHORT_SHA}"
    echo "sha_tag=${SHA_TAG}"
    echo "tag=$(get_tag)"
    echo "slim_tag=$(get_tag_slim)"
    echo "full_tag=$(get_tag_full)"
    echo "unique_tag=$(get_unique_tag)"
    echo "unique_slim_tag=$(get_unique_tag_slim)"
    echo "unique_full_tag=$(get_unique_tag_full)"
    echo "quickstart_tag=${QUICKSTART_TAG}"
    echo "python_release_version=$(get_python_docker_release_v)"
    echo "branch_name=${GITHUB_HEAD_REF:-${REF#refs/heads/}}"
    echo "repository_name=${GITHUB_REPOSITORY#*/}"
}

function validate_github_ref_for_python_tag {
    if [[ ! "$GITHUB_REF" =~ ^refs/tags/v ]]; then
        echo "Error: This workflow must be triggered by a tag starting with 'v'"
        echo "Current GITHUB_REF: $GITHUB_REF"
        exit 1
    fi
}
