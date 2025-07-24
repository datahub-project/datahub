#!/bin/bash

REF="${GITHUB_REF:-${GITHUB_REF_FALLBACK:-}}"
if [ -z "$REF" ]; then
    echo "Error: No ref available from GITHUB_REF or fallback"
    exit 1
fi

echo "GITHUB_REF: $REF"
echo "GITHUB_SHA: $GITHUB_SHA"

export MAIN_BRANCH="acryl-main"
export MAIN_BRANCH_TAG="head"

function get_short_sha {
    echo $(git rev-parse --short "$GITHUB_SHA"|head -c7)
}

export SHORT_SHA=$(get_short_sha)
echo "SHORT_SHA: $SHORT_SHA"

function get_tag {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG},g" -e 's,refs/tags/,,g' -e 's,refs/heads/,,g' -e 's,refs/pull/\([0-9]*\).*,pr\1,g'  -e 's,/,-,g')
}

function get_tag_slim {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG}-slim,g" -e 's,refs/tags/\(.*\),\1-slim,g' -e 's,refs/heads/\(.*\),\1-slim,g' -e 's,refs/pull/\([0-9]*\).*,pr\1-slim,g'  -e 's,/,-,g')
}

function get_tag_full {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG}-full,g" -e 's,refs/tags/\(.*\),\1-full,g' -e 's,refs/heads/\(.*\),\1-full,g' -e 's,refs/pull/\([0-9]*\).*,pr\1-full,g'  -e 's,/,-,g')
}

function _get_python_docker_release_v_core() {
    echo "$(echo "${REF}" | \
        sed -e "s,refs/heads/${MAIN_BRANCH},1\!0.0.0+docker.${SHORT_SHA},g" \
            -e 's,refs/heads/\(.*\),1!0.0.0+docker.\1,g' \
            -e 's,refs/tags/v\([0-9a-zA-Z.]*\).*,\1+docker,g' \
            -e 's,refs/pull/\([0-9]*\).*,1!0.0.0+docker.pr\1,g' \
            -e 's,/,-,g'
            )"
}

function get_python_docker_release_v() {
    # SaaS Only: for tagged releases, we also want to use the 1! epoch number prefix.
    # Many of our Python packages depend on acryl-datahub >= 1.x. This is fine in OSS
    # because the version numbers are also 1.x, but causes issues in SaaS because
    # our SaaS version numbers are 0.x. By adding the 1! epoch number prefix, we
    # ensure that the SaaS version number is always >= 1.x.

    if [[ "${REF}" == refs/tags/v* ]]; then
        echo "$(echo "${REF}" | \
            sed -e "s,refs/tags/v\([0-9a-zA-Z.]*\).*,1!\1+docker,g" \
                -e 's,/,-,g'
                )"
    else
        _get_python_docker_release_v_core
    fi
}

# To run these, set TEST_DOCKER_HELPERS=1 GITHUB_REF=test and run this script
if [ ${TEST_DOCKER_HELPERS:-0} -eq 1 ]; then
    REF="refs/pull/4788/merge"     _get_python_docker_release_v_core # '1!0.0.0+docker.pr4788'
    REF="refs/tags/v0.1.2-test"    _get_python_docker_release_v_core # '0.1.2+dockerdocker'
    REF="refs/tags/v0.1.2.1-test"  _get_python_docker_release_v_core # '0.1.2.1+docker'
    REF="refs/tags/v0.1.2rc1-test" _get_python_docker_release_v_core # '0.1.2rc1+docker'
    REF="refs/heads/branch-name"   _get_python_docker_release_v_core # '1!0.0.0+docker.branch-name'
    REF="refs/heads/releases/branch-name" _get_python_docker_release_v_core # 1!0.0.0+docker.releases-branch-name'

    REF="refs/tags/v0.1.2rc1"    get_tag # 'v0.1.2rc1'
    REF="refs/tags/v0.1.2rc1"    get_tag_slim # 'v0.1.2rc1-slim'
    REF="refs/tags/v0.1.2rc1"    get_tag_full # 'v0.1.2rc1-full'

    REF="refs/pull/4788/merge"    get_tag # 'pr4788'
    REF="refs/pull/4788/merge"    get_tag_slim # 'pr4788-slim'
    REF="refs/pull/4788/merge"    get_tag_full # 'pr4788-full'

    REF="refs/heads/branch-name" get_tag # 'branch-name'
    REF="refs/heads/branch-name" get_tag_slim # 'branch-name-slim'
    REF="refs/heads/branch-name" get_tag_full # 'branch-name-full'

    REF="refs/heads/releases/branch-name" get_tag # 'releases-branch-name'
    REF="refs/heads/releases/branch-name" get_tag_slim # 'releases-branch-name-slim'
    REF="refs/heads/releases/branch-name" get_tag_full # 'releases-branch-name-full'

    # SaaS Only Tests.
    REF="refs/pull/4788/merge"     get_python_docker_release_v # '1!0.0.0+docker.pr4788'
    REF="refs/tags/v0.1.2-test"    get_python_docker_release_v # '1!0.1.2+docker'
    REF="refs/tags/v0.1.2.1-test"  get_python_docker_release_v # '1!0.1.2.1+docker'
    REF="refs/tags/v0.1.2rc1-test" get_python_docker_release_v # '1!0.1.2rc1+docker'
    REF="refs/heads/releases/branch-name" get_python_docker_release_v # 1!0.0.0+docker.releases-branch-name'
fi

function get_unique_tag {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA},g" -e 's,refs/tags/,,g' -e "s,refs/heads/.*,${SHORT_SHA},g"  -e 's,refs/pull/\([0-9]*\).*,pr\1,g')
}

function get_unique_tag_slim {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA}-slim,g" -e 's,refs/tags/\(.*\),\1-slim,g' -e "s,refs/heads/.*,${SHORT_SHA}-slim,g" -e 's,refs/pull/\([0-9]*\).*,pr\1-slim,g')
}

function get_unique_tag_full {
    echo $(echo ${REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA}-full,g" -e 's,refs/tags/\(.*\),\1-full,g' -e "s,refs/heads/.*,${SHORT_SHA}-full,g" -e 's,refs/pull/\([0-9]*\).*,pr\1-full,g')
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
    echo "tag=$(get_tag)"
    echo "slim_tag=$(get_tag_slim)"
    echo "full_tag=$(get_tag_full)"
    echo "unique_tag=$(get_unique_tag)"
    echo "unique_slim_tag=$(get_unique_tag_slim)"
    echo "unique_full_tag=$(get_unique_tag_full)"
    echo "python_release_version=$(get_python_docker_release_v)"
    echo "branch_name=${GITHUB_HEAD_REF:-${REF#refs/heads/}}"
    echo "repository_name=${GITHUB_REPOSITORY#*/}"
}
