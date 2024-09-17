echo "GITHUB_REF: $GITHUB_REF"
echo "GITHUB_SHA: $GITHUB_SHA"

export MAIN_BRANCH="master"
export MAIN_BRANCH_TAG="head"

function get_short_sha {
    echo $(git rev-parse --short "$GITHUB_SHA"|head -c7)
}

export SHORT_SHA=$(get_short_sha)
echo "SHORT_SHA: $SHORT_SHA"

function get_tag {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG},g" -e 's,refs/tags/,,g' -e 's,refs/pull/\([0-9]*\).*,pr\1,g')
}

function get_tag_slim {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG}-slim,g" -e 's,refs/tags/\(.*\),\1-slim,g' -e 's,refs/pull/\([0-9]*\).*,pr\1-slim,g')
}

function get_tag_full {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG}-full,g" -e 's,refs/tags/\(.*\),\1-full,g' -e 's,refs/pull/\([0-9]*\).*,pr\1-full,g')
}

function get_python_docker_release_v {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},1!0.0.0+docker.${SHORT_SHA},g" -e 's,refs/tags/v\(.*\),1!\1+docker,g' -e 's,refs/pull/\([0-9]*\).*,1!0.0.0+docker.pr\1,g')
}

function get_unique_tag {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA},g" -e 's,refs/tags/,,g' -e 's,refs/pull/\([0-9]*\).*,pr\1,g')
}

function get_unique_tag_slim {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA}-slim,g" -e 's,refs/tags/\(.*\),\1-slim,g' -e 's,refs/pull/\([0-9]*\).*,pr\1-slim,g')
}

function get_unique_tag_full {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA}-full,g" -e 's,refs/tags/\(.*\),\1-full,g' -e 's,refs/pull/\([0-9]*\).*,pr\1-full,g')
}

function get_platforms_based_on_branch {
    if [ "${{ github.event_name }}" == 'push' && "${{ github.ref }}" == "refs/heads/${MAIN_BRANCH}" ]; then
        echo "linux/amd64,linux/arm64"
    else
        echo "linux/amd64"
    fi
}
