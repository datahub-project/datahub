echo "GITHUB_REF: $GITHUB_REF"
echo "GITHUB_SHA: $GITHUB_SHA"

export MAIN_BRANCH="master"
export MAIN_BRANCH_TAG="head"

function get_short_sha {
    echo $(git rev-parse --short "$GITHUB_SHA")
}

export SHORT_SHA=$(get_short_sha)
echo "SHORT_SHA: $SHORT_SHA"

function get_tag {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${MAIN_BRANCH_TAG}\,${SHORT_SHA},g" -e 's,refs/tags/,,g' -e 's,refs/pull/\([0-9]*\).*,pr\1,g')
}

function get_python_docker_release_v {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},0.0.0+docker.${SHORT_SHA},g" -e 's,refs/tags/v\(.*\),\1+docker,g' -e 's,refs/pull/\([0-9]*\).*,0.0.0+docker.pr\1,g')
}

function get_unique_tag {
    echo $(echo ${GITHUB_REF} | sed -e "s,refs/heads/${MAIN_BRANCH},${SHORT_SHA},g" -e 's,refs/tags/,,g' -e 's,refs/pull/\([0-9]*\).*,pr\1,g')
}