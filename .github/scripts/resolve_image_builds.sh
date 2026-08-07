#!/bin/bash
# Decide, per docker image in the quickstart set, whether this run has to bake a
# fresh image or can reuse the floating `quickstart` tag -- the last image set
# that passed smoke tests on master (published by the publish_images job).
#
# Writes three GitHub outputs:
#
#   image_build_modules  Comma-separated Gradle modules for base_build to bake.
#                        Empty means there is nothing to build; base_build still
#                        runs, so every downstream `needs:` keeps its meaning.
#   image_version_env    KEY=VALUE lines pinning each reused image to
#                        `quickstart`. Jobs that boot a stack append these to
#                        $GITHUB_ENV; compose reads them as per-service
#                        overrides of DATAHUB_VERSION.
#   image_built_repos    Space-separated docker repo names that were baked, so
#                        run-quickstart.sh can assert compose actually resolved
#                        them to this PR's tag rather than the reused one.
#
# Reuse is opt-in per image and everything unrecognised falls through to "build
# it", so a misclassification costs build time rather than silently running the
# tests against the wrong binaries.

set -euo pipefail

: "${DOCKER_REGISTRY:=acryldata}"
: "${QUICKSTART_TAG:=quickstart}"

# Gradle module | docker repo | compose version override | tag suffix.
# Mirrors the module list of :docker:buildImagesQuickstart.
IMAGES=(
  ":metadata-service:war|datahub-gms|DATAHUB_GMS_VERSION|"
  ":datahub-upgrade|datahub-upgrade|DATAHUB_UPDATE_VERSION|"
  ":metadata-jobs:mae-consumer-job|datahub-mae-consumer|DATAHUB_MAE_VERSION|"
  ":metadata-jobs:mce-consumer-job|datahub-mce-consumer|DATAHUB_MCE_VERSION|"
  ":datahub-frontend|datahub-frontend-react|DATAHUB_FRONTEND_VERSION|"
  ":datahub-actions|datahub-actions|DATAHUB_ACTIONS_VERSION|-slim"
)

# Whether this PR's diff can change the contents of one image.
#
# The couplings that are easy to get wrong, and why they are drawn this way:
#   - a backend change rebuilds the frontend too, because the GraphQL schema and
#     the generated metadata model are inputs to the frontend build; pairing a
#     PR-built GMS with a HEAD frontend would test a combination that does not
#     exist anywhere.
#   - the actions image pip-installs ../metadata-ingestion at build time, so
#     ingestion and metadata-model changes land in it.
# The `backend` filter already covers metadata-models/** and docker/**, so
# schema changes and Dockerfile changes rebuild everything through it.
image_is_affected() {
  case "$1" in
  :metadata-service:war | :datahub-upgrade | :metadata-jobs:mae-consumer-job | :metadata-jobs:mce-consumer-job)
    [[ "${BACKEND_CHANGE}" == "true" ]]
    ;;
  :datahub-frontend)
    [[ "${FRONTEND_CHANGE}" == "true" || "${BACKEND_CHANGE}" == "true" ]]
    ;;
  :datahub-actions)
    [[ "${ACTIONS_CHANGE}" == "true" || "${INGESTION_CHANGE}" == "true" || "${BACKEND_CHANGE}" == "true" ]]
    ;;
  *)
    # An image nobody classified. Build it.
    return 0
    ;;
  esac
}

# Every path prefix whose effect on the images is known: either it provably
# cannot change image contents, or one of the ci-optimization filters already
# claims it. Deciding "this image is unaffected" is only sound when every
# changed file is accounted for -- plenty of paths affect every image without
# belonging to any feature filter (the root build.gradle, gradle/**,
# buildSrc/**, or a new top-level module nobody has classified yet), and those
# have to force a full build.
#
# .github/** is deliberately absent: a workflow change can alter build args or
# the build graph itself, which is exactly the change you want built rather than
# reused.
#
# Keep this in sync with the filters in ci-optimization/action.yml. Forgetting
# to add a path here costs a full build, which is merely slow; the reverse would
# ship stale images.
CLASSIFIED_PREFIXES=(
  # Cannot end up inside a docker image.
  "docs/" "docs-website/" "smoke-test/" "e2e-test/"
  # Claimed by the frontend filter.
  "datahub-frontend/" "datahub-web-react/"
  # Claimed by the actions filter.
  "datahub-actions/"
  # Claimed by the ingestion filter.
  "metadata-ingestion/" "metadata-ingestion-modules/"
  # Claimed by the backend filter.
  "docker/" "metadata-models/" "datahub-upgrade/" "entity-registry/"
  "li-utils/" "metadata-auth/" "metadata-dao-impl/" "metadata-events/"
  "metadata-io/" "metadata-jobs/" "metadata-service/" "metadata-utils/"
  "metadata-operation-context/" "metadata-integration/" "datahub-graphql-core/"
)

path_is_classified() {
  local path="$1" prefix
  # Markdown never reaches an image, wherever it lives.
  [[ "${path}" == *.md ]] && return 0
  for prefix in "${CLASSIFIED_PREFIXES[@]}"; do
    [[ "${path}" == "${prefix}"* ]] && return 0
  done
  return 1
}

changed_files=()
while IFS= read -r path; do
  [[ -n "${path}" ]] || continue
  changed_files+=("${path}")
done < <(jq -r '.[]?' <<<"${CHANGED_FILES:-[]}" 2>/dev/null)

unclassified=()
for path in ${changed_files[@]+"${changed_files[@]}"}; do
  path_is_classified "${path}" || unclassified+=("${path}")
done

# Resolve a tag against Docker Hub using an anonymous pull token. The setup
# runner has no docker login, and these repos are public, so this avoids
# depending on registry credentials just to answer "does the tag exist".
tag_exists() {
  local repo="$1" tag="$2" token
  # A missing tag is an expected outcome, not an error, so curl's own diagnostics
  # are dropped -- the caller logs the decision it reached either way.
  token=$(curl -fsSL --max-time 30 \
    "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${repo}:pull" \
    2>/dev/null | jq -r '.token') || return 1
  curl -fsL --max-time 30 --head -o /dev/null \
    -H "Authorization: Bearer ${token}" \
    -H "Accept: application/vnd.oci.image.index.v1+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
    "https://registry-1.docker.io/v2/${repo}/manifests/${tag}" 2>/dev/null
}

# Reasons to bake the whole set, checked before any per-image reasoning so the
# log names the override rather than whichever filter also happened to match.
full_build_reason=""
if [[ "${FULL_BUILD_LABEL}" == "true" ]]; then
  full_build_reason="the build-images label is set"
elif [[ "${EVENT_NAME}" != "pull_request" ]]; then
  full_build_reason="this is a ${EVENT_NAME} run, not a pull_request"
elif [[ "${IS_FORK}" == "true" ]]; then
  # Fork PRs have no depot cache: base_build is skipped and each test job bakes
  # the whole set locally, so there is no shared build to narrow.
  full_build_reason="this is a fork PR, which builds its images in the test job"
elif [[ "${PR_PUBLISH}" == "true" ]]; then
  # A publish label pushes this PR's images to the registry for people to pull,
  # so the set has to be complete and built from this PR's source.
  full_build_reason="a publish label is pushing this PR's images"
elif [[ -n "${SMOKE_BUILD_TASK}" ]]; then
  # A smoke: label picks a different compose profile whose image set does not
  # match the table above.
  full_build_reason="a smoke: label selected the ${SMOKE_BUILD_TASK} build"
elif ((${#changed_files[@]} == 0)); then
  # A pull request always changes something, so an empty list means the file
  # list never arrived. Reusing every image off the back of that would be the
  # one failure this whole design has to avoid.
  full_build_reason="the changed-file list was empty or unreadable"
elif ((${#unclassified[@]})); then
  full_build_reason="${#unclassified[@]} changed path(s) are unclassified, starting with ${unclassified[0]}"
fi

build_modules=()
build_repos=()
version_env=()
summary=()

for entry in "${IMAGES[@]}"; do
  IFS='|' read -r module repo version_var tag_suffix <<<"${entry}"
  reuse_tag="${QUICKSTART_TAG}${tag_suffix}"

  decision="build"
  if [[ -n "${full_build_reason}" ]]; then
    why="${full_build_reason}"
  elif image_is_affected "${module}"; then
    why="affected by this diff"
  elif ! tag_exists "${DOCKER_REGISTRY}/${repo}" "${reuse_tag}"; then
    # Cheaper to discover here than inside seven parallel batch jobs that boot a
    # whole stack against an image that turns out not to exist.
    why="${reuse_tag} did not resolve in the registry"
  else
    decision="reuse"
    why="unaffected by this diff"
  fi

  if [[ "${decision}" == "build" ]]; then
    build_modules+=("${module}")
    build_repos+=("${repo}")
    summary+=("| \`${repo}\` | build | ${why} |")
    echo "build ${repo}: ${why}"
  else
    version_env+=("${version_var}=${QUICKSTART_TAG}")
    summary+=("| \`${repo}\` | \`${reuse_tag}\` | ${why} |")
    echo "reuse ${repo}:${reuse_tag}: ${why}"
  fi
done

joined_modules=""
if ((${#build_modules[@]})); then
  joined_modules=$(
    IFS=','
    echo "${build_modules[*]}"
  )
fi

{
  echo "image_build_modules=${joined_modules}"
  echo "image_built_repos=${build_repos[*]-}"
  echo "image_version_env<<IMAGE_VERSION_ENV_EOF"
  if ((${#version_env[@]})); then
    printf '%s\n' "${version_env[@]}"
  fi
  echo "IMAGE_VERSION_ENV_EOF"
} >>"${GITHUB_OUTPUT}"

{
  echo "## Image build plan"
  echo ""
  echo "| image | tag | reason |"
  echo "| --- | --- | --- |"
  printf '%s\n' "${summary[@]}"
} >>"${GITHUB_STEP_SUMMARY}"
