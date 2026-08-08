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

SERVER=":metadata-service:war :datahub-upgrade :metadata-jobs:mae-consumer-job :metadata-jobs:mce-consumer-job"
UI=":datahub-frontend"
ACTIONS=":datahub-actions"
EVERYTHING="${SERVER} ${UI} ${ACTIONS}"

# Which images a changed path can affect: "<prefix>|<space separated modules>".
# First matching prefix wins, so specific prefixes precede general ones. An
# empty module list means the path cannot change any image.
#
# These images exist to be integration-tested, so the question each rule answers
# is "can this change alter what the running process does" -- NOT "does this
# change appear anywhere in that image's compile graph". The two differ, and
# conflating them is expensive. Compile-only breakage is the job of the lint and
# build workflows; it should not cost an image rebuild here.
#
# The clearest case is the GraphQL schema. datahub-web-react/codegen.yml reads
# ../datahub-graphql-core/src/main/resources/*.graphql, so the schema is a
# compile input to the frontend -- but it only generates TypeScript types, which
# are erased at runtime. The queries the frontend actually sends come from its
# own src/**/*.graphql documents. A rebuilt frontend is therefore runtime
# equivalent to the published one, and reusing it buys a better test: an
# existing client against the new schema, which is what every rolling upgrade
# looks like and what would catch a breaking schema change. (The compile side is
# covered by datahub-web-react-lint, whose path filter now includes the schema.)
#
# The runtime column is otherwise taken from the Gradle graph reported by
# `./gradlew :docker:buildImagesQuickstart --dry-run
# -PbuildModules=:datahub-frontend`, since the Play app genuinely executes the
# shared libraries it links: entity-registry, li-utils, metadata-auth,
# metadata-events, metadata-models, metadata-operation-context, metadata-utils
# and five metadata-service subprojects. It does not link metadata-io,
# metadata-jobs, datahub-upgrade, metadata-dao-impl or datahub-graphql-core, so
# server-side business logic in those cannot change how the frontend behaves.
PATH_RULES=(
  # Cannot end up inside any image.
  "docs/|"
  "docs-website/|"
  "smoke-test/|"
  "e2e-test/|"
  # Compose templates and the version map describe how images are run, not what
  # is in them. Without this a one-file bump of quickstart_version_mapping.yaml
  # bakes all six images.
  "docker/profiles/|"
  "docker/quickstart/|"
  # Standalone Python package: no Dockerfile references it and no other
  # build.gradle depends on it.
  "datahub-agent-context/|"

  "datahub-web-react/|${UI}"
  "datahub-frontend/|${UI}"
  "docker/datahub-frontend/|${UI}"

  "datahub-actions/|${ACTIONS}"
  "docker/datahub-actions/|${ACTIONS}"
  "docker/datahub-ingestion/|${ACTIONS}"
  "docker/datahub-ingestion-base/|${ACTIONS}"
  # Only the ingestion and actions Dockerfiles inline these.
  "docker/snippets/|${ACTIONS}"
  "metadata-ingestion/|${ACTIONS}"
  "metadata-ingestion-modules/|${ACTIONS}"

  # The model defines what the server does -- entity registry, validation,
  # storage -- so the server images genuinely behave differently. Clients do
  # not. The frontend only ever emits model objects its own code populates, so
  # an older build emits a valid subset; if its populating code changed,
  # datahub-frontend/** fires anyway, and if a PDL change breaks it, that is a
  # compile failure the build jobs report in seconds. Actions is likewise
  # unaffected: MCL carries the aspect as an opaque GenericAspect blob (bytes +
  # contentType), so routing a brand new aspect type needs no generated class.
  "metadata-models/|${SERVER}"

  # Schema and resolvers alike: GraphQL executes in GMS. See the note above on
  # why a schema change does not rebuild the frontend.
  "datahub-graphql-core/|${SERVER}"

  # Frontend-facing slices of metadata-service.
  "metadata-service/auth-config/|${SERVER} ${UI}"
  "metadata-service/configuration/|${SERVER} ${UI}"
  "metadata-service/restli-api/|${SERVER} ${UI}"
  "metadata-service/restli-client/|${SERVER} ${UI}"
  "metadata-service/restli-client-api/|${SERVER} ${UI}"
  "metadata-service/|${SERVER}"

  # Shared libraries the frontend compiles.
  "entity-registry/|${SERVER} ${UI}"
  "li-utils/|${SERVER} ${UI}"
  "metadata-auth/|${SERVER} ${UI}"
  "metadata-events/|${SERVER} ${UI}"
  "metadata-operation-context/|${SERVER} ${UI}"
  "metadata-utils/|${SERVER} ${UI}"
  "vendor/|${SERVER} ${UI}"

  # Server-side business logic. This is the case the whole split exists for.
  "metadata-io/|${SERVER}"
  "metadata-dao-impl/|${SERVER}"
  "metadata-jobs/|${SERVER}"
  "datahub-upgrade/|${SERVER}"
  "metadata-integration/|${SERVER}"
  # A Gradle module consumed only by mae-consumer and metadata-service/factories.
  "ingestion-scheduler/|${SERVER}"
  "docker/datahub-gms/|${SERVER}"
  "docker/datahub-mae-consumer/|${SERVER}"
  "docker/datahub-mce-consumer/|${SERVER}"
  "docker/datahub-upgrade/|${SERVER}"

  # Anything else under docker/ is shared build machinery.
  "docker/|${EVERYTHING}"
)

# Accumulated set of modules the diff can affect. Anything not matched by a rule
# above is unclassified -- the root build.gradle, gradle/**, buildSrc/**,
# .github/**, a brand new top-level module -- and forces a full build, because
# "this image is unaffected" is only sound when every changed file is accounted
# for. .github/** is deliberately absent from the rules: a workflow change can
# alter build args or the build graph, which is exactly what you want built.
affected=""
unclassified=()

classify_path() {
  local path="$1" rule prefix modules
  # Markdown never reaches an image, wherever it lives.
  [[ "${path}" == *.md ]] && return 0
  for rule in "${PATH_RULES[@]}"; do
    prefix="${rule%%|*}"
    modules="${rule#*|}"
    if [[ "${path}" == "${prefix}"* ]]; then
      affected="${affected} ${modules}"
      return 0
    fi
  done
  return 1
}

changed_files=()
while IFS= read -r path; do
  [[ -n "${path}" ]] || continue
  changed_files+=("${path}")
done < <(jq -r '.[]?' <<<"${CHANGED_FILES:-[]}" 2>/dev/null)

for path in ${changed_files[@]+"${changed_files[@]}"}; do
  classify_path "${path}" || unclassified+=("${path}")
done

module_is_affected() {
  [[ " ${affected} " == *" $1 "* ]]
}

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
# log names the override rather than whichever rule also happened to match.
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
  elif module_is_affected "${module}"; then
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
