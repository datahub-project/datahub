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
#   image_version_env    KEY=VALUE lines pinning each reused image to the exact
#                        digest `quickstart` resolved to when this plan ran
#                        (`quickstart@sha256:...`), not the bare floating tag --
#                        a run's jobs can pull over a 10+ minute window, during
#                        which master can finish a build and move the tag.
#                        Jobs that boot a stack append these to $GITHUB_ENV;
#                        compose reads them as per-service overrides of
#                        DATAHUB_VERSION.
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

ACTIONS=":datahub-actions"
EVERYTHING=":metadata-service:war :datahub-upgrade :metadata-jobs:mae-consumer-job :metadata-jobs:mce-consumer-job :datahub-frontend ${ACTIONS}"

# Which images a changed path can affect: "<prefix>|<space separated modules>".
# First matching prefix wins, so specific prefixes precede general ones. An
# empty module list means the path cannot change any image.
#
# There are deliberately only three outcomes: nothing, actions-only, or all six.
# An earlier revision distinguished server / frontend / shared-library changes
# with a per-image map derived from the Gradle graph (see git history). Measured
# on CI, those distinctions saved nothing: any JVM image build spends ~90% of
# its Gradle work in the codegen/data-template chain shared by all of them
# (skipping the frontend removes 35 of 508 tasks), and the bake runs the
# requested targets in parallel, so a smaller set costs the same as the full
# set. server-only measured 3.8m vs full 3.9m; frontend-only 4.2m. The two
# outcomes that do pay, measured: nothing (0.7m, saves ~3.2m) and actions-only
# (2.4m, saves ~1.5m) -- the Python image is the one build that skips the shared
# JVM chain entirely.
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

  # The actions image is Python: it pip-installs ../metadata-ingestion at build
  # time and never enters the shared JVM chain, so it is cheap to build alone.
  #
  # Two snippets feed JVM service images and map to the full set:
  # setup_java_runtime.sh (gms, frontend, mae, mce, upgrade, actions) and
  # wait_for_deps.sh (gms, mae, mce, upgrade, ingestion-base). The rest of
  # docker/snippets/ feeds only the ingestion/actions images.
  "docker/snippets/setup_java_runtime.sh|${EVERYTHING}"
  "docker/snippets/wait_for_deps.sh|${EVERYTHING}"
  "docker/snippets/|${ACTIONS}"
  "datahub-actions/|${ACTIONS}"
  "docker/datahub-actions/|${ACTIONS}"
  "docker/datahub-ingestion/|${ACTIONS}"
  "docker/datahub-ingestion-base/|${ACTIONS}"
  "metadata-ingestion/|${ACTIONS}"
  "metadata-ingestion-modules/|${ACTIONS}"

  # Everything below can alter a JVM image, and per the note above there is no
  # payoff in distinguishing which one: build the full set. These rules exist
  # (rather than falling through to unclassified) so the log reports a
  # deliberate full build instead of implying the classification has rotted.
  "datahub-web-react/|${EVERYTHING}"
  "datahub-frontend/|${EVERYTHING}"
  "metadata-models/|${EVERYTHING}"
  "datahub-graphql-core/|${EVERYTHING}"
  "metadata-service/|${EVERYTHING}"
  "entity-registry/|${EVERYTHING}"
  "li-utils/|${EVERYTHING}"
  "metadata-auth/|${EVERYTHING}"
  "metadata-events/|${EVERYTHING}"
  "metadata-operation-context/|${EVERYTHING}"
  "metadata-utils/|${EVERYTHING}"
  "vendor/|${EVERYTHING}"
  "metadata-io/|${EVERYTHING}"
  "metadata-dao-impl/|${EVERYTHING}"
  "metadata-jobs/|${EVERYTHING}"
  "datahub-upgrade/|${EVERYTHING}"
  "metadata-integration/|${EVERYTHING}"
  "ingestion-scheduler/|${EVERYTHING}"
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
  for rule in "${PATH_RULES[@]}"; do
    prefix="${rule%%|*}"
    modules="${rule#*|}"
    if [[ "${path}" == "${prefix}"* ]]; then
      affected="${affected} ${modules}"
      return 0
    fi
  done
  # Markdown outside any classified tree cannot reach an image. Inside one, the
  # prefix rule above already claimed it -- deliberately: the actions image COPYs
  # its README, and the Python packages bake theirs into package metadata.
  [[ "${path}" == *.md ]] && return 0
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

# Resolve a tag to its manifest digest against Docker Hub using an anonymous
# pull token. The setup runner has no docker login, and these repos are
# public, so this avoids depending on registry credentials just to answer
# "does the tag exist, and what does it currently point at". Prints the
# digest (e.g. "sha256:abcd...") on stdout and returns 0 if the tag resolves;
# prints nothing and returns 1 if it is missing or the lookup fails.
resolve_manifest_digest() {
  local repo="$1" tag="$2" token response digest
  # A missing tag is an expected outcome, not an error, so curl's own diagnostics
  # are dropped -- the caller logs the decision it reached either way.
  token=$(curl -fsSL --max-time 30 \
    "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${repo}:pull" \
    2>/dev/null | jq -r '.token') || return 1
  response=$(curl -fsL --max-time 30 --head \
    -H "Authorization: Bearer ${token}" \
    -H "Accept: application/vnd.oci.image.index.v1+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
    "https://registry-1.docker.io/v2/${repo}/manifests/${tag}" 2>/dev/null) || return 1
  digest=$(printf '%s' "${response}" | tr -d '\r' |
    awk -F': ' 'tolower($1) == "docker-content-digest" {print $2; exit}')
  [[ -n "${digest}" ]] || return 1
  printf '%s\n' "${digest}"
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
  # A smoke: label picks a different compose profile. Everything is built, and
  # the resolved-tag assertion in run-quickstart.sh skips repositories the
  # selected profile does not run (e.g. quickstartPg has no MAE/MCE) while
  # still checking the ones it does.
  full_build_reason="a smoke: label selected the ${SMOKE_BUILD_TASK} build"
elif ((${#changed_files[@]} == 0)); then
  # A pull request always changes something, so an empty list means the file
  # list never arrived. Reusing every image off the back of that would be the
  # one failure this whole design has to avoid.
  full_build_reason="the changed-file list was empty or unreadable"
elif ((${#unclassified[@]})); then
  # The path is PR-controlled and this string reaches the step summary, which
  # GitHub renders as markdown (and git filenames may contain newlines) --
  # reduce to a safe allowlist instead of stripping known-bad characters.
  first_unclassified=$(printf '%s' "${unclassified[0]}" | tr -cd 'A-Za-z0-9._/-' | cut -c1-120)
  full_build_reason="${#unclassified[@]} changed path(s) are unclassified, starting with ${first_unclassified}"
fi

build_modules=()
build_repos=()
version_env=()
summary=()

for entry in "${IMAGES[@]}"; do
  IFS='|' read -r module repo version_var tag_suffix <<<"${entry}"
  reuse_tag="${QUICKSTART_TAG}${tag_suffix}"

  decision="build"
  digest=""
  if [[ -n "${full_build_reason}" ]]; then
    why="${full_build_reason}"
  elif module_is_affected "${module}"; then
    why="affected by this diff"
  elif ! digest=$(resolve_manifest_digest "${DOCKER_REGISTRY}/${repo}" "${reuse_tag}"); then
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
    # Pin to the digest resolved above, not the bare floating tag: a run's
    # jobs pull images at different wall-clock times over a window that can
    # exceed 10 minutes, during which a master build finishing mid-run can
    # move `quickstart` out from under a job that hasn't pulled yet. Docker/OCI
    # references allow a tag and a digest together (name:tag@digest) -- the
    # digest is authoritative for the pull, the tag stays purely cosmetic --
    # so this needs no change to the compose templates' `${VAR:-...}` tag
    # defaulting.
    version_env+=("${version_var}=${reuse_tag}@${digest}")
    summary+=("| \`${repo}\` | \`${reuse_tag}@${digest}\` | ${why} |")
    echo "reuse ${repo}:${reuse_tag}@${digest}: ${why}"
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
