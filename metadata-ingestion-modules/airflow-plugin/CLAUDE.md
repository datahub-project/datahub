# acryl-datahub-airflow-plugin Module

This module is the DataHub Airflow plugin, published to PyPI as `acryl-datahub-airflow-plugin`. It
listens to Airflow task and DAG events, converts the OpenLineage the provider produces into DataHub
DataFlow/DataJob entities, and emits them. User-facing documentation lives in
[`docs/lineage/airflow.md`](../../docs/lineage/airflow.md).

## Changelog and Release Notes

The plugin's release notes are the `## Changelog` section of
[`docs/lineage/airflow.md`](../../docs/lineage/airflow.md). There is no `CHANGELOG.md`, and nothing
generates it — regenerate it by hand whenever the released versions have drifted ahead of the
newest documented one.

### Why this is manual

The plugin is published on **every** DataHub release and takes that release's version from
`src/datahub_airflow_plugin/_version.py`, which the release job stamps. Most releases ship a wheel
with no plugin changes. The changelog lists only the versions that changed, and you attribute every
entry to a release by hand.

Churn here is low: roughly a dozen user-facing changes a year, against hundreds of published
versions.

### Attribute commits to releases

Collect commits over a **date window on `origin/master`**, then resolve each one with
`git tag --contains`. Both ends avoid branch topology, which is the part that goes wrong. Three
traps make the obvious approaches return nonsense:

- **Release tags are not on `master`.** Patch lines are tagged on release branches, so
  `git merge-base --is-ancestor v1.7.0 v1.7.0.11rc4` is false. Ranges between such tags produce
  arbitrary results — `v1.7.0..v1.7.0.11rc4` reports 75 commits for this module while the wider
  `v1.6.0..v1.7.0.11rc4` reports 4.
- **Version order is not time order.** `v1.8.0rc3` was tagged 2026-09-07, ten days _before_
  `v1.7.0.11rc4` on 2026-09-17, so `sort -V | tail -1` is not the newest release.
- **DataHub Cloud tags share the repo.** Cloud releases use the `v0.3.x` namespace, which sorts
  below every OSS `v1.x` tag. Filter to `^v1\.` or a Cloud tag wins the `sort -V` and misdates the
  entry.

Run this from the repository root, with `<last-documented>` set to the newest version already in
the changelog:

```bash
# Releases are tagged on the acryl remote; master lives on origin.
git fetch acryl --tags && git fetch origin

# Collect the commits that reach the published wheel.
SINCE=$(git log -1 --format=%cI v<last-documented>)
git log --format='%H|%ci|%s' --since="$SINCE" origin/master -- \
  metadata-ingestion-modules/airflow-plugin/src/ \
  metadata-ingestion-modules/airflow-plugin/setup.py > /tmp/af_commits.txt

# Map each commit to the lowest-numbered OSS GA release that contains it.
while IFS='|' read -r sha date subj; do
  tag=$(git tag --contains "$sha" | grep -E '^v1\.[0-9]+\.[0-9]+(\.[0-9]+)?$' | sort -V | head -1)
  printf "%-11s %s  %s\n" "${tag:--unreleased-}" "${date:0:10}" "$subj"
done < /tmp/af_commits.txt | tac
```

Commits that resolve to no tag are unreleased. A commit that reached a release only as a
cherry-pick keeps a different SHA on the release branch, so spot-check anything that looks like it
should have shipped: `git log --all --grep '#<pr-number>)'`.

### Which paths count

Only this module ships in the wheel:

- `src/` — the plugin itself.
- `setup.py` — dependency floors and extras, which users feel at install time.

`acryl-datahub` is a separate PyPI package with its own version, so changes under
`metadata-ingestion/` do not belong in this changelog even when they alter lineage behaviour.
Skip `tests/`, `scripts/` and the docker test harness.

A commit's subject can understate it. Read the diff: #18432 is titled as an integration-test fix
but migrates the Kafka hook from pydantic v1 `parse_obj` to v2 `model_validate`.

### Confirm what was actually published

Tags and published wheels are not the same set. Check PyPI before writing version headings:

```bash
curl -s https://pypi.org/pypi/acryl-datahub-airflow-plugin/json \
  | python3 -c "import sys,json;print(sorted(json.load(sys.stdin)['releases']))"
```

Give a heading only to GA versions. Keep changes that exist solely in an `rc` under a `Next`
heading until that GA release ships.

### Write the entries

Match the conventions already in `docs/lineage/airflow.md`:

- Order versions newest first, above `## Additional references`.
- Group bullets under `_Major changes_`, `_Changes_` and `_Fixes_`.
- Link each PR as `([#17439](https://github.com/datahub-project/datahub/pull/17439))`.
- Describe the user-visible symptom of a fix, not the patch. Readers decide whether to upgrade
  from the symptom.
- Cross-check every breaking change against
  [`docs/how/updating-datahub.md`](../../docs/how/updating-datahub.md), which carries the full
  migration text, and keep the two consistent.
- Link to the relevant section of the same page when one exists, rather than restating it.
- Collapse a run of unchanged versions into one heading — for example,
  `### Versions 1.7.0 through 1.7.0.11` — so a gap reads as deliberate rather than missing.

Before committing, format with Gradle (never `npx prettier`):

```bash
./gradlew :datahub-web-react:mdPrettierWrite
```

### Prompt to hand an agent

Run this from the repository root:

> Update the `## Changelog` section of `docs/lineage/airflow.md` to cover every DataHub release
> published since the newest version documented there.
>
> Follow the "Changelog and Release Notes" runbook in
> `metadata-ingestion-modules/airflow-plugin/CLAUDE.md`. In particular:
>
> - Run `git fetch acryl --tags && git fetch origin` first; releases are tagged on the `acryl`
>   remote and `master` lives on `origin`.
> - Collect candidate commits with a `--since` date window on `origin/master`, then attribute each
>   one with `git tag --contains <sha>`, filtering tags to `^v1\.` so DataHub Cloud's `v0.3.x`
>   tags do not win the sort. Never range between two tags and never use `HEAD` as the endpoint.
> - Cover `src/` and `setup.py` only. `acryl-datahub` is a separate package.
> - Read each diff rather than trusting the commit subject.
> - Cross-check published GA versions against
>   `https://pypi.org/pypi/acryl-datahub-airflow-plugin/json`, and give a heading only to GA
>   versions.
> - Cross-check breaking changes against `docs/how/updating-datahub.md` and keep them consistent.
> - Collapse runs of unchanged versions into a single heading.
> - Finish with `./gradlew :datahub-web-react:mdPrettierWrite`.
>
> Show me the diff. Do not commit or push.
