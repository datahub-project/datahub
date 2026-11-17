---
name: datahub-test-taxonomy
description: >
  Maintain the smart-testing test taxonomy: tag new or changed tests and drop deleted
  ones, tag any tests that have no tags yet, regenerate all test tags (optionally for one framework), or regenerate the
  taxonomy from the rubric. Use after adding, modifying, or deleting smoke-test pytest,
  Playwright, or metadata-ingestion tests, or when the taxonomy needs a refresh.
argument-hint: "[tag [--untagged] [--framework F] [--base BRANCH] | retag [--framework F] | taxonomy]"
model: claude-opus-5-5[1m]
allowed-tools: Bash(uv run -q --with pyyaml==6.0.3 python ${CLAUDE_SKILL_DIR}/scripts/tags.py *)
---

# Test taxonomy

Smart test selection matches a PR's tags against per-test tags. This skill keeps the tags
and the taxonomy behind them current. It edits files only: it never runs the tests it
tags, and it never commits.

## Files

| File                                             | Role                                           |
| ------------------------------------------------ | ---------------------------------------------- |
| `.github/smart-testing/taxonomy.yaml`            | The common vocabulary for every framework.     |
| `.github/smart-testing/taxonomy.overlay.yaml`    | Optional tags for repo-only features.          |
| `.github/smart-testing/taxonomy-rubric.md`       | The criteria a good taxonomy meets.            |
| `smoke-test/smart_testing/taxonomy.py`           | Enum mirror of the taxonomy (generated block). |
| `smoke-test/smart_testing/taxonomy_overlay.py`   | Mirror of the overlay tags (generated).        |
| [references/tagging.md](references/tagging.md)   | How to tag one test file.                      |
| [references/taxonomy.md](references/taxonomy.md) | How to regenerate the taxonomy.                |

| Framework    | Tag file                                          | Paths relative to | Test files                                         |
| ------------ | ------------------------------------------------- | ----------------- | -------------------------------------------------- |
| `pytest`     | `smoke-test/smart_testing/test_tags.yaml`         | `smoke-test/`     | `tests/e2e/**/test_*.py`, `tests/e2e/**/*_test.py` |
| `playwright` | `.github/smart-testing/playwright/test_tags.yaml` | repo root         | `e2e-test/ui/playwright/tests/**/*.spec.ts`        |
| `ingestion`  | `.github/smart-testing/ingestion/test_tags.yaml`  | repo root         | `metadata-ingestion/tests/**/{test_*,*_test}.py`   |

Only `pytest` tags drive CI today. The others are maintained for when their selectors
land. `scripts/tags.py` holds the real definitions (`FRAMEWORKS`).

### Base and overlay

The base files (`taxonomy.yaml`, each `test_tags.yaml`) are shared by repositories that
run the same tests, so they hold only tags and tests every one of them has. Each may have
an optional overlay next to it (`taxonomy.overlay.yaml`, `test_tags.overlay.yaml`) for
features and tests only this repository has. Every consumer merges base and overlay.

- An entry goes in the overlay when its test file, or any tag it needs, is overlay-only.
  Otherwise it goes in the base.
- A new tag goes in the overlay when the feature it names only exists here.
- `tags.py validate` fails when a base entry uses an overlay tag.

## Helper script

Every file listing, pruning, validation, and enum generation goes through the script, so
the model only does the judgement work: reading tests and choosing tags.

```bash
uv run -q --with pyyaml==6.0.3 python ${CLAUDE_SKILL_DIR}/scripts/tags.py <command> [--framework F ...]
```

| Command              | Does                                                                                                  |
| -------------------- | ----------------------------------------------------------------------------------------------------- |
| `changed [--base B]` | This branch's added, modified, and deleted test files, as JSON.                                       |
| `list`               | All test files, grouped by directory.                                                                 |
| `untagged`           | Test files that no entry matches.                                                                     |
| `prune [--write]`    | Entries matching no file (deleted paths, dead globs). `--write` removes them, comments intact.        |
| `validate`           | Every tag is in the taxonomy under its dimension, and every entry matches a file. Exits 1 on failure. |
| `enums [--write]`    | Regenerate `taxonomy.py`'s enum block and `taxonomy_overlay.py` from the taxonomy yaml files.         |

`changed` works with or without a PR, on any target branch. The base branch is `--base`,
else the open PR's base, else the repository's default branch. It diffs the **merge base** against the
**working tree** and adds untracked files. That covers committed and uncommitted edits,
and excludes anything that landed on the base after the branch forked.

## Modes

Parse `$ARGUMENTS`: the first word is the mode (default `tag`), then flags.
`--framework` is repeatable and takes `pytest`, `playwright` or `ingestion`.

### `tag`: new, changed and deleted tests (default)

With `--untagged`, the files to tag are every test that no entry matches, whether or not
this branch touched it. This is how existing gaps get backfilled. `--base` doesn't apply.

1. Get the files:

   - By default, run `tags.py changed` (plus `--base`/`--framework` if given).
   - With `--untagged`, run `tags.py untagged` (plus `--framework`) and treat every listed
     file as added.

   Only process frameworks that have files. If none do, say so and stop.

2. Run `tags.py prune --write` for those frameworks, which removes deleted tests' entries
   and dead globs.
3. Handle each added or modified file:

   - Read it and derive its tags per [references/tagging.md](references/tagging.md).
   - If an existing glob already covers it with the right tags, leave the file alone.
   - Otherwise add an exact entry, or update the existing one, in the base or the
     overlay (see [Base and overlay](#base-and-overlay)).
   - If several files in one directory share the same tags and that directory has no
     entries yet, one glob is fine.

   With many files (say more than 25), batch them to subagents the way `retag` does.

4. Run `tags.py validate` and `tags.py untagged`. Fix what they report for the files you
   touched.

### `retag`: regenerate all tags

1. Frameworks are `--framework`, or all three if it's absent. For each, run `tags.py list`.
2. Split the directories into batches of about 25 test files. Give each batch to a
   subagent (Agent tool, `model: "opus"`), running them in parallel. Each subagent gets:

   - the path of `references/tagging.md`, which it must read first
   - its file list
   - the framework's path convention

   It returns YAML entries only, with globs where a whole directory shares tags.

3. Rebuild the `tests:` sections of the tag file and its overlay from the merged entries,
   split per [Base and overlay](#base-and-overlay), with one `# ── <dir>/ ──` section per
   top-level directory in sorted order. Keep the file headers and `always_run` exactly as
   they are.
4. Run `tags.py validate` and `tags.py untagged`. Both must come back clean. Re-tag
   anything they flag.
5. Report the number of entries and tags per dimension, before and after.

### `taxonomy`: regenerate the taxonomy

1. Follow [references/taxonomy.md](references/taxonomy.md) to write
   `.github/smart-testing/taxonomy.yaml` and, if this repository has one,
   `taxonomy.overlay.yaml`.
2. Report which tags were added, removed and renamed, and which definitions changed, with
   rationale.
3. Run `tags.py enums --write`.
4. Run `tags.py validate` across all frameworks. For each entry that uses a removed or
   renamed tag, re-tag the files it covers with [references/tagging.md](references/tagging.md).
   Then validate again.
5. Run `./gradlew :smoke-test:stackFreePytest`, which covers the taxonomy/enum sync test.

## Finish

Summarize what changed, per file. List any untaggable tests and any taxonomy gaps you hit.
Don't commit: the user reviews the diff first.
