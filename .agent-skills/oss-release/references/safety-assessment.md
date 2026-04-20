# Safety Assessment Checklist

Used by `prep` Step 2b to classify a release as ✅ SAFE / ⚠️ REVIEW NEEDED / ❌ BLOCKED.
Fed by the Stage 2a release-range diff. The output is a 5-row table the user can scan
in 5 seconds plus a single overall verdict.

## Per-category checklist

You MUST produce a verdict for EVERY category below. Do not skip a category by saying
"nothing to assess" — explicitly state "✅ N/A — no files in this category changed" so
it's clear you looked. Free-form impressions ("looks fine") are not acceptable; each row
needs an explicit verdict and a one-line justification.

| Category | Files in Stage 2a output | Verdict | One-line justification |
|---|---|---|---|
| Dependencies | `pyproject.toml`, `setup.py`, `setup.cfg`, `uv.lock`, `requirements*.txt` | ✅ / ⚠️ / ❌ / N/A | e.g. "patch bump 46.0.5 → 46.0.7, upper bound preserved" |
| Breaking / API | `*.py` removals, validator signatures, CLI command files, config schema files | ✅ / ⚠️ / ❌ / N/A | e.g. "no public API changes; only test-weight JSON" |
| OSS scope | Anything matching `acryl_*`, `*cloud*`, `internal/`, or paths flagged as proprietary | ✅ / ⚠️ / ❌ / N/A | e.g. "no acryl-proprietary code in this range" |
| Build / packaging | `MANIFEST.in`, build scripts, GitHub Actions publish workflows | ✅ / ⚠️ / ❌ / N/A | e.g. "no packaging changes" |
| Schema / migration | `metadata-models/` PDL, `*.avsc`, migration SQL | ✅ / ⚠️ / ❌ / N/A | e.g. "no schema changes" |

## Dependency-bump verdict mapping

When the Dependencies row shows files touched, you MUST run
`git show <sha> -- <file>` for each affected commit and inspect the actual delta. Don't
infer from commit titles. Then evaluate:

- Patch bump (`46.0.5` → `46.0.7`), upper bound preserved → ✅ SAFE
- Minor bump (`46.0.x` → `46.1.x`), upper bound preserved → ⚠️ REVIEW NEEDED — call out which package
- Major bump (`46.x` → `47.x`) or removed upper bound → ⚠️ REVIEW NEEDED, default toward ❌
- Package added or removed entirely → ⚠️ REVIEW NEEDED — call out the package
- Lockfile-only change with no manifest delta → ✅ SAFE (deterministic resolver)

## Overall verdict rollup

| Per-category mix | Overall |
|---|---|
| All ✅ or N/A | ✅ SAFE — proceed to Step 3 |
| Any ⚠️ | ⚠️ REVIEW NEEDED — present the ⚠️ rows to the user, ask how to proceed |
| Any ❌ | ❌ BLOCKED — list what must be fixed; stop the workflow |
