# DataHub Metadata Ingestion Development Guide

Module guide for `metadata-ingestion/`: the `acryl-datahub` CLI and ingestion framework.
Root `AGENTS.md` owns cross-project rules (Python standards, testing principles,
confidentiality, commits, PRs). Nested guides own the detail:

| Working on                                        | Read                                     |
| ------------------------------------------------- | ---------------------------------------- |
| A connector under `src/datahub/ingestion/source/` | `src/datahub/ingestion/source/AGENTS.md` |
| Tests under `tests/`                              | `tests/AGENTS.md`                        |
| Connector docs under `docs/sources/`              | `docs/sources/AGENTS.md`                 |
| Review-time rules (Cursor Bugbot)                 | `.cursor/BUGBOT.md`                      |

## Build and Test Commands

Always go through Gradle: never run `pytest`, `ruff` or `mypy` directly and never activate
the venv by hand (root `AGENTS.md`, "Python Virtual Environments").

```bash
# From metadata-ingestion/
../gradlew :metadata-ingestion:installDev   # Set up the Python environment (once)
../gradlew :metadata-ingestion:lintFix      # ruff fix + format; run after every change
../gradlew :metadata-ingestion:lint         # ruff + mypy, as CI runs them
../gradlew :metadata-ingestion:testQuick    # Unit tests (tests/unit, no Docker)
../gradlew :metadata-ingestion:testFull     # Unit + Docker integration tests
../gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit/test_file.py            # One file or directory
../gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit/test_file.py::test_name # One test
```

`DATAHUB_VENV_USE_COPIES=true` makes `installDev` copy the Python binary instead of
symlinking it (Nix, immutable filesystems, Windows, some containers).

## Directory Structure

- `src/datahub/`: CLI (`cli/`, `entrypoints.py`), framework (`ingestion/api/`), sources
  (`ingestion/source/`) and sinks (`ingestion/sink/`), SDK (`sdk/`), SQL parsing
  (`sql_parsing/`), shared utilities (`utilities/`), test helpers shipped with the package
  (`testing/`)
- `src/datahub/metadata/`: generated model classes, not checked in
- `tests/unit/`, `tests/integration/`: all tests; never under `src/`
- `docs/sources/`: hand-authored connector docs; `docs/dev_guides/`: stateful ingestion,
  profiling, classification, reporting and telemetry, URN casing, probes
- `developing.md`: published developer guide (code layout, style, dependency policy,
  config guidelines, testing, golden files). It is the canonical owner of those topics.
- `scripts/`: build and docgen scripts; `examples/`: recipes and library examples

## Code Style and Configs

Cross-module Python rules live in root `AGENTS.md`; the ingestion-specific ones in
`developing.md` ("Code style", "Guidelines for Ingestion Configs"). Ruff, including isort,
is configured in `pyproject.toml` under `[tool.ruff]`; `setup.cfg` holds only mypy, pytest
markers and coverage. Connector conventions: `src/datahub/ingestion/source/AGENTS.md`.

## Dependencies and Entry Points

`setup.py` is the source of truth for dependencies, extras and source entry points
(`entry_points["datahub.ingestion.source.plugins"]`). `pyproject.toml`, `uv.lock` and
`constraints.txt` are generated from it; never edit them by hand.

```bash
../gradlew :metadata-ingestion:updateLockFile   # after any setup.py change
../gradlew :metadata-ingestion:checkLockFile    # what CI runs
```

Version-range policy and the manual steps: `developing.md`, "Dependency Management".

## Secrets, Confidentiality, Reporting

- Credential masking in logs and reports (registration helpers, `redact_*`, `exc=`):
  `src/datahub/ingestion/source/AGENTS.md`; the runtime lives in `src/datahub/masking/`.
- This is a public repo: generic placeholder names only, no customer identifiers or ticket
  IDs anywhere (root `AGENTS.md`, "Confidentiality in Committed Code").
- Structured reporting and telemetry: `docs/dev_guides/reporting_telemetry.md`.

## Connector Documentation

Authoring rules (file split, heading contract, terminology): `docs/sources/AGENTS.md`.

```bash
../gradlew :metadata-ingestion:docGen               # regenerate after doc changes
../gradlew :docs-website:yarnStart                  # preview on localhost:3001
../gradlew :datahub-web-react:mdPrettierWrite       # format markdown before committing
```
