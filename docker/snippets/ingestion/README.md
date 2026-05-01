# Snippets used by bundled venvs unified build

These files are used when building the **bundled ingestion venvs** (full and slim). The Dockerfile copies them into the image and invokes the build script.

## Files

| File                             | Purpose                                                                                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `build_bundled_venvs_unified.py` | Builds grouped venvs, installs `acryl-datahub[...]==<BUNDLED_CLI_VERSION>` with constraints, creates relative symlinks so legacy `{plugin}-bundled` paths resolve. |
| `build_bundled_venvs_unified.sh` | Wrapper that validates env and runs the Python script.                                                                                                             |
| `bundled_venv_config.py`         | Extras resolution (including **slim** `-slim` variants), `BUNDLED_VENV_PLUGINS_*` group parsing, `build_group_plans`.                                              |
| `constraints.txt`                | **Single source of truth for CVE minimums and shared pins.** Used by the bundled venv build and by Dockerfile pip install steps that install from requirements.    |

The Dockerfile copies `constraints.txt` to `$DATAHUB_BUNDLED_VENV_PATH`, and copies these modules to `/tmp` for the bundled-venv build stages. Published **`acryldata/datahub-actions`** **full** and **slim** images also ship the same files under **`/opt/datahub/bundled-venv-build/`** and export **`ENV`** for **`DATAHUB_BUNDLED_VENV_PATH`**, **`BUNDLED_VENV_PLUGINS`**, **`BUNDLED_VENV_PLUGINS_COMMON`**, **`BUNDLED_VENV_SLIM_MODE`**, and **`BUNDLED_CLI_VERSION`** (matching the bundled-venv build) so downstream Dockerfiles can append plugins and rerun the builder without cloning this repository (see `docs/docker/bundled-ingestion-venvs.md`).

## Group env vars: `BUNDLED_VENV_PLUGINS_<group_id>`

Sharing is configured with variables **`BUNDLED_VENV_PLUGINS_` + suffix**. The suffix is normalized to **lowercase** to form the group label and canonical venv directory **`{label}-venv`** (avoids colliding with `{plugin}-bundled` entrypoint paths):

| Env var example                | Group label | Canonical venv |
| ------------------------------ | ----------- | -------------- |
| `BUNDLED_VENV_PLUGINS_COMMON`  | `common`    | `common-venv`  |
| `BUNDLED_VENV_PLUGINS_GC_DOCS` | `gc_docs`   | `gc_docs-venv` |

Each value is a **comma-separated** list of plugin names (same names as `setup.py` extras / `BUNDLED_VENV_PLUGINS` entries). Multiple `BUNDLED_VENV_PLUGINS_*` variables define multiple groups.

If **no** `BUNDLED_VENV_PLUGINS_*` variables are set, each plugin in `BUNDLED_VENV_PLUGINS` gets its own real directory `{plugin}-bundled` (singletons).

`BUNDLED_VENV_PLUGINS` remains the **authoritative full list** of plugins that must receive a `{plugin}-bundled` path; every member of every group must appear in that list, and each plugin appears in **at most one** group (or as a singleton).

## Slim mode and PySpark

When **`BUNDLED_VENV_SLIM_MODE`** is `true`, extras for plugins in `PLUGINS_WITH_SLIM_VARIANT` use the corresponding **`-slim`** extra (e.g. `s3-slim` instead of `s3`) so PySpark is not pulled for data-lake stacks that support it.

After each slim build, the script runs **`import pyspark`** in that venv and **fails the build** if it succeeds (defense in depth).

## Single CLI version

**`BUNDLED_CLI_VERSION`** is the only bundled-ingestion CLI version used for PyPI installs (`acryl-datahub[...]==<version>`). Local builds use editable `/metadata-ingestion` when present.

## Default install recipe per group

Unless overridden in code/tests via an internal `extras` list on a group dict, install extras are the **sorted union** of each member’s legacy per-plugin extras (including `PLUGIN_ADDITIONAL_EXTRAS` and slim rules above).

## Environment variables

| Variable                            | Description                                                                                                  |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `BUNDLED_VENV_PLUGINS`              | Comma-separated list of every plugin that must have a `{plugin}-bundled` path.                               |
| `BUNDLED_VENV_PLUGINS_<suffix>`     | Comma-separated plugins sharing `{suffix.lower()}-venv`; symlinks `{plugin}-bundled` → that dir when needed. |
| `BUNDLED_VENV_AUX_PRIMARY_<suffix>` | Optional. One plugin name from that group; suffix matches the same group’s `BUNDLED_VENV_PLUGINS_<suffix>`.  |
| `BUNDLED_CLI_VERSION`               | PyPI version pin for `acryl-datahub` (single version for all bundled venvs).                                 |
| `DATAHUB_BUNDLED_VENV_PATH`         | Directory for venvs and `constraints.txt` (default `/opt/datahub/venvs`).                                    |
| `BUNDLED_VENV_SLIM_MODE`            | When `true`, use `-slim` extras where defined and verify PySpark is absent after install.                    |

## Validation

- Each plugin in `BUNDLED_VENV_PLUGINS` appears **exactly once** across all groups’ plugin lists or as a singleton.
- A plugin cannot be listed in two groups.
- Group plugin lists must be subsets of `BUNDLED_VENV_PLUGINS`.
- Empty group lists are rejected; duplicate group labels after lowercasing (e.g. two keys that map to `common`) are rejected.
