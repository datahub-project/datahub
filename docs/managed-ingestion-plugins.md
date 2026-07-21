---
toc_max_heading_level: 4
---

# Ingestion Plugins

DataHub's ingestion plugin system lets you install community and custom connectors (sources, sinks, and transformers) without modifying or reinstalling the core `acryl-datahub` package. Plugins are installed directly into the current Python environment via `uv pip install` (preferred) or `pip install` (fallback) and discovered at runtime via `importlib.metadata`.

## Quick Start

### Install a Plugin

```shell
# By id from a configured registry (resolves the repo and verifies the
# published checksum when the registry index provides one)
datahub plugin install salesforce-source

# From a GitHub repository
datahub plugin install github:acme/datahub-salesforce-source

# From a local wheel file
datahub plugin install ./datahub_salesforce_source-1.0.0-py3-none-any.whl

# A specific version from GitHub
datahub plugin install github:acme/datahub-salesforce-source@v1.2.0
```

When you install by id, DataHub looks the id up across your configured
registries (see [Plugin Registries](#plugin-registries)), resolves it to the
plugin's GitHub release, and — if the index entry declares a `sha256` — verifies
the downloaded wheel against it before installing. Ids you find via
`datahub plugin search` can be installed directly this way.

### Use a Plugin in a Recipe

Once installed, the plugin's source (or sink/transformer) type is available in recipes just like any built-in connector:

```yaml
source:
  type: salesforce-source
  config:
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    instance_url: https://mycompany.salesforce.com
```

### UI / Managed Ingestion

For UI-based managed ingestion, the executor installs plugins into the ephemeral venv before running the recipe:

```shell
# Executor runs these before datahub ingest
datahub plugin install github:acme/datahub-salesforce-source@v1.0.0
datahub plugin install github:acme/datahub-custom-transformer@v2.1.0
```

When you pick a community plugin in the UI, its registry `sha256` is carried
through with the install spec, and the executor verifies the downloaded wheel
against it before installing — so UI-driven installs are checksum-verified, not
just CLI installs.

The recipe itself only references types — no special `plugins` key is needed:

```yaml
source:
  type: salesforce-source
  config:
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    instance_url: https://mycompany.salesforce.com
```

If a referenced type is not installed, the pipeline will fail with a clear error message suggesting how to install it.

### List Installed Plugins

```shell
datahub plugin list
```

### Uninstall a Plugin

```shell
datahub plugin uninstall salesforce-source
```

## How It Works

When you run `datahub plugin install`, the following happens:

1. **Resolve** — The spec (e.g., `github:owner/repo@v1.0.0`) is resolved to a downloadable wheel URL via the GitHub Releases API.
2. **Download** — For wheel assets, the file is downloaded locally using the GitHub API (supports private repos with `GITHUB_TOKEN`).
3. **Install** — `uv pip install` (preferred) or `pip install` (fallback) runs in the current Python environment.
4. **Verify** — The plugin's manifest (`datahub-plugin.yaml`) is found via `importlib.metadata` to confirm the package is a valid DataHub plugin.

At runtime, when a recipe references a type like `salesforce-source`, DataHub's registry scans installed packages via `importlib.metadata` for plugins providing that type and imports the connector class directly.

## CLI Reference

### `datahub plugin install`

Install a plugin from GitHub, a local wheel, or a pip spec.

```shell
datahub plugin install <spec> [--version VERSION]
```

**Arguments:**

| Argument    | Description                                                                      |
| ----------- | -------------------------------------------------------------------------------- |
| `spec`      | Plugin source. Examples: `github:owner/repo`, `./plugin.whl`, `my-plugin==1.0.0` |
| `--version` | Version to install (for GitHub specs, overrides the `@version` in the spec)      |

**Spec formats:**

- `salesforce-source` — Plugin id resolved via a configured registry (installs the indexed GitHub release, checksum-verified when the index provides a `sha256`)
- `github:owner/repo` — Latest release from a GitHub repository
- `github:owner/repo@v1.2.0` — Specific release tag
- `/path/to/plugin.whl` — Local wheel file
- `my-plugin==1.0.0` — Pip package specifier

A bare id is only resolved through the registry when it matches a listed plugin;
otherwise it is treated as a pip package. Registry release lookups tolerate the
`v` tag prefix, so an index version of `0.1.0` resolves a `v0.1.0` git tag.

### `datahub plugin uninstall`

Remove an installed plugin.

```shell
datahub plugin uninstall <plugin-id>
```

### `datahub plugin list`

Show all installed plugins.

```shell
datahub plugin list
```

### `datahub plugin info`

Show detailed metadata for an installed plugin.

```shell
datahub plugin info <plugin-id>
```

### `datahub plugin search`

Search the community plugin index.

```shell
datahub plugin search <query> [--type source|sink|transformer]
```

### `datahub plugin init`

Scaffold a new plugin project.

```shell
datahub plugin init --type=source my-salesforce-source
```

This generates a complete, release-ready project structure including source code stubs, tests, and a GitHub Actions release workflow.

### `datahub plugin validate`

Validate a plugin project before publishing.

```shell
datahub plugin validate [path]
```

Checks:

- `datahub-plugin.yaml` schema is valid
- `pyproject.toml` has correct entry point configuration
- Declared import paths resolve
- Wheel builds successfully

## Plugin Registries

By default, DataHub searches the community plugin index. You can also configure enterprise registries for internal plugins.

### Configure a Registry

```shell
# Add an enterprise registry
datahub plugin registry add acme-internal https://git.internal.acme.com/raw/datahub-plugins/main/index.json

# List configured registries
datahub plugin registry list

# Remove a registry
datahub plugin registry remove acme-internal

# Force refresh all caches
datahub plugin registry refresh
```

### Registry Config File

Registries are stored at `~/.datahub/plugins/config.yaml`:

```yaml
registries:
  - name: community
    url: https://raw.githubusercontent.com/datahub-project/datahub-plugins/main/index.json
    enabled: true
  - name: acme-internal
    url: https://git.internal.acme.com/raw/datahub-plugins/main/index.json
    auth_type: bearer
    token_env: ACME_GIT_TOKEN
```

### Authentication

For private registries, set the `auth_type` and `token_env` fields. The value of `token_env` is the name of the environment variable holding the auth token:

```shell
export ACME_GIT_TOKEN=ghp_xxxxxxxxxxxx
```

For GitHub private repos, the standard `GITHUB_TOKEN` environment variable is used automatically.

### Registry Index Format

A registry is a single JSON index (one "descriptor") that catalogs **many**
plugins, each pointing to its own GitHub repository. The index is either a flat
array or an object with a `plugins` array. Unknown fields are ignored, so the
schema can grow without breaking older clients.

```json
{
  "plugins": [
    {
      "id": "salesforce-source",
      "repo": "acme/datahub-salesforce-source",
      "version": "1.2.0",
      "type": "source",
      "description": "Ingest metadata from Salesforce",
      "author": "Acme",
      "trust_tier": "verified",
      "icon_url": "https://acme.example.com/sf.png",
      "sha256": "9f2b…"
    },
    {
      "id": "slack-sink",
      "repo": "someorg/datahub-slack-sink",
      "version": "0.3.1",
      "type": "sink"
    }
  ]
}
```

| Field          | Required | Description                                                                 |
| -------------- | -------- | --------------------------------------------------------------------------- |
| `id`           | Yes      | Plugin id used by `datahub plugin install <id>` and shown in `search`       |
| `repo`         | Yes      | `owner/repo` of the plugin's GitHub repository (each entry can differ)       |
| `version`      | Yes      | Release version (resolves the matching git tag, with or without a `v`)      |
| `type`         | No       | `source`, `sink`, or `transformer` (default `source`)                       |
| `sha256`       | No       | Checksum of the release wheel; verified at install time when present        |
| `trust_tier`   | No       | `community` (default), `verified`, or `official` — shown as a badge         |
| `description` / `author` / `display_name` / `icon_url` / `recipe_template` | No | Display metadata for `search` and the ingestion UI |

Because each entry names its own `repo`, one index in a single GitHub repo can
point to any number of separate plugin repositories. You can also configure
multiple registries; `search` and `list` aggregate across all enabled ones.

### Building the Index

You don't hand-write `index.json`. A registry maintainer keeps a small curated
**sources file** — just which plugins are listed, at which version, and each
plugin's trust tier — and generates the index from it:

```yaml
# sources.yaml
plugins:
  - repo: acme/datahub-salesforce-source
    version: "1.2.0"
    trust_tier: verified # governance — the maintainer's call, not the author's
  - repo: treff7es/my-test-source
    version: "0.1.0"
```

```shell
datahub plugin index-build --sources sources.yaml --out index.json
```

For each source, `index-build` resolves the GitHub release, downloads the wheel
**once**, and uses it for both the `sha256` and the bundled `datahub-plugin.yaml`.
Everything else in the entry — `capabilities`, `support_status`, `icon_url`,
`description`, `type` — is lifted from that manifest, which the plugin author
generates from their `@capability` / `@support_status` decorators via
`datahub plugin sync`. So capabilities always reflect the
plugin's actual code; only curation (the plugin list and trust tier) is manual.

A plugin gets listed by opening a pull request that adds its `repo` and `version`
to the registry's `sources.yaml`; the registry's CI re-runs `index-build`.

## Creating a Plugin

### 1. Scaffold

```shell
datahub plugin init --type=source my-salesforce-source
cd my-salesforce-source
```

This creates:

```
my-salesforce-source/
    pyproject.toml
    README.md
    LICENSE
    src/
        my_salesforce_source/
            __init__.py
            source.py
            config.py
            datahub-plugin.yaml
    tests/
        __init__.py
        test_source.py
    .github/
        workflows/
            release.yml
            test.yml
```

The `datahub-plugin.yaml` manifest lives inside the package directory (not at the project root) so that it is included as package data in the built wheel and discoverable by `importlib.metadata` after installation.

### 2. Implement

Edit `src/my_salesforce_source/source.py` to implement your connector. Your source class should extend `datahub.ingestion.api.source.Source`:

```python
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

from my_salesforce_source.config import MySalesforceSourceConfig


class MySalesforceSource(Source):
    def __init__(self, config: MySalesforceSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySalesforceSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self):
        # Emit MetadataWorkUnit objects here
        yield from []

    def get_report(self):
        return SourceReport()
```

### 3. Define the Manifest

Edit `src/my_salesforce_source/datahub-plugin.yaml`:

```yaml
api_version: datahub/v1
id: my-salesforce-source
name: My Salesforce Source
type: source
entry_point: my_salesforce_source.source:MySalesforceSource
description: Ingest metadata from Salesforce CRM
author: Your Name
url: https://github.com/yourname/my-salesforce-source
compatibility:
  datahub_min: "0.12.0"
  python_min: "3.9"
```

The `url` field is optional but recommended — it is shown to users after install and in `datahub plugin list` to help them find documentation.

### 4. Test Locally

```shell
pip install -e .
datahub plugin validate .
datahub ingest -c test-recipe.yaml
```

### 5. Publish

Tag a release on GitHub. The scaffolded GitHub Actions workflow builds a wheel and attaches it to the release automatically:

```shell
git tag v1.0.0
git push --tags
```

Users can then install with:

```shell
datahub plugin install github:yourname/my-salesforce-source
```

### Plugin Manifest Reference

The `datahub-plugin.yaml` file is the manifest that describes your plugin:

| Field           | Required | Type   | Description                                          |
| --------------- | -------- | ------ | ---------------------------------------------------- |
| `api_version`   | No       | string | Always `datahub/v1`                                  |
| `id`            | Yes      | string | Unique plugin identifier (e.g., `salesforce-source`) |
| `name`          | Yes      | string | Human-readable name                                  |
| `type`          | Yes      | enum   | `source`, `sink`, or `transformer`                   |
| `entry_point`   | Yes      | string | Python import path (e.g., `mod.source:MySource`)     |
| `config_class`  | No       | string | Python import path for the config class              |
| `description`   | No       | string | Short description of the plugin                      |
| `author`        | No       | string | Plugin author                                        |
| `url`           | No       | string | Project homepage or documentation URL                |
| `compatibility` | No       | object | Minimum version requirements                         |

## Architecture

### File Layout

```
~/.datahub/plugins/
    config.yaml                     # Registry configuration (global)
    .index_cache/                   # Cached registry indexes
```

Plugins are installed directly into the current Python environment via `uv pip install` (preferred) or `pip install` (fallback). There is no lockfile — installed plugins are discovered at runtime by scanning `importlib.metadata` for packages containing a `datahub-plugin.yaml` manifest. Each virtual environment naturally has its own set of installed packages. Editable installs (`pip install -e .`) are also supported for local development — discovery falls back to `importlib.util.find_spec()` when `dist.files` doesn't contain the manifest (as with PEP 660 editable installs).

### Registry Integration

The plugin system integrates with DataHub's existing `PluginRegistry` via a fallback mechanism. When a recipe references a type that is not built-in, the registry consults the plugin loader before raising an error. This means:

- All built-in connectors work exactly as before
- Plugin connectors are loaded on demand from installed packages
- No configuration changes are needed — just install and use

## Troubleshooting

### Plugin Not Found After Install

Verify the plugin is discovered:

```shell
datahub plugin list
datahub plugin info <plugin-id>
```

Check that the `type` field in `datahub-plugin.yaml` matches how you're using it (e.g., `source` for a source connector).

### Dependency Conflicts

Plugins are installed directly into the current Python environment. If two plugins require conflicting versions of the same dependency, the package manager (uv or pip) will resolve them at install time. If you encounter issues, check what's installed:

```shell
datahub plugin info <plugin-id>
pip list | grep <dependency>
```

### Import Errors

If a plugin fails to load, check the debug logs:

```shell
datahub --debug ingest -c recipe.yaml
```

Common causes:

- The plugin's `entry_point` in `datahub-plugin.yaml` doesn't match the actual module path
- The plugin depends on a system library that isn't installed
- Python version mismatch (check `compatibility.python_min` in the manifest)

### GitHub Rate Limiting

For frequent installs or searches against GitHub, set a token:

```shell
export GITHUB_TOKEN=ghp_xxxxxxxxxxxx
```
