- Start Date: 2026-03-01
- RFC PR: (pending)
- Discussion Issue: N/A
- Implementation PR(s): (pending)

# Ingestion Plugin System

## Summary

Add a plugin system to DataHub's Python ingestion framework that lets users install community and custom connectors (sources, sinks, transformers) from GitHub Releases, local wheels, or private registries — without modifying the core `acryl-datahub` package. Plugins are installed directly into the current Python environment via `uv pip install` (preferred) or `pip install` (fallback), and discovered at runtime via `importlib.metadata`.

## Basic example

**Install a community connector:**

```shell
datahub plugin install github:acme/datahub-salesforce-source
```

**Use it in a recipe (no code changes to DataHub):**

```yaml
source:
  type: salesforce-source
  config:
    client_id: ${SALESFORCE_CLIENT_ID}
    instance_url: https://mycompany.salesforce.com
```

**Scaffold a new plugin project:**

```shell
datahub plugin init --type=source my-custom-source
```

## Motivation

DataHub's current connector ecosystem has a high contribution barrier:

1. **Publishing a connector requires PyPI packaging.** Authors need a PyPI account, must understand Python packaging metadata (`pyproject.toml`, entry points), and must publish releases manually or via CI.

2. **Installation risks dependency conflicts.** Users run `pip install datahub-salesforce-source` into their global or shared DataHub environment. If the plugin pins `requests>=2.30` and DataHub pins `requests<2.28`, the install breaks.

3. **Discovery is ad-hoc.** There is no central index of community connectors. Users rely on GitHub search, blog posts, or word-of-mouth.

4. **No guardrails for plugin quality.** There is no validation, trust tier system, or community review process for third-party connectors.

These barriers mean that most custom connectors remain internal forks or one-off scripts, limiting the ecosystem's growth.

### Constraints

- Must not break existing behavior: built-in connectors and `pip install acryl-datahub[source]` must work exactly as before.
- Must work with the standard Python packaging toolchain (pip, wheels, venv).
- Must support private/enterprise plugins behind authentication.

## Requirements

- **R1: Direct installation.** Plugins are installed directly into the current Python environment via `uv pip install` (preferred) or `pip install` (fallback). Discovery uses `importlib.metadata` to find installed plugin packages.
- **R2: GitHub-native distribution.** Authors publish wheels as GitHub Release assets. Users install via `datahub plugin install github:owner/repo`.
- **R3: Metadata-based discovery.** Installed plugins are discovered by scanning `importlib.metadata.distributions()` for packages containing a `datahub-plugin.yaml` manifest. No lockfile is needed.
- **R4: Transparent registry integration.** Installed plugins appear as normal source/sink/transformer types in recipes. No special syntax or configuration is needed beyond `type: plugin-name`.
- **R5: CLI for lifecycle management.** Commands for install, uninstall, list, update, search, info, init (scaffold), and validate.
- **R6: Community and enterprise registries.** A JSON index of available plugins, fetchable from public or private URLs with optional authentication.
- **R7: Developer scaffolding.** `datahub plugin init` generates a complete, release-ready project with source stubs, tests, manifest, and GitHub Actions CI/CD.

### Extensibility

- **Registry types.** The plugin system defines `source`, `sink`, and `transformer` capability types. New types can be added by extending the `PluginCapabilityType` enum.
- **Registry backends.** The `RegistryClient` fetches JSON indexes from URLs. Alternative backends (S3, Artifactory, database) can be added by extending the client.
- **Loader strategy.** The `PluginLoader` class can be subclassed to support alternative isolation strategies (e.g., subprocess execution, containers) in the future.

## Non-Requirements

- **Runtime sandboxing.** Plugins execute in the same process as DataHub. We do not sandbox or restrict their system access. This is consistent with pip packages and entry-point-based plugins.
- **Automatic conflict resolution.** If two plugins have conflicting transitive dependencies and are loaded in the same pipeline, we do not attempt to resolve this. We warn at install time.
- **Windows support.** The initial implementation targets Linux and macOS. Windows compatibility is future work.

## Detailed design

### End-to-End Workflow

The plugin system supports a complete lifecycle from authoring a custom connector through running it in production. The five stages are: **Create → Build → Deploy → Install → Run**.

```
 ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
 │  CREATE   │───▶│  BUILD   │───▶│  DEPLOY  │───▶│ INSTALL  │───▶│   RUN    │
 │           │    │          │    │          │    │          │    │          │
 │ scaffold  │    │ wheel    │    │ GitHub   │    │ CLI or   │    │ recipe   │
 │ implement │    │ validate │    │ Release  │    │ UI       │    │ execute  │
 │ test      │    │          │    │ Registry │    │          │    │          │
 └──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
```

#### Stage 1: Create

Scaffold a new plugin project with `datahub plugin init`:

```shell
datahub plugin init --type=source my-salesforce-source
cd my-salesforce-source
```

This generates a complete project structure:

```
my-salesforce-source/
    pyproject.toml                          # Build config with DataHub dependency
    README.md
    LICENSE
    src/
        my_salesforce_source/
            __init__.py
            source.py                       # Source stub extending datahub Source
            config.py                       # Pydantic config class
            datahub-plugin.yaml             # Plugin manifest (bundled as package data)
    tests/
        __init__.py
        test_source.py
    .github/
        workflows/
            release.yml                     # Builds wheel + attaches to GitHub Release
            test.yml                        # Runs tests on PR/push
```

Implement the connector by editing `source.py`. The generated stub extends `datahub.ingestion.api.source.Source` with the standard DataHub connector interface:

```python
class MySalesforceSource(Source):
    def __init__(self, config: MySalesforceSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MySalesforceSource":
        config = MySalesforceSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self):
        # Emit MetadataWorkUnit objects here
        yield from []
```

Edit the `datahub-plugin.yaml` manifest to declare the plugin's identity:

```yaml
api_version: datahub/v1
id: my-salesforce-source
name: My Salesforce Source
type: source
entry_point: my_salesforce_source.source:MySalesforceSource
config_class: my_salesforce_source.config:MySalesforceSourceConfig
description: Ingest metadata from Salesforce CRM
author: Your Name
url: https://github.com/yourname/my-salesforce-source
compatibility:
  datahub_min: "0.12.0"
  python_min: "3.9"
```

Test locally with an editable install:

```shell
pip install -e .
datahub plugin list                      # Verify the plugin appears
datahub ingest -c test-recipe.yaml       # Run against a test environment
```

#### Stage 2: Build

Validate the plugin project before publishing:

```shell
datahub plugin validate .
```

This checks that:

- `datahub-plugin.yaml` schema is valid
- `pyproject.toml` has correct entry point configuration
- Declared import paths resolve
- The wheel builds successfully

Build the wheel manually (or let CI do it):

```shell
pip install build
python -m build --wheel
# Output: dist/my_salesforce_source-0.1.0-py3-none-any.whl
```

#### Stage 3: Deploy

Deploy means publishing the wheel where users can install it from. There are three options:

**Option A: GitHub Release (recommended).** The scaffolded `.github/workflows/release.yml` automates this. Push a version tag and the workflow builds a wheel and attaches it to the GitHub Release:

```shell
git tag v0.1.0
git push --tags
# GitHub Actions builds the wheel and attaches it to the release
```

Users can then install with:

```shell
datahub plugin install github:yourname/my-salesforce-source
```

**Option B: Plugin registry.** Submit the plugin to the community registry (or an enterprise registry) by adding an entry to the registry's `index.json`:

```json
{
  "id": "my-salesforce-source",
  "repo": "yourname/my-salesforce-source",
  "version": "0.1.0",
  "type": "source",
  "description": "Ingest metadata from Salesforce CRM",
  "author": "Your Name"
}
```

Users can then discover it via `datahub plugin search salesforce` and install by plugin ID.

**Option C: Direct distribution.** Share the `.whl` file directly (internal artifact store, S3, email). Users install with:

```shell
datahub plugin install ./my_salesforce_source-0.1.0-py3-none-any.whl
```

#### Stage 4: Install

See [Installation: CLI and UI](#installation-cli-and-ui) below for the full details on both installation paths.

#### Stage 5: Run

Once installed, the plugin works transparently in any recipe. No special syntax is needed:

```yaml
source:
  type: my-salesforce-source
  config:
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    instance_url: https://mycompany.salesforce.com
```

```shell
datahub ingest -c recipe.yaml
```

At runtime, when the `PluginRegistry` encounters a type that is not built-in, it falls back to the `PluginLoader`, which scans installed packages via `importlib.metadata` for a matching plugin manifest and imports the connector class. From that point on, the plugin behaves identically to a built-in connector.

If the type is not installed, the pipeline fails with a clear error message suggesting how to install it.

---

### Installation: CLI and UI

Plugins can be installed through two paths depending on how DataHub ingestion is run.

#### CLI Installation (local ingestion)

For users running ingestion from the command line, plugins are installed directly into the local Python environment:

```shell
# From a GitHub repository (latest release)
datahub plugin install github:acme/datahub-salesforce-source

# Specific version
datahub plugin install github:acme/datahub-salesforce-source@v1.2.0

# From a local wheel file
datahub plugin install ./datahub_salesforce_source-1.0.0-py3-none-any.whl

# From pip (PyPI or private index)
datahub plugin install my-datahub-plugin==2.0.0
```

The CLI install flow:

```
datahub plugin install github:acme/datahub-salesforce-source@v1.2.0
  │
  ├─ 1. Parse spec → extract owner, repo, version
  │
  ├─ 2. Resolve → GitHub Releases API → find .whl asset URL
  │
  ├─ 3. Download → fetch wheel via GitHub API (supports private repos with GITHUB_TOKEN)
  │
  ├─ 4. Install → uv pip install <wheel> (falls back to pip)
  │
  └─ 5. Verify → find datahub-plugin.yaml via importlib.metadata
```

After installation, manage plugins with:

```shell
datahub plugin list                          # Show installed plugins
datahub plugin info <plugin-id>              # Detailed metadata
datahub plugin uninstall <plugin-id>         # Remove a plugin
datahub plugin search <query>                # Search registries
```

#### UI Installation (managed ingestion)

For managed ingestion (DataHub UI), the plugin must be available in the executor's Python environment before the ingestion run starts. We add a new dedicated field for external plugins, separate from the existing built-in extras field.

**What exists today.** The ingestion UI has two advanced fields that affect the executor environment:

- **Extra DataHub plugins** (`extra_pip_plugins`) — Installs built-in DataHub extras. The executor builds a requirement like `acryl-datahub[main_plugin,extra1,extra2]` and runs `pip install` on it. This only works for extras bundled in the `acryl-datahub` package (e.g., `["debug"]` → `acryl-datahub[debug]`). It does **not** support `github:` specs, wheel URLs, or external plugin packages.

- **Extra pip requirements** (`extra_pip_requirements`) — Runs raw `pip install` for arbitrary pip specs (e.g., `["sqlparse==0.4.3"]`).

Both fields are passed as `extraArgs` in the ingestion source config and forwarded to the executor via: GraphQL → GMS aspect (`DataHubIngestionSourceConfig.extraArgs`) → Kafka MCL → `executor_action.py` → `acryl.executor.SubProcessIngestionTask`.

**New field: `datahub_plugins`.** We add a new `extraArgs` key called `datahub_plugins` with a dedicated UI field. This field accepts external plugin specs that the executor installs via `datahub plugin install` before each ingestion run:

```
┌─────────────────────────────────────────────────────┐
│ Create Ingestion Source                             │
│                                                     │
│ Source Type:  [Custom]                              │
│ Recipe:       (yaml editor)                         │
│                                                     │
│ ▶ Advanced                                          │
│   External Plugins:                                 │
│   ┌─────────────────────────────────────────────┐   │
│   │ ["github:acme/datahub-salesforce-source@v1… │   │
│   └─────────────────────────────────────────────┘   │
│   Extra DataHub plugins:                            │
│   ┌─────────────────────────────────────────────┐   │
│   │ ["debug"]                                   │   │
│   └─────────────────────────────────────────────┘   │
│   Extra pip requirements:                           │
│   ┌─────────────────────────────────────────────┐   │
│   │                                             │   │
│   └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

The field accepts a JSON array of plugin specs — the same formats as the CLI:

```json
[
  "github:acme/datahub-salesforce-source@v1.0",
  "github:internal/custom-transformer@v2.1"
]
```

**Changes required across the stack:**

| Layer                 | File(s)                                     | Change                                                                                                                                                  |
| --------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **UI**                | `AdvancedSection.tsx`, `NameSourceStep.tsx` | Add new form field with key `datahub_plugins`, label "External Plugins", placeholder showing `github:` spec format                                      |
| **GraphQL**           | No schema change needed                     | `datahub_plugins` is just another `extraArgs` entry, flows through the existing `StringMapEntryInput` mechanism                                         |
| **GMS**               | No change needed                            | `extraArgs` is `map[string, string]` — the new key is stored alongside existing ones                                                                    |
| **Execution request** | No change needed                            | `arguments.putAll(extraArgs)` already forwards all keys to the executor                                                                                 |
| **Executor**          | `runner.py`, `VenvConfig`                   | Add `datahub_plugins: list[str]` field. In `setup_venv()`, after installing `acryl-datahub[extras]`, run `datahub plugin install <spec>` for each entry |

**Executor integration detail.** The executor's `VenvConfig` adds a new field:

```python
class VenvConfig(VenvConfigMixin, pydantic.BaseModel):
    version: str = "latest"
    main_plugin: str | None = None
    extra_pip_requirements: list[str] = []
    extra_pip_plugins: list[str] = []       # Built-in extras → acryl-datahub[extra]
    datahub_plugins: list[str] = []         # External plugins → datahub plugin install <spec>
    extra_env_vars: dict = {}
```

During venv setup, after the existing `pip install acryl-datahub[main,extras]` step, the executor runs:

```python
for spec in venv_config.datahub_plugins:
    await runner.run(["datahub", "plugin", "install", spec])
```

This runs the full plugin install flow (GitHub resolution, wheel download, pip install, manifest verification) inside the executor's venv. The existing `extra_pip_plugins` field continues to work unchanged for built-in extras.

**Data flow:**

```
UI: "External Plugins" field
  → extraArgs: [{ key: "datahub_plugins", value: '["github:acme/salesforce-source@v1.0"]' }]
  → GraphQL mutation: updateIngestionSource

GMS: persists in DataHubIngestionSourceConfig.extraArgs

Execution request: arguments.putAll(extraArgs)
  → args: { "datahub_plugins": '["github:acme/salesforce-source@v1.0"]', ... }

Executor: SubProcessIngestionTask
  → VenvConfig.datahub_plugins = ["github:acme/salesforce-source@v1.0"]
  → setup_venv():
      1. pip install acryl-datahub[main_plugin,extra_pip_plugins]  (existing)
      2. datahub plugin install github:acme/salesforce-source@v1.0  (NEW)
  → run_ingest.sh recipe.yaml
```

**Alternative: pre-installed plugins (operator-managed).** For deployments with persistent executor environments, the operator can pre-install plugins once instead of per-run:

```shell
# In the executor Dockerfile or on the executor host
datahub plugin install github:acme/datahub-salesforce-source@v1.0.0
```

All ingestion runs in that environment can then use `type: salesforce-source` without any per-run installation.

**Authentication.** For private GitHub repositories, the `GITHUB_TOKEN` environment variable provides authentication. The executor operator configures this in the execution environment. The UI can pass it via the "Extra Environment Variables" field if needed. For private registries, the `token_env` field in the registry config names the environment variable holding the auth token.

---

### Plugin Marketplace

A plugin marketplace in the DataHub web UI would let users browse, install, and manage plugins without using the CLI. This section outlines the design direction and the incremental steps to get there.

#### Incremental path

The marketplace can be built incrementally, each step delivering standalone value:

**Step 1: `datahub_plugins` field + executor support.** The dedicated "External Plugins" UI field (described above) and executor integration to call `datahub plugin install`. Users manually enter `github:` specs. This delivers end-to-end UI-driven external plugin installation.

**Step 2: Registry search in the UI.** Add a read-only GraphQL query that proxies the `RegistryClient.search()` call through GMS. The ingestion source type picker shows external plugins from the registry alongside built-in connectors, with a badge indicating trust tier (community/verified/official). When a user selects an external plugin, the UI automatically populates the `datahub_plugins` field with the correct `github:` spec.

**Step 3: Dedicated plugin management page.** A full marketplace page under Settings or Ingestion with:

1. **Browse** — Search and filter plugins from all configured registries. Each plugin card shows name, description, author, type, version, trust tier, and an Install button.
2. **Installed** — List all plugins installed in the managed ingestion environment. Shows version, update availability, and an Uninstall button.
3. **Plugin detail** — Full metadata, README, compatibility info, and installation history.

#### Architecture (Step 3)

The full marketplace requires a backend API to bridge the UI with the Python plugin system:

```
┌──────────────┐     GraphQL      ┌──────────────┐     REST/exec     ┌──────────────┐
│   React UI   │ ───────────────▶ │     GMS      │ ────────────────▶ │  Ingestion   │
│  (frontend)  │                  │  (backend)   │                   │  Executor    │
│              │ ◀─────────────── │              │ ◀──────────────── │  (Python)    │
│  Browse      │   Plugin types,  │  GraphQL     │  Install result,  │  datahub     │
│  Install     │   install status │  resolvers   │  plugin list      │  plugin      │
│  Manage      │                  │              │                   │  install     │
└──────────────┘                  └──────────────┘                   └──────────────┘
```

**GraphQL API.** New types and operations for the frontend:

```graphql
type Plugin {
  id: String!
  name: String!
  type: PluginCapabilityType! # SOURCE, SINK, TRANSFORMER
  version: String!
  description: String
  author: String
  trustTier: String # community, verified, official
  installed: Boolean!
  installedVersion: String
}

type Query {
  listInstalledPlugins: [Plugin!]!
  searchPlugins(query: String!, type: PluginCapabilityType): [Plugin!]!
}

type Mutation {
  installPlugin(spec: String!, version: String): Plugin!
  uninstallPlugin(id: String!): Boolean!
}
```

**Backend implementation.** Two approaches for the GMS-to-executor communication:

| Approach              | How it works                                                                                | Pros                                                   | Cons                               |
| --------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------ | ---------------------------------- |
| **Executor API**      | GMS calls a REST endpoint on the ingestion executor that wraps `PluginManager`              | Clean separation, executor manages its own environment | Requires a new API on the executor |
| **Execution request** | GMS emits a `MANAGE_PLUGIN` execution request (like `RUN_INGEST`) that the executor handles | Reuses existing MCL/Kafka infrastructure               | Async — UI must poll for result    |

**Registry proxy.** The GMS backend proxies registry search requests so the frontend doesn't call external registry URLs directly. This allows the backend to handle authentication, caching, and rate limiting centrally.

---

### Package Structure

All new code lives under `src/datahub/plugin/`:

```
src/datahub/plugin/
    __init__.py
    plugin_config.py       # Pydantic models: manifest, discovered plugin, registry config
    plugin_manager.py      # Install/uninstall/list orchestration + importlib.metadata discovery
    plugin_loader.py       # Load plugin classes into PluginRegistry via importlib.metadata
    github_resolver.py     # Resolve github:owner/repo to downloadable wheel URL
    registry_client.py     # Fetch and search community/enterprise plugin indexes
    scaffold.py            # Generate new plugin project skeleton
```

CLI commands are in `src/datahub/cli/plugin_cli.py`.

### Plugin Manifest

Every plugin package includes a `datahub-plugin.yaml` inside the Python package directory (not the project root), so that it is bundled as package data in the wheel and discoverable via `importlib.metadata` after installation:

```yaml
api_version: datahub/v1
id: salesforce-source
name: Salesforce Source
type: source
entry_point: salesforce_source.source:SalesforceSource
config_class: salesforce_source.config:SalesforceConfig
description: Ingest metadata from Salesforce CRM
author: Acme Corp
url: https://github.com/acme/datahub-salesforce-source
compatibility:
  datahub_min: "0.12.0"
  python_min: "3.9"
```

The manifest is parsed into a `PluginManifest` Pydantic model with validation:

- `id` must be non-empty (lowercase letters, digits, and hyphens)
- `entry_point` must contain `:` or `.` (a valid Python import path)
- `type` must be one of `source`, `sink`, `transformer`

The `id` field is the key used in recipe `type:` fields. The optional `url` field links to documentation or the project homepage and is shown to users after install and in `datahub plugin list`.

### Plugin Discovery

Installed plugins are discovered via `importlib.metadata.distributions()`. The `discover_plugins()` function scans all installed packages in the current Python environment, looking for those that contain a `datahub-plugin.yaml` manifest file. This approach:

- **Requires no lockfile.** The Python packaging system is the single source of truth.
- **Works per-venv automatically.** Each virtual environment has its own set of installed packages, so `importlib.metadata` naturally returns only the plugins installed in that environment.
- **Is robust.** No state file can become corrupted or out of sync with reality.
- **Supports editable installs.** For `pip install -e .` during development, the discovery function uses a two-strategy approach: first scanning `dist.files` (works for normal installs), then falling back to `importlib.util.find_spec()` via `top_level.txt` (works for editable installs where `dist.files` only contains the `.pth` redirect).

The `PluginLoader` singleton caches the discovery result and exposes a `reload()` method to force re-scanning after an install.

### Runtime Loading

The `PluginLoader` bridges `importlib.metadata` discovery with DataHub's `PluginRegistry`:

```python
class PluginLoader:
    def try_load(self, key: str, registry_type: str) -> Optional[Type]:
        """If a plugin provides this key, import and return the class."""
        plugin = self.plugins.get(key)
        if plugin is not None and plugin.manifest.type.value == registry_type:
            return import_path(plugin.manifest.entry_point)
        return None
```

The change to `PluginRegistry` is minimal — a fallback hook in the `get()` method:

```python
# In PluginRegistry.get(), before raising KeyError:
if key not in self._mapping:
    if self._plugin_loader is not None:
        cls = self._plugin_loader.try_load(key, self._registry_type)
        if cls is not None:
            self._register(key, cls, override=True)
            return cls
    raise KeyError(...)
```

Since plugins are installed in the current environment, no `sys.path` manipulation is needed. The entry point from the manifest is imported directly. If a plugin is installed but fails to import, the error is surfaced as a `ConfigurationError` with the real exception message, not a generic "not found" error.

### GitHub Resolver

The `GitHubResolver` parses specs matching `github:owner/repo[@version]`:

1. Calls `GET /repos/{owner}/{repo}/releases` (or `/releases/tags/{tag}` for specific versions)
2. Finds the first `.whl` asset in the release
3. For wheel assets, `download_wheel()` downloads the file locally using the GitHub API asset URL with `Accept: application/octet-stream` — this supports both public and private repositories
4. Returns the local wheel path to pip/uv for installation

For private repositories, the `GITHUB_TOKEN` environment variable provides authentication. The download uses the GitHub API URL (not `browser_download_url`) so that the auth token works correctly for private repo assets. If no wheel is found, falls back to `git+https://github.com/{owner}/{repo}@{tag}`.

### Community and Enterprise Registries

A JSON index at a configurable URL lists available plugins:

```json
{
  "plugins": [
    {
      "id": "salesforce-source",
      "name": "Salesforce Source",
      "description": "Ingest metadata from Salesforce CRM",
      "type": "source",
      "repo": "acme/datahub-salesforce-source",
      "version": "1.2.0",
      "author": "Acme Corp",
      "trust_tier": "verified"
    }
  ]
}
```

The `RegistryClient` fetches and caches this index locally (1-hour TTL at `~/.datahub/plugins/.index_cache/`). Multiple registries can be configured with priority ordering and optional authentication:

```yaml
# ~/.datahub/plugins/config.yaml
registries:
  - name: community
    url: https://raw.githubusercontent.com/datahub-project/datahub-plugins/main/index.json
    enabled: true
  - name: acme-internal
    url: https://git.internal.acme.com/raw/datahub-plugins/main/index.json
    auth_type: bearer
    token_env: ACME_GIT_TOKEN
```

## How we teach this

### Naming

- **Plugin** — A third-party connector package distributed independently of core DataHub.
- **Plugin manifest** — The `datahub-plugin.yaml` file declaring a plugin's identity and capabilities.
- **Plugin registry** — A remote JSON index listing available plugins (analogous to npm registry).
- **Plugin marketplace** — The UI in the DataHub web app for browsing and installing plugins (future).

### Audience

- **Plugin users** (data engineers) need to know: install via CLI (`datahub plugin install`) or via the UI (Extra DataHub plugins field), and that installed plugins "just work" in recipes.
- **Plugin authors** (connector developers) need to know: `datahub plugin init`, the manifest format, and the GitHub Release workflow.
- **Platform operators** need to know: enterprise registry configuration, authentication, executor setup, and (future) marketplace deployment.

### Documentation

- A new `docs/managed-ingestion-plugins.md` page covers user-facing documentation.
- The `datahub plugin init` scaffolding includes a README with publishing instructions.
- `datahub plugin --help` and subcommand help text provides inline guidance.

This is a new concept rather than a continuation of existing patterns. The existing `docs/plugins.md` covers Java GMS auth/authz plugins — a completely separate system. The new docs should be clearly distinguished.

## Drawbacks

1. **Process-level isolation only.** Plugins share the same Python process and environment. A plugin with a segfault-inducing C extension or a dependency conflict will crash the entire pipeline. True isolation would require subprocess execution or containers, adding significant complexity.

2. **Shared dependency tree.** Since plugins are installed directly into the current environment, dependency conflicts between plugins (or between plugins and core DataHub) are resolved by pip at install time. This means the last-installed version wins if there's a conflict.

3. **Discovery scan cost.** `discover_plugins()` scans all installed distributions via `importlib.metadata`. This is acceptable for CLI commands and cached by the `PluginLoader` singleton at runtime, but could become noticeable with hundreds of installed packages.

4. **GitHub-centric.** The primary distribution mechanism assumes GitHub Releases. Organizations using GitLab, Bitbucket, or internal artifact stores need additional resolver implementations or must use the generic pip spec format.

## Alternatives

### Alternative 1: Entry-point-only approach

Require plugin authors to publish to PyPI and register via `importlib.metadata` entry points, as DataHub already supports.

**Rejected because:** This requires every plugin author to publish to PyPI (high friction) and doesn't support GitHub-native distribution or private enterprise registries.

### Alternative 2: Subprocess execution

Run each plugin in a separate Python process, communicating via pipes or sockets.

**Rejected because:** This adds significant complexity (serialization, IPC protocol, error propagation) and latency. The `Source` interface yields `MetadataWorkUnit` objects that contain complex nested Avro/Pydantic models — serializing these across process boundaries is non-trivial.

### Alternative 3: Docker-based plugins

Run each plugin in its own container.

**Rejected because:** This requires Docker as a dependency, adds seconds of startup latency per plugin, and complicates the development/debugging experience. It also doesn't work well for local development or CLI-based workflows.

### Prior art

We investigated three plugin/extension systems in depth and drew ideas from each:

**Apache Airflow Providers** — The closest analog in the Python data ecosystem. Airflow providers are separate PyPI packages (`apache-airflow-providers-google`, etc.) that bundle operators, hooks, sensors, and connections for a specific platform. They are discovered at runtime via `importlib.metadata` entry points, and "just work" in DAGs once installed — users write `PostgresOperator()` without caring whether it's built-in or from a provider package.

- _Adopted:_ `importlib.metadata`-based discovery as the core mechanism — no lockfile, no registration step, the Python packaging system is the source of truth. Separate packages installed into the same Python environment. Transparent runtime loading where plugins appear as first-class types in recipes. The provider manifest concept (Airflow uses `provider.yaml`) → our `datahub-plugin.yaml`. The pattern of lazy loading where providers are only imported when actually used.
- _Diverged:_ Airflow providers must be published to PyPI, which is high friction for internal connectors. We added GitHub Releases as a first-class distribution channel. Airflow has no CLI for provider lifecycle management (users run `pip install` directly) — we added `datahub plugin install/uninstall/search` to abstract over the resolution and installation details. Airflow has no community registry or search — discovery is through the Airflow docs or PyPI search.

**Airbyte Connectors** — Airbyte takes a fundamentally different approach: each connector runs as a separate Docker container communicating via a standardized JSON protocol over stdin/stdout. Connectors are distributed as Docker images on Docker Hub, discovered via a connector catalog (a JSON file in the Airbyte repo), and managed through the Airbyte UI or `octavia` CLI.

- _Adopted:_ The connector catalog concept (a centralized JSON index listing available connectors with metadata) → our community registry `index.json`. The idea that connectors can be built and published independently of the core platform. The scaffolding/code generation approach (`airbyte-cdk init` generates a connector project) → our `datahub plugin init`. The connector manifest with structured metadata (name, type, version, Docker image) → our plugin manifest with structured fields.
- _Diverged:_ Airbyte's Docker-per-connector model provides strong isolation but adds significant overhead (seconds of startup, Docker as a dependency, complex local development). We chose in-process loading for simplicity and performance — DataHub's `Source` interface yields complex `MetadataWorkUnit` objects with nested Avro/Pydantic models that are expensive to serialize across process boundaries. Airbyte's protocol requires all data to flow through JSON serialization/deserialization, which we avoid. Airbyte manages connectors centrally in the platform — we keep plugin installation as a Python environment concern, making it work naturally with venvs, CI, and containerized deployments.

**Claude Code Plugin System** — Anthropic's Claude Code uses a plugin system where plugins are directories containing a `plugin.json` manifest, commands (markdown files with frontmatter), agents, skills, and hooks. Plugins can be installed from GitHub repositories or local paths, and are discovered by scanning the `~/.claude/plugins/` directory.

- _Adopted:_ The manifest-driven approach (`plugin.json` → our `datahub-plugin.yaml`) where a single declarative file describes what the plugin provides. The GitHub-native installation (`claude plugins install owner/repo`) → our `datahub plugin install github:owner/repo`. The scaffolding workflow for new plugins. The concept of a plugin declaring its capabilities (commands, agents → our source, sink, transformer types). The pattern of plugins being discovered at runtime and loaded on demand.
- _Diverged:_ Claude Code plugins are file-based (markdown files, shell scripts) rather than Python packages — they don't need pip or a package manager. Our plugins are standard Python wheels that integrate with the Python packaging ecosystem. Claude Code plugins run in isolated subprocesses, while ours run in-process. Claude Code's plugin system is primarily for extending a CLI tool's behavior (commands, hooks), while ours extends a data processing framework (connectors with a specific Source/Sink/Transformer interface).

## Rollout / Adoption Strategy

This is a purely additive feature with no breaking changes:

1. **Existing users** are unaffected. Built-in connectors, `pip install acryl-datahub[source]` extras, and entry-point-based third-party packages all continue to work. The plugin loader is only consulted as a fallback when a key is not found in the built-in registry.

2. **Adoption is opt-in.** Users choose to install plugins via the new CLI. There is no automatic migration of existing third-party packages.

3. **Rollout phases:**
   - **Phase 1:** Core runtime, CLI, GitHub resolver. Users can install plugins from GitHub or local wheels. **Implemented.**
   - **Phase 2:** Community registry index, search, and enterprise registry support. **Implemented.**
   - **Phase 3:** Developer scaffolding (`datahub plugin init`), validation tooling (`datahub plugin validate`), and authoring guide. **Implemented.**
   - **Phase 4:** `datahub_plugins` UI field + executor integration. Add a dedicated "External Plugins" field in the ingestion source advanced settings, and update the executor to call `datahub plugin install` for each entry during venv setup.
   - **Phase 5:** Registry search in the UI. Add a GraphQL query proxying registry search so the ingestion source type picker shows external plugins alongside built-in connectors, auto-populating the `datahub_plugins` field.
   - **Phase 6 (future):** Full marketplace UI with browse, install, and manage pages backed by a GraphQL API.

## Future Work

- **Plugin sandboxing:** Runtime restrictions on plugin capabilities (network access, filesystem access) using Python's audit hooks or seccomp.
- **Conflict detection:** At install time, compare dependency trees across plugins and warn about version mismatches.
- **Auto-update:** Periodic checks for newer plugin versions with optional auto-update.
- **Checksum verification:** Verify downloaded wheel SHA-256 against the registry index at install time, and optionally re-verify at load time.
- **GitLab/Bitbucket resolvers:** Additional resolver implementations for non-GitHub hosting.

## Unresolved questions

1. **Naming convention for plugin IDs.** Should we enforce a naming convention (e.g., `<system>-source`, `<system>-sink`) or allow freeform IDs? The current implementation enforces `^[a-z][a-z0-9-]*$`.

2. **Plugin signing.** Should we support GPG or Sigstore signing for plugin wheels? This would add a trust layer but increases complexity for plugin authors.

3. **Maximum plugin count.** At what point do many installed plugins cause performance issues (e.g., import time, dependency conflicts)? We should benchmark with 10-20 plugins installed simultaneously.

4. **Executor integration.** The new `datahub_plugins` field requires the executor to call `datahub plugin install` during venv setup. This requires changes to the `acryl.executor` package (`VenvConfig`, `setup_venv`). Should the `datahub plugin install` step happen before or after the main `pip install acryl-datahub[extras]` step? Before is safer (plugin dependencies are resolved first), but after ensures the DataHub framework is available for manifest validation.

5. **Trust tiers for marketplace.** How should the `trust_tier` field (community/verified/official) be governed? Who verifies plugins, and what does verification entail?
