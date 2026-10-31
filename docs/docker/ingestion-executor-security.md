
# Ingestion executor security and hardening

The **ingestion executor** runs `datahub ingest` for recipes submitted from DataHub (UI, GraphQL, or OpenAPI). Treat that as **code execution on the executor**: recipe fields can import Python (custom source and transformer types, Kafka `oauth_cb`, schema registry classes) and, on non-locked images, install extra packages (`extra_pip_requirements`). Anyone with **Manage Metadata Ingestion** (included in the **Admin** role) — or **Edit** plus **Execute** on an ingestion source — controls what the executor runs. Grant those privileges only to operators you trust with that access. Combine that with **least privilege** for secrets and executor-related tokens.

The executor Action authenticates to GMS as a **trusted ingestion worker** (system client credentials in OSS quickstart via `DATAHUB_SYSTEM_CLIENT_ID` / `DATAHUB_SYSTEM_CLIENT_SECRET` in `executor.yaml`). With the default `SECRET_SERVICE_CALLER_GUARD_MODE=ENFORCE`, browser sessions and user PATs cannot call `getSecretValues`; **datahub-actions** can. On DataHub Cloud, the equivalent worker is an **embedded executor** (Remote Executor). See [Secret security considerations](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md#secret-security-considerations).

## Locked (`*-locked`) images

**Locked** variants of **`datahub-actions`** (tags such as **`*-locked`**) are intended for deployments that rely on **pre-built bundled virtual environments** under **`DATAHUB_BUNDLED_VENV_PATH`** (default **`/opt/datahub/venvs`**) instead of installing connectors at container runtime. In those images, **`pip`** and **`uv`** are removed from the runtime image, and default package index URLs are set to unusable localhost endpoints as defense in depth—see the **`final-locked`** stage in **`docker/datahub-actions/Dockerfile`** in the repository.

**Tradeoff:** You cannot extend a locked image at runtime with arbitrary **`pip install`**. Add connectors by **rebuilding** the image with the bundled venv builder and variables such as **`BUNDLED_VENV_PLUGINS`** and **`BUNDLED_CLI_VERSION`**. Do not use locked images as the base for “append plugins via Dockerfile **RUN** pip” flows that expect a package manager in the final layer; see [Bundled ingestion virtual environments](/docs/docker/bundled-ingestion-venvs.md).

Remote Executor (**`datahub-executor`**) images follow the same bundled-venv contract where applicable; deployment details: [Configuring Remote Executor](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md).

## Package index control (private mirror / PyPI proxy)

Even when connectors are resolved from PyPI, **controlling which indexes the environment can use** reduces supply-chain risk: route installs through an **internal mirror** (for example Artifactory, Nexus, or DevPI), allow only approved packages, and restrict **outbound egress** so executors cannot reach arbitrary third-party index URLs.

When **building** custom images from the DataHub Dockerfiles, you can steer installs at build time using mirror-related build arguments and environment variables set on the Python base image (for example **`PIP_MIRROR_URL`**, **`PIP_EXTRA_INDEX_URL`**, and **`UV_INDEX_URL`** in **`docker/datahub-actions/Dockerfile`**). Align runtime or orchestration-level networking with the same policy so executor workloads cannot bypass your mirror.

Non-locked images may still expose package managers at runtime; **locked** images aim to avoid relying on runtime installers altogether. Operational controls (mirror, egress, RBAC) remain **defense in depth** and should not replace reviewing trust boundaries above.

## Restricting `extra_pip_requirements`

A recipe's **`extra_pip_requirements`** (the "Extra Pip Requirements" field under **Advanced** in the UI) installs additional packages into the run's virtual environment on non-locked images. Installing a package runs its build scripts, so a typosquatted or malicious package is code execution on the executor. The executor always rejects entries that are not a single package specification: requirements-file options such as **`--index-url`**, **`--extra-index-url`**, **`--find-links`**, **`-r`**, **`-e`** or **`--hash`**, and entries containing a line break. A recipe's extra environment variables are never applied to the install step; they reach only the ingestion run itself. The package index, uv's configuration file (found through **`HOME`** / **`XDG_CONFIG_HOME`**) and TLS trust (**`SSL_CERT_FILE`**) therefore always come from the executor's own environment, and names starting with **`UV_`** or **`PIP_`** are rejected in a recipe outright. To route installs through a private index, set **`UV_INDEX_URL`** / **`UV_EXTRA_INDEX_URL`** in the executor's environment rather than in a recipe.

Two environment variables on the executor (the **`datahub-actions`** container, or the Remote Executor) restrict the feature further:

| Variable                                     | Default | Effect                                                                                                                                           |
| -------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `INGESTION_EXTRA_PIP_REQUIREMENTS_ENABLED`   | `true`  | `false` fails any run whose recipe sets a non-empty `extra_pip_requirements`. Use this with images that already bundle every connector you need. |
| `INGESTION_EXTRA_PIP_REQUIREMENTS_ALLOWLIST` | unset   | When set, every `extra_pip_requirements` entry must match one of the listed entries; runs with any other entry fail.                             |

The allowlist is a comma-separated list, or a JSON array when an entry itself contains a comma. Each entry is matched after any `${VAR}` references in the recipe have been expanded:

- A bare package name (`pandas`) allows any version and any extras of that package, but not a direct reference such as `pandas @ https://...`.
- A name with a version specifier (`requests==2.32.5`, `pkg>=1,<2`) allows only that exact specifier.
- A direct reference or local path (`my-plugin @ https://pypi.example.com/my_plugin-1.0-py3-none-any.whl`, `my-plugin@/opt/plugins/my-plugin`, `/opt/plugins/my-plugin`) allows only that same URL or path.

```bash
INGESTION_EXTRA_PIP_REQUIREMENTS_ALLOWLIST='["pandas", "requests==2.32.5", "pkg>=1,<2"]'
```

A rejected entry fails the run before anything is installed, and the run's logs name the entry. Package names are compared case-insensitively, as pip does.

## Related documentation

- [Bundled ingestion virtual environments](/docs/docker/bundled-ingestion-venvs.md)
- [Ingestion Executor](/docs/actions/actions/executor.md)
- [Configuring Remote Executor](/docs/managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md)
