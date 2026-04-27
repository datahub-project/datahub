### Overview

The `hex-v2` module ingests metadata from Hex into DataHub using the **Hex CLI** as its primary data source. It is a full superset of the `hex` connector and is the recommended choice for new installations.

The key difference from `hex` is lineage: `hex-v2` parses SQL directly from each project's YAML export and resolves upstream tables via the data connection's platform type. This produces lineage on the first run without any warehouse ingestion dependency.

### Prerequisites

#### Hex CLI

The connector requires the `hex` CLI binary (version **1.2.2**). By default, if the binary is not found on `PATH`, it is downloaded automatically from the [Hex CLI releases page](https://github.com/hex-inc/hex-cli/releases) and cached in `~/.datahub/tools/hex/`. No manual installation is required in most environments.

To disable auto-download and manage the binary yourself:

```yaml
source:
  type: hex-v2
  config:
    auto_install_hex_cli: false
    hex_cli_path: /path/to/hex
```

To verify the binary is present before running ingestion:

```bash
hex --version
# should output: hex 1.2.2
```

#### Workspace Name

Find your workspace name in your Hex home page URL:

```
https://app.hex.tech/<workspace_name>
```

Example: In `https://app.hex.tech/acryl-partnership`, the workspace name is `acryl-partnership`. Some workspaces use a UUID as their identifier — use whatever appears in the URL.

#### Authentication

Requires a Hex API token. The connector bootstraps CLI authentication automatically on startup using the configured `token`.

**Token options:**

- **Workspace Token** (recommended): Read-only token sufficient for all ingestion. Generate one in your Hex workspace settings under **API** → **Workspace Tokens**.
- **PAT (Personal Access Token)**: Ingests with the token owner's permissions.
