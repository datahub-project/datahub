# Snowflake Agent Setup for DataHub

This module provides tools to generate and deploy Snowflake UDFs (User-Defined Functions) that enable Snowflake Cortex Intelligence to query DataHub metadata.

## Overview

The module generates SQL scripts to create:

1. Network rules and external access integrations for DataHub API calls
2. Python UDFs for searching and retrieving metadata from DataHub
3. Stored procedures for dynamic SQL execution
4. Cortex Agent configuration with DataHub tools

## Implementation

UDFs use the `datahub-agent-context` package wrapper methods that abstract away GraphQL/REST API complexity.

## Usage

### Basic Usage

```bash
python -m datahub.ai.snowflake.snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations
```

### Direct Execution

Add `--execute` and `--sf-password` to automatically run the generated scripts:

```bash
python -m datahub.ai.snowflake.snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-password YOUR_PASSWORD \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations \
  --execute
```

### Direct Execution with SSO

Use `--sf-authenticator=externalbrowser` for SSO authentication (no password required):

```bash
python -m datahub.ai.snowflake.snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-authenticator externalbrowser \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations \
  --execute
```

This will open your browser for SSO authentication. Ideal for organizations using SAML, Okta, or other identity providers configured with Snowflake.

## Options

| Option                  | Description                                                                                      | Default                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| `--sf-account`          | Snowflake account identifier                                                                     | Required                                            |
| `--sf-user`             | Snowflake user name                                                                              | Required                                            |
| `--sf-role`             | Snowflake role                                                                                   | Required                                            |
| `--sf-warehouse`        | Snowflake warehouse name                                                                         | Required                                            |
| `--sf-database`         | Snowflake database name                                                                          | Required                                            |
| `--sf-schema`           | Snowflake schema name                                                                            | Required                                            |
| `--datahub-url`         | DataHub instance URL                                                                             | Required                                            |
| `--datahub-token`       | DataHub Personal Access Token                                                                    | Required                                            |
| `--datahub-ips`         | DataHub IP addresses for network rule                                                            | `('52.7.66.10', '44.217.146.124', '34.193.80.100')` |
| `--agent-name`          | Agent name in Snowflake                                                                          | `DATAHUB_SQL_AGENT`                                 |
| `--agent-display-name`  | Agent display name in UI                                                                         | `DataHub SQL Assistant`                             |
| `--agent-color`         | Agent color in UI                                                                                | `blue`                                              |
| `--output-dir`          | Output directory for SQL files                                                                   | `./snowflake_setup`                                 |
| `--enable-mutations`    | Include mutation/write tools (tags, descriptions, owners, etc.)                                  | `True` (enabled)                                    |
| `--no-enable-mutations` | Disable mutation tools (read-only mode with 9 UDFs instead of 20)                                | N/A                                                 |
| `--execute`             | Execute scripts directly                                                                         | `False`                                             |
| `--sf-password`         | Snowflake password (required if --execute is used with `snowflake` authenticator)                | None                                                |
| `--sf-authenticator`    | Authentication method: `snowflake` (password), `externalbrowser` (SSO), or `oauth` (token-based) | `snowflake`                                         |

## Authentication Methods

Three authentication methods are supported when using `--execute`:

### 1. Password Authentication (Default)

Standard username/password authentication:

```bash
--execute --sf-password YOUR_PASSWORD
```

### 2. SSO / External Browser Authentication (Recommended)

Browser-based SSO authentication (SAML, Okta, Azure AD, etc.):

```bash
--execute --sf-authenticator externalbrowser
```

When using this method:

- A browser window will automatically open for authentication
- No password is required on the command line
- Ideal for organizations with federated identity providers
- Your Snowflake account must be configured for SSO

### 3. OAuth Authentication

Token-based OAuth authentication:

```bash
--execute --sf-authenticator oauth
```

Note: OAuth authentication requires additional token configuration in your environment.

## Read-Only vs Read+Write Modes

By default, the generator creates 20 UDFs including both read and write operations:

- **Read-only tools (9 UDFs)**: Search, retrieve metadata, lineage, queries, documents
- **Mutation tools (11 UDFs)**: Add/remove tags, update descriptions, manage owners, domains, glossary terms, and structured properties

Use `--no-enable-mutations` to generate only the 9 read-only UDFs for environments where metadata modifications should be restricted.

## Generated Files

1. **00_configuration.sql** - Configuration variables and secrets
2. **01_network_rules.sql** - Network rules and external access integration
3. **02_datahub_udfs.sql** - DataHub API UDFs (9 read-only or 20 read+write depending on `--enable-mutations`)
4. **03_stored_procedure.sql** - Dynamic SQL execution procedure
5. **04_cortex_agent.sql** - Cortex Agent definition

## Manual Execution

If not using `--execute`, run the generated SQL files in order:

```sql
-- 1. Set up configuration and secrets
@00_configuration.sql;

-- 2. Create network rules
@01_network_rules.sql;

-- 3. Create DataHub UDFs
@02_datahub_udfs.sql;

-- 4. Create stored procedure
@03_stored_procedure.sql;

-- 5. Create Cortex Agent
@04_cortex_agent.sql;
```

## Related Documentation

- [DataHub Agent Context Documentation](../../../datahub-agent-context/README.md)
- [Snowflake Python UDFs](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python)
- [Snowflake Cortex Intelligence](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-intelligence)
