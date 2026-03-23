# DataHub Datapack Command

> **Experimental**: This command is under active development. Command surface and behavior may change in future releases.

The `datapack` command manages curated collections of metadata (data packs) that can be loaded into DataHub for demos, testing, or bootstrapping new instances. Data packs contain pre-built MCPs (Metadata Change Proposals) with datasets, dashboards, lineage, ownership, glossary terms, and more.

## Quick Start

```shell
# List available data packs
datahub datapack list

# Load the showcase-ecommerce pack (rich demo with 1000+ entities)
datahub datapack load showcase-ecommerce

# Remove it when done
datahub datapack unload showcase-ecommerce
```

## Commands

### list

List all available data packs from the registry.

```shell
datahub datapack list [--tag TAG] [--format table|json]
```

**Options:**

- `--tag` - Filter packs by tag (e.g., `demo`, `snowflake`, `lineage`)
- `--format` - Output format: `table` (default) or `json`

**Example:**

```shell
datahub datapack list --tag demo

# Name            Description                                Size     Trust     Tags
# bootstrap       Default DataHub bootstrap data...          ~100 KB  verified  demo, bootstrap
# showcase-ecommerce        Rich demo dataset with 1049 entities...    ~2.7 MB  verified  demo, rich, snowflake, ...
# covid-bigquery  COVID-19 BigQuery dataset with 215 MCEs   ~3.2 MB  verified  demo, bigquery, covid
```

### info

Show detailed information about a specific data pack.

```shell
datahub datapack info NAME
```

**Example:**

```shell
datahub datapack info showcase-ecommerce

# Name:            showcase-ecommerce
# Description:     Rich demo dataset with 1049 entities...
# URL:             https://raw.githubusercontent.com/datahub-project/static-assets/...
# Size:            ~2.7 MB
# Trust:           verified
# Tags:            demo, rich, snowflake, looker, powerbi, tableau, lineage, governance
# Reference time:  2025-07-08T16:15:42.552000+00:00
# Cached:          yes
# Loaded:          yes (run_id=datapack-showcase-ecommerce-..., at 2026-03-22T...)
```

### load

Download and load a data pack into DataHub.

```shell
datahub datapack load NAME [OPTIONS]
```

**Options:**

- `--url URL` - Load from an arbitrary URL instead of the registry. Supports `http://`, `https://`, and `file://` schemes.
- `--dry-run` - Preview what would be loaded without ingesting.
- `--no-cache` - Force re-download even if the pack is cached.
- `--force` - Override server version compatibility checks.
- `--as-of DATETIME` - Set the target time for time-shifting (default: current time). Useful for making historical data appear fresh.
- `--no-time-shift` - Load with original timestamps (skip time-shifting).
- `--trust-community` - Allow loading community-contributed packs.
- `--trust-custom` - Allow loading from unverified URLs.

**What happens during load:**

1. **Registry lookup** - Resolves the pack name to a URL
2. **Download & cache** - Downloads the MCP file (cached in `~/.datahub/datapack-cache/`)
3. **Schema downshift** - Queries the server's entity registry to filter out unsupported aspects (prevents errors from Cloud-only features on OSS)
4. **Referential integrity check** - Warns about dangling URN references
5. **Time-shifting** - Rebases timestamps so the data appears fresh
6. **Ingestion** - Runs the MCP file through the standard ingestion pipeline
7. **Load tracking** - Records the run ID for clean unload

**Examples:**

```shell
# Load the showcase-ecommerce pack
datahub datapack load showcase-ecommerce

# Load from a local file
datahub datapack load my-data --url file:///path/to/data.json --trust-custom

# Load with timestamps anchored to a specific date
datahub datapack load showcase-ecommerce --as-of 2025-06-15

# Preview without loading
datahub datapack load showcase-ecommerce --dry-run
```

### unload

Remove all entities that were loaded by a data pack.

```shell
datahub datapack unload NAME [--hard] [--dry-run]
```

Uses the ingestion rollback infrastructure to revert the load. Only works for packs loaded via `datahub datapack load`.

**Options:**

- `--hard` - Hard-delete entities (irreversible). Default is soft-delete (reversible).
- `--dry-run` - Show what would be deleted without deleting.

**Example:**

```shell
# Soft-delete (reversible)
datahub datapack unload showcase-ecommerce

# Hard-delete (irreversible)
datahub datapack unload showcase-ecommerce --hard
```

## Built-in Data Packs

| Pack | Description | Entities | Platforms |
|------|-------------|----------|-----------|
| **bootstrap** | Lightweight bootstrap data with basic datasets, dashboards, users, and tags | ~50 | Kafka, Hive, HDFS |
| **showcase-ecommerce** | Rich e-commerce demo with lineage, governance, glossary, domains, and data products | ~1,050 | Snowflake, Looker, PowerBI, Tableau, dbt, Spark, PostgreSQL, S3 |
| **covid-bigquery** | COVID-19 BigQuery public datasets with ownership and lineage | ~100 | BigQuery |

## Trust Model

Data packs have three trust tiers:

- **Verified** - Published by the DataHub project. Loads without prompting.
- **Community** - Third-party contributed packs in the registry. Requires `--trust-community`.
- **Custom** - Loaded via `--url` from an arbitrary source. Requires `--trust-custom`.

SHA256 checksums in the registry provide integrity verification for community packs. Verified packs from trusted origins (GitHub/datahub-project) skip checksum verification.

## Ingestion Source

Data packs can also be loaded via standard ingestion recipes using `type: datapack`:

```yaml
source:
  type: datapack
  config:
    pack_name: "showcase-ecommerce"
    # OR: pack_url: "https://example.com/data.json"
    no_time_shift: false
    as_of: "2025-06-15T00:00:00Z"
    trust_community: false
    trust_custom: false
    no_cache: false
```

This is useful for scheduled or automated loading via `datahub ingest`.

## Technical Details

### Schema Downshift

When loading a pack, the CLI queries the server's entity registry (`/openapi/v1/registry/models/entity/specifications`) to discover which `(entityType, aspectName)` pairs are supported. MCPs with unsupported aspects are automatically filtered out. This allows a single data pack to work across both DataHub OSS and Acryl Cloud, with Cloud-only aspects gracefully skipped on OSS.

### Time-Shifting

Each pack has a `reference_timestamp` indicating when it was captured. During load, all temporal fields (timestamps in system metadata, timeseries aspects, audit stamps) are shifted by `now - reference_timestamp` so the data appears fresh. Use `--as-of` to anchor to a different time, or `--no-time-shift` to keep original timestamps.

### Caching

- **Pack files**: Cached in `~/.datahub/datapack-cache/` by URL hash
- **Registry**: Cached in `~/.datahub/datapack-registry-cache.json` with 1-hour TTL
- **Schema**: Cached in `~/.datahub/openapi_schema_cache/` by server URL + commit hash

Use `--no-cache` on load to force re-download.
