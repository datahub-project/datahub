### Overview

[Qualytics](https://www.qualytics.io/) is a data quality platform that profiles connected
datastores, infers and enforces quality checks against them, and records the anomalies
those checks produce.

This source plugin ingests Qualytics quality checks as DataHub **assertions**, check
results and anomalies as **assertion run events**, and Qualytics profiles as **dataset and
field profiles**, attached to the datasets your warehouse or lake source has already
ingested. Stateful ingestion removes assertions whose checks were deleted in Qualytics.

### Prerequisites

To ingest metadata from Qualytics you will need:

- A Qualytics deployment and its API base URL, **including the `/api` suffix** — for
  example `https://acme.qualytics.io/api`. Qualytics is single-tenant, so each
  deployment needs its own recipe. Omitting the suffix is the most common setup
  mistake; `datahub ingest --test-source-connection` detects it and tells you the URL
  to use instead.
- An API token. Create one in the Qualytics UI under **Settings → Tokens**. A dedicated
  read-only service user is recommended, so the connector's activity is
  distinguishable in Qualytics' audit log.
- Network access from wherever DataHub ingestion runs to the Qualytics deployment. Many
  deployments are private; if yours is fronted by a corporate or private CA, set
  `ca_cert_path` rather than disabling `verify_ssl`.
- **The datasets Qualytics profiles must already be in DataHub**, ingested by your
  warehouse or lake source. This connector enriches those datasets; it does not create
  them. See below.

#### Required Permissions

The connector only ever reads. Qualytics does not expose granular API scopes, so
permissions follow from the token's user: it needs read access to the objects below,
which in practice means membership of a team with visibility of the datastores you want
ingested. Anything the token cannot see is simply absent from the ingestion — the run
succeeds with fewer entities rather than failing.

##### Minimum, for any ingestion

| Qualytics object | Endpoint          | Why                                                                           |
| ---------------- | ----------------- | ----------------------------------------------------------------------------- |
| Datastores       | `GET /datastores` | Resolves each container onto the platform DataHub already catalogues it under |
| Containers       | `GET /containers` | The tables, views and files metadata attaches to                              |

Without these, nothing can be emitted. `test_connection` reports them as basic
connectivity.

##### Per capability

| Capability                  | Config                                | Endpoints                                                             | Notes                                                  |
| --------------------------- | ------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------ |
| `DESCRIPTIONS` — assertions | `emit_assertions` (default on)        | `GET /quality-checks`                                                 | Quality checks become DataHub assertions               |
| — assertion results         | `emit_assertion_results` (default on) | `GET /anomalies`                                                      | Failure history. Requires `emit_assertions`            |
| `DATA_PROFILING`            | `emit_profiles` (default on)          | `GET /containers/{id}/profile`, `GET /containers/{id}/field-profiles` | Row counts and column statistics                       |
| `PLATFORM_INSTANCE`         | `platform_instance`                   | —                                                                     | No permission needed                                   |
| `DELETION_DETECTION`        | `stateful_ingestion`                  | The endpoints above                                                   | Needs to list everything, so it can tell what has gone |
| `TEST_CONNECTION`           | —                                     | `GET /datastores`, `GET /containers`                                  | Verifies reachability and the token                    |

Turning a feature off means its endpoint is never called, so a token without access to,
say, profiles can still ingest assertions — set `emit_profiles: false` and the run is
clean.

##### Checking a token

```bash
curl -sS -H "Authorization: Bearer $QUALYTICS_TOKEN" \
  "https://acme.qualytics.io/api/datastores?page=1&size=1"
```

A `401` or `403` means the token is invalid or its user has no visibility. Prefer
`datahub ingest -c recipe.yml --test-source-connection`, which checks each capability
separately and names the one that failed.

#### Making assertions land on the right datasets

The connector's value depends on attaching to the datasets your warehouse and lake
sources already emit. It resolves each Qualytics datastore to a DataHub platform in two
ways:

1. **`datastore_to_platform_map`** (explicit, recommended). Keyed by the datastore's
   **name or its numeric id** — names read better in a recipe, ids survive a rename.
   Supply the `platform`, `platform_instance` and `env` your warehouse source used.
2. **Inference** (`infer_source_platform`, default `true`). The Qualytics connection
   type maps to a DataHub platform — mostly one-to-one, with a few renames
   (`postgresql` → `postgres`, `sqlserver` → `mssql`, `abfs` → `abs`). Inferred URNs use
   `default_source_platform_instance` and `default_source_env`, which default to unset.

Datastores that resolve to nothing are skipped with a warning naming the datastore, and
counted in the report as `datastores_unresolved`. The connector deliberately skips
rather than guesses: an assertion attached to a URN no source emits is invisible and
misleading, which is worse than no assertion at all.

Two settings people get wrong:

- **`platform_instance` is the _Qualytics_ deployment**, not the warehouse. It
  namespaces this deployment's assertions. The warehouse's instance is
  `default_source_platform_instance`, or the per-datastore `platform_instance` inside
  `datastore_to_platform_map`.
- **URN casing must match.** A casing mismatch yields a perfectly valid URN that
  matches no existing dataset. Left unset, `convert_urns_to_lowercase` follows each
  platform's own DataHub source: Snowflake datastores are lowercased, as the Snowflake
  source does by default, and others keep their case. Set it only if your warehouse
  recipe changed that default, per datastore if your estate is mixed.

Object stores are the least certain case. DataHub names an S3/GCS/ABS dataset after its
_table path_, and where the table boundary sits is decided by your `path_spec` — a
folder of partitioned parquet may be one dataset to DataHub while Qualytics sees each
file as a container. The connector reconstructs the path faithfully and counts these
separately as `urns_resolved_by_path_reconstruction`; if that number is high and your
assertions are not appearing, use the explicit map.

#### Running more than one Qualytics deployment

Qualytics object ids are per-deployment database sequences, so the same id means
different things in different deployments. If two Qualytics deployments feed one
DataHub, give each recipe a distinct `platform_instance`. Without it, the two
deployments' assertions collide on identical URNs and overwrite each other.

#### If you also use the Qualytics → DataHub push integration

Qualytics can push metadata into DataHub from the Qualytics side (**Integrations → Data
Catalog → DataHub**). Both directions are supported and are designed to run side by side —
they cover different ground, and only this connector emits assertions, assertion results
and dataset profiles.

The constraint is that a given aspect should have one writer. The default split is:

| Aspect                                                             | Written by                                                   |
| ------------------------------------------------------------------ | ------------------------------------------------------------ |
| Assertions, assertion results, dataset and field profiles          | This connector                                               |
| Descriptions, structured properties (quality score and dimensions) | The push integration                                         |
| Anomaly incidents                                                  | The push integration                                         |
| Tags                                                               | The push integration — this connector does not emit them yet |

That split needs no configuration today: this connector does not emit incidents,
structured properties or tags at all, so the push integration owns them by default and
the two cannot disagree. When those features arrive here they will come with toggles
defaulting to off, preserving the same split.

One known divergence: the push integration maps Qualytics' `mariadb` connection type to
the `mysql` platform, while this connector maps it to `mariadb`, matching DataHub's
dedicated MariaDB source. If you run both against MariaDB datastores, pin the platform
explicitly in `datastore_to_platform_map` so the two agree.
