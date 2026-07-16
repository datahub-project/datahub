### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Snowflake Shares

If you are using [Snowflake Shares](https://docs.snowflake.com/en/user-guide/data-sharing-provider) to share data across different Snowflake accounts, and you have set up DataHub recipes for ingesting metadata from all these accounts, you may end up having multiple similar dataset entities corresponding to virtual versions of the same table in different Snowflake accounts. The DataHub Snowflake connector can automatically link such tables together through Siblings and Lineage relationships if the user provides information necessary to establish the relationship using the `shares` configuration in the recipe.

**Note:** The `shares` configuration is also strongly recommended when using the `marketplace.enabled` feature to ingest Snowflake internal marketplace listings as Data Products. Without it, ingestion still produces the Data Products, but `_find_listing_for_purchase` cannot map purchased databases back to their listings, so the Data Products end up with no associated datasets (assets) and a `structured_reporter.warning` is emitted. See the [Internal Marketplace](#internal-marketplace) section below for details.

##### Example

- Snowflake account `account1` (ingested as platform_instance `instance1`) owns a database `db1`. A share `X` is created in `account1` that includes database `db1` along with schemas and tables inside it.
- Now, `X` is shared with Snowflake account `account2` (ingested as platform_instance `instance2`). A database `db1_from_X` is created from inbound share `X` in `account2`. In this case, all tables and views included in share `X` will also be present in `instance2.db1_from_X`.
- This can be represented in `shares` configuration section as
  ```yaml
  shares:
    X: # name of the share
      database: db1
      platform_instance: instance1
      consumers: # list of all databases created from share X
        - database: db1_from_X
          platform_instance: instance2
  ```
- If share `X` is shared with more Snowflake accounts and a database is created from share `X` in those accounts, then additional entries need to be added to the `consumers` list for share `X`, one per Snowflake account. The same `shares` config can then be copied across recipes for all accounts.

#### Internal Marketplace

If you are using the Snowflake internal marketplace (private data sharing within your organization via Data Exchange) and want to ingest marketplace listings as DataHub Data Products, you can operate in two modes:

##### Mode 1: Consumer Mode (Default) - Track Purchased Listings

For organizations that **purchase/install** internal marketplace listings:

1. **Grant the required privileges** (already covered in the Prerequisites section above):

   ```sql
   -- Basic marketplace access
   grant imported privileges on database snowflake to role datahub_role;

   -- For SHOW SHARES to discover INBOUND shares
   use role accountadmin;
   grant import share on account to role datahub_role;
   ```

2. **Enable marketplace ingestion** in your recipe:

   ```yaml
   marketplace:
     enabled: true
     marketplace_mode: "consumer" # Default
     # Optional: Configure time window for usage statistics
     start_time: "-7 days" # Default: -1 day
     end_time: "now"
     bucket_duration: "DAY" # Options: "DAY", "HOUR"
   ```

   The `marketplace` configuration inherits from `BaseTimeWindowConfig`, allowing you to control the time window for extracting marketplace usage statistics. This follows the same pattern as other DataHub connectors.

3. **Configure the `shares` mapping** (strongly recommended for linking Data Products to their purchased databases):

   Snowflake does not expose a direct link between imported databases and the marketplace listings they came from. Without `shares`, ingestion still succeeds but each affected Data Product is emitted without assets and a `structured_reporter.warning` is logged.

   ```yaml
   shares:
     <SHARE_NAME>: # From: SHOW SHARES
       database: "<SOURCE_DATABASE>" # Source database in the share
       platform_instance: null
       consumers:
         - database: "<IMPORTED_DATABASE>" # Your purchased/imported database
           platform_instance: null
           # Optional but recommended: Explicit listing mapping
           listing_global_name: "PROVIDER.REGION.LISTING_NAME" # From: SHOW AVAILABLE LISTINGS
   ```

   **Explicit Listing Mapping (Recommended)**: Add `listing_global_name` to explicitly link your purchased database to its marketplace listing. This ensures accurate Data Product associations, especially when you have multiple listings with similar names. Without it, the connector will attempt to match listings by finding the source database name within the listing's global name or title (case-insensitive substring match).

   **To discover the correct values**, run these SQL commands in Snowflake:

   ```sql
   -- Find marketplace listings
   SHOW AVAILABLE LISTINGS IS_ORGANIZATION = TRUE;

   -- Find imported databases
   SELECT DATABASE_NAME, TYPE FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES
   WHERE TYPE = 'IMPORTED DATABASE' AND DELETED IS NULL;

   -- Find shares and their mappings
   SHOW SHARES;
   ```

   **Without the `shares` configuration:**

   - Data Products will be created from marketplace listings
   - Owners and custom properties will be populated
   - Data Products will NOT have any associated datasets (assets)
   - Warning messages will be logged

   **With the `shares` configuration:**

   - Data Products will be created with all metadata
   - Purchased databases will be linked as Data Product assets
   - Tables from imported databases will show as part of the Data Product

##### Mode 2: Provider Mode - Track Published Listings

For organizations that **publish/share** internal marketplace listings:

1. **Grant the required privileges** (already covered in the Prerequisites section above):

   ```sql
   -- Basic marketplace access
   grant imported privileges on database snowflake to role datahub_role;

   -- For SHOW SHARES to discover OUTBOUND shares (provider mode)
   -- The role can ONLY see shares it owns or inherits
   use role securityadmin;
   grant role sysadmin to role datahub_role;

   -- OR use SYSADMIN/ACCOUNTADMIN directly in your recipe:
   -- role: SYSADMIN
   ```

   **Important Note**: In Snowflake, `SHOW SHARES` returns OUTBOUND shares based on ownership. A role can see:

   - Shares owned by the current role (e.g., if `datahub_role` created the share)
   - Shares owned by roles it inherits from (e.g., `datahub_role` inherits from `SYSADMIN`)
   - But NOT shares owned by other roles (e.g., shares owned by `ACCOUNTADMIN` that `datahub_role` doesn't inherit from)

   **Solutions:**

   - If your shares are owned by `ACCOUNTADMIN` or `SYSADMIN`: Grant `SYSADMIN` role to `datahub_role` (recommended)
   - If `datahub_role` creates the shares: No additional grant needed
   - Alternative: Use `SYSADMIN` or `ACCOUNTADMIN` directly in your ingestion recipe

   Without proper role hierarchy, provider mode cannot discover outbound shares owned by other roles, and tables won't be added as assets to Data Products.

2. **Enable marketplace ingestion in provider mode** in your recipe:

   ```yaml
   marketplace:
     enabled: true
     marketplace_mode: "provider"
     # Assign owners to your Data Products
     internal_marketplace_owner_patterns:
       "^Your Listing.*": ["data-team"]
     # Optional: restrict the database fallback to specific schemas per listing.
     # Required when DESC SHARE is not permitted (see troubleshooting below).
     listing_to_schemas_overrides:
       YOUR_LISTING_GLOBAL_NAME: [YOUR_SCHEMA]

   # Include your source databases being shared
   database_pattern:
     allow:
       - "YOUR_SOURCE_DATABASE"
   ```

3. **No `shares` configuration needed!**

   Provider mode automatically:

   - Discovers your OUTBOUND shares with marketplace listings
   - Links them to your source databases
   - Creates Data Products with your source databases as assets

**What you'll get in provider mode:**

- Data Products for your published marketplace listings
- Source databases automatically linked as assets
- Owner assignment from config patterns
- Works without any imported databases

**Example use case:**
You publish a "Customer 360" listing from your `CUSTOMER_DATA` database. Provider mode will:

1. Find the listing via `SHOW AVAILABLE LISTINGS`
2. Find the OUTBOUND share via `SHOW SHARES`
3. Link the `CUSTOMER_DATA` database to the Data Product
4. No manual configuration needed!

##### Mode 3: Both - Track Both Perspectives

Set `marketplace_mode: "both"` to track both purchased listings (consumer) AND published listings (provider) in the same ingestion.

For more details, see the marketplace configuration guide in the connector documentation.

##### Mapping Marketplace Organizations to Existing Domains

Marketplace ingestion does not create domains. Map each Snowflake organization to an existing DataHub domain (URN, GUID, or name) via `marketplace.organization_to_domain`:

```yaml
marketplace:
  enabled: true
  organization_to_domain:
    "ACME Corp": "urn:li:domain:finance"
    "Weather Co": "data-products" # resolved via DomainRegistry
```

Unmapped organizations produce Data Products with no domain. The `domains` aspect is written as `CREATE` only, so UI-assigned domains survive subsequent runs.

**Caveats:**

- Editing `organization_to_domain` for an already-ingested organization has no effect — soft-delete the Data Product's `domains` aspect to re-map.
- Provider renames leave the old `Marketplace:Provider:<old>` tag on the purchased database container; remove it manually.

##### Troubleshooting Marketplace Ingestion

**Common Permission Errors:**

If marketplace ingestion fails or produces incomplete data, check for these common permission issues:

1. **"Failed to get marketplace listings - insufficient permissions"**

   - **Cause**: Role lacks access to run `SHOW AVAILABLE LISTINGS`
   - **Solution**: Grant imported privileges:
     ```sql
     grant imported privileges on database snowflake to role datahub_role;
     ```

2. **"Failed to describe provider share - insufficient permissions"**

   - **Cause**: `DESC SHARE` requires ownership of the share. Snowflake Marketplace creates shares
     under `ACCOUNTADMIN`, and Snowflake does not allow transferring share ownership.
   - **Automatic fallback**: The connector falls back to enumerating tables from the share's source
     database (visible in `SHOW SHARES`). By default this includes all schemas in that database;
     use `listing_to_schemas_overrides` to restrict to the specific schemas the share exposes:
     ```yaml
     marketplace:
       listing_to_schemas_overrides:
         YOUR_LISTING_GLOBAL_NAME: [YOUR_SCHEMA]
     ```
   - **For precise `DESC SHARE` output**: Set `role: ACCOUNTADMIN` in your recipe (broader
     permissions, not recommended for production).

3. **"Failed to query imported database tables - insufficient permissions"**

   - **Cause**: Role lacks access to query `INFORMATION_SCHEMA` in imported databases
   - **Solution**: Grant usage and references on the imported database:
     ```sql
     grant usage on database "<imported-database>" to role datahub_role;
     grant usage on all schemas in database "<imported-database>" to role datahub_role;
     grant references on all tables in database "<imported-database>" to role datahub_role;
     ```

4. **Data Products created but no assets appear**

   - **Cause**: Missing `shares` configuration (consumer mode)
   - **Solution**: Add explicit `listing_global_name` mapping in your `shares` config (see consumer mode documentation above)

5. **Incomplete listing metadata**
   - **Cause**: `fetch_internal_marketplace_listing_details: true` requires additional time per listing
   - **Note**: This is expected behavior. The connector runs `DESCRIBE AVAILABLE LISTING` for each listing to fetch enriched metadata (descriptions, owners, documentation links). For large catalogs (100+ listings), consider setting this to `false` to improve performance.

**Debugging Tips:**

- Check the DataHub logs for structured warnings about marketplace ingestion failures
- All marketplace errors are logged with clear titles and context (e.g., "Optional listing enrichment failed")
- Verify your role has `imported privileges` on the `snowflake` database
- Test your SQL grants manually using the DataHub role before running ingestion

#### Lineage and Usage

DataHub supports two strategies for extracting lineage and usage information from Snowflake:

##### New Strategy (Default - `use_queries_v2: true`)

The default and recommended approach uses an optimized query extraction method that:

- **Better Performance**: Fetches query logs in a single optimized query instead of multiple separate queries
- **Enhanced Features**:
  - Query entities generation (`include_queries`)
  - Query popularity statistics (`include_query_usage_statistics`)
  - User filtering with patterns (`pushdown_deny_usernames`, `pushdown_allow_usernames`)
  - Database pattern pushdown for performance (`push_down_database_pattern_access_history`)
  - Query deduplication strategies (`query_dedup_strategy`)

##### Legacy Strategy (`use_queries_v2: false`)

The older approach that will be deprecated in future versions:

- Uses separate extractors for lineage and usage
- Less performant due to multiple query executions
- Limited feature support compared to the new strategy

Both strategies access the same Snowflake system tables (`account_usage.query_history`, `account_usage.access_history`), but the new strategy provides significant performance improvements and additional functionality.

##### Snowflake Streams as Upstream Lineage Sources

DataHub extracts lineage when a query reads from a Snowflake Stream. Coverage details:

- **Multi-target `INSERT ALL` from a Stream** — emits one lineage entry per downstream table, including column-level lineage. Requires `use_queries_v2: true` (the default).
- **Single-target queries reading from a Stream** — fall back to SQL parsing of the query text rather than direct extraction from the audit log.
- **Audit log placeholder names** — Snowflake occasionally emits placeholder object names (`$SYS_VIEW_<id>` or other `$`-prefixed names) for stream-driven queries. When this happens for a given row, that row falls back to SQL parsing so DataHub never builds lineage from unusable URNs.

The Stream entity itself is also extracted as a top-level dataset; the lineage above is in addition to that.

#### Metadata Pattern Pushdown

When ingesting metadata from large Snowflake environments, you can improve performance by pushing down pattern filters directly to Snowflake SQL queries using the `push_down_metadata_patterns` configuration option.

> **Note:** This option applies only to **metadata extraction** queries (`information_schema.databases`, `schemata`, `tables`, `views`). For pushing down filters on **lineage/usage** queries (`account_usage.access_history`), use `push_down_database_pattern_access_history` instead. These two options are independent and target completely separate query paths.

##### Configuration

```yaml
source:
  type: snowflake
  config:
    # Enable pattern pushdown for improved performance
    push_down_metadata_patterns: true

    # Your existing patterns - MUST follow Snowflake RLIKE syntax
    database_pattern:
      allow:
        - "PROD_.*" # Matches databases starting with PROD_
        - "ANALYTICS.*" # Matches databases starting with ANALYTICS
      deny:
        - ".*_TEMP$" # Excludes databases ending with _TEMP

    table_pattern:
      allow:
        - ".*" # Allow all tables
      deny:
        - ".*_BACKUP$" # Exclude tables ending with _BACKUP
```

##### View Pattern Limitation

By default, Snowflake views are fetched using `SHOW VIEWS`, which does **not** support SQL-level filtering. When `push_down_metadata_patterns: true`, the `view_pattern` is pushed down **only** if `fetch_views_from_information_schema: true` is also set. Otherwise, view filtering falls back to Python's `re.match()`, even with pushdown enabled.

This means if you write patterns in Snowflake RLIKE syntax (e.g., `PROD.*` for prefix matching), they will still work correctly with Python filtering since `PROD.*` is valid in both. However, patterns that rely on RLIKE's full-string matching semantics (e.g., exact match `PROD_DB` without `.*`) will behave differently — Python `re.match()` treats it as a prefix match.

**Recommendation**: If you enable `push_down_metadata_patterns`, also enable `fetch_views_from_information_schema: true` to ensure consistent behavior for view patterns.

##### Important: Snowflake RLIKE Syntax Differences

When `push_down_metadata_patterns: true`, patterns are executed in Snowflake using the `RLIKE` operator instead of Python's `re.match()`. These have **different matching behaviors**:

| Behavior             | Python `re.match()`                       | Snowflake `RLIKE`                   | Fix for Snowflake     |
| -------------------- | ----------------------------------------- | ----------------------------------- | --------------------- |
| **Prefix match**     | `'PROD'` matches `'PROD_DB'`              | `'PROD'` does NOT match `'PROD_DB'` | Use `PROD.*`          |
| **Suffix match**     | `'.*_BACKUP'` matches `'TABLE_BACKUP_V2'` | Does NOT match                      | Use `.*_BACKUP$`      |
| **Start anchor**     | `'^PROD'` matches `'PROD_DB'`             | Does NOT match                      | Use `PROD.*`          |
| **Alternation**      | `'PROD\|DEV'` matches `'PROD_DB'`         | Does NOT match                      | Use `(PROD\|DEV).*`   |
| **Case sensitivity** | Case-insensitive by default               | Case-sensitive by default           | Handled automatically |

**Key difference**: Python `re.match()` anchors at the START only (prefix matching), while Snowflake `RLIKE` requires a FULL STRING match.

##### Pattern Conversion Examples

| Intent                       | Without Pushdown (Python) | With Pushdown (Snowflake RLIKE) |
| ---------------------------- | ------------------------- | ------------------------------- |
| Starts with `PROD`           | `PROD`                    | `PROD.*`                        |
| Ends with `_BACKUP`          | `.*_BACKUP`               | `.*_BACKUP$`                    |
| Contains `TEMP`              | `.*TEMP.*`                | `.*TEMP.*` (same)               |
| Exact match                  | `PROD_DB`                 | `PROD_DB` (same)                |
| Match `PROD` or `DEV` prefix | `PROD\|DEV`               | `(PROD\|DEV).*`                 |

##### Testing Your Patterns

Before enabling pushdown in production, test your patterns in Snowflake:

```sql
-- Test prefix matching
SELECT 'PROD_DB' RLIKE 'PROD';      -- FALSE (needs .*)
SELECT 'PROD_DB' RLIKE 'PROD.*';    -- TRUE

-- Test suffix matching
SELECT 'TABLE_BACKUP_V2' RLIKE '.*_BACKUP';    -- FALSE (needs $)
SELECT 'TABLE_BACKUP' RLIKE '.*_BACKUP$';      -- TRUE

-- Test FQN with escaped dots
SELECT 'PROD_DB.PUBLIC.TABLE' RLIKE 'PROD_DB\\.PUBLIC\\..*';  -- TRUE
```

#### Semantic Views

DataHub supports ingestion of Snowflake Semantic Views, which are business-defined views that define metrics, dimensions, and relationships for consistent data modeling and AI-powered analytics.

##### Configuration

Semantic view ingestion is disabled by default (requires Snowflake Enterprise Edition or above). You can enable it using the following configuration options:

```yaml
# Enable semantic view ingestion (requires Enterprise Edition)
semantic_views:
  enabled: true # Default: false
  column_lineage: true # Default: false - enable column-level lineage

# Filter semantic views using regex patterns
semantic_view_pattern:
  allow:
    - "ANALYTICS_DB.PUBLIC.*"
    - "SALES_DB.*"
  deny:
    - ".*_INTERNAL"
```

##### Features

- **Metadata Extraction**: Extracts semantic view definitions (YAML), columns, comments, and timestamps
- **Lineage Support**: Semantic views participate in lineage extraction like regular views
- **Tags Support**: Tags applied to semantic views are extracted if `extract_tags` is enabled
- **External URLs**: Direct links to Snowflake Snowsight UI for semantic views

##### Requirements

- Semantic views require appropriate Snowflake edition and privileges
- Requires `REFERENCES` or `SELECT` privileges on semantic views (they are treated as views in Snowflake's permission model)
- The semantic view definition (SQL DDL) is extracted when available through the `GET_DDL` function

#### Stages, Tasks, and Pipes

DataHub supports ingestion of Snowflake Stages, Tasks, and Snowpipe objects. All three features are disabled by default and can be enabled independently.

##### Stages (`include_stages: true`)

Stages are ingested as containers nested under their parent schema. Internal stages additionally emit a placeholder dataset representing the staged data, which is used for pipe lineage resolution. External stages (S3, GCS, Azure) resolve their URLs to the corresponding cloud platform dataset URN.

```yaml
include_stages: true
stage_pattern:
  allow:
    - "MY_DB.MY_SCHEMA.*"
```

##### Tasks (`include_tasks: true`)

Tasks are ingested as DataJob entities grouped under a per-schema DataFlow. Predecessor dependencies between tasks are captured as `inputDatajobs` on the DataJobInputOutput aspect, preserving the DAG structure.

```yaml
include_tasks: true
task_pattern:
  allow:
    - "MY_DB.MY_SCHEMA.*"
```

##### Pipes (`include_pipes: true`)

Snowpipe objects are ingested as DataJob entities with lineage derived from parsing the `COPY INTO` statement. The pipe's source stage resolves to an upstream dataset (internal placeholder or external cloud URN) and the target table resolves to a downstream dataset. Enabling pipes automatically scans stages for lineage resolution, even if `include_stages` is false.

The parser handles the following `COPY INTO` patterns:

- Column-list targets — `COPY INTO t(a, b) FROM @stage` resolves the target table correctly
- Subquery sources — `COPY INTO t FROM (SELECT ... FROM @s1 UNION ALL SELECT ... FROM @s2)` captures all referenced stages as upstream datasets
- Non-`COPY` pipe bodies are silently skipped; stage refs that cannot be normalized to a three-part FQN emit a warning in the ingestion report

```yaml
include_pipes: true
pipe_pattern:
  allow:
    - "MY_DB.MY_SCHEMA.*"
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- Some features require specific Snowflake editions or additional privileges. This includes dynamic tables, semantic views, advanced lineage features, and tags.
- Dynamic tables require the `monitor` privilege for full metadata extraction. Without this privilege, dynamic table entities will still appear in DataHub but their DDL is not accessible, so lineage will not be extracted.
- Semantic views require `REFERENCES` or `SELECT` privileges for metadata extraction. Without these privileges, semantic views will not be visible to DataHub.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [ongoing Snowflake incident](https://status.snowflake.com/), we will not be able to get the metadata until that incident is resolved.
- Lineage extraction, when got directly from Snowflake access history, has some limitations, as documented [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes), [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-column-lineage), and [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-object-modified-by-ddl-column).

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Snowsight Links Point to the Wrong URL (Private Link)

By default, DataHub generates Snowsight links of the form `https://app.snowflake.com/<region>/<account>/...`. If Snowsight is only reachable through private link in your environment, these links will not work for your users.

To fix this, set `snowsight_base_url` in the recipe to your private-link Snowsight URL. Retrieve the value from Snowflake as `ACCOUNTADMIN`:

```sql
SELECT SYSTEM$GET_PRIVATELINK_CONFIG();
-- Use either snowsight-privatelink-url or regionless-snowsight-privatelink-url.
```

```yaml
source:
  type: snowflake
  config:
    snowsight_base_url: "https://app.us-east-1.privatelink.snowflakecomputing.com/"
```
