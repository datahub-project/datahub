### Overview

The `snowflake` module ingests metadata from Snowflake into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Requires specific privileges to read metadata from your Snowflake warehouse.

### Authentication

Authentication is most simply done via a Snowflake user and password.

Alternatively, other authentication methods are supported via the `authentication_type` config option.

#### Key Pair Authentication

To set up Key Pair authentication, follow the three steps in [this guide](https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-authentication):

- Generate the private key
- Generate the public key
- Assign the public key to the DataHub user to be configured in the recipe.

Pass in the following values in the recipe config instead of a password, ensuring the private key maintains proper PEM format with line breaks at the beginning, end, and approximately every 64 characters within the key:

```yml
authentication_type: KEY_PAIR_AUTHENTICATOR
private_key: <Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----'>

# Optional - if using encrypted private key
private_key_password: <Password for your private key>
```

#### Okta OAuth

To set up Okta OAuth authentication, roughly follow the four steps in [this guide](https://docs.snowflake.com/en/user-guide/oauth-okta).

Pass in the following values, as described in the article, for your recipe's `oauth_config`:

- `provider`: okta
- `client_id`: `<OAUTH_CLIENT_ID>`
- `client_secret`: `<OAUTH_CLIENT_SECRET>`
- `authority_url`: `<OKTA_OAUTH_TOKEN_ENDPOINT>`
- `scopes`: The list of your _Okta_ scopes, i.e. with the `session:role:` prefix

DataHub only supports two OAuth grant types: `client_credentials` and `password`.
The steps slightly differ based on which you decide to use.

##### Client Credentials Grant Type (Simpler)

- When creating an Okta App Integration, choose type `API Services`
  - Ensure client authentication method is `Client secret`
  - Note your `Client ID`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  - _Ensure the user's `Login Name` matches your Okta application's `Client ID`_
  - Ensure the user has been granted your DataHub role

##### Password Grant Type

- When creating an Okta App Integration, choose type `OIDC` -> `Native Application`
  - Add Grant Type `Resource Owner Password`
  - Ensure client authentication method is `Client secret`
- Create an Okta user to sign into, noting the `Username` and `Password`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  - _Ensure the user's `Login Name` matches your Okta user's `Username` (likely an email)_
  - Ensure the user has been granted your DataHub role
- When running ingestion, provide the required `oauth_config` fields,
  including `client_id` and `client_secret`, plus your Okta user's `Username` and `Password`
  - Note: the `username` and `password` config options are not nested under `oauth_config`

### Snowflake Shares

If you are using [Snowflake Shares](https://docs.snowflake.com/en/user-guide/data-sharing-provider) to share data across different Snowflake accounts, and you have set up DataHub recipes for ingesting metadata from all these accounts, you may end up having multiple similar dataset entities corresponding to virtual versions of the same table in different Snowflake accounts. The DataHub Snowflake connector can automatically link such tables together through Siblings and Lineage relationships if the user provides information necessary to establish the relationship using the `shares` configuration in the recipe.

#### Example

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

### Lineage and Usage

DataHub supports two strategies for extracting lineage and usage information from Snowflake:

#### New Strategy (Default - `use_queries_v2: true`)

The default and recommended approach uses an optimized query extraction method that:

- **Better Performance**: Fetches query logs in a single optimized query instead of multiple separate queries
- **Enhanced Features**:
  - Query entities generation (`include_queries`)
  - Query popularity statistics (`include_query_usage_statistics`)
  - User filtering with patterns (`pushdown_deny_usernames`, `pushdown_allow_usernames`)
  - Database pattern pushdown for performance (`push_down_database_pattern_access_history`)
  - Query deduplication strategies (`query_dedup_strategy`)

#### Legacy Strategy (`use_queries_v2: false`)

The older approach that will be deprecated in future versions:

- Uses separate extractors for lineage and usage
- Less performant due to multiple query executions
- Limited feature support compared to the new strategy

Both strategies access the same Snowflake system tables (`account_usage.query_history`, `account_usage.access_history`), but the new strategy provides significant performance improvements and additional functionality.

### Metadata Pattern Pushdown

When ingesting metadata from large Snowflake environments, you can improve performance by pushing down pattern filters directly to Snowflake SQL queries using the `push_down_metadata_patterns` configuration option.

> **Note:** This option applies only to **metadata extraction** queries (`information_schema.databases`, `schemata`, `tables`, `views`). For pushing down filters on **lineage/usage** queries (`account_usage.access_history`), use `push_down_database_pattern_access_history` instead. These two options are independent and target completely separate query paths.

#### Configuration

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

#### View Pattern Limitation

By default, Snowflake views are fetched using `SHOW VIEWS`, which does **not** support SQL-level filtering. When `push_down_metadata_patterns: true`, the `view_pattern` is pushed down **only** if `fetch_views_from_information_schema: true` is also set. Otherwise, view filtering falls back to Python's `re.match()`, even with pushdown enabled.

This means if you write patterns in Snowflake RLIKE syntax (e.g., `PROD.*` for prefix matching), they will still work correctly with Python filtering since `PROD.*` is valid in both. However, patterns that rely on RLIKE's full-string matching semantics (e.g., exact match `PROD_DB` without `.*`) will behave differently — Python `re.match()` treats it as a prefix match.

**Recommendation**: If you enable `push_down_metadata_patterns`, also enable `fetch_views_from_information_schema: true` to ensure consistent behavior for view patterns.

#### Important: Snowflake RLIKE Syntax Differences

When `push_down_metadata_patterns: true`, patterns are executed in Snowflake using the `RLIKE` operator instead of Python's `re.match()`. These have **different matching behaviors**:

| Behavior             | Python `re.match()`                       | Snowflake `RLIKE`                   | Fix for Snowflake     |
| -------------------- | ----------------------------------------- | ----------------------------------- | --------------------- |
| **Prefix match**     | `'PROD'` matches `'PROD_DB'`              | `'PROD'` does NOT match `'PROD_DB'` | Use `PROD.*`          |
| **Suffix match**     | `'.*_BACKUP'` matches `'TABLE_BACKUP_V2'` | Does NOT match                      | Use `.*_BACKUP$`      |
| **Start anchor**     | `'^PROD'` matches `'PROD_DB'`             | Does NOT match                      | Use `PROD.*`          |
| **Alternation**      | `'PROD\|DEV'` matches `'PROD_DB'`         | Does NOT match                      | Use `(PROD\|DEV).*`   |
| **Case sensitivity** | Case-insensitive by default               | Case-sensitive by default           | Handled automatically |

**Key difference**: Python `re.match()` anchors at the START only (prefix matching), while Snowflake `RLIKE` requires a FULL STRING match.

#### Pattern Conversion Examples

| Intent                       | Without Pushdown (Python) | With Pushdown (Snowflake RLIKE) |
| ---------------------------- | ------------------------- | ------------------------------- |
| Starts with `PROD`           | `PROD`                    | `PROD.*`                        |
| Ends with `_BACKUP`          | `.*_BACKUP`               | `.*_BACKUP$`                    |
| Contains `TEMP`              | `.*TEMP.*`                | `.*TEMP.*` (same)               |
| Exact match                  | `PROD_DB`                 | `PROD_DB` (same)                |
| Match `PROD` or `DEV` prefix | `PROD\|DEV`               | `(PROD\|DEV).*`                 |

#### Testing Your Patterns

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

### Semantic Views

DataHub supports ingestion of Snowflake Semantic Views, which are business-defined views that define metrics, dimensions, and relationships for consistent data modeling and AI-powered analytics.

#### Configuration

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

#### Features

- **Metadata Extraction**: Extracts semantic view definitions (YAML), columns, comments, and timestamps
- **Lineage Support**: Semantic views participate in lineage extraction like regular views
- **Tags Support**: Tags applied to semantic views are extracted if `extract_tags` is enabled
- **External URLs**: Direct links to Snowflake Snowsight UI for semantic views

#### Requirements

- Semantic views require appropriate Snowflake edition and privileges
- Requires `REFERENCES` or `SELECT` privileges on semantic views (they are treated as views in Snowflake's permission model)
- The semantic view definition (SQL DDL) is extracted when available through the `GET_DDL` function

### Caveats

- Some features require specific Snowflake editions or additional privileges. This includes dynamic tables, semantic views, advanced lineage features, and tags.
- Dynamic tables require the `monitor` privilege for metadata extraction. Without this privilege, dynamic tables will not be visible to DataHub.
- Semantic views require `REFERENCES` or `SELECT` privileges for metadata extraction. Without these privileges, semantic views will not be visible to DataHub.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [ongoing Snowflake incident](https://status.snowflake.com/), we will not be able to get the metadata until that incident is resolved.
- Lineage extraction, when got directly from Snowflake access history, has some limitations, as documented [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes), [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-column-lineage), and [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-object-modified-by-ddl-column).
