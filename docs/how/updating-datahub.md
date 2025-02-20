# Updating DataHub

<!--

## <version number>

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

-->

This file documents any backwards-incompatible changes in DataHub and assists people when migrating to a new version.

## Next

### Breaking Changes

- #12580: The OpenAPI source handled nesting incorrectly. 12580 fixes it to create proper nested field paths, however, this will re-write the incorrect schemas of existing OpenAPI runs.

- #12408: The `platform` field in the DataPlatformInstance GraphQL type is removed. Clients need to retrieve the platform via the optional `dataPlatformInstance` field.

### Known Issues

- #12601: Jetty 12 introduces a stricter handling of url encoding. We are currently applying a workaround to prevent a regression, while technically breaking the official specifications.

### Potential Downtime

### Deprecations

### Other Notable Changes

- #12641: Adds a new MCP validator that prevents deletes of any `CorpUser` entity that has a newly introduced `CorpUserInfo#system` flag set to true.
- #12433: Fixes the searchable annotations in the model supporting `Dashboard` to `Dashboard` lineage within the `DashboardInfo` aspect. Mainly, users of Sigma and PowerBI Apps ingestion may be affected by this adjustment. Consequently, a [reindex](https://datahubproject.io/docs/how/restore-indices/) will be automatically triggered during the system upgrade.

## 0.15.0

- OpenAPI Update: PIT Keep Alive parameter added to scroll endpoints. NOTE: This parameter requires the `pointInTimeCreationEnabled` feature flag to be enabled and the `elasticSearch.implementation` configuration to be `elasticsearch`. This feature is not supported for OpenSearch at this time and the parameter will not be respected without both of these set.
- OpenAPI Update 2: Previously there was an incorrectly marked parameter named `sort` on the generic list entities endpoint for v3. This parameter is deprecated and only supports a single string value while the documentation indicates it supports a list of strings. This documentation error has been fixed and the correct field, `sortCriteria`, is now documented which supports a list of strings.

### Known Issues

- Persistence Exception: No Rows Updated may occur if a transaction does not change any aspect's data.

### Breaking Changes

- #12223: For dbt Cloud ingestion, the "View in dbt" link will point at the "Explore" page in the dbt Cloud UI. You can revert to the old behavior of linking to the dbt Cloud IDE by setting `external_url_mode: ide".
- #12191 - Configs `include_view_lineage` and `include_view_column_lineage` are removed from snowflake ingestion source. View and External Table DDL lineage will always be ingested when definitions are available.
- #12181 - Configs `include_view_lineage`, `include_view_column_lineage` and `lineage_parse_view_ddl` are removed from bigquery ingestion source. View and Snapshot lineage will always be ingested when definitions are available.
- #12077: `Kafka` source no longer ingests schemas from schema registry as separate entities by default, set `ingest_schemas_as_entities` to `true` to ingest them
- #11486 - Criterion's `value` parameter has been previously deprecated. Use of `value` instead of `values` is no longer supported and will be completely removed on the next major version.
- #11484 - Metadata service authentication enabled by default
- #11484 - Rest API authorization enabled by default
- #10472 - `SANDBOX` added as a FabricType. No rollbacks allowed once metadata with this fabric type is added without manual cleanups in databases.
- #11619 - schema field/column paths can no longer be empty strings
- #11619 - schema field/column paths can no longer be duplicated within the schema
- #11570 - The `DatahubClientConfig`'s server field no longer defaults to `http://localhost:8080`. Be sure to explicitly set this.
- #11570 - If a `datahub_api` is explicitly passed to a stateful ingestion config provider, it will be used. We previously ignored it if the pipeline context also had a graph object.
- #11518 - DataHub Garbage Collection: Various entities that are soft-deleted (after 10d) or are timeseries _entities_ (dataprocess, execution requests) will be removed automatically using logic in the `datahub-gc` ingestion source.
- #12020 - Removed `sql_parser` configuration from the Redash source, as Redash now exclusively uses the sqlglot-based parser for lineage extraction.
- #12020 - Removed `datahub.utilities.sql_parser`, `datahub.utilities.sql_parser_base` and `datahub.utilities.sql_lineage_parser_impl` module along with `SqlLineageSQLParser` and `DefaultSQLParser`. Use `create_lineage_sql_parsed_result` from `datahub.sql_parsing.sqlglot_lineage` module instead.
- #11518 - DataHub Garbage Collection: Various entities that are soft-deleted
  (after 10d) or are timeseries *entities* (dataprocess, execution requests)
  will be removed automatically using logic in the `datahub-gc` ingestion
  source.
- #12067 - Default behavior of DataJobPatchBuilder in Python sdk has been
  changed to NOT fill out `created` and `lastModified` auditstamps by default
  for input and output dataset edges. This should not have any user-observable
  impact (time-based lineage viz will still continue working based on observed time), but could break assumptions previously being made by clients.
- #12158 - Users provisioned with `user.props` will need to be enabled before login in order to be granted access to DataHub.

### Potential Downtime

### Deprecations

- #12056: The DataHub Airflow plugin no longer supports Airflow 2.1 and Airflow 2.2.
- #11701: The Fivetran `sources_to_database` field is deprecated in favor of setting directly within `sources_to_platform_instance.<key>.database`.
- #11560 - The PowerBI ingestion source configuration option include_workspace_name_in_dataset_urn determines whether the workspace name is included in the PowerBI dataset's URN.<br/> PowerBI allows to have identical name of semantic model and their tables across the workspace, It will overwrite the semantic model in-case of multi-workspace ingestion.<br/>
  Entity urn with `include_workspace_name_in_dataset_urn: false`

  ```
   urn:li:dataset:(urn:li:dataPlatform:powerbi,[<PlatformInstance>.]<SemanticModelName>.<TableName>,<ENV>)
  ```

  Entity urn with `include_workspace_name_in_dataset_urn: true`

  ```
   urn:li:dataset:(urn:li:dataPlatform:powerbi,[<PlatformInstance>.].<WorkspaceName>.<SemanticModelName>.<TableName>,<ENV>)
  ```

  The config `include_workspace_name_in_dataset_urn` is default to `false` for backward compatibility, However, we recommend enabling this flag after performing the necessary cleanup.
  If stateful ingestion is enabled, running ingestion with the latest CLI version will handle the cleanup automatically. Otherwise, we recommend soft deleting all powerbi data via the DataHub CLI:
  `datahub delete --platform powerbi --soft` and then re-ingest with the latest CLI version, ensuring the `include_workspace_name_in_dataset_urn` configuration is set to true.

### Other Notable Changes

- #12236: Data flow and data job entities may additionally produce container aspect that will require a corresponding upgrade of server. Otherwise server can reject the aspect.
- #12056: The DataHub Airflow plugin now defaults to the v2 plugin implementation.
- #11742: For PowerBi ingestion, `use_powerbi_email` is now enabled by default when extracting ownership information.
- #11549 - Manage Operations Privilege is extended from throttle control to all system management and operations APIs.

## 0.14.1

### Breaking Changes

- #9857 (#10773) `lower` method was removed from `get_db_name` of `SQLAlchemySource` class. This change will affect the urns of all related to `SQLAlchemySource` entities.

  Old `urn`, where `data_base_name` is `Some_Database`:

  ```
  - urn:li:dataJob:(urn:li:dataFlow:(mssql,demodata.Foo.stored_procedures,PROD),Proc.With.SpecialChar)
  ```

  New `urn`, where `data_base_name` is `Some_Database`:

  ```
  - urn:li:dataJob:(urn:li:dataFlow:(mssql,DemoData.Foo.stored_procedures,PROD),Proc.With.SpecialChar)
  ```

  Re-running with stateful ingestion should automatically clear up the entities with old URNS and add entities with new URNs, therefore not duplicating the containers or jobs.

- #11313 - `datahub get` will no longer return a key aspect for entities that don't exist.
- #11369 - The default datahub-rest sink mode has been changed to `ASYNC_BATCH`. This requires a server with version 0.14.0+.
- #11214 Container properties aspect will produce an additional field that will require a corresponding upgrade of server. Otherwise server can reject the aspects.
- #10190 - `extractor_config.set_system_metadata` of `datahub` source has been moved to be a top level config in the recipe under `flags.set_system_metadata`

### Potential Downtime

### Deprecations

### Other Notable Changes

- Downgrade to previous version is not automatically supported.
- Data Product Properties Unset side effect introduced
  - Previously, Data Products could be set as linked to multiple Datasets if modified directly via the REST API rather than linked through the UI or GraphQL. This side effect aligns the REST API behavior with the GraphQL behavior by introducting a side effect that enforces the 1-to-1 constraint between Data Products and Datasets
  - NOTE: There is a pathological pattern of writes for Data Products that can introduce issues with write processing that can occur with this side effect. If you are constantly changing all of the Datasets associated with a Data Product back and forth between multiple Data Products it will result in a high volume of writes due to the need to unset previous associations.

## 0.14.0.2

### Breaking Changes

- Protobuf CLI will no longer create binary encoded protoc custom properties. Flag added `-protocProp` in case this
  behavior is required.
- #10814 Data flow info and data job info aspect will produce an additional field that will require a corresponding upgrade of server. Otherwise server can reject the aspects.
- #10868 - OpenAPI V3 - Creation of aspects will need to be wrapped within a `value` key and the API is now symmetric with respect to input and outputs.

Example Global Tags Aspect:

Previous:

```json
{
  "tags": [
    {
      "tag": "string",
      "context": "string"
    }
  ]
}
```

New (optional fields `systemMetadata` and `headers`):

```json
{
  "value": {
    "tags": [
      {
        "tag": "string",
        "context": "string"
      }
    ]
  },
  "systemMetadata": {},
  "headers": {}
}
```

- #10858 Profiling configuration for Glue source has been updated.

  Previously, the configuration was:

  ```yaml
  profiling: {}
  ```

  Now, it needs to be:

  ```yaml
  profiling:
    enabled: true
  ```

### Potential Downtime

### Deprecations

- OpenAPI v1: OpenAPI v1 is collectively defined as all endpoints which are not prefixed with `/v2` or `/v3`. The v1 endpoints
  will be deprecated in no less than 6 months. Endpoints will be replaced with equivalents in the `/v2` or `/v3` APIs.
  No loss of functionality expected unless explicitly mentioned in Breaking Changes.

### Other Notable Changes

- #10498 - Tableau ingestion can now be configured to ingest multiple sites at once and add the sites as containers. The feature is currently only available for Tableau Server.
- #10466 - Extends configuration in `~/.datahubenv` to match `DatahubClientConfig` object definition. See full configuration in https://datahubproject.io/docs/python-sdk/clients/. The CLI should now respect the updated configurations specified in `~/.datahubenv` across its functions and utilities. This means that for systems where ssl certification is disabled, setting `disable_ssl_verification: true` in `~./datahubenv` will apply to all CLI calls.
- #11002 - We will not auto-generate a `~/.datahubenv` file. You must either run `datahub init` to create that file, or set environment variables so that the config is loaded.
- #11023 - Added a new parameter to datahub's `put` cli command: `--run-id`. This parameter is useful to associate a given write to an ingestion process. A use-case can be mimick transformers when a transformer for aspect being written does not exist.
- #11051 - Ingestion reports will now trim the summary text to a maximum of 800k characters to avoid generating `dataHubExecutionRequestResult` that are too large for GMS to handle.

## 0.13.3

### Breaking Changes

- #10419 - `aws_region` is now a required configuration in the DynamoDB connector. The connector will no longer loop through all AWS regions; instead, it will only use the region passed into the recipe configuration.
- #10389 - Custom validators, mutators, side-effects dropped a previously required constructor
- #10472 - `RVW` added as a FabricType. No rollbacks allowed once metadata with this fabric type is added without manual cleanups in databases.

### Potential Downtime

### Deprecations

### Other Notable Change

## 0.13.1

### Breaking Changes

- #9934 and #10075 - Stateful ingestion is now enabled by default if a `pipeline_name` is set and either a datahub-rest sink or `datahub_api` is specified. It will still be disabled by default when any other sink type is used or if there is no pipeline name set.
- #10002 - The `DataHubGraph` client no longer makes a request to the backend during initialization. If you want to preserve the old behavior, call `graph.test_connection()` after constructing the client.
- #10026 - The dbt `use_compiled_code` option has been removed, because we now support capturing both source and compiled dbt SQL. This can be configured using `include_compiled_code`, which will be default enabled in 0.13.1.
- #10055 - Assertion entities generated by dbt are now associated with the dbt dataset entity, and not the entity in the data warehouse.
- #10090 - For Redshift ingestion, `use_lineage_v2` is now enabled by default.
- #10147 - For looker ingestion, the browse paths for looker Dashboard, Chart, View, Explore have been updated to align with Looker UI. This does not affect URNs or lineage but primarily affects (improves) browsing experience.
- #10164 - For dbt ingestion, `entities_enabled.model_performance` and `include_compiled_code` are now both enabled by default. Upgrading dbt ingestion will also require upgrading the backend to 0.13.1.
- #10066 - For view access controls, `SEARCH_AUTHORIZATION_ENABLED` replaced by `VIEW_AUTHORIZATION_ENABLED` to more accurately represent the feature.
- #8231 - Google Analytics 3 has been fully sunsetted by Google as of July 2023, so we now support GA4 thanks to this PR and no longer support GA3 (which would have been broken since last year anyways).
- #10278 - Renaming Presto-On-Hive Source to Hive Metastore source to reflect better its purpose

### Potential Downtime

### Deprecations

### Other Notable Changes

## 0.13.0

### Breaking Changes

- Updating MySQL version for quickstarts to 8.2, may cause quickstart issues for existing instances.
- Neo4j 5.x, may require migration from 4.x
- Build requires JDK17 (Runtime Java 11)
- Build requires Docker Compose > 2.20
- #9731 - The `acryl-datahub` CLI now requires Python 3.8+
- #9601 - The Unity Catalog(UC) ingestion source config `include_metastore` is now disabled by default. This change will affect the urns of all entities in the workspace.<br/>
  Entity Hierarchy with `include_metastore: true` (Old)

  ```
  - UC Metastore
    - Catalog
      - Schema
        - Table
  ```

  Entity Hierarchy with `include_metastore: false` (New)

  ```
  - Catalog
    - Schema
      - Table
  ```

  We recommend using `platform_instance` for differentiating across metastores.

  If stateful ingestion is enabled, running ingestion with latest cli version will perform all required cleanup. Otherwise, we recommend soft deleting all databricks data via the DataHub CLI:
  `datahub delete --platform databricks --soft` and then reingesting with latest cli version.

- #9601 - The Unity Catalog(UC) ingestion source config `include_hive_metastore` is now enabled by default. This requires config `warehouse_id` to be set. You can disable `include_hive_metastore` by setting it to `False` to avoid ingesting legacy hive metastore catalog in Databricks.
- #9904 - The default Redshift `table_lineage_mode` is now MIXED, instead of `STL_SCAN_BASED`. Improved lineage generation is also available by enabling `use_lineaege_v2`. This v2 implementation will become the default in a future release.

### Potential Downtime

### Deprecations

- Spark 2.x (including previous JDK8 build requirements)

### Other Notable Changes

## 0.12.1

### Breaking Changes

- #9244: The `redshift-legacy` and `redshift-legacy-usage` sources, which have been deprecated for >6 months, have been removed. The new `redshift` source is a superset of the functionality provided by those legacy sources.
- `database_alias` config is no longer supported in SQL sources namely - Redshift, MySQL, Oracle, Postgres, Trino, Presto-on-hive. The config will automatically be ignored if it's present in your recipe. It has been deprecated since v0.9.6.
- #9257: The Python SDK urn types are now autogenerated. The new classes are largely backwards compatible with the previous, manually written classes, but many older methods are now deprecated in favor of a more uniform interface. The only breaking change is that the signature for the director constructor e.g. `TagUrn("tag", ["tag_name"])` is no longer supported, and the simpler `TagUrn("tag_name")` should be used instead.
  The canonical place to import the urn classes from is `datahub.metadata.urns.*`. Other import paths, like `datahub.utilities.urns.corpuser_urn.CorpuserUrn` are retained for backwards compatibility, but are considered deprecated.
- #9286: The `DataHubRestEmitter.emit` method no longer returns anything. It previously returned a tuple of timestamps.
- #8951: A great expectations based profiler has been added for the Unity Catalog source.
  To use the old profiler, set `method: analyze` under the `profiling` section in your recipe.
  To use the new profiler, set `method: ge`. Profiling is disabled by default, so to enable it,
  one of these methods must be specified.

### Potential Downtime

### Deprecations

### Other Notable Changes

## 0.12.0

### Breaking Changes

- #8687 (datahub-helm #365 #353) - If Helm is used for installation and Neo4j is enabled, update the prerequisites Helm chart to version >=0.1.2 and adjust your value overrides in the `neo4j:` section according to the new structure.
- #9044 - GraphQL APIs for adding ownership now expect either an `ownershipTypeUrn` referencing a customer ownership type or a (deprecated) `type`. Where before adding an ownership without a concrete type was allowed, this is no longer the case. For simplicity you can use the `type` parameter which will get translated to a custom ownership type internally if one exists for the type being added.
- #9010 - In Redshift source's config `incremental_lineage` is set default to off.
- #8810 - Removed support for SQLAlchemy 1.3.x. Only SQLAlchemy 1.4.x is supported now.
- #8942 - Removed `urn:li:corpuser:datahub` owner for the `Measure`, `Dimension` and `Temporal` tags emitted
  by Looker and LookML source connectors.
- #8853 - The Airflow plugin no longer supports Airflow 2.0.x or Python 3.7. See the docs for more details.
- #8853 - Introduced the Airflow plugin v2. If you're using Airflow 2.3+, the v2 plugin will be enabled by default, and so you'll need to switch your requirements to include `pip install 'acryl-datahub-airflow-plugin[plugin-v2]'`. To continue using the v1 plugin, set the `DATAHUB_AIRFLOW_PLUGIN_USE_V1_PLUGIN` environment variable to `true`.
- #8943 - The Unity Catalog ingestion source has a new option `include_metastore`, which will cause all urns to be changed when disabled.
  This is currently enabled by default to preserve compatibility, but will be disabled by default and then removed in the future.
  If stateful ingestion is enabled, simply setting `include_metastore: false` will perform all required cleanup.
  Otherwise, we recommend soft deleting all databricks data via the DataHub CLI:
  `datahub delete --platform databricks --soft` and then reingesting with `include_metastore: false`.
- #8846 - Changed enum values in resource filters used by policies. `RESOURCE_TYPE` became `TYPE` and `RESOURCE_URN` became `URN`.
  Any existing policies using these filters (i.e. defined for particular `urns` or `types` such as `dataset`) need to be upgraded
  manually, for example by retrieving their respective `dataHubPolicyInfo` aspect and changing part using filter i.e.

```yaml
   "resources": {
     "filter": {
       "criteria": [
         {
           "field": "RESOURCE_TYPE",
           "condition": "EQUALS",
           "values": [
             "dataset"
           ]
         }
       ]
     }
```

into

```yaml
   "resources": {
     "filter": {
       "criteria": [
         {
           "field": "TYPE",
           "condition": "EQUALS",
           "values": [
             "dataset"
           ]
         }
       ]
     }
```

for example, using `datahub put` command. Policies can be also removed and re-created via UI.

- #9077 - The BigQuery ingestion source by default sets `match_fully_qualified_names: true`.
  This means that any `dataset_pattern` or `schema_pattern` specified will be matched on the fully
  qualified dataset name, i.e. `<project_name>.<dataset_name>`. We attempt to support the old
  pattern format by prepending `.*\\.` to dataset patterns lacking a period, so in most cases this
  should not cause any issues. However, if you have a complex dataset pattern, we recommend you
  manually convert it to the fully qualified format to avoid any potential issues.
- #9110 - The Unity Catalog source will now generate urns based on `env` properly. If you have
  been setting `env` in your recipe to something besides `PROD`, we will now generate urns
  with that new env variable, invalidating your existing urns.

### Potential Downtime

### Deprecations

### Other Notable Changes

- Session token configuration has changed, all previously created session tokens will be invalid and users will be prompted to log in. Expiration time has also been shortened which may result in more login prompts with the default settings.
  There should be no other interruption due to this change.

## 0.11.0

### Breaking Changes

### Potential Downtime

- #8611 Search improvements requires reindexing indices. A `system-update` job will run which will set indices to read-only and create a backup/clone of each index. During the reindexing new components will be prevented from start-up until the reindex completes. The logs of this job will indicate a % complete per index. Depending on index sizes and infrastructure this process can take 5 minutes to hours however as a rough estimate 1 hour for every 2.3 million entities.

### Deprecations

- #8525: In LDAP ingestor, the `manager_pagination_enabled` changed to general `pagination_enabled`
- MAE Events are no longer produced. MAE events have been deprecated for over a year.

### Other Notable Changes

- In this release we now enable you to create and delete pinned announcements on your DataHub homepage! If you have the “Manage Home Page Posts” platform privilege you’ll see a new section in settings called “Home Page Posts” where you can create and delete text posts and link posts that your users see on the home page.
- The new search and browse experience, which was first made available in the previous release behind a feature flag, is now on by default. Check out our release notes for v0.10.5 to get more information and documentation on this new Browse experience.
- In addition to the ranking changes mentioned above, this release includes changes to the highlighting of search entities to understand why they match your query. You can also sort your results alphabetically or by last updated times, in addition to relevance. In this release, we suggest a correction if your query has a typo in it.
- #8300: Clickhouse source now inherited from TwoTierSQLAlchemy. In old way we have platform_instance -> container -> co
  container db (None) -> container schema and now we have platform_instance -> container database.
- #8300: Added `uri_opts` argument; now we can add any options for clickhouse client.
- #8659: BigQuery ingestion no longer creates DataPlatformInstance aspects by default.
  This will only affect users that were depending on this aspect for custom functionality,
  and can be enabled via the `include_data_platform_instance` config option.
- OpenAPI entity and aspect endpoints expanded to improve developer experience when using this API with additional aspects to be added in the near future.
- The CLI now supports recursive deletes.
- Batching of default aspects on initial ingestion (SQL)
- Improvements to multi-threading. Ingestion recipes, if previously reduced to 1 thread, can be restored to the 15 thread default.
- Gradle 7 upgrade moderately improves build speed
- DataHub Ingestion slim images reduced in size by 2GB+
- Glue Schema Registry fixed

## 0.10.5

### Breaking Changes

- #8201: Python SDK: In the DataFlow class, the `cluster` argument is deprecated in favor of `env`.
- #8263: Okta source config option `okta_profile_to_username_attr` default changed from `login` to `email`.
  This determines which Okta profile attribute is used for the corresponding DataHub user
  and thus may change what DataHub users are generated by the Okta source. And in a follow up `okta_profile_to_username_regex` has been set to `.*` which taken together with previous change brings the defaults in line with OIDC.
- #8331: For all sql-based sources that support profiling, you can no longer specify
  `profile_table_level_only` together with `include_field_xyz` config options to ingest
  certain column-level metrics. Instead, set `profile_table_level_only` to `false` and
  individually enable / disable desired field metrics.
- #8451: The `bigquery-beta` and `snowflake-beta` source aliases have been dropped. Use `bigquery` and `snowflake` as the source type instead.
- #8472: Ingestion runs created with Pipeline.create will show up in the DataHub ingestion tab as CLI-based runs. To revert to the previous behavior of not showing these runs in DataHub, pass `no_default_report=True`.
- #8513: `snowflake` connector will use user's `email` attribute as is in urn. To revert to previous behavior disable `email_as_user_identifier` in recipe.

### Potential Downtime

- BrowsePathsV2 upgrade will now be handled by the `system-update` job in non-blocking mode. This process generates data needed for the new search
  and browse feature. This process must complete before enabling the new search and browse UI and while upgrading entities will be missing from the UI.
  If not using the new search and browse UI, there will be no impact and the update will complete in the background.

### Deprecations

- #8198: In the Python SDK, the `PlatformKey` class has been renamed to `ContainerKey`.

### Other Notable Changes

0.10.5 introduces the new Unified Search & Browse experience and is disabled by default. You can control whether or not you want to see just the new search filtering experience, the new search and browse experience together, or keep the existing search and browse experiences by toggling the two environment variable feature flags `SHOW_SEARCH_FILTERS_V2` and `SHOW_BROWSE_V2` in your GMS container.

**Upgrade Considerations:**

- With the release of Browse V2, we have created a job to run in GMS that will backfill your existing data with new `browsePathsV2` aspects. This job loops over entity types that need a `browsePathsV2` aspect (Dataset, Dashboard, Chart, DataJob, DataFlow, MLModel, MLModelGroup, MLFeatureTable, and MLFeature) and generates one for them. For entities that may have Container parents (Datasets and Dashboards) we will try to fetch their parent containers in order to generate this new aspect. For those deployments with large amounts of data, consider whether running this upgrade job makes sense as it may be a heavy operation and take some time to complete. If you wish to skip this job, simply set the `BACKFILL_BROWSE_PATHS_V2` environment variable flag to `false` in your GMS container. Without this backfill job, though, you will need to rely on the newest CLI of ingestion to create these `browsePathsV2` aspects when running ingestion otherwise your browse sidebar will be out-of-sync.
- Since the new browse experience replaces the old, consider whether having the `SHOW_BROWSE_V2` environment variable feature flag on is the right decision for your organization. If you’re creating custom browse paths with the `browsePaths` aspect, you can continue to do the same with the new experience, however you will have to generate `browsePathsV2` aspects instead which are documented [here](https://datahubproject.io/docs/browsev2/browse-paths-v2/).

## 0.10.4

### Breaking Changes

### Potential Downtime

### Deprecations

- #8045: With the introduction of custom ownership types, the `Owner` aspect has been updated where the `type` field is deprecated in favor of a new field `typeUrn`. This latter field is an urn reference to the new OwnershipType entity. GraphQL endpoints have been updated to use the new field. For pre-existing ownership aspect records, DataHub now has logic to map the old field to the new field.

### Other notable Changes

- #8191: Updates GMS's health check endpoint to account for its dependency on external components. Notably, at this time, elasticsearch. This means that DataHub operators can now use GMS health status more reliably.

## 0.10.3

### Breaking Changes

- #7900: The `catalog_pattern` and `schema_pattern` options of the Unity Catalog source now match against the fully qualified name of the catalog/schema instead of just the name. Unless you're using regex `^` in your patterns, this should not affect you.
- #7942: Renaming the `containerPath` aspect to `browsePathsV2`. This means any data with the aspect name `containerPath` will be invalid. We had not exposed this in the UI or used it anywhere, but it was a model we recently merged to open up other work. This should not affect many people if anyone at all unless you were manually creating `containerPath` data through ingestion on your instance.
- #8068: In the `datahub delete` CLI, if an `--entity-type` filter is not specified, we automatically delete across all entity types. The previous behavior was to use a default entity type of dataset.
- #8068: In the `datahub delete` CLI, the `--start-time` and `--end-time` parameters are not required for timeseries aspect hard deletes. To recover the previous behavior of deleting all data, use `--start-time min --end-time max`.

### Potential Downtime

### Deprecations

- The signature of `Source.get_workunits()` is changed from `Iterable[WorkUnit]` to the more restrictive `Iterable[MetadataWorkUnit]`.
- Legacy usage creation via the `UsageAggregation` aspect, `/usageStats?action=batchIngest` GMS endpoint, and `UsageStatsWorkUnit` metadata-ingestion class are all deprecated.

### Other notable Changes

## 0.10.2

### Breaking Changes

- #7016 Add `add_database_name_to_urn` flag to Oracle source which ensure that Dataset urns have the DB name as a prefix to prevent collision (.e.g. {database}.{schema}.{table}). ONLY breaking if you set this flag to true, otherwise behavior remains the same.
- The Airflow plugin no longer includes the DataHub Kafka emitter by default. Use `pip install acryl-datahub-airflow-plugin[datahub-kafka]` for Kafka support.
- The Airflow lineage backend no longer includes the DataHub Kafka emitter by default. Use `pip install acryl-datahub[airflow,datahub-kafka]` for Kafka support.
- Java SDK PatchBuilders have been modified in a backwards incompatible way to align more with the Python SDK and support more use cases. Any application utilizing the Java SDK for patch building may be affected on upgrading this dependency.

### Deprecations

- The docker image and script for updating from Elasticsearch 6 to 7 is no longer being maintained and will be removed from the `/contrib` section of
  the repository. Please refer to older releases if needed.

## 0.10.0

### Breaking Changes

- #7103 This should only impact users who have configured explicit non-default names for DataHub's Kafka topics. The environment variables used to configure Kafka topics for DataHub used in the `kafka-setup` docker image have been updated to be in-line with other DataHub components, for more info see our docs on [Configuring Kafka in DataHub
  ](https://datahubproject.io/docs/how/kafka-config). They have been suffixed with `_TOPIC` where as now the correct suffix is `_TOPIC_NAME`. This change should not affect any user who is using default Kafka names.
- #6906 The Redshift source has been reworked and now also includes usage capabilities. The old Redshift source was renamed to `redshift-legacy`. The `redshift-usage` source has also been renamed to `redshift-usage-legacy` will be removed in the future.

### Potential Downtime

- #6894 Search improvements requires reindexing indices. A `system-update` job will run which will set indices to read-only and create a backup/clone of each index. During the reindexing new components will be prevented from start-up until the reindex completes. The logs of this job will indicate a % complete per index. Depending on index sizes and infrastructure this process can take 5 minutes to hours however as a rough estimate 1 hour for every 2.3 million entities.

#### Helm Notes

Helm without `--atomic`: The default timeout for an upgrade command is 5 minutes. If the reindex takes longer (depending on data size) it will continue to run in the background even though helm will report a failure. Allow this job to finish and then re-run the helm upgrade command.

Helm with `--atomic`: In general, it is recommended to not use the `--atomic` setting for this particular upgrade since the system update job will be terminated before completion. If `--atomic` is preferred, then increase the timeout using the `--timeout` flag to account for the reindexing time (see note above for estimating this value).

### Deprecations

## 0.9.6

### Breaking Changes

- #6742 The metadata file sink's output format no longer contains nested JSON strings for MCP aspects, but instead unpacks the stringified JSON into a real JSON object. The previous sink behavior can be recovered using the `legacy_nested_json_string` option. The file source is backwards compatible and supports both formats.
- #6901 The `env` and `database_alias` fields have been marked deprecated across all sources. We recommend using `platform_instance` where possible instead.

### Potential Downtime

### Deprecations

- #6851 - Sources bigquery-legacy and bigquery-usage-legacy have been removed

### Other notable Changes

- If anyone faces issues with login please clear your cookies. Some security updates are part of this release. That may cause login issues until cookies are cleared.

## 0.9.4 / 0.9.5

### Breaking Changes

- #6243 apache-ranger authorizer is no longer the core part of DataHub GMS, and it is shifted as plugin. Please refer updated documentation [Configuring Authorization with Apache Ranger](./configuring-authorization-with-apache-ranger.md#configuring-your-datahub-deployment) for configuring `apache-ranger-plugin` in DataHub GMS.
- #6243 apache-ranger authorizer as plugin is not supported in DataHub Kubernetes deployment.
- #6243 Authentication and Authorization plugins configuration are removed from [application.yaml](../../metadata-service/configuration/src/main/resources/application.yaml). Refer documentation [Migration Of Plugins From application.yaml](../plugins.md#migration-of-plugins-from-applicationyml) for migrating any existing custom plugins.
- `datahub check graph-consistency` command has been removed. It was a beta API that we had considered but decided there are better solutions for this. So removing this.
- `graphql_url` option of `powerbi-report-server` source deprecated as the options is not used.
- #6789 BigQuery ingestion: If `enable_legacy_sharded_table_support` is set to False, sharded table names will be suffixed with \_yyyymmdd to make sure they don't clash with non-sharded tables. This means if stateful ingestion is enabled then old sharded tables will be recreated with a new id and attached tags/glossary terms/etc will need to be added again. _This behavior is not enabled by default yet, but will be enabled by default in a future release._

### Potential Downtime

### Deprecations

### Other notable Changes

- #6611 - Snowflake `schema_pattern` now accepts pattern for fully qualified schema name in format `<catalog_name>.<schema_name>` by setting config `match_fully_qualified_names : True`. Current default `match_fully_qualified_names: False` is only to maintain backward compatibility. The config option `match_fully_qualified_names` will be deprecated in future and the default behavior will assume `match_fully_qualified_names: True`."
- #6636 - Sources `snowflake-legacy` and `snowflake-usage-legacy` have been removed.

## 0.9.3

### Breaking Changes

- The beta `datahub check graph-consistency` command has been removed.

### Potential Downtime

### Deprecations

- PowerBI source: `workspace_id_pattern` is introduced in place of `workspace_id`. `workspace_id` is now deprecated and set for removal in a future version.

### Other notable Changes

## 0.9.2

- LookML source will only emit views that are reachable from explores while scanning your git repo. Previous behavior can be achieved by setting `emit_reachable_views_only` to False.
- LookML source will always lowercase urns for lineage edges from views to upstream tables. There is no fallback provided to previous behavior because it was inconsistent in application of lower-casing earlier.
- dbt config `node_type_pattern` which was previously deprecated has been removed. Use `entities_enabled` instead to control whether to emit metadata for sources, models, seeds, tests, etc.
- The dbt source will always lowercase urns for lineage edges to the underlying data platform.
- The DataHub Airflow lineage backend and plugin no longer support Airflow 1.x. You can still run DataHub ingestion in Airflow 1.x using the [PythonVirtualenvOperator](https://airflow.apache.org/docs/apache-airflow/1.10.15/_api/airflow/operators/python_operator/index.html?highlight=pythonvirtualenvoperator#airflow.operators.python_operator.PythonVirtualenvOperator).

### Breaking Changes

- #6570 `snowflake` connector now populates created and last modified timestamps for snowflake datasets and containers. This version of snowflake connector will not work with **datahub-gms** version older than `v0.9.3`

### Potential Downtime

### Deprecations

### Other notable Changes

## 0.9.1

### Breaking Changes

- We have promoted `bigquery-beta` to `bigquery`. If you are using `bigquery-beta` then change your recipes to use the type `bigquery`.

### Potential Downtime

### Deprecations

### Other notable Changes

## 0.9.0

### Breaking Changes

- Java version 11 or greater is required.
- For any of the GraphQL search queries, the input no longer supports value but instead now accepts a list of values. These values represent an OR relationship where the field value must match any of the values.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.45`

### Breaking Changes

- The `getNativeUserInviteToken` and `createNativeUserInviteToken` GraphQL endpoints have been renamed to
  `getInviteToken` and `createInviteToken` respectively. Additionally, both now accept an optional `roleUrn` parameter.
  Both endpoints also now require the `MANAGE_POLICIES` privilege to execute, rather than `MANAGE_USER_CREDENTIALS`
  privilege.
- One of the default policies shipped with DataHub (`urn:li:dataHubPolicy:7`, or `All Users - All Platform Privileges`)
  has been edited to no longer include `MANAGE_POLICIES`. Its name has consequently been changed to
  `All Users - All Platform Privileges (EXCEPT MANAGE POLICIES)`. This change was made to prevent all users from
  effectively acting as superusers by default.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.44`

### Breaking Changes

- Browse Paths have been upgraded to a new format to align more closely with the intention of the feature.
  Learn more about the changes, including steps on upgrading, here: <https://datahubproject.io/docs/advanced/browse-paths-upgrade>
- The dbt ingestion source's `disable_dbt_node_creation` and `load_schema` options have been removed. They were no longer necessary due to the recently added sibling entities functionality.
- The `snowflake` source now uses newer faster implementation (earlier `snowflake-beta`). Config properties `provision_role` and `check_role_grants` are not supported. Older `snowflake` and `snowflake-usage` are available as `snowflake-legacy` and `snowflake-usage-legacy` sources respectively.

### Potential Downtime

- [Helm] If you're using Helm, please ensure that your version of the `datahub-actions` container is bumped to `v0.0.7` or `head`.
  This version contains changes to support running ingestion in debug mode. Previous versions are not compatible with this release.
  Upgrading to helm chart version `0.2.103` will ensure that you have the compatible versions by default.

### Deprecations

### Other notable Changes

## `v0.8.42`

### Breaking Changes

- Python 3.6 is no longer supported for metadata ingestion
- #5451 `GMS_HOST` and `GMS_PORT` environment variables deprecated in `v0.8.39` have been removed. Use `DATAHUB_GMS_HOST` and `DATAHUB_GMS_PORT` instead.
- #5478 DataHub CLI `delete` command when used with `--hard` option will delete soft-deleted entities which match the other filters given.
- #5471 Looker now populates `userEmail` in dashboard user usage stats. This version of looker connnector will not work with older version of **datahub-gms** if you have `extract_usage_history` looker config enabled.
- #5529 - `ANALYTICS_ENABLED` environment variable in **datahub-gms** is now deprecated. Use `DATAHUB_ANALYTICS_ENABLED` instead.
- #5485 `--include-removed` option was removed from delete CLI

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.41`

### Breaking Changes

- The `should_overwrite` flag in `csv-enricher` has been replaced with `write_semantics` to match the format used for other sources. See the [documentation](https://datahubproject.io/docs/generated/ingestion/sources/csv-enricher/) for more details
- Closing an authorization hole in creating tags adding a Platform Privilege called `Create Tags` for creating tags. This is assigned to `datahub` root user, along
  with default All Users policy. Notice: You may need to add this privilege (or `Manage Tags`) to existing users that need the ability to create tags on the platform.
- #5329 Below profiling config parameters are now supported in `BigQuery`:

  - profiling.profile_if_updated_since_days (default=1)
  - profiling.profile_table_size_limit (default=1GB)
  - profiling.profile_table_row_limit (default=50000)

  Set above parameters to `null` if you want older behaviour.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.40`

### Breaking Changes

- #5240 `lineage_client_project_id` in `bigquery` source is removed. Use `storage_project_id` instead.

### Potential Downtime

### Deprecations

### Other notable Changes

## `v0.8.39`

### Breaking Changes

- Refactored the `health` field of the `Dataset` GraphQL Type to be of type **list of HealthStatus** (was type **HealthStatus**). See [this PR](https://github.com/datahub-project/datahub/pull/5222/files) for more details.

### Potential Downtime

### Deprecations

- #4875 Lookml view file contents will no longer be populated in custom_properties, instead view definitions will be always available in the View Definitions tab.
- #5208 `GMS_HOST` and `GMS_PORT` environment variables being set in various containers are deprecated in favour of `DATAHUB_GMS_HOST` and `DATAHUB_GMS_PORT`.
- `KAFKA_TOPIC_NAME` environment variable in **datahub-mae-consumer** and **datahub-gms** is now deprecated. Use `METADATA_AUDIT_EVENT_NAME` instead.
- `KAFKA_MCE_TOPIC_NAME` environment variable in **datahub-mce-consumer** and **datahub-gms** is now deprecated. Use `METADATA_CHANGE_EVENT_NAME` instead.
- `KAFKA_FMCE_TOPIC_NAME` environment variable in **datahub-mce-consumer** and **datahub-gms** is now deprecated. Use `FAILED_METADATA_CHANGE_EVENT_NAME` instead.

### Other notable Changes

- #5132 Profile tables in `snowflake` source only if they have been updated since configured (default: `1`) number of day(s). Update the config `profiling.profile_if_updated_since_days` as per your profiling schedule or set it to `None` if you want older behaviour.

## `v0.8.38`

### Breaking Changes

### Potential Downtime

### Deprecations

### Other notable Changes

- Create & Revoke Access Tokens via the UI
- Create and Manage new users via the UI
- Improvements to Business Glossary UI
- FIX - Do not require reindexing to migrate to using the UI business glossary

## `v0.8.36`

### Breaking Changes

- In this release we introduce a brand new Business Glossary experience. With this new experience comes some new ways of indexing data in order to make viewing and traversing the different levels of your Glossary possible. Therefore, you will have to [restore your indices](https://datahubproject.io/docs/how/restore-indices/) in order for the new Glossary experience to work for users that already have existing Glossaries. If this is your first time using DataHub Glossaries, you're all set!

### Potential Downtime

### Deprecations

### Other notable Changes

- #4961 Dropped profiling is not reported by default as that caused a lot of spurious logging in some cases. Set `profiling.report_dropped_profiles` to `True` if you want older behaviour.

## `v0.8.35`

### Breaking Changes

### Potential Downtime

### Deprecations

- #4875 Lookml view file contents will no longer be populated in custom_properties, instead view definitions will be always available in the View Definitions tab.

### Other notable Changes

## `v0.8.34`

### Breaking Changes

- #4644 Remove `database` option from `snowflake` source which was deprecated since `v0.8.5`
- #4595 Rename confusing config `report_upstream_lineage` to `upstream_lineage_in_report` in `snowflake` connector which was added in `0.8.32`

### Potential Downtime

### Deprecations

- #4644 `host_port` option of `snowflake` and `snowflake-usage` sources deprecated as the name was confusing. Use `account_id` option instead.

### Other notable Changes

- #4760 `check_role_grants` option was added in `snowflake` to disable checking roles in `snowflake` as some people were reporting long run times when checking roles.
