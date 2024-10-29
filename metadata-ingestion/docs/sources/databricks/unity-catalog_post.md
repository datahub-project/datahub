

### Advanced

#### Multiple Databricks Workspaces

If you have multiple databricks workspaces **that point to the same Unity Catalog metastore**, our suggestion is to use separate recipes for ingesting the workspace-specific Hive Metastore catalog and Unity Catalog metastore's information schema.

To ingest Hive metastore information schema
- Setup one ingestion recipe per workspace
- Use platform instance equivalent to workspace name
- Ingest only hive_metastore catalog in the recipe using config `catalogs: ["hive_metastore"]`

To ingest Unity Catalog information schema
- Disable hive metastore catalog ingestion in the recipe using config `include_hive_metastore: False`
- Ideally, just ingest from one workspace
- To ingest from both workspaces (e.g. if each workspace has different permissions and therefore restricted view of the UC metastore):
    - Use same platform instance for all workspaces using same UC metastore
    - Ingest usage from only one workspace (you lose usage from other workspace)
    - Use filters to only ingest each catalog once, but shouldn’t be necessary


### Troubleshooting

#### No data lineage captured or missing lineage

Check that you meet the [Unity Catalog lineage requirements](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements).

Also check the [Unity Catalog limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to make sure that lineage would be expected to exist in this case.

#### Lineage extraction is too slow

Currently, there is no way to get table or column lineage in bulk from the Databricks Unity Catalog REST api. Table lineage calls require one API call per table, and column lineage calls require one API call per column. If you find metadata extraction taking too long, you can turn off column level lineage extraction via the `include_column_lineage` config flag.
