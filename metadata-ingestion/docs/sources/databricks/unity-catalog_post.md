#### Troubleshooting

##### No data lineage captured or missing lineage

Check that you meet the [Unity Catalog lineage requirements](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements).

Also check the [Unity Catalog limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to make sure that lineage would be expected to exist in this case.

##### Lineage extraction is too slow

Currently, there is no way to get table or column lineage in bulk from the Databricks Unity Catalog REST api. Table lineage calls require one API call per table, and column lineage calls require one API call per column. If you find metadata extraction taking too long, you can turn off column level lineage extraction via the `include_column_lineage` config flag.
