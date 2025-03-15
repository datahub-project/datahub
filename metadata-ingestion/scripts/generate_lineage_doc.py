import os
import json
from typing import Dict
from docgen_types import Platform
from docgen import get_capability_text, get_capability_supported_badge
def generate(platforms: Dict[str, Platform]) -> None:
    source_dir = "../docs/generated/lineage"
    os.makedirs(source_dir, exist_ok=True)
    doc_file = f"{source_dir}/lineage-feature-guide.md"
    with open(doc_file, "w+") as f:
        f.write(
            "import FeatureAvailability from '@site/src/components/FeatureAvailability';\n\n"
        )
        f.write("# About DataHub Lineage\n\n")
        f.write("<FeatureAvailability/>\n")

        f.write(
            """
Data lineage is a **map that shows how data flows through your organization.** It details where your data originates, how it travels, and where it ultimately ends up. 
This can happen within a single system (like data moving between Snowflake tables) or across various platforms.

With data lineage, you can
- Maintaining Data Integrity
- Simplify and Refine Complex Relationships
- Perform [Lineage Impact Analysis](../../act-on-metadata/impact-analysis.md)
- [Propagate Metadata](https://blog.datahubproject.io/acryl-data-introduces-lineage-support-and-automated-propagation-of-governance-information-for-339c99536561) Across Lineage


## Viewing Lineage

You can view lineage under **Lineage** tab or **Lineage Visualization** screen.


<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-tab.png" />
</p>

By default, the UI shows the latest version of the lineage. The time picker can be used to filter out edges within the latest version to exclude those that were last updated outside of the time window. Selecting time windows in the patch will not show you historical lineages. It will only filter the view of the latest version of the lineage.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/lineage-view.png" />
</p>

In this example, data flows from Airflow/BigQuery to Snowflake tables, then to the Hive dataset, and ultimately to the features of Machine Learning Models.


:::tip The Lineage Tab is greyed out - why can’t I click on it?
This means you have not yet ingested lineage metadata for that entity. Please ingest lineage to proceed.

:::

## Column Level Lineage Support

Column-level lineage **tracks changes and movements for each specific data column.** This approach is often contrasted with table-level lineage, which specifies lineage at the table level.
Below is how column-level lineage can be set with dbt and Postgres tables.

<p align="center">
<img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/column-level-lineage.png" />
</p>

## Adding Lineage

### Ingestion Source

If you're using an ingestion source that supports extraction of Lineage (e.g. **Table Lineage Capability**), then lineage information can be extracted automatically.
For detailed instructions, refer to the [source documentation](https://datahubproject.io/integrations) for the source you are using.

### UI

As of `v0.9.5`, DataHub supports the manual editing of lineage between entities. Data experts are free to add or remove upstream and downstream lineage edges in both the Lineage Visualization screen as well as the Lineage tab on entity pages. Use this feature to supplement automatic lineage extraction or establish important entity relationships in sources that do not support automatic extraction. Editing lineage by hand is supported for Datasets, Charts, Dashboards, and Data Jobs.
Please refer to our [UI Guides on Lineage](../../features/feature-guides/ui-lineage.md) for more information.

:::caution Recommendation on UI-based lineage

Lineage added by hand and programmatically may conflict with one another to cause unwanted overwrites.
It is strongly recommend that lineage is edited manually in cases where lineage information is not also extracted in automated fashion, e.g. by running an ingestion source.

:::

### API

If you are not using a Lineage-support ingestion source, you can programmatically emit lineage edges between entities via API.
Please refer to [API Guides on Lineage](../../api/tutorials/lineage.md) for more information.


## Lineage Support

DataHub supports **[automatic table- and column-level lineage detection](#automatic-lineage-extraction-support)** from BigQuery, Snowflake, dbt, Looker, PowerBI, and 20+ modern data tools. 
For data tools with limited native lineage tracking, [**DataHub's SQL Parser**](../../lineage/sql_parsing.md) detects lineage with 97-99% accuracy, ensuring teams will have high quality lineage graphs across all corners of their data stack.

### Types of Lineage Connections

Types of lineage connections supported in DataHub and the example codes are as follows.

* Dataset to Dataset
    * [Dataset Lineage](../../../metadata-ingestion/examples/library/lineage_emitter_rest.py)
    * [Finegrained Dataset Lineage](../../../metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py)
    * [Datahub BigQuery Lineage](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql/snowflake.py#L249)
    * [Dataset Lineage via MCPW REST Emitter](../../../metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py)
    * [Dataset Lineage via Kafka Emitter](../../../metadata-ingestion/examples/library/lineage_emitter_kafka.py)
* [DataJob to DataFlow](../../../metadata-ingestion/examples/library/lineage_job_dataflow.py)
* [DataJob to Dataset](../../../metadata-ingestion/examples/library/lineage_dataset_job_dataset.py)
* [Chart to Dashboard](../../../metadata-ingestion/examples/library/lineage_chart_dashboard.py)
* [Chart to Dataset](../../../metadata-ingestion/examples/library/lineage_dataset_chart.py)

### Automatic Lineage Extraction Support

This is a summary of automatic lineage extraciton support in our data source. Please refer to the **Important Capabilities** table in the source documentation. Note that even if the source does not support automatic extraction, you can still add lineage manually using our API & SDKs.\n"""
        )

        f.write(
            "\n| Source | Table-Level Lineage | Column-Level Lineage | Related Configs |\n"
        )
        f.write("| ---------- | ------ | ----- |----- |\n")

        for platform_id, platform in platforms.items():
            for plugin in sorted(
                    platform.plugins.values(),
                    key=lambda x: str(x.doc_order) if x.doc_order else x.name,
            ):
                if len(platform.plugins) > 1:
                    # We only need to show this if there are multiple modules.
                    platform_plugin_name = f"{platform.name} `{plugin.name}`"
                else:
                    platform_plugin_name = platform.name

                # Initialize variables
                table_level_supported = "❌"
                column_level_supported = "❌"
                config_names = ""

                if plugin.capabilities and len(plugin.capabilities):
                    plugin_capabilities = plugin.capabilities

                    for cap_setting in plugin_capabilities:
                        capability_text = get_capability_text(cap_setting.capability)
                        capability_supported = get_capability_supported_badge(
                            cap_setting.supported
                        )

                        if (
                                capability_text == "Table-Level Lineage"
                                and capability_supported == "✅"
                        ):
                            table_level_supported = "✅"

                        if (
                                capability_text == "Column-level Lineage"
                                and capability_supported == "✅"
                        ):
                            column_level_supported = "✅"

                if not (table_level_supported == "❌" and column_level_supported == "❌"):
                    if plugin.config_json_schema:
                        config_properties = json.loads(plugin.config_json_schema).get(
                            "properties", {}
                        )
                        config_names = "<br />".join(
                            [
                                f"- {property_name}"
                                for property_name in config_properties
                                if "lineage" in property_name
                            ]
                        )
                lineage_not_applicable_sources = [
                    "azure-ad",
                    "csv",
                    "demo-data",
                    "dynamodb",
                    "iceberg",
                    "json-schema",
                    "ldap",
                    "openapi",
                    "pulsar",
                    "sqlalchemy",
                ]
                if platform_id not in lineage_not_applicable_sources:
                    f.write(
                        f"| [{platform_plugin_name}](../../generated/ingestion/sources/{platform_id}.md) | {table_level_supported} | {column_level_supported} | {config_names}|\n"
                    )

        f.write(
            """

### SQL Parser Lineage Extraction

If you’re using a different database system for which we don’t support column-level lineage out of the box, but you do have a database query log available, 
we have a SQL queries connector that generates column-level lineage and detailed table usage statistics from the query log.

If these does not suit your needs, you can use the new `DataHubGraph.parse_sql_lineage()` method in our SDK. (See the source code [here](https://datahubproject.io/docs/python-sdk/clients/))

For more information, refer to the [Extracting Column-Level Lineage from SQL](https://blog.datahubproject.io/extracting-column-level-lineage-from-sql-779b8ce17567) 


:::tip Our Roadmap
We're actively working on expanding lineage support for new data sources.
Visit our [Official Roadmap](https://feature-requests.datahubproject.io/roadmap) for upcoming updates!
:::

## References

- [DataHub Basics: Lineage 101](https://www.youtube.com/watch?v=rONGpsndzRw&t=1s)
- [DataHub November 2022 Town Hall](https://www.youtube.com/watch?v=BlCLhG8lGoY&t=1s) - Including Manual Lineage Demo
- [Data in Context: Lineage Explorer in DataHub](https://blog.datahubproject.io/data-in-context-lineage-explorer-in-datahub-a53a9a476dc4)
- [Harnessing the Power of Data Lineage with DataHub](https://blog.datahubproject.io/harnessing-the-power-of-data-lineage-with-datahub-ad086358dec4)
- [Data Lineage: What It Is And Why It Matters](https://blog.datahubproject.io/data-lineage-what-it-is-and-why-it-matters-1a8d9846f0bd)
                        """
        )

    print("Lineage Documentation Generation Complete")
