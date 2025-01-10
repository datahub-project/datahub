import dataclasses
import glob
import json
import logging
import os
import pathlib
import re
import sys
import textwrap
from importlib.metadata import metadata, requires
from typing import Dict, List, Optional

import click
from docgen_types import Platform, Plugin
from docs_config_table import gen_md_table_from_json_schema

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.decorators import SourceCapability, SupportStatus
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)


def get_snippet(long_string: str, max_length: int = 100) -> str:
    snippet = ""
    if len(long_string) > max_length:
        snippet = long_string[:max_length].strip() + "... "
    else:
        snippet = long_string.strip()

    snippet = snippet.replace("\n", " ")
    snippet = snippet.strip() + " "
    return snippet


def get_support_status_badge(support_status: SupportStatus) -> str:
    if support_status == SupportStatus.CERTIFIED:
        return "![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)"
    if support_status == SupportStatus.INCUBATING:
        return "![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)"
    if support_status == SupportStatus.TESTING:
        return "![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)"

    return ""


def get_capability_supported_badge(supported: bool) -> str:
    return "✅" if supported else "❌"


def get_capability_text(src_capability: SourceCapability) -> str:
    """
    Returns markdown format cell text for a capability, hyperlinked to capability feature page if known
    """
    capability_docs_mapping: Dict[SourceCapability, str] = {
        SourceCapability.DELETION_DETECTION: "../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal",
        SourceCapability.DOMAINS: "../../../domains.md",
        SourceCapability.PLATFORM_INSTANCE: "../../../platform-instances.md",
        SourceCapability.DATA_PROFILING: "../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md",
        SourceCapability.CLASSIFICATION: "../../../../metadata-ingestion/docs/dev_guides/classification.md",
    }

    capability_doc = capability_docs_mapping.get(src_capability)
    return (
        src_capability.value
        if not capability_doc
        else f"[{src_capability.value}]({capability_doc})"
    )


def does_extra_exist(extra_name: str) -> bool:
    for key, value in metadata("acryl-datahub").items():
        if key == "Provides-Extra" and value == extra_name:
            return True
    return False


def get_additional_deps_for_extra(extra_name: str) -> List[str]:
    all_requirements = requires("acryl-datahub") or []
    # filter for base dependencies
    base_deps = set([x.split(";")[0] for x in all_requirements if "extra ==" not in x])
    # filter for dependencies for this extra
    extra_deps = set(
        [x.split(";")[0] for x in all_requirements if f"extra == '{extra_name}'" in x]
    )
    # calculate additional deps that this extra adds
    delta_deps = extra_deps - base_deps
    return list(delta_deps)


def relocate_path(orig_path: str, relative_path: str, relocated_path: str) -> str:
    newPath = os.path.join(os.path.dirname(orig_path), relative_path)
    assert os.path.exists(newPath)

    newRelativePath = os.path.relpath(newPath, os.path.dirname(relocated_path))
    return newRelativePath


def rewrite_markdown(file_contents: str, path: str, relocated_path: str) -> str:
    def new_url(original_url: str, file_path: str) -> str:
        if original_url.startswith(("http://", "https://", "#")):
            return original_url
        import pathlib

        file_ext = pathlib.Path(original_url).suffix
        if file_ext.startswith(".md"):
            return original_url
        elif file_ext in [".png", ".svg", ".gif", ".pdf"]:
            new_url = relocate_path(path, original_url, relocated_path)
            return new_url
        return original_url

    # Look for the [text](url) syntax. Note that this will also capture images.
    #
    # We do a little bit of parenthesis matching here to account for parens in URLs.
    # See https://stackoverflow.com/a/17759264 for explanation of the second capture group.
    new_content = re.sub(
        r"\[(.*?)\]\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)",
        lambda x: f"[{x.group(1)}]({new_url(x.group(2).strip(),path)})",  # type: ignore
        file_contents,
    )

    new_content = re.sub(
        # Also look for the [text]: url syntax.
        r"^\[(.+?)\]\s*:\s*(.+?)\s*$",
        lambda x: f"[{x.group(1)}]: {new_url(x.group(2), path)}",
        new_content,
    )
    return new_content


def load_plugin(plugin_name: str, out_dir: str) -> Plugin:
    logger.debug(f"Loading {plugin_name}")
    class_or_exception = source_registry._ensure_not_lazy(plugin_name)
    if isinstance(class_or_exception, Exception):
        raise class_or_exception
    source_type = source_registry.get(plugin_name)
    logger.debug(f"Source class is {source_type}")

    if hasattr(source_type, "get_platform_name"):
        platform_name = source_type.get_platform_name()
    else:
        platform_name = (
            plugin_name.title()
        )  # we like platform names to be human readable

    platform_id = None
    if hasattr(source_type, "get_platform_id"):
        platform_id = source_type.get_platform_id()
    if platform_id is None:
        raise ValueError(f"Platform ID not found for {plugin_name}")

    plugin = Plugin(
        name=plugin_name,
        platform_id=platform_id,
        platform_name=platform_name,
        classname=".".join([source_type.__module__, source_type.__name__]),
    )

    if hasattr(source_type, "get_platform_doc_order"):
        platform_doc_order = source_type.get_platform_doc_order()
        plugin.doc_order = platform_doc_order

    plugin_file_name = "src/" + "/".join(source_type.__module__.split("."))
    if os.path.exists(plugin_file_name) and os.path.isdir(plugin_file_name):
        plugin_file_name = plugin_file_name + "/__init__.py"
    else:
        plugin_file_name = plugin_file_name + ".py"
    if os.path.exists(plugin_file_name):
        plugin.filename = plugin_file_name
    else:
        logger.info(
            f"Failed to locate filename for {plugin_name}. Guessed {plugin_file_name}, but that doesn't exist"
        )

    if hasattr(source_type, "__doc__"):
        plugin.source_docstring = textwrap.dedent(source_type.__doc__ or "")

    if hasattr(source_type, "get_support_status"):
        plugin.support_status = source_type.get_support_status()

    if hasattr(source_type, "get_capabilities"):
        capabilities = list(source_type.get_capabilities())
        capabilities.sort(key=lambda x: x.capability.value)
        plugin.capabilities = capabilities

    try:
        extra_plugin = plugin_name if does_extra_exist(plugin_name) else None
        plugin.extra_deps = (
            get_additional_deps_for_extra(extra_plugin) if extra_plugin else []
        )
    except Exception as e:
        logger.info(
            f"Failed to load extras for {plugin_name} due to exception {e}", exc_info=e
        )

    if hasattr(source_type, "get_config_class"):
        source_config_class: ConfigModel = source_type.get_config_class()

        plugin.config_json_schema = source_config_class.schema_json(indent=2)
        plugin.config_md = gen_md_table_from_json_schema(source_config_class.schema())

        # Write the config json schema to the out_dir.
        config_dir = pathlib.Path(out_dir) / "config_schemas"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / f"{plugin_name}_config.json").write_text(
            plugin.config_json_schema
        )

    return plugin


@dataclasses.dataclass
class PluginMetrics:
    discovered: int = 0
    loaded: int = 0
    generated: int = 0
    failed: int = 0


@dataclasses.dataclass
class PlatformMetrics:
    discovered: int = 0
    generated: int = 0
    warnings: List[str] = dataclasses.field(default_factory=list)


@click.command()
@click.option("--out-dir", type=str, required=True)
@click.option("--extra-docs", type=str, required=False)
@click.option("--source", type=str, required=False)
def generate(
    out_dir: str, extra_docs: Optional[str] = None, source: Optional[str] = None
) -> None:  # noqa: C901
    plugin_metrics = PluginMetrics()
    platform_metrics = PlatformMetrics()

    platforms: Dict[str, Platform] = {}
    for plugin_name in sorted(source_registry.mapping.keys()):
        if source and source != plugin_name:
            continue

        if plugin_name in {
            "snowflake-summary",
            "snowflake-queries",
            "bigquery-queries",
        }:
            logger.info(f"Skipping {plugin_name} as it is on the deny list")
            continue

        plugin_metrics.discovered += 1
        try:
            plugin = load_plugin(plugin_name, out_dir=out_dir)
        except Exception as e:
            logger.error(
                f"Failed to load {plugin_name} due to exception {e}", exc_info=e
            )
            plugin_metrics.failed += 1
            continue
        else:
            plugin_metrics.loaded += 1

            # Add to the platform list if not already present.
            platforms.setdefault(
                plugin.platform_id,
                Platform(
                    id=plugin.platform_id,
                    name=plugin.platform_name,
                ),
            ).add_plugin(plugin_name=plugin.name, plugin=plugin)

    if extra_docs:
        for path in glob.glob(f"{extra_docs}/**/*[.md|.yaml|.yml]", recursive=True):
            if m := re.search("/docs/sources/(.*)/(.*).md", path):
                platform_name = m.group(1).lower()  # TODO: rename this to platform_id
                file_name = m.group(2)
                destination_md: str = (
                    f"../docs/generated/ingestion/sources/{platform_name}.md"
                )

                with open(path, "r") as doc_file:
                    file_contents = doc_file.read()
                final_markdown = rewrite_markdown(file_contents, path, destination_md)

                if file_name == "README":
                    # README goes as platform level docs
                    # all other docs are assumed to be plugin level
                    platforms[platform_name].custom_docs_pre = final_markdown

                elif "_" in file_name:
                    plugin_doc_parts = file_name.split("_")
                    if len(plugin_doc_parts) != 2:
                        raise ValueError(
                            f"{file_name} needs to be of the form <plugin>_pre.md or <plugin>_post.md"
                        )
                    plugin_name, suffix = plugin_doc_parts
                    if suffix == "pre":
                        platforms[platform_name].plugins[
                            plugin_name
                        ].custom_docs_pre = final_markdown
                    elif suffix == "post":
                        platforms[platform_name].plugins[
                            plugin_name
                        ].custom_docs_post = final_markdown
                    else:
                        raise ValueError(
                            f"{file_name} needs to be of the form <plugin>_pre.md or <plugin>_post.md"
                        )

                else:  # assume this is the platform post.
                    # TODO: Probably need better error checking here.
                    platforms[platform_name].plugins[
                        file_name
                    ].custom_docs_post = final_markdown
            elif yml_match := re.search("/docs/sources/(.*)/(.*)_recipe.yml", path):
                platform_name = yml_match.group(1).lower()
                plugin_name = yml_match.group(2)
                platforms[platform_name].plugins[
                    plugin_name
                ].starter_recipe = pathlib.Path(path).read_text()

    sources_dir = f"{out_dir}/sources"
    os.makedirs(sources_dir, exist_ok=True)

    # Sort platforms by platform name.
    platforms = dict(sorted(platforms.items(), key=lambda x: x[1].name.casefold()))

    i = 0
    for platform_id, platform in platforms.items():
        if source and platform_id != source:
            continue
        platform_metrics.discovered += 1
        platform_doc_file = f"{sources_dir}/{platform_id}.md"
        # if "name" not in platform_docs:
        #     # We seem to have discovered written docs that corresponds to a platform, but haven't found linkage to it from the source classes
        #     warning_msg = f"Failed to find source classes for platform {platform_id}. Did you remember to annotate your source class with @platform_name({platform_id})?"
        #     logger.error(warning_msg)
        #     metrics["source_platforms"]["warnings"].append(warning_msg)  # type: ignore
        #     continue

        with open(platform_doc_file, "w") as f:
            i += 1
            f.write(f"---\nsidebar_position: {i}\n---\n\n")
            f.write(
                "import Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';\n\n"
            )
            f.write(f"# {platform.name}\n")

            if len(platform.plugins) > 1:
                # More than one plugin used to provide integration with this platform
                f.write(
                    f"There are {len(platform.plugins)} sources that provide integration with {platform.name}\n"
                )
                f.write("\n")
                f.write("<table>\n")
                f.write("<tr>")
                for col_header in ["Source Module", "Documentation"]:
                    f.write(f"<td>{col_header}</td>")
                f.write("</tr>")

                # Sort plugins in the platform.
                # It's a dict, so we need to recreate it.
                platform.plugins = dict(
                    sorted(
                        platform.plugins.items(),
                        key=lambda x: str(x[1].doc_order) if x[1].doc_order else x[0],
                    )
                )

                #                f.write("| Source Module | Documentation |\n")
                #                f.write("| ------ | ---- |\n")
                for plugin_name, plugin in platform.plugins.items():
                    f.write("<tr>\n")
                    f.write(f"<td>\n\n`{plugin_name}`\n\n</td>\n")
                    f.write(
                        f"<td>\n\n\n{plugin.source_docstring or ''} [Read more...](#module-{plugin_name})\n\n\n</td>\n"
                    )
                    f.write("</tr>\n")
                #                    f.write(
                #                        f"| `{plugin}` | {get_snippet(platform_docs['plugins'][plugin]['source_doc'])}[Read more...](#module-{plugin}) |\n"
                #                    )
                f.write("</table>\n\n")
            # insert platform level custom docs before plugin section
            f.write(platform.custom_docs_pre or "")
            # all_plugins = platform_docs["plugins"].keys()

            for plugin_name, plugin in platform.plugins.items():
                if len(platform.plugins) > 1:
                    # We only need to show this if there are multiple modules.
                    f.write(f"\n\n## Module `{plugin_name}`\n")

                if plugin.support_status != SupportStatus.UNKNOWN:
                    f.write(get_support_status_badge(plugin.support_status) + "\n\n")
                if plugin.capabilities and len(plugin.capabilities):
                    f.write("\n### Important Capabilities\n")
                    f.write("| Capability | Status | Notes |\n")
                    f.write("| ---------- | ------ | ----- |\n")
                    for cap_setting in plugin.capabilities:
                        f.write(
                            f"| {get_capability_text(cap_setting.capability)} | {get_capability_supported_badge(cap_setting.supported)} | {cap_setting.description} |\n"
                        )
                    f.write("\n")

                f.write(f"{plugin.source_docstring or ''}\n")
                # Insert custom pre section
                f.write(plugin.custom_docs_pre or "")
                f.write("\n### CLI based Ingestion\n")
                if plugin.extra_deps and len(plugin.extra_deps):
                    f.write("\n#### Install the Plugin\n")
                    if plugin.extra_deps != []:
                        f.write("```shell\n")
                        f.write(f"pip install 'acryl-datahub[{plugin}]'\n")
                        f.write("```\n")
                    else:
                        f.write(
                            f"The `{plugin}` source works out of the box with `acryl-datahub`.\n"
                        )
                if plugin.starter_recipe:
                    f.write("\n### Starter Recipe\n")
                    f.write(
                        "Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.\n\n\n"
                    )
                    f.write(
                        "For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).\n"
                    )
                    f.write("```yaml\n")
                    f.write(plugin.starter_recipe)
                    f.write("\n```\n")
                if plugin.config_json_schema:
                    assert plugin.config_md is not None
                    f.write("\n### Config Details\n")
                    f.write(
                        """<Tabs>
                <TabItem value="options" label="Options" default>\n\n"""
                    )
                    f.write(
                        "Note that a `.` is used to denote nested fields in the YAML recipe.\n\n"
                    )
                    # f.write(
                    #     "\n<details open>\n<summary>View All Configuration Options</summary>\n\n"
                    # )
                    f.write(plugin.config_md)
                    f.write("\n\n")
                    # f.write("\n</details>\n\n")
                    f.write(
                        f"""</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.\n\n
```javascript
{plugin.config_json_schema}
```\n\n
</TabItem>
</Tabs>\n\n"""
                    )

                # insert custom plugin docs after config details
                f.write(plugin.custom_docs_post or "")
                if plugin.classname:
                    f.write("\n### Code Coordinates\n")
                    f.write(f"- Class Name: `{plugin.classname}`\n")
                    if plugin.filename:
                        f.write(
                            f"- Browse on [GitHub](../../../../metadata-ingestion/{plugin.filename})\n\n"
                        )
                plugin_metrics.generated += 1

            # Using an h2 tag to prevent this from showing up in page's TOC sidebar.
            f.write("\n<h2>Questions</h2>\n\n")
            f.write(
                f"If you've got any questions on configuring ingestion for {platform.name}, feel free to ping us on [our Slack](https://slack.datahubproject.io).\n"
            )
            platform_metrics.generated += 1
    print("Ingestion Documentation Generation Complete")
    print("############################################")
    print(
        json.dumps(
            {
                "plugin_metrics": dataclasses.asdict(plugin_metrics),
                "platform_metrics": dataclasses.asdict(platform_metrics),
            },
            indent=2,
        )
    )
    print("############################################")
    if plugin_metrics.failed > 0:
        sys.exit(1)

    # Create Lineage doc
    generate_lineage_doc(platforms)


def generate_lineage_doc(platforms: Dict[str, Platform]) -> None:
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


if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()
