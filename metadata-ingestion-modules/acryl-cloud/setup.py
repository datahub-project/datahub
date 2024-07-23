import json
import pathlib

from setuptools import setup

_codegen_config_file = pathlib.Path("./src/acryl_datahub_cloud/_codegen_config.json")
_codegen_config: dict = json.loads(_codegen_config_file.read_text())

# Adding pydantic<2 since we use pydantic models to map to pyarrow models and that is only compatible in pydantic v1
stats_common = {"pandas", "pyarrow", "duckdb", "pydantic<2"}
aws_common = {"boto3"}
open_search_common = {"opensearch-py==2.4.2"}

plugins = {
    "datahub-lineage-features": stats_common | open_search_common,
    "datahub-reporting-forms": stats_common | aws_common,
    "datahub-reporting-extract-graph": stats_common | aws_common | open_search_common,
    "datahub-reporting-extract-sql": stats_common | aws_common,
    "datahub-usage-feature-reporting": stats_common | aws_common| {"opensearch-py==2.4.2", "polars", "elasticsearch==7.13.4", "numpy<2", "scipy"},
}

dev_requirements = {
    # acryl-datahub[dev] pulls in more things than are strictly necessary, but it's fine.
    "acryl-datahub[dev]",
    *list(
        dependency
        for plugin in [
            "datahub-reporting-forms",
            "datahub-reporting-extract-graph",
            "datahub-reporting-extract-sql",
            "datahub-lineage-features",
            "datahub-usage-feature-reporting"
        ]
        for dependency in plugins[plugin]
    ),
}

setup(
    **{
        **_codegen_config,
        "install_requires": [
            *_codegen_config["install_requires"],
        ],
        "entry_points": {
            **_codegen_config["entry_points"],
            "console_scripts": [
                "acryl-datahub-cloud = acryl_datahub_cloud.cli:main",
            ],
            "datahub.ingestion.source.plugins": [
                "datahub-reporting-forms = acryl_datahub_cloud.datahub_reporting.forms:DataHubReportingFormsSource",
                "datahub-reporting-extract-graph = acryl_datahub_cloud.datahub_reporting.extract_graph:DataHubReportingExtractGraphSource",
                "datahub-reporting-extract-sql = acryl_datahub_cloud.datahub_reporting.extract_sql:DataHubReportingExtractSQLSource",
                "datahub-lineage-features = acryl_datahub_cloud.lineage_features.source:DataHubLineageFeaturesSource",
                "datahub-usage-reporting = acryl_datahub_cloud.datahub_usage_reporting.usage_feature_reporter:DataHubUsageFeatureReportingSource",
            ],
        },
    },
    extras_require={
        **{plugin: list(dependencies) for (plugin, dependencies) in plugins.items()},
        "all": list(
            set().union(*[requirements for _plugin, requirements in plugins.items()])
        ),
        "dev": list(dev_requirements),
    }
)
