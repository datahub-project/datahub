#!/usr/bin/env python3
"""
DH001: logger.warning is not allowed in file.

Fast script to check for logger.warning usage in restricted files.
This runs much faster than flake8 and integrates well with ruff.
"""

import re
import sys
from pathlib import Path
from typing import List, Set

# Files that are allowed to use logger.warning
ALLOWED_FILES: Set[str] = {
    # These are files that are allowed to use logger.warning actually because of some reason
    # Add here if any
    # File below this are being added as a temporary measure so that we can ensure no new problems are introduced.
    # But these should be removed one by one and bring them until this linting
    "src/datahub/ingestion/source/nifi.py",
    "src/datahub/ingestion/source/superset.py",
    "src/datahub/ingestion/source/elastic_search.py",
    "src/datahub/ingestion/source/confluent_schema_registry.py",
    "src/datahub/ingestion/source/openapi_parser.py",
    "src/datahub/ingestion/source/sql/mssql/source.py",
    "src/datahub/ingestion/source/sql/vertica.py",
    "src/datahub/ingestion/source/sql/clickhouse.py",
    "src/datahub/ingestion/source/aws/glue.py",
    "src/datahub/ingestion/source/powerbi/rest_api_wrapper/data_resolver.py",
    "src/datahub/ingestion/source/state/stateful_ingestion_base.py",
    "src/datahub/ingestion/source/bigquery_v2/bigquery_schema_gen.py",
    "src/datahub/ingestion/source/unity/proxy.py",
    "src/datahub/ingestion/source/tableau/tableau.py",
    "src/datahub/ingestion/source/bigquery_v2/bigquery.py",
    "src/datahub/ingestion/source/grafana/lineage.py",
    "src/datahub/ingestion/source/bigquery_v2/lineage.py",
    "src/datahub/ingestion/source/looker/looker_usage.py",
    "src/datahub/ingestion/source/looker/looker_common.py",
    "src/datahub/ingestion/source/powerbi/rest_api_wrapper/powerbi_api.py",
    "src/datahub/ingestion/source/kafka/kafka.py",
    "src/datahub/ingestion/source/looker/lookml_source.py",
    "src/datahub/ingestion/source/unity/analyze_profiler.py",
    "src/datahub/ingestion/source/dremio/dremio_aspects.py",
    "src/datahub/ingestion/source/looker/looker_lib_wrapper.py",
    "src/datahub/ingestion/source/azure/abs_folder_utils.py",
    "src/datahub/ingestion/source/bigquery_v2/profiler.py",
    "src/datahub/ingestion/source/debug/datahub_debug.py",
    "src/datahub/ingestion/source/dremio/dremio_profiling.py",
    "src/datahub/ingestion/source/pulsar.py",
    "src/datahub/ingestion/source/unity/hive_metastore_proxy.py",
    "src/datahub/ingestion/source/state_provider/datahub_ingestion_checkpointing_provider.py",
    "src/datahub/ingestion/source/datahub/datahub_database_reader.py",
    "src/datahub/ingestion/source/bigquery_v2/usage.py",
    "src/datahub/ingestion/source/usage/usage_common.py",
    "src/datahub/ingestion/source/powerbi/m_query/pattern_handler.py",
    "src/datahub/ingestion/source/looker/looker_source.py",
    "src/datahub/ingestion/source/looker/looker_dataclasses.py",
    "src/datahub/ingestion/source/unity/ge_profiler.py",
    "src/datahub/ingestion/source/bigquery_v2/bigquery_config.py",
    "src/datahub/ingestion/source/bigquery_v2/queries_extractor.py",
    "src/datahub/ingestion/source/redshift/lineage.py",
    "src/datahub/ingestion/source/powerbi/config.py",
    "src/datahub/ingestion/source/sigma/sigma_api.py",
    "src/datahub/ingestion/source/powerbi/powerbi.py",
    "src/datahub/ingestion/source/data_lake_common/data_lake_utils.py",
    "src/datahub/ingestion/source/sql/sql_generic_profiler.py",
    "src/datahub/ingestion/source/unity/proxy_profiling.py",
    "src/datahub/ingestion/source/looker/lookml_concept_context.py",
    "src/datahub/ingestion/source/snowflake/snowflake_config.py",
    "src/datahub/ingestion/source/redash.py",
    "src/datahub/ingestion/source/qlik_sense/qlik_sense.py",
    "src/datahub/ingestion/source/file.py",
    "src/datahub/ingestion/source/unity/tag_entities.py",
    "src/datahub/ingestion/source/dremio/dremio_entities.py",
    "src/datahub/ingestion/source/state_provider/file_ingestion_checkpointing_provider.py",
    "src/datahub/ingestion/source/sigma/sigma.py",
    "src/datahub/ingestion/source/metadata/business_glossary.py",
    "src/datahub/ingestion/source/sql/oracle.py",
    "src/datahub/ingestion/source/sql/athena.py",
    "src/datahub/ingestion/source/sql/teradata.py",
    "src/datahub/ingestion/source/looker/view_upstream.py",
    "src/datahub/ingestion/source/tableau/tableau_validation.py",
    "src/datahub/ingestion/source/dbt/dbt_common.py",
    "src/datahub/ingestion/source/unity/proxy_types.py",
    "src/datahub/ingestion/source/unity/config.py",
    "src/datahub/ingestion/source/s3/source.py",
    "src/datahub/ingestion/source/redshift/config.py",
    "src/datahub/ingestion/source/looker/looker_template_language.py",
    "src/datahub/ingestion/source/aws/s3_boto_utils.py",
    "src/datahub/ingestion/source/kafka_connect/sink_connectors.py",
    "src/datahub/ingestion/source/sql/hive.py",
    "src/datahub/ingestion/source/aws/tag_entities.py",
    "src/datahub/ingestion/source/unity/source.py",
    "src/datahub/ingestion/source/dbt/dbt_core.py",
    "src/datahub/ingestion/source/grafana/field_utils.py",
    "src/datahub/ingestion/source/redshift/lineage_v2.py",
    "src/datahub/ingestion/source/metadata/lineage.py",
    "src/datahub/ingestion/source/sql/sql_common.py",
    "src/datahub/ingestion/source/datahub/datahub_kafka_reader.py",
    "src/datahub/ingestion/source/redshift/redshift.py",
    "src/datahub/ingestion/source/iceberg/iceberg_common.py",
    "src/datahub/ingestion/source/kafka_connect/common.py",
    "src/datahub/ingestion/source/ge_data_profiler.py",
    "src/datahub/ingestion/source/looker/lookml_config.py",
    "src/datahub/ingestion/source/kafka_connect/kafka_connect.py",
    "src/datahub/ingestion/source/qlik_sense/qlik_api.py",
    "src/datahub/ingestion/source/unity/usage.py",
    "src/datahub/ingestion/source/identity/azure_ad.py",
    "src/datahub/ingestion/source/cassandra/cassandra_utils.py",
    "src/datahub/ingestion/source/gc/execution_request_cleanup.py",
    "src/datahub/ingestion/source/ge_profiling_config.py",
    "src/datahub/ingestion/source/bigquery_v2/bigquery_schema.py",
    "src/datahub/ingestion/source/grafana/grafana_source.py",
}


def check_file(file_path: Path) -> List[str]:
    """
    Check a single file for logger.warning usage.

    Returns:
        List of error messages, empty if no violations found.
    """
    errors = []

    # Convert to relative path for comparison with allowed files
    try:
        rel_path = str(file_path.relative_to(Path.cwd()))
    except ValueError:
        rel_path = str(file_path)

    # Skip if file is in allowed list
    if rel_path in ALLOWED_FILES:
        return errors

    # Only check files in the source directory we care about
    if not rel_path.startswith("src/datahub/ingestion/source"):
        return errors

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except (IOError, UnicodeDecodeError):
        return errors

    # Pattern to match logger.warning usage
    # Matches: logger.warning, self.logger.warning, etc.
    pattern = re.compile(r"\blogger\.warning\b")

    for line_num, line in enumerate(lines, 1):
        # Skip comments and strings (simple heuristic)
        if line.strip().startswith("#"):
            continue

        if pattern.search(line):
            errors.append(
                f"{rel_path}:{line_num}: DH001 logger.warning is not allowed in this file. "
                f"Consider using strcutured warnings, or add to allowed files list."
            )

    return errors


def main() -> int:
    """Main entry point."""
    all_errors = []

    # Always scan the source directory
    source_dir = Path("src/datahub/ingestion/source")
    if not source_dir.exists():
        print("Error: src/datahub/ingestion/source directory not found")
        return 1

    # Find all Python files in the source directory
    files_with_errors = set()
    python_files = source_dir.rglob("*.py")
    for file_path in python_files:
        errors = check_file(file_path)
        if errors:
            files_with_errors.add(str(file_path))
        all_errors.extend(errors)

    if all_errors:
        for error in all_errors:
            print(error)
        print("Files with errors:")
        for file in files_with_errors:
            print(f'"{file}",')

    print(
        f"Found {len(all_errors)} errors with {len(ALLOWED_FILES)} exception files added"
    )
    if all_errors:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
