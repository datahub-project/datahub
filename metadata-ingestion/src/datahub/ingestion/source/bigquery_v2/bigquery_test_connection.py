import logging
from datetime import datetime, timedelta
from typing import Dict, List, Union

from google.cloud import bigquery

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigQuerySchemaApi
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.sql_parsing.schema_resolver import SchemaResolver

logger: logging.Logger = logging.getLogger(__name__)


class BigQueryTestConnection:
    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        _report: Dict[Union[SourceCapability, str], CapabilityReport] = dict()

        try:
            connection_conf = BigQueryV2Config.parse_obj_allow_extras(config_dict)
            client: bigquery.Client = connection_conf.get_bigquery_client()
            assert client

            test_report.basic_connectivity = BigQueryTestConnection.connectivity_test(
                client
            )

            connection_conf.start_time = datetime.now()
            connection_conf.end_time = datetime.now() + timedelta(minutes=1)

            report: BigQueryV2Report = BigQueryV2Report()
            project_ids: List[str] = []
            projects = client.list_projects()

            for project in projects:
                if connection_conf.project_id_pattern.allowed(project.project_id):
                    project_ids.append(project.project_id)

            metadata_read_capability = (
                BigQueryTestConnection.metadata_read_capability_test(
                    project_ids, connection_conf
                )
            )
            if SourceCapability.SCHEMA_METADATA not in _report:
                _report[SourceCapability.SCHEMA_METADATA] = metadata_read_capability

            if connection_conf.include_table_lineage:
                lineage_capability = BigQueryTestConnection.lineage_capability_test(
                    connection_conf, project_ids, report
                )
                if SourceCapability.LINEAGE_COARSE not in _report:
                    _report[SourceCapability.LINEAGE_COARSE] = lineage_capability

            if connection_conf.include_usage_statistics:
                usage_capability = BigQueryTestConnection.usage_capability_test(
                    connection_conf, project_ids, report
                )
                if SourceCapability.USAGE_STATS not in _report:
                    _report[SourceCapability.USAGE_STATS] = usage_capability

            test_report.capability_report = _report
            return test_report

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{e}"
            )
            return test_report

    @staticmethod
    def connectivity_test(client: bigquery.Client) -> CapabilityReport:
        ret = client.query("select 1")
        if ret.error_result:
            return CapabilityReport(
                capable=False, failure_reason=f"{ret.error_result['message']}"
            )
        else:
            return CapabilityReport(capable=True)

    @staticmethod
    def metadata_read_capability_test(
        project_ids: List[str], config: BigQueryV2Config
    ) -> CapabilityReport:
        for project_id in project_ids:
            try:
                logger.info(f"Metadata read capability test for project {project_id}")
                client: bigquery.Client = config.get_bigquery_client()
                assert client
                bigquery_data_dictionary = BigQuerySchemaApi(
                    report=BigQueryV2Report().schema_api_perf,
                    projects_client=config.get_projects_client(),
                    client=client,
                )
                result = bigquery_data_dictionary.get_datasets_for_project_id(
                    project_id, 10
                )
                if len(result) == 0:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=f"Dataset query returned empty dataset. It is either empty or no dataset in project {project_id}",
                    )
                tables = bigquery_data_dictionary.get_tables_for_dataset(
                    project_id=project_id,
                    dataset_name=result[0].name,
                    tables={},
                    with_data_read_permission=config.have_table_data_read_permission,
                    report=BigQueryV2Report(),
                )
                if len(list(tables)) == 0:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=f"Tables query did not return any table. It is either empty or no tables in project {project_id}.{result[0].name}",
                    )

            except Exception as e:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Dataset query failed with error: {e}",
                )

        return CapabilityReport(capable=True)

    @staticmethod
    def lineage_capability_test(
        connection_conf: BigQueryV2Config,
        project_ids: List[str],
        report: BigQueryV2Report,
    ) -> CapabilityReport:
        lineage_extractor = BigqueryLineageExtractor(
            connection_conf,
            report,
            schema_resolver=SchemaResolver(platform="bigquery"),
            identifiers=BigQueryIdentifierBuilder(connection_conf, report),
        )
        for project_id in project_ids:
            try:
                logger.info(f"Lineage capability test for project {project_id}")
                lineage_extractor.test_capability(project_id)
            except Exception as e:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Lineage capability test failed with: {e}",
                )

        return CapabilityReport(capable=True)

    @staticmethod
    def usage_capability_test(
        connection_conf: BigQueryV2Config,
        project_ids: List[str],
        report: BigQueryV2Report,
    ) -> CapabilityReport:
        usage_extractor = BigQueryUsageExtractor(
            connection_conf,
            report,
            schema_resolver=SchemaResolver(platform="bigquery"),
            identifiers=BigQueryIdentifierBuilder(connection_conf, report),
        )
        for project_id in project_ids:
            try:
                logger.info(f"Usage capability test for project {project_id}")
                failures_before_test = len(report.failures)
                usage_extractor.test_capability(project_id)
                if failures_before_test != len(report.failures):
                    return CapabilityReport(
                        capable=False,
                        failure_reason="Usage capability test failed. Check the logs for further info",
                    )
            except Exception as e:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Usage capability test failed with: {e} for project {project_id}",
                )
        return CapabilityReport(capable=True)
