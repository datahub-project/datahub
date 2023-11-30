import logging
import re
from typing import Dict, List, Union

import requests

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    Measure,
    PowerBIDataset,
    Table,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    RegularAPIResolver,
)


class ProfilerConstant:
    DATASET_EXECUTE_QUERIES_POST = "DATASET_EXECUTE_QUERIES_POST"


ENDPOINT = {
    ProfilerConstant.DATASET_EXECUTE_QUERIES_POST: "{POWERBI_BASE_URL}/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries",
}


logger = logging.getLogger(__name__)


def get_column_name(table_and_col: str) -> str:
    regex = re.compile(".*\\[(.*)\\]$")
    m = regex.match(table_and_col)
    if m:
        return m.group(1)
    return ""


def process_sample_result(result_data: dict) -> dict:
    sample_data_by_column: Dict[str, List[str]] = {}
    rows = result_data["results"][0]["tables"][0]["rows"]
    for sample in rows:
        for key, value in sample.items():
            if not value:
                continue
            column_name = get_column_name(key)
            if column_name not in sample_data_by_column:
                sample_data_by_column[column_name] = []
            sample_data_by_column[column_name].append(str(value))
    return sample_data_by_column


def process_column_result(result_data: dict) -> dict:
    sample_data_by_column: Dict[str, str] = {}
    rows = result_data["results"][0]["tables"][0]["rows"]
    for sample in rows:
        for key, value in sample.items():
            if not value:
                continue
            column_name = get_column_name(key)
            if column_name != "unique_count":
                value = str(value)
            sample_data_by_column[column_name] = value
    return sample_data_by_column


class PowerBiDatasetProfilingResolver(RegularAPIResolver):
    config: PowerBiDashboardSourceConfig
    overview_stats: dict

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        config: PowerBiDashboardSourceConfig,
    ):
        super(PowerBiDatasetProfilingResolver, self).__init__(
            client_id, client_secret, tenant_id
        )
        self.config = config
        self.overview_stats = {}

    def get_row_count(self, dataset: PowerBIDataset, table: Table) -> int:
        query = f"""
        EVALUATE ROW("count", COUNTROWS ( '{table.name}' ))
        """
        try:
            data = self.execute_query(dataset, query)
            rows = data["results"][0]["tables"][0]["rows"]
            count = rows[0]["[count]"]
            return count
        except requests.exceptions.RequestException as ex:
            logger.warning(getattr(ex.response, "text", ""))
            logger.warning(
                f"Profiling failed for getting row count for dataset {dataset.id}, with status code {getattr(ex.response, 'status_code', None)}",
            )
        except (KeyError, IndexError) as ex:
            logger.warning(
                f"Profiling failed for getting row count for dataset {dataset.id}, with {ex}"
            )
        return 0

    def get_data_sample(self, dataset: PowerBIDataset, table: Table) -> dict:
        try:
            query = f"EVALUATE TOPN(3, '{table.name}')"
            data = self.execute_query(dataset, query)
            return process_sample_result(data)
        except requests.exceptions.RequestException as ex:
            logger.warning(getattr(ex.response, "text", ""))
            logger.warning(
                f"Getting sample with TopN failed for dataset {dataset.id}, with status code {getattr(ex.response, 'status_code', None)}",
            )
        except (KeyError, IndexError) as ex:
            logger.warning(
                f"Getting sample with TopN failed for dataset {dataset.id}, with {ex}"
            )

        return {}

    def get_column_data(
        self, dataset: PowerBIDataset, table: Table, column: Union[Column, Measure]
    ) -> dict:
        try:
            logger.info(f"Column data query for {dataset.name}, {column.name}")
            query = f"""
            EVALUATE ROW(
                "min", MIN('{table.name}'[{column.name}]),
                "max", MAX('{table.name}'[{column.name}]),
                "unique_count", COUNTROWS ( DISTINCT ( '{table.name}'[{column.name}] ) )
            )"""
            data = self.execute_query(dataset, query)
            return process_column_result(data)
        except requests.exceptions.RequestException as ex:
            logger.warning(getattr(ex.response, "text", ""))
            logger.warning(
                f"Getting column statistics failed for dataset {dataset.name}, {column.name}, with status code {getattr(ex.response, 'status_code', None)}",
            )
        except (KeyError, IndexError) as ex:
            logger.warning(
                f"Getting column statistics failed for dataset {dataset.name}, {column.name}, with {ex}"
            )

        return {}

    def execute_query(self, dataset: PowerBIDataset, query: str) -> dict:
        dataset_query_endpoint: str = ENDPOINT[
            ProfilerConstant.DATASET_EXECUTE_QUERIES_POST
        ]
        # Replace place holders
        dataset_query_endpoint = dataset_query_endpoint.format(
            POWERBI_BASE_URL=self.BASE_URL,
            WORKSPACE_ID=dataset.workspace_id,
            DATASET_ID=dataset.id,
        )
        # Hit PowerBi
        logger.info(f"Request to query endpoint URL={dataset_query_endpoint}")
        payload = {
            "queries": [
                {
                    "query": query,
                }
            ],
            "serializerSettings": {
                "includeNulls": True,
            },
        }
        response = self._request_session.post(
            dataset_query_endpoint,
            json=payload,
            headers=self.get_authorization_header(),
        )
        response.raise_for_status()
        return response.json()

    def profile_dataset(
        self,
        dataset: PowerBIDataset,
        table: Table,
        workspace_name: str,
    ) -> None:
        if not self.config.profiling.enabled:
            # Profiling not enabled
            return

        if not self.config.profile_pattern.allowed(
            f"{workspace_name}.{dataset.name}.{table.name}"
        ):
            logger.info(
                f"Table {table.name} in {dataset.name}, not allowed for profiling"
            )
            return

        logger.info(f"Profiling table: {table.name}")
        row_count = self.get_row_count(dataset, table)
        sample = self.get_data_sample(dataset, table)

        table.row_count = row_count
        column_count = 0

        columns: List[Union[Column, Measure]] = [
            *(table.columns or []),
            *(table.measures or []),
        ]
        for column in columns:
            if column.isHidden:
                continue

            if sample and sample.get(column.name, None):
                column.sample_values = sample.get(column.name, None)

            column_stats = self.get_column_data(dataset, table, column)

            for key, value in column_stats.items():
                setattr(column, key, value)

            column_count += 1

        table.column_count = column_count
