import os
import time
from typing import Dict, List

from google.api_core import exceptions
from google.api_core.client_options import ClientOptions
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.datacatalog_v1 import DataCatalogClient
from pydantic import Field

from datahub.configuration.common import PermissiveConfigModel
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)


class BigqueryConnectionConfigPermissive(
    BigQueryConnectionConfig, PermissiveConfigModel
):
    taxonomy: str = Field(
        default="DataHubSmokeTest", description="DataHub Synced Glossary Terms"
    )


class BigqueryTestHelper:
    def __init__(self, config: BigqueryConnectionConfigPermissive):
        self.config = BigqueryConnectionConfigPermissive.parse_obj(config)
        self.ptm_client = self.config.get_policy_tag_manager_client()
        self.bq_client = self.config.get_bigquery_client()
        self.dc_client = DataCatalogClient(
            client_options=ClientOptions(**self.config.extra_client_options)
            if self.config.extra_client_options
            else None
        )
        self.bq_location = self.config.extra_client_options.get(
            "location", "US"
        ).lower()
        self.bq_project = (
            self.config.project_on_behalf
            if self.config.project_on_behalf
            else self.config.credential.project_id
            if self.config.credential
            else None
        )

    def create_dataset(self, dataset_name: str) -> Dataset:
        assert self.bq_project
        dataset_reference = DatasetReference(
            dataset_id=dataset_name,
            project=self.bq_project,
        )

        try:
            dataset_id = self.bq_client.get_dataset(dataset_reference)
            if dataset_id:
                self.bq_client.delete_dataset(dataset_reference, delete_contents=True)
                time.sleep(5)
        except exceptions.NotFound:
            pass

        dataset = self.bq_client.create_dataset(dataset_reference)

        return dataset

    def create_table(
        self, dataset_name: str, table_name: str, number_of_tables: int
    ) -> List[Table]:
        assert self.bq_project

        dataset_reference = DatasetReference(
            dataset_id=dataset_name,
            project=self.bq_project,
        )
        tables: List[Table] = []
        for i in range(number_of_tables):
            table_reference = TableReference(
                dataset_ref=dataset_reference, table_id=f"{table_name}_{i}"
            )

            table = self.bq_client.create_table(table_reference)

            table.schema = [
                SchemaField("field_1", "STRING", mode="REQUIRED"),
                SchemaField("field_2", "INTEGER", mode="REQUIRED"),
            ]

            self.bq_client.update_table(
                table=table,
                fields=["schema"],
            )
            tables.append(table)
        return tables

    def get_table(self, table_name: str) -> Table:
        (project_id, dataset_id, table_id) = table_name.split(".")

        dataset_reference = DatasetReference(
            dataset_id=dataset_id,
            project=project_id,
        )

        table_reference = TableReference(
            dataset_ref=dataset_reference, table_id=table_id
        )

        table = self.bq_client.get_table(table_reference)
        return table

    def delete_table(self, dataset_name: str, table_name: str) -> None:
        assert self.bq_project
        dataset_reference = DatasetReference(
            dataset_id=dataset_name,
            project=self.bq_project,
        )

        table_reference = TableReference(
            dataset_ref=dataset_reference, table_id=table_name
        )

        self.bq_client.delete_table(table_reference)

    def delete_dataset(self, dataset_name: str, delete_contents: bool = True) -> None:
        assert self.bq_project
        dataset_reference = DatasetReference(
            dataset_id=dataset_name,
            project=self.bq_project,
        )
        try:
            self.bq_client.delete_dataset(
                dataset_reference, delete_contents=delete_contents
            )
        except exceptions.NotFound:
            pass

    def expect_table_description(self, table_name: str, description: str) -> None:
        table = self.get_table(table_name)
        assert table.description == description, (
            f"Table description {table.description} does not match with expected description: {description}"
        )

    def expect_column_description(
        self, table_name: str, column_name: str, description: str
    ) -> None:
        table = self.get_table(table_name)
        column = next(filter(lambda x: x.name == column_name, table.schema), None)
        assert column is not None, f"Column {column_name} not found"
        assert column.description == description, (
            f"Column description {column.description} does not match with expected description: {description}"
        )

    def expect_table_labels(self, table_name: str, labels: Dict[str, str]) -> None:
        table = self.get_table(table_name)
        for key, value in labels.items():
            assert table.labels[key] == value, (
                f"Label {table.labels[key]} does not match with {value}"
            )

    def expect_policy_tags(
        self, table_name: str, column: str, policy_tags: List[str]
    ) -> None:
        table = self.get_table(table_name)
        schema_field: SchemaField = next(
            filter(lambda x: x.name == column, table.schema)
        )
        bq_tag_names = []
        assert (
            policy_tags and schema_field.policy_tags and schema_field.policy_tags.names
        ), "Column has no policy tags"

        for bq_tag in schema_field.policy_tags.names:
            t = self.ptm_client.get_policy_tag(name=bq_tag)
            bq_tag_names.append(t.display_name)

        for policy_tag in policy_tags:
            assert policy_tag in bq_tag_names, (
                f"Policy tag {policy_tag} not found in bq_tag_names {bq_tag_names}"
            )

    def __del__(self):
        self.bq_client.close()
        if self.config._credentials_path:
            os.unlink(self.config._credentials_path)
