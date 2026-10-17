"""Probe provider for DynamoDB: the tables ingestion would consider.

Metadata only: table names, from ListTables. Never Scan or BatchGetItem,
which is how ingestion samples items to infer a schema.
"""

from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

from botocore.exceptions import ClientError, NoRegionError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, take
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.dynamodb.dynamodb import (
    DynamoDBConfig,
    dynamodb_dataset_name,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient


class DynamoDBMetadataProbe(ProbeProviderBase):
    def __init__(self, config: DynamoDBConfig) -> None:
        self._config = config

    @classmethod
    def for_config(cls, config: DynamoDBConfig) -> "DynamoDBMetadataProbe":
        return cls(config)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """A botocore ClientError's code (`AccessDeniedException`,
        `UnrecognizedClientException`). Only the code: the message names the
        calling principal's ARN."""
        if not isinstance(exc, ClientError):
            return None
        return aws_error_code(exc) or None

    def _client(self) -> "DynamoDBClient":
        def open_client() -> "DynamoDBClient":
            try:
                # The connector's own builder, so the session, role chain,
                # endpoint and retry settings are ingestion's.
                return self._config.get_dynamodb_client()
            except NoRegionError as e:
                raise ProbeArgumentError(
                    "no AWS region is configured: set aws_region in the recipe "
                    "(DynamoDB ingestion reads the tables of that one region)"
                ) from e

        return self._open_once("dynamodb", open_client, close=lambda c: c.close())

    @probe_method(kind=DatasetSubTypes.TABLE, row_limit_param="limit")
    def tables(self, limit: int = 200) -> List[Dict[str, str]]:
        """Tables in the recipe's region, including ones table_pattern would
        exclude: a denied table is reported, not hidden. Each `name` is
        `region.table`, the string table_pattern is matched against and the
        name the dataset is emitted under; `table` and `region` are its parts.
        Ingestion reads one region per run (aws_region), so a table in another
        region is never listed. Metadata only: no items are read."""
        client = self._client()
        region = client.meta.region_name

        def records() -> Iterator[Dict[str, str]]:
            # Unlike ingestion's _list_tables, a failed page raises: an empty
            # listing must not stand in for one that could not be read.
            for page in client.get_paginator("list_tables").paginate():
                for table in page.get("TableNames") or []:
                    yield {
                        "name": dynamodb_dataset_name(region, table),
                        "table": table,
                        "region": region,
                    }

        return take(records(), limit)
