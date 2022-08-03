import json
import math
import sys
from typing import Iterable, Union

import pandas as pd
from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.schema_classes import DatasetLineageTypeClass


class IBRedashLineagesSourceConfig(ConfigModel):
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")
    lineage_query_id: str = Field(
        default="LINEAGES_QUERY_ID",
        description="Target redash query that contains all the " "lineages data",
    )
    api_page_limit: int = Field(
        default=sys.maxsize,
        description="Limit on number of pages queried for ingesting dashboards and charts API "
        "during pagination. ",
    )


@platform_name("RedashLineage")
@config_class(IBRedashLineagesSourceConfig)
class IBRedashLineagesSource(Source):
    batch_size = 1000
    config: IBRedashLineagesSourceConfig
    client: Redash
    report: SourceReport

    def __init__(self, config: IBRedashLineagesSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: IBRedashLineagesSourceConfig = config
        self.report: SourceReport = SourceReport()

        self.config.connect_uri = self.config.connect_uri.strip("/")
        print("self.config.connect_uri:", config.connect_uri)
        self.client = Redash(self.config.connect_uri, self.config.api_key)
        self.client.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Handling retry and backoff
        retries = 3
        backoff_factor = 10
        status_forcelist = (500, 503, 502, 504)
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.client.session.mount("http://", adapter)
        self.client.session.mount("https://", adapter)

        self.api_page_limit = self.config.api_page_limit or math.inf

    @classmethod
    def create(cls, config_dict, ctx):
        config = IBRedashLineagesSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        lineages_json = self.get_lineages(self.config.lineage_query_id)
        lineages_grouped = pd.read_json(json.dumps(lineages_json)).groupby(
            ["DstType", "DstLocationCode", "DstDBName", "DstSchemaName", "DstTableName"]
        )

        for key_tuple, lineages in lineages_grouped:
            first = lineages.iloc[0]
            dst_dataset_urn = self.build_dataset_urn(
                first.DstType,
                first.DstLocationCode,
                first.DstDBName,
                first.DstSchemaName,
                first.DstTableName,
            )

            src_urns = []
            # TODO for to map()/transform()
            for index, row in lineages.iterrows():
                src_urns.append(
                    self.build_dataset_urn(
                        row.SrcType,
                        row.SrcLocationCode,
                        row.SrcDBName,
                        row.SrcSchemaName,
                        row.SrcTableName,
                    )
                )

            yield MetadataWorkUnit(
                dst_dataset_urn,
                mce=builder.make_lineage_mce(
                    src_urns, dst_dataset_urn, DatasetLineageTypeClass.COPY
                ),
            )

    def get_lineages(self, query_id) -> str:
        url = f"//api/queries/{query_id}/results"
        return self.client._post(url).json()["query_result"]["data"]["rows"]

    @staticmethod
    def build_dataset_urn(
        dataset_type, location_code, db_name, schema_name, table_name
    ) -> str:
        if dataset_type.lower() == "kafka":
            return builder.make_dataset_urn(
                "kafka", f"{location_code.lower()}.{db_name}.{table_name}", "PROD"
            )
        else:
            return builder.make_dataset_urn(
                "mssql",
                f"{location_code.lower()}.{db_name}.{schema_name}.{table_name}",
                "PROD",
            )

    def get_report(self):
        return self.report
