from re import A
from attr import frozen
from dataclasses import dataclass, field
from typing import Sequence, List, Optional, Iterable, Union
import pydantic

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import Table
from delta_sharing.rest_client import QueryTableMetadataResponse

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

@dataclass(frozen=True)
class QueryTableMetadataResponse_extended(QueryTableMetadataResponse):
    table: Table


class SharingClient_extended(SharingClient):
    """
    An extension of the delta sharing class SharingClient in order to query metadata including the table origin.
    This is done in order to ingest more easily.
    """

    def query_table_metadata(self, table: Table) -> QueryTableMetadataResponse_extended:
        """
        List all metadata for a specified table in a Delta Sharing Server.
        :return: all metadata in a specified table.
        """

        response = self._rest_client.query_table_metadata(table=table)

        return QueryTableMetadataResponse_extended(protocol=response.protocol, metadata=response.metadata, table=table)

    def query_all_table_metadata(self) -> Sequence[QueryTableMetadataResponse_extended]:
        """
        List all metadata in all tables that can be accessed by you in a Delta Sharing Server.
        :return: all metadata that can be accessed.
        """
        tables = self.list_all_tables()
        querytablesmetadata = [self.query_table_metadata(table=table) for table in tables]

        return querytablesmetadata

class DeltaLakeSourceConfig(ConfigModel):

    url: str
    token: str
    shareCredentialsVersion: str ="1"
    
    share_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    schema_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class DeltaLakeSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)

@dataclass
class GenericFileSource(Source):
    config: DeltaLakeSourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    # @classmethod
    # def create(cls, config_dict, ctx):
    #     config = DeltaLakeSourceConfig.parse_obj(config_dict)
    #     return cls(ctx, config)

    # def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
    #     for i, obj in enumerate(iterate_generic_file(self.config.filename)):
    #         wu: Union[MetadataWorkUnit, UsageStatsWorkUnit]
    #         if isinstance(obj, UsageAggregationClass):
    #             wu = UsageStatsWorkUnit(f"file://{self.config.filename}:{i}", obj)
    #         elif isinstance(obj, MetadataChangeProposal):
    #             wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mcp_raw=obj)
    #         else:
    #             wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mce=obj)
    #         self.report.report_workunit(wu)
    #         yield wu

    #TODO def get_field_type():

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass

#TODO: report dropped and filtered in WU

if __name__ == "__main__":

    import delta_sharing

    #TODO: get key from recipe

    # Get the access keys for delta-sharing & start the client
    profile = delta_sharing.protocol.DeltaSharingProfile(share_credentials_version=1, endpoint="https://sharing.delta.io/delta-sharing/", bearer_token="faaie590d541265bcab1f2de9813274bf233")
    client = SharingClient_extended(profile)

    # get all shared metadata
    metadata_list=client.query_all_table_metadata()

    #Filter tables, schemas and shares
    filter_table=AllowDenyPattern(allow=["COVID_19_NYT"], deny=["LA"])
    metadata_list=[metadata for metadata in metadata_list if filter_table.allowed(metadata.table.name)]

    filter_schema=AllowDenyPattern(allow=["default"], deny=["LA"])
    metadata_list=[metadata for metadata in metadata_list if filter_schema.allowed(metadata.table.schema)]

    filter_share=AllowDenyPattern(allow=["delta_sharing"], deny=["LA"])
    metadata_list=[metadata for metadata in metadata_list if filter_share.allowed(metadata.table.share)]

    #TODO: catch if no data left to process

    #TODO: transform to datahub dataset entity

    #TODO: publish data


