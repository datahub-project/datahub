import json
import os.path
from dataclasses import dataclass, field
from typing import Iterable, Iterator, Union

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import UsageAggregationClass


def _iterate_file(path: str) -> list:
    with open(path, "r") as f:
        obj_list = json.load(f)
    if not isinstance(obj_list, list):
        obj_list = [obj_list]
    return obj_list


def iterate_mce_file(path: str) -> Iterator[MetadataChangeEvent]:
    for obj in _iterate_file(path):
        mce: MetadataChangeEvent = MetadataChangeEvent.from_obj(obj)
        yield mce


def iterate_generic_file(
    path: str,
) -> Iterator[
    Union[MetadataChangeEvent, MetadataChangeProposal, UsageAggregationClass]
]:
    for i, obj in enumerate(_iterate_file(path)):
        item: Union[MetadataChangeEvent, MetadataChangeProposal, UsageAggregationClass]
        if "proposedSnapshot" in obj:
            item = MetadataChangeEvent.from_obj(obj)
        elif "aspect" in obj:
            item = MetadataChangeProposal.from_obj(obj)
        else:
            item = UsageAggregationClass.from_obj(obj)
        if not item.validate():
            raise ValueError(f"failed to parse: {obj} (index {i})")
        yield item


class FileSourceConfig(ConfigModel):
    filename: str = Field(description="Path to file to ingest.")


@platform_name("File")
@config_class(FileSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@dataclass
class GenericFileSource(TestableSource):
    """
    This plugin pulls metadata from a previously generated file. The [file sink](../../../../metadata-ingestion/sink_docs/file.md) can produce such files, and a number of samples are included in the [examples/mce_files](../../../../metadata-ingestion/examples/mce_files) directory.
    """

    config: FileSourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = FileSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        for i, obj in enumerate(iterate_generic_file(self.config.filename)):
            wu: Union[MetadataWorkUnit, UsageStatsWorkUnit]
            if isinstance(obj, UsageAggregationClass):
                wu = UsageStatsWorkUnit(f"file://{self.config.filename}:{i}", obj)
            elif isinstance(obj, MetadataChangeProposal):
                wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mcp_raw=obj)
            else:
                wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mce=obj)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        config = FileSourceConfig.parse_obj(config_dict)
        is_file = os.path.isfile(config.filename)
        readable = os.access(config.filename, os.R_OK)
        if is_file and readable:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )
        elif not is_file:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=f"{config.filename} doesn't appear to be a valid file.",
                )
            )
        elif not readable:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=f"Cannot read file {config.filename}"
                )
            )
        else:
            # not expected to be here
            raise Exception("Not expected to be here.")
