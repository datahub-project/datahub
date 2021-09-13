import json
from dataclasses import dataclass, field
from typing import Iterable, Iterator, Union

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
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
    filename: str


@dataclass
class GenericFileSource(Source):
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
