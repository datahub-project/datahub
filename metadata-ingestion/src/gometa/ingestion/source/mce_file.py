import json
from dataclasses import dataclass, field
from pydantic import BaseModel
from typing import Optional, Iterable
from gometa.ingestion.api.source import Source, SourceReport
from gometa.ingestion.source.metadata_common import MetadataWorkUnit
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

class MetadataFileSourceConfig(BaseModel):
    filename: str

@dataclass
class MetadataFileSource(Source):
    config: MetadataFileSourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = MetadataFileSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        with open(self.config.filename, 'r') as f:
            mce_obj_list = json.load(f)
        if not isinstance(mce_obj_list, list):
            mce_obj_list = [mce_obj_list]
        
        for i, obj in enumerate(mce_obj_list):
            mce: MetadataChangeEvent = MetadataChangeEvent.from_obj(obj)
            wu = MetadataWorkUnit(f"file://{self.config.filename}:{i}", mce)
            self.report.report_workunit(wu)
            yield wu
    
    def get_report(self):
        return self.report
        
    def close(self):
        pass
