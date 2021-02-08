import json
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Optional, Iterable
from gometa.ingestion.api.source import Source
from gometa.ingestion.source.metadata_common import MetadataWorkUnit
from gometa.metadata import json_converter
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

class MetadataFileSourceConfig(BaseModel):
    filename: str

@dataclass
class MetadataFileSource(Source):
    config: MetadataFileSourceConfig

    @classmethod
    def create(cls, config_dict, ctx):
        config = MetadataFileSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        with open(self.config.filename, 'r') as f:
            mce_obj_list = json.load(f)
        if not isinstance(mce_obj_list, list):
            mce_obj_list = [mce_obj_list]
        
        for obj in mce_obj_list:
            mce = json_converter.from_json_object(obj, MetadataChangeEvent.RECORD_SCHEMA)
            # TODO: autogenerate workunit IDs
            wu = MetadataWorkUnit('fake mce', mce)
            yield wu
        
    def close(self):
        pass
