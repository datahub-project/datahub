from typing import Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from datahub.ingestion.source.kafka.stream_processing.constants import (
    StreamProcessingEngine,
)


class StreamProcessingJob(BaseModel):
    engine: StreamProcessingEngine
    # Stable id used to build the DataJob urn (query id / statement name / application id).
    job_id: str
    name: str
    input_topics: List[str] = Field(default_factory=list)
    output_topics: List[str] = Field(default_factory=list)
    # Human-readable transform SQL, surfaced as a DataJob custom property.
    query: Optional[str] = None
    # SQL whose table identifiers are the backing topic names, fed to the SQL parser
    # for column-level lineage. None when we cannot produce topic-identifier SQL.
    parse_query: Optional[str] = None
    sql_dialect: Optional[str] = None
    custom_properties: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def parse_query_requires_dialect(self) -> "StreamProcessingJob":
        if self.parse_query and not self.sql_dialect:
            raise ValueError("sql_dialect is required when parse_query is set")
        return self
