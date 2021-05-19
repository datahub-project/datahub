import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from typing import Counter as CounterType
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union

import bson
import pymongo
from feast.value_type import ValueType
from mypy_extensions import TypedDict
from pydantic import PositiveInt
from pymongo.mongo_client import MongoClient

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass

# map Feast types to DataHub classes
_field_type_mapping: Dict[Union[ValueType, str], Type] = {
    ValueType.BYTES: BytesTypeClass,
    ValueType.STRING: StringTypeClass,
    ValueType.INT32: NumberTypeClass,
    ValueType.INT64: NumberTypeClass,
    ValueType.DOUBLE: NumberTypeClass,
    ValueType.FLOAT: NumberTypeClass,
    ValueType.BOOL: BooleanTypeClass,
    ValueType.UNIX_TIMESTAMP: TimeTypeClass,
    ValueType.BYTES_LIST: ArrayTypeClass,
    ValueType.STRING_LIST: ArrayTypeClass,
    ValueType.INT32_LIST: ArrayTypeClass,
    ValueType.INT64_LIST: ArrayTypeClass,
    ValueType.DOUBLE_LIST: ArrayTypeClass,
    ValueType.FLOAT_LIST: ArrayTypeClass,
    ValueType.BOOL_LIST: ArrayTypeClass,
    ValueType.UNIX_TIMESTAMP_LIST: ArrayTypeClass,
}


class FeastConfig(ConfigModel):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    core_url: str = "localhost:6565"
    options: dict = {}


@dataclass
class FeastSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


@dataclass
class FeastSource(Source):
    config: FeastConfig
    report: FeastSourceReport

    def __init__(self, ctx: PipelineContext, config: FeastConfig):
        super().__init__(ctx)
        self.config = config
        self.report = FeastSourceReport()

        options = {}
        if self.config.core_url is not None:
            options["core_url"] = self.config.core_url
        options = {
            **options,
            **self.config.options,
        }

        self.feast_client = pymongo.MongoClient(self.config.core_url, **options)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FeastSource":
        config = FeastConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_field_type(
        self, field_type: Union[Type, str], collection_name: str
    ) -> SchemaFieldDataType:
        """
        Maps types encountered in Feast to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of a Python object
            collection_name:
                name of collection (for logging)
        """
        TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.report_warning(
                collection_name, f"unable to map type {field_type} to metadata schema"
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        env = "PROD"
        platform = "feast"

    def get_report(self) -> FeastSourceReport:
        return self.report

    def close(self):
        self.feast_client.close()
