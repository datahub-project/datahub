"""Eventually figure out if we can auto-gen these classes from avro"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, List
from abc import ABCMeta, abstractmethod
import logging

logger = logging.getLogger(__name__)

class SerializationEnum(str, Enum):
    avrojson = 'avro-json'
    restli  = 'restli'

class SnapshotType(str, Enum):
    dataset = 'dataset'
    corpuser = 'corpuser'

def get_mce_from_dict(type_name:str, dict_for_mce: dict, serialization: SerializationEnum):
        if serialization == SerializationEnum.avrojson:
            return (type_name, dict_for_mce)
        if serialization == SerializationEnum.restli:
            return {type_name: dict_for_mce}


class UnionParticipant(metaclass=ABCMeta):
    """A base interface for all types that are part of a union"""
    @abstractmethod
    def get_type_name(self) -> str:
        pass

    @abstractmethod
    def for_mce(self, serialization: SerializationEnum):
        logger.warn("union called")
        pass

@dataclass
class KafkaSchema(UnionParticipant):
    documentSchema: str
    type_name = "com.linkedin.schema.KafkaSchema"

    def get_type_name(self):
        return self.type_name

    def for_mce(self, serialization: SerializationEnum):
        logger.warn("kafka called")
        dict_for_mce = self.__dict__
        return get_mce_from_dict(self.get_type_name(), dict_for_mce, serialization)


@dataclass
class SQLSchema(UnionParticipant):
    tableSchema: str
    type_name = "com.linkedin.schema.SQLSchema"

    def get_type_name(self):
        return self.type_name

    def for_mce(self, serialization: SerializationEnum):
        logger.warn("sql schema called")
        dict_for_mce = self.__dict__
        return get_mce_from_dict(self.get_type_name(), dict_for_mce, serialization)



class Aspect(UnionParticipant):

    def for_mce(self, serialization: SerializationEnum):
        # Assumes that all aspect implementations will produce a valid dict
        # This might not work if Aspects contain Union types inside, we would have
        # to introspect and convert those into (type: dict) tuples or {type: dict} dicts
        logger.warn("aspect called")
        dict_for_mce = self.__dict__
        return get_mce_from_dict(self.get_type_name(), dict_for_mce, serialization)

class MetadataSnapshot(UnionParticipant, metaclass=ABCMeta):
    """A base interface for all types that are part of a metadata snapshot"""
    
    aspects = []

    @abstractmethod
    def get_urn(self):
        pass

    @abstractmethod
    def get_type(self) -> SnapshotType:
        pass

    def with_aspect(self, aspect: Aspect):
        self.aspects.append(aspect)

    def for_mce(self, serialization: SerializationEnum):
        logger.warn(f"aspects = {self.aspects}")
        dict_for_mce = {
            "urn": self.get_urn(),
            "aspects": [ a.for_mce(serialization) for a in self.aspects]
        }
        logger.warn(f"MetadataSnapshot = {dict_for_mce}")

        if serialization == SerializationEnum.avrojson:
            return (self.get_type_name(), dict_for_mce)
        if serialization == SerializationEnum.restli:
            return {"snapshot" : dict_for_mce}


@dataclass
class DatasetMetadataSnapshot(MetadataSnapshot):
    """Helper class to create Dataset Metadata Changes"""
    platform: str
    dataset_name: str
    env: Optional[str] = "PROD"
    type_name = "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot"

    def __post_init__(self):
        self.urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{self.dataset_name},{self.env})"

    def get_type_name(self):
        return self.type_name

    def get_urn(self):
        return self.urn

    def get_type(self):
        return SnapshotType.dataset

import copy 
@dataclass
class SchemaMetadata(Aspect):
    schemaName: str
    platform: str
    version: int
    created: dict
    lastModified: dict
    hash: str
    platformSchema: UnionParticipant
    fields: [dict]
    type_name = "com.linkedin.schema.SchemaMetadata"

    def get_type_name(self):
        return self.type_name

    def for_mce(self, serialization: SerializationEnum):
        platform_schema_serialized = self.platformSchema.for_mce(serialization)
        logger.warn(f'schema_serialized={platform_schema_serialized}')
        as_dict = copy.deepcopy(self.__dict__)
        as_dict["platformSchema"] = platform_schema_serialized
        return get_mce_from_dict(self.get_type_name(), as_dict, serialization)

@dataclass
class DatasetProperties(Aspect):
    description: Optional[str]
    uri: Optional[str]
    tags: Optional[List[str]]
    customProperties: Optional[dict]

    def get_type_name(self):
        return "com.linkedin.pegasus2avro.dataset.DatasetProperties"


class MetadataChangeEvent:
    """Helper class to represent and serialize Metadata Change Events (MCE)"""
    
    def with_snapshot(self, metadata_snapshot: MetadataSnapshot):
        self.snapshot = metadata_snapshot
        return self

    def as_mce(self, serialization: SerializationEnum):
        if self.snapshot:
            serialized_snapshot = self.snapshot.for_mce(serialization)
        else:
            logger.warn("No snapshot present in this MCE, seems like a bug! {self.dict()}")

        if serialization == SerializationEnum.avrojson:
            mce = {'auditHeader': None, "proposedDelta": None}
            if self.snapshot:
                mce["proposedSnapshot"] = serialized_snapshot
            return mce
        if serialization == SerializationEnum.restli:
            return (self.snapshot.get_type(), serialized_snapshot)

    def __repr__(self):
        mce = self.as_mce(SerializationEnum.avrojson)
        return str(mce)

