import re
from enum import Enum
from typing import List

import pandas as pd

import datahub.emitter.mce_builder as builder


class IBGenericPathElements:
    location_code: str
    parent1: str
    parent2: str
    parent3: str
    object_name: str

    def __init__(
        self,
        location_code: str,
        parent1: str,
        parent2: str,
        parent3: str,
        object_name: str,
    ):
        self.location_code = location_code
        self.parent1 = parent1
        self.parent2 = parent2
        self.parent3 = parent3
        self.object_name = object_name

    def __str__(self) -> str:
        return f"{self.location_code}, {self.parent1}, {self.parent2}, {self.parent3}, {self.object_name}"


class IBPathElementType(Enum):
    LOCATION = 1
    CLUSTER = 2
    OBJECT = 3


class IBPathElementInfo:
    element_type: IBPathElementType
    name: str
    value: str

    def __init__(self, element_type: IBPathElementType, name: str, value: str):
        self.element_type = element_type
        self.name = name
        self.value = value


class DatasetUtils:
    @staticmethod
    def map_path(
        platform: str, object_subtype: str, generic_path: IBGenericPathElements
    ) -> List[IBPathElementInfo]:
        path: List[IBPathElementInfo]
        if platform == "kafka":
            path = DatasetUtils._map_kafka_path(object_subtype, generic_path)
        elif platform == "mssql":
            path = DatasetUtils._map_mssql_path(object_subtype, generic_path)
        else:
            raise ValueError(f"Unknown platform {platform}")

        result: List[IBPathElementInfo] = []
        for p in path:
            if pd.isna(p.value):
                raise ValueError(
                    f"Null value is not allowed for {p.name}, path: {generic_path}, platform: {platform}"
                )
            if p.element_type == IBPathElementType.LOCATION:
                p.value = p.value.lower()
            result.append(p)
        return result

    @staticmethod
    def build_dataset_urn(platform: str, *path: IBPathElementInfo):
        return builder.make_dataset_urn(
            platform, DatasetUtils.join_path(".", *path), "PROD"
        )

    @staticmethod
    def join_path(separator: str, *path: IBPathElementInfo):
        replace_chars_regex = re.compile("[/\\\\&?*=]")
        return separator.join(
            map(lambda p: replace_chars_regex.sub("-", p.value), path)
        )

    @staticmethod
    def _map_kafka_path(
        object_subtype: str, generic_path: IBGenericPathElements
    ) -> List[IBPathElementInfo]:
        subtype = object_subtype if pd.notna(object_subtype) else "Kafka Topic"
        return [
            IBPathElementInfo(
                IBPathElementType.LOCATION,
                "DataCenter",
                generic_path.location_code,
            ),
            IBPathElementInfo(
                IBPathElementType.CLUSTER,
                "Kafka Cluster",
                generic_path.parent1,
            ),
            IBPathElementInfo(
                IBPathElementType.OBJECT,
                subtype,
                generic_path.object_name,
            ),
        ]

    @staticmethod
    def _map_mssql_path(
        object_subtype: str, generic_path: IBGenericPathElements
    ) -> List[IBPathElementInfo]:
        subtype = object_subtype if pd.notna(object_subtype) else "Table"
        return [
            IBPathElementInfo(
                IBPathElementType.LOCATION,
                "DataCenter",
                generic_path.location_code,
            ),
            IBPathElementInfo(
                IBPathElementType.LOCATION,
                "Server",
                generic_path.parent1,
            ),
            IBPathElementInfo(
                IBPathElementType.CLUSTER,
                "Database",
                generic_path.parent2,
            ),
            IBPathElementInfo(
                IBPathElementType.CLUSTER,
                "Schema",
                generic_path.parent3,
            ),
            IBPathElementInfo(
                IBPathElementType.OBJECT,
                subtype,
                generic_path.object_name,
            ),
        ]
