import dataclasses
import json
import logging
import pprint
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Set, cast, runtime_checkable

import humanfriendly
import pydantic
from pydantic import BaseModel
from typing_extensions import Literal, Protocol

from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.report_helpers import format_datetime_relative
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import SubTypesClass, UpstreamLineageClass
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)
LogLevel = Literal["ERROR", "WARNING", "INFO", "DEBUG"]


@runtime_checkable
class SupportsAsObj(Protocol):
    def as_obj(self) -> dict: ...


@dataclass
class Report(SupportsAsObj):
    @staticmethod
    def to_str(some_val: Any) -> str:
        if isinstance(some_val, Enum):
            return some_val.name
        else:
            return str(some_val)

    @staticmethod
    def to_pure_python_obj(some_val: Any) -> Any:
        """A cheap way to generate a dictionary."""

        if isinstance(some_val, SupportsAsObj):
            return some_val.as_obj()
        elif isinstance(some_val, pydantic.BaseModel):
            return Report.to_pure_python_obj(some_val.dict())
        elif dataclasses.is_dataclass(some_val) and not isinstance(some_val, type):
            # The `is_dataclass` function returns `True` for both instances and classes.
            # We need an extra check to ensure an instance was passed in.
            # https://docs.python.org/3/library/dataclasses.html#dataclasses.is_dataclass
            return dataclasses.asdict(some_val)
        elif isinstance(some_val, list):
            return [Report.to_pure_python_obj(v) for v in some_val if v is not None]
        elif isinstance(some_val, timedelta):
            return humanfriendly.format_timespan(some_val)
        elif isinstance(some_val, datetime):
            try:
                return format_datetime_relative(some_val)
            except Exception:
                # we don't want to fail reporting because we were unable to pretty print a timestamp
                return str(datetime)
        elif isinstance(some_val, dict):
            return {
                Report.to_str(k): Report.to_pure_python_obj(v)
                for k, v in some_val.items()
                if v is not None
            }
        elif isinstance(some_val, (int, float, bool)):
            return some_val
        else:
            # fall through option
            return Report.to_str(some_val)

    def compute_stats(self) -> None:
        """A hook to compute derived stats"""
        pass

    def as_obj(self) -> dict:
        self.compute_stats()
        return {
            str(key): Report.to_pure_python_obj(value)
            for (key, value) in self.__dict__.items()
            # ignore nulls and fields starting with _
            if value is not None and not str(key).startswith("_")
        }

    def as_string(self) -> str:
        return pprint.pformat(self.as_obj(), width=150, sort_dicts=False)

    def as_json(self) -> str:
        return json.dumps(self.as_obj())

    # TODO add helper method for warning / failure status + counts?


@dataclass
class SourceReportSubtypes:
    urn: str
    entity_type: str
    subType: str = field(default="unknown")
    aspects: Set[str] = field(default_factory=set)


class ReportAttribute(BaseModel):
    severity: LogLevel = "DEBUG"
    help: Optional[str] = None

    @property
    def logger_sev(self) -> int:
        log_levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }
        return log_levels[self.severity]

    def log(self, msg: str) -> None:
        logger.log(level=self.logger_sev, msg=msg, stacklevel=3)


@dataclass
class ExamplesReport(Report, Closeable):
    _urns_seen: Set[str] = field(default_factory=set)
    entities: Dict[str, list] = field(default_factory=lambda: defaultdict(LossyList))
    aspects: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )
    aspects_by_subtypes: Dict[str, Dict[str, Dict[str, int]]] = field(
        default_factory=lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
    )
    aspect_urn_samples: Dict[str, Dict[str, LossyList[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(LossyList))
    )
    _file_based_dict: Optional[FileBackedDict[SourceReportSubtypes]] = None

    def __post_init__(self) -> None:
        self._file_based_dict = FileBackedDict(
            tablename="urn_aspects",
            extra_columns={
                "urn": lambda val: val.urn,
                "entityType": lambda val: val.entity_type,
                "subTypes": lambda val: val.subType,
                "aspects": lambda val: json.dumps(sorted(list(val.aspects))),
            },
        )

    def close(self) -> None:
        self.compute_stats()
        if self._file_based_dict is not None:
            self._file_based_dict.close()
            self._file_based_dict = None

    def _store_workunit_data(self, wu: MetadataWorkUnit) -> None:
        urn = wu.get_urn()

        if not isinstance(wu.metadata, MetadataChangeEvent):
            mcps = [wu.metadata]
        else:
            mcps = list(mcps_from_mce(wu.metadata))

        for mcp in mcps:
            entityType = mcp.entityType
            aspectName = mcp.aspectName

            if urn not in self._urns_seen:
                self._urns_seen.add(urn)
                self.entities[entityType].append(urn)

            if aspectName is None:
                continue
            self.aspects[entityType][aspectName] += 1
            self.aspect_urn_samples[entityType][aspectName].append(urn)
            sub_type = "unknown"
            if isinstance(mcp.aspect, UpstreamLineageClass):
                upstream_lineage = cast(UpstreamLineageClass, mcp.aspect)
                if upstream_lineage.fineGrainedLineages:
                    self.aspect_urn_samples[entityType]["fineGrainedLineages"].append(
                        urn
                    )
                    self.aspects[entityType]["fineGrainedLineages"] += 1
            elif isinstance(mcp.aspect, SubTypesClass):
                sub_type = mcp.aspect.typeNames[0]
            assert self._file_based_dict is not None
            if urn in self._file_based_dict:
                if sub_type != "unknown":
                    self._file_based_dict[urn].subType = sub_type
                self._file_based_dict[urn].aspects.add(aspectName)
                self._file_based_dict.mark_dirty(urn)
            else:
                self._file_based_dict[urn] = SourceReportSubtypes(
                    urn=urn,
                    entity_type=entityType,
                    subType=sub_type,
                    aspects={aspectName},
                )

    def compute_stats(self) -> None:
        if self._file_based_dict is None:
            return
        query = """
        SELECT entityType, subTypes, aspects, count(*) as count
        FROM urn_aspects 
        group by entityType, subTypes, aspects
        """

        entity_subtype_aspect_counts: Dict[str, Dict[str, Dict[str, int]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        )
        for row in self._file_based_dict.sql_query(query):
            entity_type = row["entityType"]
            sub_type = row["subTypes"]
            count = row["count"]
            aspects_raw = row["aspects"] or "[]"

            aspects = json.loads(aspects_raw)
            for aspect in aspects:
                entity_subtype_aspect_counts[entity_type][sub_type][aspect] += count

        self.aspects_by_subtypes.clear()
        for entity_type, subtype_counts in entity_subtype_aspect_counts.items():
            for sub_type, aspect_counts in subtype_counts.items():
                self.aspects_by_subtypes[entity_type][sub_type] = dict(aspect_counts)


class EntityFilterReport(ReportAttribute):
    type: str

    processed_entities: LossyList[str] = pydantic.Field(default_factory=LossyList)
    dropped_entities: LossyList[str] = pydantic.Field(default_factory=LossyList)

    def processed(self, entity: str, type: Optional[str] = None) -> None:
        self.log(f"Processed {type or self.type} {entity}")
        self.processed_entities.append(entity)

    def dropped(self, entity: str, type: Optional[str] = None) -> None:
        self.log(f"Filtered {type or self.type} {entity}")
        self.dropped_entities.append(entity)

    def as_obj(self) -> dict:
        return {
            "filtered": self.dropped_entities.as_obj(),
            "processed": self.processed_entities.as_obj(),
        }

    @staticmethod
    def field(type: str, severity: LogLevel = "DEBUG") -> "EntityFilterReport":
        """A helper to create a dataclass field."""

        return dataclasses.field(
            default_factory=lambda: EntityFilterReport(type=type, severity=severity)
        )
