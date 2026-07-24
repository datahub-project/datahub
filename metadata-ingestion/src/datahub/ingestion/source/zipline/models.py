import json
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from datahub.ingestion.source.zipline.constants import (
    CUSTOM_JSON_COLUMN_TAGS_KEY,
    K_ARG_MAP_KEY,
    K_OPERATIONS,
    OPERATION_NAMES,
    TIME_UNIT_SUFFIX,
    sanitize,
)


class _ZiplineBaseModel(BaseModel):
    # Ignore unmapped fields so schema drift across chronon/zipline releases
    # doesn't fail the run.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    # Set by the reader to the originating file, so downstream warnings about a
    # dropped object (e.g. missing name) can point at an actionable location.
    _source_file: Optional[str] = PrivateAttr(default=None)

    @property
    def source_file(self) -> Optional[str]:
        return self._source_file


class Window(_ZiplineBaseModel):
    length: int
    time_unit: int = Field(alias="timeUnit")

    @property
    def suffix(self) -> str:
        return f"{self.length}{TIME_UNIT_SUFFIX.get(self.time_unit, '')}"


class Aggregation(_ZiplineBaseModel):
    input_column: Optional[str] = Field(default=None, alias="inputColumn")
    operation: Optional[int] = None
    arg_map: Dict[str, str] = Field(default_factory=dict, alias="argMap")
    windows: List[Window] = Field(default_factory=list)
    buckets: List[str] = Field(default_factory=list)

    def _operation_suffix(self) -> str:
        op_name = OPERATION_NAMES.get(self.operation or -1, "unknown")
        if self.operation in K_OPERATIONS:
            # LAST_K -> "last" + k, matching Chronon's `_get_op_suffix`.
            return f"{op_name[:-2]}{self.arg_map.get(K_ARG_MAP_KEY, '')}"
        return op_name

    def output_column_names(self) -> List[str]:
        """Mirrors ``ai.chronon.group_by.get_output_col_names`` so feature names
        match Chronon's backfill output columns exactly."""
        base_name = f"{self.input_column}_{self._operation_suffix()}"
        if self.windows:
            windowed = [f"{base_name}_{window.suffix}" for window in self.windows]
        else:
            windowed = [base_name]

        if self.buckets:
            return [
                f"{name}_by_{bucket}" for bucket in self.buckets for name in windowed
            ]
        return windowed


class Query(_ZiplineBaseModel):
    selects: Dict[str, str] = Field(default_factory=dict)
    wheres: List[str] = Field(default_factory=list)
    time_column: Optional[str] = Field(default=None, alias="timeColumn")
    partition_column: Optional[str] = Field(default=None, alias="partitionColumn")


class EventSource(_ZiplineBaseModel):
    table: Optional[str] = None
    topic: Optional[str] = None
    query: Optional[Query] = None
    is_cumulative: Optional[bool] = Field(default=None, alias="isCumulative")


class EntitySource(_ZiplineBaseModel):
    snapshot_table: Optional[str] = Field(default=None, alias="snapshotTable")
    mutation_table: Optional[str] = Field(default=None, alias="mutationTable")
    mutation_topic: Optional[str] = Field(default=None, alias="mutationTopic")
    query: Optional[Query] = None


class Source(_ZiplineBaseModel):
    events: Optional[EventSource] = None
    entities: Optional[EntitySource] = None
    # A JoinSource nests an entire Join; kept raw and skipped by the mapper,
    # since resolving it needs the parent join's output.
    join_source: Optional[Dict[str, object]] = Field(default=None, alias="joinSource")

    @property
    def batch_table(self) -> Optional[str]:
        if self.events is not None:
            return self.events.table
        if self.entities is not None:
            return self.entities.snapshot_table
        return None

    @property
    def topic(self) -> Optional[str]:
        if self.events is not None:
            return self.events.topic
        if self.entities is not None:
            return self.entities.mutation_topic
        return None

    @property
    def query(self) -> Optional[Query]:
        if self.events is not None:
            return self.events.query
        if self.entities is not None:
            return self.entities.query
        return None


class Derivation(_ZiplineBaseModel):
    name: Optional[str] = None
    expression: Optional[str] = None


class MetaData(_ZiplineBaseModel):
    name: Optional[str] = None
    online: Optional[bool] = None
    production: Optional[bool] = None
    custom_json: Optional[str] = Field(default=None, alias="customJson")
    table_properties: Dict[str, str] = Field(
        default_factory=dict, alias="tableProperties"
    )
    output_namespace: Optional[str] = Field(default=None, alias="outputNamespace")
    team: Optional[str] = None
    offline_schedule: Optional[str] = Field(default=None, alias="offlineSchedule")
    description: Optional[str] = None
    deprecation_date: Optional[str] = Field(default=None, alias="deprecationDate")

    def parsed_custom_json(self) -> Dict[str, object]:
        if not self.custom_json:
            return {}
        try:
            parsed = json.loads(self.custom_json)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def has_malformed_custom_json(self) -> bool:
        """True when customJson is present but not decodable — the caller (which
        holds the report) surfaces this, since the model itself cannot warn."""
        if not self.custom_json:
            return False
        try:
            json.loads(self.custom_json)
        except json.JSONDecodeError:
            return True
        return False

    def tags(self, key: str) -> Dict[str, str]:
        """Read a tag bag (e.g. groupby_tags) from the customJson blob."""
        value = self.parsed_custom_json().get(key)
        if not isinstance(value, dict):
            return {}
        return {str(k): str(v) for k, v in value.items()}

    def column_tags(self) -> Dict[str, Dict[str, str]]:
        value = self.parsed_custom_json().get(CUSTOM_JSON_COLUMN_TAGS_KEY)
        if not isinstance(value, dict):
            return {}
        result: Dict[str, Dict[str, str]] = {}
        for column, tag_bag in value.items():
            if isinstance(tag_bag, dict):
                result[str(column)] = {str(k): str(v) for k, v in tag_bag.items()}
        return result

    def output_table_name(self) -> Optional[str]:
        """Mirrors ``ai.chronon.utils.output_table_name`` (namespaced, sanitized)."""
        if not self.name:
            return None
        table = sanitize(self.name)
        return f"{self.output_namespace}.{table}" if self.output_namespace else table


class GroupBy(_ZiplineBaseModel):
    meta_data: MetaData = Field(default_factory=MetaData, alias="metaData")
    sources: List[Source] = Field(default_factory=list)
    key_columns: List[str] = Field(default_factory=list, alias="keyColumns")
    aggregations: List[Aggregation] = Field(default_factory=list)
    derivations: List[Derivation] = Field(default_factory=list)

    def feature_names(self) -> List[str]:
        """Output feature columns (aggregation outputs after derivations)."""
        if self.aggregations:
            pre_derived: List[str] = []
            for aggregation in self.aggregations:
                pre_derived.extend(aggregation.output_column_names())
        else:
            # No aggregations: the selected non-key columns are the features.
            pre_derived = [
                column
                for source in self.sources
                if source.query is not None
                for column in source.query.selects
            ]
        return _apply_derivations(pre_derived, self.derivations)


class JoinPart(_ZiplineBaseModel):
    group_by: GroupBy = Field(alias="groupBy")
    key_mapping: Dict[str, str] = Field(default_factory=dict, alias="keyMapping")
    prefix: Optional[str] = None


class Join(_ZiplineBaseModel):
    meta_data: MetaData = Field(default_factory=MetaData, alias="metaData")
    left: Optional[Source] = None
    join_parts: List[JoinPart] = Field(default_factory=list, alias="joinParts")
    derivations: List[Derivation] = Field(default_factory=list)


class StagingQuery(_ZiplineBaseModel):
    meta_data: MetaData = Field(default_factory=MetaData, alias="metaData")
    query: Optional[str] = None
    start_partition: Optional[str] = Field(default=None, alias="startPartition")
    setups: List[str] = Field(default_factory=list)


def _apply_derivations(
    pre_derived: List[str], derivations: List[Derivation]
) -> List[str]:
    """Mirror Chronon's projection: a ``*`` derivation passes through every
    non-renamed column; explicit derivations contribute their own name."""
    if not derivations:
        return pre_derived

    renamed_expressions = {d.expression for d in derivations}
    is_wildcard = any(d.expression == "*" for d in derivations)
    wildcard_columns = (
        [c for c in pre_derived if c not in renamed_expressions] if is_wildcard else []
    )

    output: List[str] = []
    for derivation in derivations:
        if derivation.name == "*":
            output.extend(wildcard_columns)
        elif derivation.name is not None:
            output.append(derivation.name)
    return output
