from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from datahub.configuration.datetimes import parse_user_datetime
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport
from datahub.metadata.urns import CorpUserUrn, DatasetUrn


@dataclass(frozen=True)
class QueryEntryContext:
    """What QueryEntry validation needs beyond the record itself.

    Bare table names in lineage hints become dataset URNs, which needs the
    platform, platform instance and env; malformed hints are counted on the report.
    """

    platform: str
    platform_instance: Optional[str]
    env: str
    report: Optional[SqlQueriesSourceReport] = None


class QueryEntry(BaseModel):
    query: str
    timestamp: Optional[datetime] = None
    user: Optional[CorpUserUrn] = None
    downstream_tables: List[DatasetUrn] = Field(default_factory=list)
    upstream_tables: List[DatasetUrn] = Field(default_factory=list)
    session_id: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> Any:
        return None if v is None else parse_user_datetime(str(v))

    @field_validator("user", mode="before")
    @classmethod
    def parse_user(cls, v: Any) -> Any:
        if v is None:
            return None

        # Exported query logs routinely carry "" for system/background queries.
        # The field is Optional, so treat that as "no actor" exactly like null —
        # rejecting the row instead would discard an otherwise valid query.
        if isinstance(v, str) and not v.strip():
            return None

        return v if isinstance(v, CorpUserUrn) else CorpUserUrn(v)

    @field_validator("downstream_tables", "upstream_tables", mode="before")
    @classmethod
    def parse_tables(cls, v: Any, info: ValidationInfo) -> Any:
        if not v:
            return []

        # A bare string is iterable, so without this it fans out into one URN
        # per character and writes fabricated lineage silently.
        if isinstance(v, (str, bytes)):
            raise ValueError(
                "upstream_tables/downstream_tables must be a list of table names, "
                f"not a bare string: {v!r}"
            )

        context = info.context
        if context is not None and not isinstance(context, QueryEntryContext):
            raise TypeError(
                f"QueryEntry validation context must be a QueryEntryContext, "
                f"got {type(context).__name__}"
            )
        report = context.report if context is not None else None

        result: List[DatasetUrn] = []
        for item in v:
            if isinstance(item, DatasetUrn):
                result.append(item)
            elif isinstance(item, str):
                stripped = item.strip()
                if stripped:
                    if context is None:
                        raise ValueError(
                            "A QueryEntryContext is required to turn table names into URNs"
                        )
                    urn_string = make_dataset_urn_with_platform_instance(
                        name=stripped,
                        platform=context.platform,
                        platform_instance=context.platform_instance,
                        env=context.env,
                    )
                    result.append(DatasetUrn.from_string(urn_string))
            elif report is not None:
                # Dropping a lineage hint silently would hide missing lineage,
                # so surface it in the report rather than only the log.
                report.num_invalid_table_entries += 1
                report.warning(
                    title="Invalid table entry",
                    message="Ignoring malformed table reference in lineage hints",
                    context=f"type={type(item).__name__}, value={item!r}",
                )

        return result

    @classmethod
    def create(
        cls, entry_dict: Dict[str, Any], *, context: QueryEntryContext
    ) -> "QueryEntry":
        return cls.model_validate(entry_dict, context=context)
