from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Union

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn


@dataclass
class Workspace:
    name: str


@dataclass
class SqlCell:
    """A SQL cell from the /v1/cells REST API."""

    cell_id: str
    cell_label: Optional[str]
    sql_source: str
    # None → in-memory transform, no external DB
    data_connection_id: Optional[str]


@dataclass
class ExploreCell:
    """A visualisation cell from the /v1/cells REST API."""

    cell_id: str
    cell_label: Optional[str]
    # dataframe reference not exposed by REST (only in YAML export)
    dataframe: Optional[str]
    chart_type: Optional[str]


@dataclass
class RunRecord:
    """Latest run from /v1/projects/{id}/runs."""

    run_id: str
    status: str
    start_time: datetime
    elapsed_seconds: Optional[float] = None


@dataclass
class Status:
    name: str


@dataclass
class Category:
    name: str
    description: Optional[str] = None


@dataclass
class Collection:
    name: str


@dataclass(frozen=True)
class Owner:
    email: str


@dataclass
class Analytics:
    appviews_all_time: Optional[int]
    appviews_last_7_days: Optional[int]
    appviews_last_14_days: Optional[int]
    appviews_last_30_days: Optional[int]
    last_viewed_at: Optional[datetime]


@dataclass
class Project:
    id: str
    title: str
    description: Optional[str]
    last_edited_at: Optional[datetime] = None
    last_published_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    status: Optional[Status] = None
    categories: Optional[List[Category]] = None  # TODO: emit category description!
    collections: Optional[List[Collection]] = None
    creator: Optional[Owner] = None
    owner: Optional[Owner] = None
    analytics: Optional[Analytics] = None
    upstream_datasets: List[Union[DatasetUrn, SchemaFieldUrn, str]] = field(
        default_factory=list
    )
    upstream_schema_fields: List[Union[DatasetUrn, SchemaFieldUrn]] = field(
        default_factory=list
    )
    latest_run: Optional[RunRecord] = None


@dataclass
class Component:
    id: str
    title: str
    description: Optional[str]
    last_edited_at: Optional[datetime] = None
    last_published_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    status: Optional[Status] = None
    categories: Optional[List[Category]] = None
    collections: Optional[List[Collection]] = None
    creator: Optional[Owner] = None
    owner: Optional[Owner] = None
    analytics: Optional[Analytics] = None
