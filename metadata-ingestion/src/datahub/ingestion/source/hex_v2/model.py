from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


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
class DataConnection:
    """A Hex data connection from `hex connection list`."""

    id: str
    name: str
    connection_type: str
    description: Optional[str] = None


@dataclass
class SqlCell:
    """A SQL cell extracted from a project YAML export."""

    cell_id: str
    cell_label: Optional[str]
    sql_source: str
    # None means the cell operates on in-memory dataframes, not an external DB
    data_connection_id: Optional[str]


@dataclass
class RunRecord:
    """A single run record from `hex run list`."""

    run_id: str
    status: str
    start_time: datetime
    elapsed_seconds: Optional[float] = None


@dataclass
class Project:
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
    # Populated after YAML export + SQL parsing
    upstream_datasets: List[str] = field(default_factory=list)
    # Populated after `hex run list`
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
