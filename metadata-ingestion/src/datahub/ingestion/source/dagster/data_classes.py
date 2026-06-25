"""Normalized, parsed representations of Dagster GraphQL responses.

The raw GraphQL payload uses tagged unions (``__typename``) that are awkward to
consume directly. The client (``dagster_api.py``) parses the raw payload into the
dataclasses below so the source (``dagster.py``) deals only with plain Python
data and never touches GraphQL shapes.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

# A Dagster asset key is an ordered list of path segments, e.g. ["prod", "events"].
AssetKey = Sequence[str]


@dataclass
class DagsterOwner:
    """An owner declared on an asset or job. Exactly one of email/team is set."""

    email: Optional[str] = None
    team: Optional[str] = None


@dataclass
class DagsterTag:
    key: str
    value: Optional[str] = None


@dataclass
class DagsterLink:
    """A documentation or source-code link harvested from metadata entries."""

    url: str
    description: str


@dataclass
class DagsterColumn:
    name: str
    native_type: str
    description: Optional[str] = None
    nullable: bool = True


@dataclass
class DagsterAssetMetadata:
    """Metadata entries from an asset, normalized by kind."""

    custom_properties: dict = field(default_factory=dict)
    links: List[DagsterLink] = field(default_factory=list)
    columns: Optional[List[DagsterColumn]] = None


@dataclass
class DagsterAsset:
    key: List[str]
    group_name: Optional[str] = None
    op_names: List[str] = field(default_factory=list)
    job_names: List[str] = field(default_factory=list)
    description: Optional[str] = None
    compute_kind: Optional[str] = None
    kinds: List[str] = field(default_factory=list)
    owners: List[DagsterOwner] = field(default_factory=list)
    tags: List[DagsterTag] = field(default_factory=list)
    upstream_keys: List[List[str]] = field(default_factory=list)
    downstream_keys: List[List[str]] = field(default_factory=list)
    metadata: DagsterAssetMetadata = field(default_factory=DagsterAssetMetadata)


@dataclass
class DagsterJob:
    name: str
    location_name: str
    description: Optional[str] = None
    owners: List[DagsterOwner] = field(default_factory=list)
    tags: List[DagsterTag] = field(default_factory=list)


@dataclass
class DagsterRepository:
    name: str
    location_name: str
    jobs: List[DagsterJob] = field(default_factory=list)
    assets: List[DagsterAsset] = field(default_factory=list)
