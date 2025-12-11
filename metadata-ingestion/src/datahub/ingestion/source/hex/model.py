# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Union

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn


@dataclass
class Workspace:
    name: str


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
    upstream_datasets: List[Union[DatasetUrn, SchemaFieldUrn]] = field(
        default_factory=list
    )
    upstream_schema_fields: List[Union[DatasetUrn, SchemaFieldUrn]] = field(
        default_factory=list
    )


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
