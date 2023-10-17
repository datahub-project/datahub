"""
Generates data for performance testing of warehouse sources.

In the future, we could try to create a more realistic dataset
by anonymizing and reduplicating a production datahub instance's data.
We could also get more human data by using Faker.

This is a work in progress, built piecemeal as needed.
"""
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, TypeVar, Union, cast

from faker import Faker

from tests.performance.data_model import (
    Column,
    ColumnMapping,
    ColumnType,
    Container,
    FieldAccess,
    Query,
    StatementType,
    Table,
    View,
)

T = TypeVar("T")

OPERATION_TYPES: List[StatementType] = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "CUSTOM",
    "UNKNOWN",
]


@dataclass(frozen=True)
class NormalDistribution:
    mu: float
    sigma: float

    def sample(self) -> int:
        return int(random.gauss(mu=self.mu, sigma=self.sigma))

    def sample_with_floor(self, floor: int = 1) -> int:
        return max(int(random.gauss(mu=self.mu, sigma=self.sigma)), floor)


@dataclass
class SeedMetadata:
    # Each list is a layer of containers, e.g. [[databases], [schemas]]
    containers: List[List[Container]]

    tables: List[Table]
    views: List[View]
    start_time: datetime
    end_time: datetime

    @property
    def all_tables(self) -> List[Table]:
        return self.tables + cast(List[Table], self.views)


def generate_data(
    num_containers: Union[List[int], int],
    num_tables: int,
    num_views: int,
    columns_per_table: NormalDistribution = NormalDistribution(5, 2),
    parents_per_view: NormalDistribution = NormalDistribution(2, 1),
    view_definition_length: NormalDistribution = NormalDistribution(150, 50),
    time_range: timedelta = timedelta(days=14),
) -> SeedMetadata:
    # Assemble containers
    if isinstance(num_containers, int):
        num_containers = [num_containers]

    containers: List[List[Container]] = []
    for i, num_in_layer in enumerate(num_containers):
        layer = [
            Container(
                f"{i}-container-{j}",
                parent=random.choice(containers[-1]) if containers else None,
            )
            for j in range(num_in_layer)
        ]
        containers.append(layer)

    # Assemble tables
    tables = [
        Table(
            f"table-{i}",
            container=random.choice(containers[-1]),
            columns=[
                f"column-{j}-{uuid.uuid4()}"
                for j in range(columns_per_table.sample_with_floor())
            ],
            column_mapping=None,
        )
        for i in range(num_tables)
    ]
    views = [
        View(
            f"view-{i}",
            container=random.choice(containers[-1]),
            columns=[
                f"column-{j}-{uuid.uuid4()}"
                for j in range(columns_per_table.sample_with_floor())
            ],
            column_mapping=None,
            definition=f"{uuid.uuid4()}-{'*' * view_definition_length.sample_with_floor(10)}",
            parents=random.sample(tables, parents_per_view.sample_with_floor()),
        )
        for i in range(num_views)
    ]

    for table in tables + views:
        _generate_column_mapping(table)

    now = datetime.now(tz=timezone.utc)
    return SeedMetadata(
        containers=containers,
        tables=tables,
        views=views,
        start_time=now - time_range,
        end_time=now,
    )


def generate_queries(
    seed_metadata: SeedMetadata,
    num_selects: int,
    num_operations: int,
    num_unique_queries: int,
    num_users: int,
    tables_per_select: NormalDistribution = NormalDistribution(3, 5),
    columns_per_select: NormalDistribution = NormalDistribution(10, 5),
    upstream_tables_per_operation: NormalDistribution = NormalDistribution(2, 2),
    query_length: NormalDistribution = NormalDistribution(100, 50),
) -> Iterable[Query]:
    faker = Faker()
    query_texts = [
        faker.paragraph(query_length.sample_with_floor(30) // 30)
        for _ in range(num_unique_queries)
    ]

    all_tables = seed_metadata.tables + seed_metadata.views
    users = [f"user-{i}@xyz.com" for i in range(num_users)]
    for i in range(num_selects):  # Pure SELECT statements
        tables = _sample_list(all_tables, tables_per_select)
        all_columns = [
            FieldAccess(column, table) for table in tables for column in table.columns
        ]
        yield Query(
            text=random.choice(query_texts),
            type="SELECT",
            actor=random.choice(users),
            timestamp=_random_time_between(
                seed_metadata.start_time, seed_metadata.end_time
            ),
            fields_accessed=_sample_list(all_columns, columns_per_select),
        )

    for i in range(num_operations):
        modified_table = random.choice(seed_metadata.tables)
        n_col = len(modified_table.columns)
        num_columns_modified = NormalDistribution(n_col / 2, n_col / 2)
        upstream_tables = _sample_list(all_tables, upstream_tables_per_operation)

        all_columns = [
            FieldAccess(column, table)
            for table in upstream_tables
            for column in table.columns
        ]
        yield Query(
            text=random.choice(query_texts),
            type=random.choice(OPERATION_TYPES),
            actor=random.choice(users),
            timestamp=_random_time_between(
                seed_metadata.start_time, seed_metadata.end_time
            ),
            # Can have no field accesses, e.g. on a standard INSERT
            fields_accessed=_sample_list(all_columns, num_columns_modified, 0),
            object_modified=modified_table,
        )


def _generate_column_mapping(table: Table) -> ColumnMapping:
    d = {}
    for column in table.columns:
        d[column] = Column(
            name=column,
            type=random.choice(list(ColumnType)),
            nullable=random.random() < 0.1,  # Fixed 10% chance for now
        )
    table.column_mapping = d
    return d


def _sample_list(lst: List[T], dist: NormalDistribution, floor: int = 1) -> List[T]:
    return random.sample(lst, min(dist.sample_with_floor(floor), len(lst)))


def _random_time_between(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=(end - start).total_seconds() * random.random())
