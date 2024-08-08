"""
Generates data for performance testing of warehouse sources.

In the future, we could try to create a more realistic dataset
by anonymizing and reduplicating a production datahub instance's data.
We could also get more human data by using Faker.

This is a work in progress, built piecemeal as needed.
"""
import random
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Collection, Iterable, List, Optional, TypeVar, Union, cast

from faker import Faker

from tests.performance.data_model import (
    Column,
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

ID_COLUMN = "id"  # Use to allow joins between all tables


class Distribution(metaclass=ABCMeta):
    @abstractmethod
    def _sample(self) -> int:
        raise NotImplementedError

    def sample(
        self, *, floor: Optional[int] = None, ceiling: Optional[int] = None
    ) -> int:
        value = self._sample()
        if floor is not None:
            value = max(value, floor)
        if ceiling is not None:
            value = min(value, ceiling)
        return value


@dataclass(frozen=True)
class NormalDistribution(Distribution):
    mu: float
    sigma: float

    def _sample(self) -> int:
        return int(random.gauss(mu=self.mu, sigma=self.sigma))


@dataclass(frozen=True)
class LomaxDistribution(Distribution):
    """See https://en.wikipedia.org/wiki/Lomax_distribution.

    Equivalent to pareto(scale, shape) - scale; scale * beta_prime(1, shape)
    """

    scale: float
    shape: float

    def _sample(self) -> int:
        return int(self.scale * (random.paretovariate(self.shape) - 1))


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
    columns_per_table: Distribution = NormalDistribution(5, 2),
    parents_per_view: Distribution = NormalDistribution(2, 1),
    view_definition_length: Distribution = NormalDistribution(150, 50),
    time_range: timedelta = timedelta(days=14),
) -> SeedMetadata:
    # Assemble containers
    if isinstance(num_containers, int):
        num_containers = [num_containers]

    containers: List[List[Container]] = []
    for i, num_in_layer in enumerate(num_containers):
        layer = [
            Container(
                f"{_container_type(i)}_{j}",
                parent=random.choice(containers[-1]) if containers else None,
            )
            for j in range(num_in_layer)
        ]
        containers.append(layer)

    # Assemble tables and views, lineage, and definitions
    tables = [
        _generate_table(i, containers[-1], columns_per_table) for i in range(num_tables)
    ]
    views = [
        View(
            **{  # type: ignore
                **_generate_table(i, containers[-1], columns_per_table).__dict__,
                "name": f"view_{i}",
                "definition": f"--{'*' * view_definition_length.sample(floor=0)}",
            },
        )
        for i in range(num_views)
    ]

    for view in views:
        view.upstreams = random.sample(tables, k=parents_per_view.sample(floor=1))

    generate_lineage(tables, views)

    now = datetime.now(tz=timezone.utc)
    return SeedMetadata(
        containers=containers,
        tables=tables,
        views=views,
        start_time=now - time_range,
        end_time=now,
    )


def generate_lineage(
    tables: Collection[Table],
    views: Collection[Table],
    # Percentiles: 75th=0, 80th=1, 95th=2, 99th=4, 99.99th=15
    upstream_distribution: Distribution = LomaxDistribution(scale=3, shape=5),
) -> None:
    num_upstreams = [upstream_distribution.sample(ceiling=100) for _ in tables]
    # Prioritize tables with a lot of upstreams themselves
    factor = 1 + len(tables) // 10
    table_weights = [1 + (num_upstreams[i] * factor) for i in range(len(tables))]
    view_weights = [1] * len(views)

    # TODO: Python 3.9 use random.sample with counts
    sample = []
    for table, weight in zip(tables, table_weights):
        for _ in range(weight):
            sample.append(table)
    for view, weight in zip(views, view_weights):
        for _ in range(weight):
            sample.append(view)
    for i, table in enumerate(tables):
        table.upstreams = random.sample(  # type: ignore
            sample,
            k=num_upstreams[i],
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
        faker.paragraph(query_length.sample(floor=30) // 30)
        for _ in range(num_unique_queries)
    ]

    all_tables = seed_metadata.tables + seed_metadata.views
    users = [f"user_{i}@xyz.com" for i in range(num_users)]
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


def _container_type(i: int) -> str:
    if i == 0:
        return "database"
    elif i == 1:
        return "schema"
    else:
        return f"{i}container"


def _generate_table(
    i: int, parents: List[Container], columns_per_table: Distribution
) -> Table:
    num_columns = columns_per_table.sample(floor=1)

    columns = OrderedDict({ID_COLUMN: Column(ID_COLUMN, ColumnType.INTEGER, False)})
    for j in range(num_columns):
        name = f"column_{j}"
        columns[name] = Column(
            name=name,
            type=random.choice(list(ColumnType)),
            nullable=random.random() < 0.1,  # Fixed 10% chance for now
        )
    return Table(
        f"table_{i}",
        container=random.choice(parents),
        columns=columns,
        upstreams=[],
    )


def _sample_list(lst: List[T], dist: NormalDistribution, floor: int = 1) -> List[T]:
    return random.sample(lst, min(dist.sample(floor=floor), len(lst)))


def _random_time_between(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=(end - start).total_seconds() * random.random())


if __name__ == "__main__":
    z = generate_data(10, 1000, 10)
