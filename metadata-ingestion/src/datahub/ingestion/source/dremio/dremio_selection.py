"""Which Dremio containers and datasets ingestion keeps, as pure functions of
the recipe and facts about one object.

Shared by ingestion (DremioFilter.is_schema_allowed, DremioSource.process_dataset)
and by `probe filter` (DremioSourceConfig.probe_verdict_override).
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Literal, Protocol, Sequence, Union

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

SCHEMA_PATTERN = "schema_pattern"
DATASET_PATTERN = "dataset_pattern"
INCLUDE_SYSTEM_TABLES = "include_system_tables"
# Not a config field: Dremio Reflections live under this root, and ingestion
# never emits them.
REFLECTION_ROOT = "_accelerator_"
# Not a config field: ingestion's dataset query joins INFORMATION_SCHEMA.COLUMNS,
# so a dataset Dremio holds no column metadata for (a source table nothing has
# queried yet) never reaches it.
NO_COLUMN_METADATA = "no_column_metadata"

DREMIO_SYSTEM_TABLES_PATTERN = [
    r"^information_schema$",
    r"^sys$",
    r"^information_schema\..*",
    r"^sys\..*",
]

# DremioEdition values, restated: dremio_api imports this module.
COMMUNITY_EDITION = "COMMUNITY"


class DremioSelectionConfig(Protocol):
    @property
    def schema_pattern(self) -> AllowDenyPattern: ...

    @property
    def dataset_pattern(self) -> AllowDenyPattern: ...

    @property
    def include_system_tables(self) -> bool: ...


def _deny_only(config: DremioSelectionConfig) -> AllowDenyPattern:
    return AllowDenyPattern(allow=[".*"], deny=config.schema_pattern.deny)


def container_verdict(config: DremioSelectionConfig, path: Sequence[str]) -> Verdict:
    """One container (source, space or folder) by its full path, root first.

    A root passes schema_pattern on its bare name, or as the first segment of
    a dotted allow entry; a folder on its lower-cased dotted path, or as a
    prefix of an allow entry ending in `.*`. Ingestion reaches a folder only
    under a root that passed: `folder_verdict` adds that.
    """
    if not path:
        return Verdict.include()
    full_name = ".".join(path).lower()
    if not config.include_system_tables and not AllowDenyPattern(
        deny=DREMIO_SYSTEM_TABLES_PATTERN
    ).allowed(full_name):
        return Verdict.exclude(INCLUDE_SYSTEM_TABLES)

    schema_pattern = config.schema_pattern
    if len(path) == 1:
        root = path[0]
        if schema_pattern.allowed(root):
            return Verdict.include()
        for pattern in schema_pattern.allow:
            if "." in pattern and pattern.lower().startswith(root.lower() + "."):
                if _deny_only(config).allowed(root):
                    return Verdict.include()
        return Verdict.exclude(SCHEMA_PATTERN)

    if schema_pattern.allowed(full_name):
        return Verdict.include()
    for pattern in schema_pattern.allow:
        if pattern.endswith(".*") and pattern[:-2].lower().startswith(full_name + "."):
            if _deny_only(config).allowed(full_name):
                return Verdict.include()
    return Verdict.exclude(SCHEMA_PATTERN)


def folder_verdict(config: DremioSelectionConfig, path: Sequence[str]) -> Verdict:
    """A folder is walked only under a root container that passed, and then
    judged on its own path; a folder that fails does not hide the folders
    below it (DremioAPIOperations.get_containers_for_location)."""
    root = container_verdict(config, path[:1])
    if not root.included:
        return root
    return container_verdict(config, path)


def sql_schema_filter_value(edition: str, schema: str, table: str) -> str:
    """The string ingestion's dataset query filters on
    (DremioSQLQueries.SCHEMA_FILTER_FIELD over TABLE_SCHEMA): the dotted
    schema on Community, which reads INFORMATION_SCHEMA; the dotted path
    including the dataset's own name on Enterprise and Cloud, whose SYS views
    put the whole path in that column."""
    if edition == COMMUNITY_EDITION:
        return schema
    return f"{schema}.{table}"


def _regexp_like(patterns: List[str], value: str) -> bool:
    # Dremio's REGEXP_LIKE finds the pattern anywhere in the value (it is not
    # anchored, unlike AllowDenyPattern), and the query compares upper-cased
    # text with upper-cased patterns, so this is re.search on both upper-cased.
    # Java and Python regex agree on the syntax schema patterns use.
    return re.search("|".join(f"({p})" for p in patterns), value) is not None


def sql_schema_filter_allows(pattern: AllowDenyPattern, value: str) -> bool:
    """Whether a dataset's row survives the schema_pattern condition ingestion
    pushes into its dataset query (DremioSQLQueries.pattern_condition)."""
    field = value.upper().replace(", ", ".").replace("[", "").replace("]", "")
    allow = DremioSQLQueries.pushed_patterns(pattern.allow, allow=True)
    if allow and not _regexp_like(allow, field):
        return False
    deny = DremioSQLQueries.pushed_patterns(pattern.deny, allow=False)
    return not (deny and _regexp_like(deny, field))


class _Unknown(Enum):
    UNKNOWN = "unknown"


# A fact `probe filter` was not given: that rule is skipped.
UNKNOWN: Literal[_Unknown.UNKNOWN] = _Unknown.UNKNOWN


@dataclass(frozen=True)
class DatasetFacts:
    # The container path, root first.
    path: Sequence[str]
    name: str
    # What the dataset query's schema condition reads (sql_schema_filter_value).
    # Ingestion leaves this and has_columns UNKNOWN: its query has applied both.
    schema_filter_value: Union[str, _Unknown] = UNKNOWN
    has_columns: Union[bool, _Unknown] = UNKNOWN


def dataset_verdict(config: DremioSelectionConfig, facts: DatasetFacts) -> Verdict:
    """The dataset query's schema condition and column join, then the
    Reflection root, then dataset_pattern on the lower-cased dotted path: the
    order a row meets them on its way to DremioSource.process_dataset.
    include_system_tables plays no part: the query never returns a system
    table."""
    if facts.schema_filter_value is not UNKNOWN and not sql_schema_filter_allows(
        config.schema_pattern, facts.schema_filter_value
    ):
        return Verdict.exclude(SCHEMA_PATTERN)
    if facts.has_columns is False:
        return Verdict.exclude(NO_COLUMN_METADATA)
    if facts.path and facts.path[0] == REFLECTION_ROOT:
        return Verdict.exclude(REFLECTION_ROOT)
    # Joined as process_dataset joins it, so even an empty path matches alike.
    if not config.dataset_pattern.allowed(
        f"{'.'.join(facts.path)}.{facts.name}".lower()
    ):
        return Verdict.exclude(DATASET_PATTERN)
    return Verdict.include()
