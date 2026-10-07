"""Which Glue databases, tables and jobs ingestion keeps, as pure functions of
the recipe and of facts about one object.

Shared by GlueSource (ingestion) and GlueSourceConfig.probe_verdict_override
(`probe filter`). The probe passes None for a fact a bare name cannot supply,
and that rule is then skipped (the override warns).
"""

from typing import Optional, Protocol

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

IGNORE_RESOURCE_LINKS = "ignore_resource_links"


class GlueSelectionConfig(Protocol):
    @property
    def database_pattern(self) -> AllowDenyPattern: ...

    @property
    def table_pattern(self) -> AllowDenyPattern: ...

    @property
    def catalog_id(self) -> Optional[str]: ...

    @property
    def ignore_resource_links(self) -> Optional[bool]: ...

    @property
    def extract_transforms(self) -> Optional[bool]: ...


def database_verdict(
    config: GlueSelectionConfig,
    *,
    name: Optional[str],
    catalog_id: Optional[str],
    is_resource_link: Optional[bool],
) -> Verdict:
    """get_all_databases' order: a resource link is never even listed under
    ignore_resource_links, then database_pattern, then a database another
    catalog owns when catalog_id pins one ("" or None: the caller's own).

    `name` is None when the probe judges a table with no --parent: the
    database pattern is then the parent check's, not this one's."""
    if config.ignore_resource_links and is_resource_link:
        return Verdict.exclude(IGNORE_RESOURCE_LINKS)
    if name is not None and not config.database_pattern.allowed(name):
        return Verdict.exclude("database_pattern")
    if config.catalog_id and catalog_id and catalog_id != config.catalog_id:
        return Verdict.exclude("catalog_id")
    return Verdict.include()


def table_link_verdict(
    config: GlueSelectionConfig, *, is_resource_link: bool
) -> Verdict:
    """A table-level Lake Formation resource link (one with a TargetTable),
    which sits inside an ordinary database, so the database rule misses it."""
    if config.ignore_resource_links and is_resource_link:
        return Verdict.exclude(IGNORE_RESOURCE_LINKS)
    return Verdict.include()


def table_pattern_verdict(
    config: GlueSelectionConfig, *, database: str, table: str
) -> Verdict:
    """_gen_table_wu: table_pattern is matched on "database.table"."""
    full_name = f"{database}.{table}"
    if not config.database_pattern.allowed(database):
        return Verdict.exclude("database_pattern")
    if not config.table_pattern.allowed(full_name):
        return Verdict.exclude("table_pattern", matched_target=full_name)
    return Verdict.include()


def table_verdict(
    config: GlueSelectionConfig,
    *,
    database: str,
    table: str,
    is_resource_link: Optional[bool],
) -> Verdict:
    """get_tables_from_database (the link), then _gen_table_wu (the patterns)."""
    if is_resource_link is not None:
        by_link = table_link_verdict(config, is_resource_link=is_resource_link)
        if not by_link.included:
            return by_link
    return table_pattern_verdict(config, database=database, table=table)


def jobs_verdict(config: GlueSelectionConfig) -> Verdict:
    # Optional[bool]: null switches jobs off too, so this is not a kind switch.
    if config.extract_transforms:
        return Verdict.include()
    return Verdict.exclude("extract_transforms")
