"""Config validation that fails fast, and the probe helpers that translate a
`--parent` path into the identifiers ingestion actually filters on."""

import pathlib
from typing import Any, Dict, List, Optional, Tuple

import pytest

from datahub.ingestion.agent.verdicts import ClassifyContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from tests.unit.unity.conftest import unity_config


def test_local_temp_path_must_be_an_existing_directory(
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(ValueError):
        unity_config(local_temp_path=str(tmp_path / "missing"))
    assert unity_config(local_temp_path=str(tmp_path)).local_temp_path == tmp_path


def test_profiling_needs_a_warehouse_from_either_level() -> None:
    with pytest.raises(ValueError):
        unity_config(profiling={"enabled": True, "method": "analyze"})
    inherited = unity_config(
        warehouse_id="wh", profiling={"enabled": True, "method": "analyze"}
    )
    assert inherited.profiling.warehouse_id == "wh"


class _Warnings:
    def __init__(self) -> None:
        self.messages: List[str] = []

    def __call__(self, message: str) -> None:
        self.messages.append(message)


def test_table_verdict_target_uses_the_parent_or_single_pinned_catalog() -> None:
    """The probe's --parent (database) or a single pinned catalog qualifies a
    table; with several pinned catalogs and no parent it declines with a
    warning rather than guessing."""
    warnings = _Warnings()

    assert (
        unity_config().probe_filter_target("sales", "orders", warnings, database="main")
        == "main.sales.orders"
    )
    assert (
        unity_config(catalogs=["main"]).probe_filter_target("sales", "orders", warnings)
        == "main.sales.orders"
    )
    assert warnings.messages == []

    # Several pinned catalogs and no --parent: there is no single right answer.
    assert (
        unity_config(catalogs=["main", "dev"]).probe_filter_target(
            "sales", "orders", warnings
        )
        is None
    )
    assert warnings.messages


def _ctx(
    config: UnityCatalogSourceConfig, kind: str, name: str, parents: Tuple[str, ...]
) -> Tuple[ClassifyContext, _Warnings]:
    warnings = _Warnings()
    ctx = ClassifyContext(
        config=config,
        name=name,
        fqn=".".join([*parents, name]),
        pattern_field=None,
        parent_path=parents,
        warn=warnings,
        kind=kind,
    )
    return ctx, warnings


@pytest.mark.parametrize(
    ("config_kwargs", "kind", "parents", "expected", "warned"),
    [
        ({}, DatasetContainerSubTypes.CATALOG, (), "main", False),
        (
            {"include_metastore": True},
            DatasetContainerSubTypes.CATALOG,
            ("ms",),
            "ms.main",
            False,
        ),
        (
            {"include_metastore": True},
            DatasetContainerSubTypes.CATALOG,
            (),
            "main",
            True,
        ),
        ({}, DatasetContainerSubTypes.SCHEMA, ("main",), "main.sales", False),
        (
            {"catalogs": ["main"]},
            DatasetContainerSubTypes.SCHEMA,
            (),
            "main.sales",
            False,
        ),
        ({}, DatasetContainerSubTypes.SCHEMA, (), "sales", True),
    ],
)
def test_container_verdicts_match_the_ids_ingestion_filters_on(
    config_kwargs: Dict[str, Any],
    kind: str,
    parents: Tuple[str, ...],
    expected: Optional[str],
    warned: bool,
) -> None:
    """Catalog / schema verdicts must be judged on the same id ingestion
    filters on (metastore-prefixed when include_metastore is set), and warn
    when the parent path is too short to build that id."""
    name = "main" if kind == DatasetContainerSubTypes.CATALOG else "sales"
    ctx, warnings = _ctx(unity_config(**config_kwargs), kind, name, parents)

    assert unity_config(**config_kwargs).probe_match_target(ctx) == expected
    assert bool(warnings.messages) == warned
