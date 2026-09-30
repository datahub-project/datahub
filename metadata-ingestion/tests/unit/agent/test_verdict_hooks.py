"""Framework behaviour of the shared verdict hooks, on fake configs.

Each fake is registered through the same two seams test_filter_check's
fixtures use, so nothing here depends on a real connector.
"""

from typing import Annotated, Dict, List, Mapping, Optional, Sequence, Type

import pytest
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def _register(monkeypatch: pytest.MonkeyPatch, config_cls: Type[ConfigModel]) -> None:
    monkeypatch.setattr(filter_check, "config_class_for", lambda _st: config_cls)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])


def _judge(
    kind: str,
    names: List[str],
    config_dict: Optional[Dict[str, object]] = None,
    parent_path: Sequence[str] = (),
    try_allow: Optional[Sequence[str]] = None,
    try_deny: Optional[Sequence[str]] = None,
) -> FilterCheckResult:
    return check_filters(
        source_type="fake",
        config_dict=config_dict or {},
        kind=kind,
        parent_path=parent_path,
        names=names,
        try_allow=try_allow,
        try_deny=try_deny,
    )


class _Switched(ConfigModel):
    extract_lakehouses: bool = True
    lakehouse_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.FABRIC_LAKEHOUSE)
    ] = Field(default=AllowDenyPattern.allow_all())

    @classmethod
    def probe_kind_switches(cls) -> Mapping[str, str]:
        return {str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE): "extract_lakehouses"}


def test_a_declared_switch_that_is_off_excludes_the_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Switched)
    result = _judge(
        str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE),
        ["lh"],
        config_dict={"extract_lakehouses": False},
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "extract_lakehouses")
    ]


def test_a_declared_switch_that_is_on_leaves_the_pattern_in_charge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Switched)
    result = _judge(
        str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE),
        ["lh", "other"],
        config_dict={"lakehouse_pattern": {"allow": ["^lh$"]}},
    )
    assert [r.included for r in result.results] == [True, False]


def test_the_sql_default_switches_still_apply_without_a_declaration() -> None:
    # MySQL declares no probe_kind_switches; include_views must keep working.
    result = check_filters(
        source_type="mysql",
        config_dict={
            "host_port": "localhost:3306",
            "username": "u",
            "password": "p",
            "include_views": False,
        },
        kind=str(DatasetSubTypes.VIEW),
        parent_path=["db"],
        names=["v"],
    )
    assert result.results[0].excluded_by == "include_views"
