import re
from types import SimpleNamespace
from typing import Annotated

import pytest
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent.introspect import (
    _pattern_field_for_config_class,
    is_pattern_field,
    pattern_field_for_config,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _Cfg(ConfigModel):
    schema_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    view_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    topic_patterns: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    # A same-named field that is NOT a pattern — must never be resolved to.
    table_pattern: str = "not-an-allow-deny-pattern"


def test_resolves_by_convention():
    assert (
        _pattern_field_for_config_class(_Cfg, DatasetContainerSubTypes.SCHEMA)
        == "schema_pattern"
    )
    assert _pattern_field_for_config_class(_Cfg, DatasetSubTypes.VIEW) == "view_pattern"


def test_resolves_the_plural_form():
    assert (
        _pattern_field_for_config_class(_Cfg, DatasetSubTypes.TOPIC) == "topic_patterns"
    )


def test_ignores_a_same_named_non_pattern_field():
    # `table_pattern: str` exists but is not an AllowDenyPattern.
    assert _pattern_field_for_config_class(_Cfg, DatasetSubTypes.TABLE) is None


def test_returns_none_when_absent():
    assert (
        _pattern_field_for_config_class(_Cfg, DatasetContainerSubTypes.DATABASE) is None
    )


def test_multiword_kind_collapses_to_underscores():
    class _K(ConfigModel):
        flink_job_pattern: AllowDenyPattern = Field(
            default=AllowDenyPattern.allow_all()
        )

    assert _pattern_field_for_config_class(_K, "Flink Job") == "flink_job_pattern"


_LIST_CHILDREN_IMPORT_RE = re.compile(
    r"from ([\w\.]+) import\s*\(?\s*(list_\w+_children)"
)
_LIST_CHILDREN_CALL_RE = re.compile(r"(\w+)\.list_children\(")


def test_a_declared_hint_beats_the_name_convention():
    """The convention would find `table_pattern`; the hint must win."""

    class _Hinted(ConfigModel):
        table_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
        collection_pattern: Annotated[
            AllowDenyPattern, Filters(DatasetSubTypes.TABLE)
        ] = Field(default=AllowDenyPattern.allow_all())

    assert (
        _pattern_field_for_config_class(_Hinted, DatasetSubTypes.TABLE)
        == "collection_pattern"
    )


def test_a_hint_does_not_remove_the_field_from_convention_matching():
    """A hint adds a binding; `dataset_pattern` still answers a Dataset lookup."""

    class _BigIdLike(ConfigModel):
        dataset_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = (
            Field(default=AllowDenyPattern.allow_all())
        )

    assert (
        _pattern_field_for_config_class(_BigIdLike, DatasetSubTypes.TABLE)
        == "dataset_pattern"
    )
    assert _pattern_field_for_config_class(_BigIdLike, "Dataset") == "dataset_pattern"


def test_a_field_can_hint_more_than_one_kind():
    """One field can filter two kinds (Salesforce's object_pattern filters
    both standard and custom objects) by stacking a Filters per kind."""

    class _SalesforceLike(ConfigModel):
        object_pattern: Annotated[
            AllowDenyPattern,
            Filters(DatasetSubTypes.SALESFORCE_STANDARD_OBJECT),
            Filters(DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT),
        ] = Field(default=AllowDenyPattern.allow_all())

    assert (
        _pattern_field_for_config_class(
            _SalesforceLike, DatasetSubTypes.SALESFORCE_STANDARD_OBJECT
        )
        == "object_pattern"
    )
    assert (
        _pattern_field_for_config_class(
            _SalesforceLike, DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT
        )
        == "object_pattern"
    )


def test_two_fields_hinting_the_same_kind_raise_naming_both():
    class _Conflict(ConfigModel):
        a_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
            default=AllowDenyPattern.allow_all()
        )
        b_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
            default=AllowDenyPattern.allow_all()
        )

    with pytest.raises(ValueError, match="a_pattern.*b_pattern|b_pattern.*a_pattern"):
        _pattern_field_for_config_class(_Conflict, DatasetSubTypes.TABLE)


def test_a_hint_on_a_non_pattern_field_raises():
    class _Bad(ConfigModel):
        thing: Annotated[str, Filters(DatasetSubTypes.TABLE)] = "nope"

    with pytest.raises(ValueError, match="not an AllowDenyPattern"):
        _pattern_field_for_config_class(_Bad, DatasetSubTypes.TABLE)


def test_annotating_a_field_keeps_it_recognised_as_a_pattern_field():
    """Guards the one way this change could silently break describe/scaffold."""

    class _Ann(ConfigModel):
        p: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
            default=AllowDenyPattern.allow_all()
        )

    assert is_pattern_field(_Ann.model_fields["p"].annotation)


def test_the_hint_wins_over_an_instance_level_convention_match():
    """pattern_field_for_config checks live attributes; the hint still wins."""

    class _Hinted(ConfigModel):
        table_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
        collection_pattern: Annotated[
            AllowDenyPattern, Filters(DatasetSubTypes.TABLE)
        ] = Field(default=AllowDenyPattern.allow_all())

    assert (
        pattern_field_for_config(_Hinted(), DatasetSubTypes.TABLE)
        == "collection_pattern"
    )


def test_an_object_without_model_fields_still_resolves_by_convention():
    """Test fixtures are plain SimpleNamespaces — they have no hints."""
    cfg = SimpleNamespace(schema_pattern=AllowDenyPattern.allow_all())
    assert (
        pattern_field_for_config(cfg, DatasetContainerSubTypes.SCHEMA)
        == "schema_pattern"
    )


def test_no_probe_capable_config_declares_conflicting_hints():
    """A second field claiming a kind would make resolution ambiguous, and the
    probe would silently filter on the wrong pattern."""
    # function-scoped: walking every connector package is expensive and must not
    # run at collection time for the rest of this module's fast unit tests.
    import importlib
    import pkgutil
    from collections import defaultdict

    import datahub.ingestion.source as srcpkg
    from datahub.configuration.common import Filters

    conflicts = {}
    for mod in pkgutil.walk_packages(srcpkg.__path__, srcpkg.__name__ + "."):
        try:
            module = importlib.import_module(mod.name)
        except Exception:
            continue  # optional connector dependency not installed
        for obj in vars(module).values():
            if not (isinstance(obj, type) and hasattr(obj, "model_fields")):
                continue
            by_kind = defaultdict(list)
            for fname, field in obj.model_fields.items():
                for meta in field.metadata:
                    if isinstance(meta, Filters):
                        by_kind[str(meta.kind)].append(fname)
            for kind, names in by_kind.items():
                if len(names) > 1:
                    conflicts[f"{obj.__name__}:{kind}"] = sorted(names)
    assert not conflicts, conflicts
