import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import (
    GlueSource,
    GlueSourceConfig,
    _hive_type_requires_v2_paths,
)


def _build_glue_source(*, simplify: bool) -> GlueSource:
    return GlueSource(
        ctx=PipelineContext(run_id="glue-simplify-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            extract_transforms=False,
            simplify_nested_field_paths=simplify,
        ),
    )


@pytest.mark.parametrize(
    "hive_type, expected",
    [
        # Only uniontype requires v2 paths; every other shape downgrades safely.
        ("string", False),
        ("int", False),
        ("bigint", False),
        ("decimal(10,2)", False),
        ("varchar(255)", False),
        ("map<string,int>", False),
        ("array<string>", False),
        ("array<struct<id:int,name:string>>", False),
        ("struct<id:int,name:string>", False),
        ("uniontype<int,string>", True),
        ("  UNIONTYPE<int,string>  ", True),  # leading whitespace + uppercase
    ],
)
def test_hive_type_requires_v2_paths(hive_type: str, expected: bool) -> None:
    assert _hive_type_requires_v2_paths(hive_type) is expected


def test_flag_off_keeps_v2_for_scalar() -> None:
    source = _build_glue_source(simplify=False)
    fields = source._build_schema_fields(
        hive_column_name="email",
        hive_column_type="string",
        description=None,
        default_nullable=True,
    )
    assert len(fields) == 1
    assert fields[0].fieldPath == "[version=2.0].[type=string].email"


def test_flag_on_downgrades_scalar_to_v1() -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="email",
        hive_column_type="string",
        description=None,
        default_nullable=True,
    )
    assert len(fields) == 1
    assert fields[0].fieldPath == "email"


def test_flag_on_preserves_v2_for_uniontype() -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="payload",
        hive_column_type="uniontype<int,string>",
        description=None,
        default_nullable=True,
    )
    assert fields, "expected at least one SchemaField for uniontype"
    for f in fields:
        assert f.fieldPath.startswith("[version=2.0]"), (
            f"uniontype produced non-v2 fieldPath: {f.fieldPath}"
        )


@pytest.mark.parametrize(
    "hive_type, expected_paths",
    [
        (
            "struct<id:int,name:string>",
            {"person", "person.id", "person.name"},
        ),
        # Nested struct still produces unique dotted paths after downgrade.
        (
            "struct<inner:struct<id:int>,name:string>",
            {"person", "person.inner", "person.inner.id", "person.name"},
        ),
    ],
)
def test_flag_on_downgrades_struct_to_v1(hive_type: str, expected_paths: set) -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="person",
        hive_column_type=hive_type,
        description=None,
        default_nullable=True,
    )
    paths = {f.fieldPath for f in fields}
    assert paths == expected_paths
    for f in fields:
        assert not f.fieldPath.startswith("[version=2.0]")


@pytest.mark.parametrize(
    "hive_type, expected_paths",
    [
        ("array<string>", {"tags"}),
        # Array-of-struct: container + leaf struct fields all downgrade.
        ("array<struct<id:int,name:string>>", {"tags", "tags.id", "tags.name"}),
        # Deeply nested arrays still produce unique paths after downgrade.
        (
            "array<struct<nested:array<struct<x:int>>>>",
            {"tags", "tags.nested", "tags.nested.x"},
        ),
    ],
)
def test_flag_on_downgrades_array_to_v1(hive_type: str, expected_paths: set) -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="tags",
        hive_column_type=hive_type,
        description=None,
        default_nullable=True,
    )
    paths = {f.fieldPath for f in fields}
    assert paths == expected_paths
    for f in fields:
        assert not f.fieldPath.startswith("[version=2.0]")


@pytest.mark.parametrize(
    "hive_type, expected_paths",
    [
        ("map<string,int>", {"counts"}),
        # Map value-side struct fields downgrade to flat dotted paths too.
        ("map<string,struct<a:int,b:string>>", {"counts", "counts.a", "counts.b"}),
    ],
)
def test_flag_on_downgrades_map_to_v1(hive_type: str, expected_paths: set) -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="counts",
        hive_column_type=hive_type,
        description=None,
        default_nullable=True,
    )
    paths = {f.fieldPath for f in fields}
    assert paths == expected_paths
    for f in fields:
        assert not f.fieldPath.startswith("[version=2.0]")


def test_flag_on_preserves_description_on_downgrade() -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="event_id",
        hive_column_type="int",
        description="primary event identifier",
        default_nullable=True,
    )
    assert len(fields) == 1
    assert fields[0].fieldPath == "event_id"
    assert fields[0].description == "primary event identifier"
