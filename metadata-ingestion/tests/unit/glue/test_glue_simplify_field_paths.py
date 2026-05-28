import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import (
    GlueSource,
    GlueSourceConfig,
    _is_complex_hive_type,
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
        # Uniontype nested inside other complex types must also be detected,
        # otherwise its variants collide on v1 downgrade and GMS dedups one.
        ("struct<payload:uniontype<int,string>>", True),
        ("array<uniontype<int,string>>", True),
        ("map<string,uniontype<int,string>>", True),
        ("struct<a:array<struct<b:uniontype<int,string>>>>", True),
    ],
)
def test_is_complex_hive_type(hive_type: str, expected: bool) -> None:
    assert _is_complex_hive_type(hive_type) is expected


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


@pytest.mark.parametrize(
    "hive_type",
    [
        "uniontype<int,string>",
        # Nested unions must also be detected — otherwise the variant SchemaField
        # entries collapse to the same v1 path and one is silently dropped by GMS.
        "struct<payload:uniontype<int,string>>",
        "array<uniontype<int,string>>",
        "map<string,uniontype<int,string>>",
    ],
)
def test_flag_on_preserves_v2_for_uniontype(hive_type: str) -> None:
    source = _build_glue_source(simplify=True)
    fields = source._build_schema_fields(
        hive_column_name="payload",
        hive_column_type=hive_type,
        description=None,
        default_nullable=True,
    )
    assert fields, f"expected at least one SchemaField for {hive_type}"
    for f in fields:
        assert f.fieldPath.startswith("[version=2.0]"), (
            f"{hive_type!r} produced non-v2 fieldPath: {f.fieldPath}"
        )
    # And critically: no collisions in the path set (the very property the helper
    # exists to protect).
    paths = [f.fieldPath for f in fields]
    assert len(set(paths)) == len(paths), (
        f"{hive_type!r} produced duplicate fieldPaths: {paths}"
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


def test_flag_on_keeps_v2_when_downgrade_would_produce_empty_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Column names ending with `]` are stripped to an empty path by the v2->v1
    # converter. The empty path would yield a malformed schemaField URN that
    # silently dedups across columns, so we must fall back to the v2 path and warn.
    source = _build_glue_source(simplify=True)
    with caplog.at_level("WARNING"):
        fields = source._build_schema_fields(
            hive_column_name="arr[idx]",
            hive_column_type="string",
            description=None,
            default_nullable=True,
        )
    assert len(fields) == 1
    assert fields[0].fieldPath == "[version=2.0].[type=string].arr[idx]"
    assert any(
        "produced empty path" in r.message and "arr[idx]" in r.message
        for r in caplog.records
    )


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
