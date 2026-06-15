"""Tests for Glue ingestion with special characters in database/table names.

Root cause: Glue allows / in names; UrnEncoder does not encode / so literal /
ends up in the URN and breaks DataHub lookups. Fix: pre-encode / as %2F before
URN construction.

Pattern-matching root cause: AllowDenyPattern treats allow entries as regexes.
Users must escape ( and ) when filtering by a literal database name.
Engineering workaround: use raw__user-provided_\(upd/upo\)_framework.
"""

from typing import Any, Dict, List, Tuple

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig


def _make_table(db_name: str, table_name: str) -> Dict[str, Any]:
    return {
        "Name": table_name,
        "DatabaseName": db_name,
        "CatalogId": "123456789012",
        "StorageDescriptor": {
            "Columns": [{"Name": "id", "Type": "bigint", "Comment": ""}],
            "Location": "s3://my-bucket/data",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Compressed": False,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "SortColumns": [],
            "StoredAsSubDirectories": False,
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {},
    }


def _make_source() -> GlueSource:
    return GlueSource(
        ctx=PipelineContext(run_id="test-special-chars"),
        config=GlueSourceConfig(
            aws_region="us-east-1",
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            extract_transforms=False,
            extract_lakeformation_tags=False,
        ),
    )


SPECIAL_CHAR_CASES: List[Tuple[str, str]] = [
    # Reported case: parens + slash in database name
    ("raw__user-provided_(upd/upo)_framework", "draft_base_file_0120"),
    # Slashes only in database name
    ("db/with/slashes", "normal_table"),
    # Slashes only in table name
    ("normal_db", "table/with/slashes"),
    # Slashes in both
    ("db/slash", "table/slash"),
    # Parens and slashes in both
    ("my_(database)/v2", "my_(table)/v1"),
    # Safe chars — must be completely unchanged
    ("safe-db_name.v2", "safe-table_name.v1"),
]

SPECIAL_CHAR_IDS = [f"{db}.{tbl}" for db, tbl in SPECIAL_CHAR_CASES]


@pytest.mark.parametrize("db_name,table_name", SPECIAL_CHAR_CASES, ids=SPECIAL_CHAR_IDS)
def test_special_char_name_produces_workunits(db_name: str, table_name: str) -> None:
    """Tables with special chars in names must produce workunits, not be silently dropped."""
    source = _make_source()
    table = _make_table(db_name, table_name)
    workunits = list(source._gen_table_wu(table=table))
    assert workunits, f"No workunits produced for {db_name}.{table_name}"


@pytest.mark.parametrize("db_name,table_name", SPECIAL_CHAR_CASES, ids=SPECIAL_CHAR_IDS)
def test_special_char_name_urn_has_no_unencoded_slash(
    db_name: str, table_name: str
) -> None:
    """The name field in a dataset URN must not contain a literal /."""
    source = _make_source()
    table = _make_table(db_name, table_name)
    workunits = list(source._gen_table_wu(table=table))

    for wu in workunits:
        urn = wu.get_urn()
        if not urn.startswith("urn:li:dataset:"):
            continue
        # URN format: urn:li:dataset:(urn:li:dataPlatform:glue,<name>,<env>)
        # Strip the outer wrapper and extract the name component.
        inner = urn[len("urn:li:dataset:(") : -1]
        parts = inner.split(",")
        if len(parts) >= 3:
            # parts[0] = platform URN, parts[-1] = env, middle = name (may contain commas)
            name_part = ",".join(parts[1:-1])
            assert "/" not in name_part, (
                f"Unencoded / in URN name '{name_part}' for {db_name}.{table_name}"
            )


def test_safe_name_urn_unchanged() -> None:
    """Names with no special chars must produce the same URN as before this fix."""
    source = _make_source()
    table = _make_table("safe_db", "safe_table")
    workunits = list(source._gen_table_wu(table=table))
    dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
    assert any("safe_db.safe_table" in urn for urn in dataset_urns), (
        "Safe names must appear unmodified in the URN"
    )


def test_paren_name_still_encoded_as_before() -> None:
    """Names with ( and ) must still encode them as %28/%29 (backward compat)."""
    source = _make_source()
    table = _make_table("db_with_(parens)", "tbl")
    workunits = list(source._gen_table_wu(table=table))
    dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
    assert any("%28parens%29" in urn for urn in dataset_urns), (
        "( and ) must be encoded as %28 and %29"
    )


def test_database_pattern_with_literal_parens_requires_escaping() -> None:
    """AllowDenyPattern treats entries as regexes; special chars need escaping.

    Engineering workaround for users filtering on databases like
    raw__user-provided_(upd/upo)_framework: escape as
    raw__user-provided_\\(upd/upo\\)_framework in the recipe.
    """
    db_name = "raw__user-provided_(upd/upo)_framework"

    # Unescaped: ( and ) are regex metacharacters — match fails
    unescaped = AllowDenyPattern(allow=["raw__user-provided_(upd/upo)_framework"])
    assert not unescaped.allowed(db_name), (
        "Unescaped pattern should not match literal name "
        "because ( and ) are regex metacharacters"
    )

    # Properly escaped: matches the literal name
    escaped = AllowDenyPattern(allow=[r"raw__user-provided_\(upd/upo\)_framework"])
    assert escaped.allowed(db_name)

    # Default allow-all pattern always works regardless of special chars
    assert AllowDenyPattern().allowed(db_name)


def test_engineering_suggested_table_pattern_works() -> None:
    """Verify the exact pattern engineering suggested to customers works.

    Pattern: raw__user-provided_\\(upd/upo\\)_framework\\.draft_base_file_0120$
    This is a table_pattern.allow entry matching the full db.table string with
    escaped parens, escaped dot separator, and end-of-string anchor.
    """
    full_table_name = "raw__user-provided_(upd/upo)_framework.draft_base_file_0120"

    # Unescaped: fails because ( and ) are regex metacharacters
    unescaped = AllowDenyPattern(
        allow=["raw__user-provided_(upd/upo)_framework.draft_base_file_0120"]
    )
    assert not unescaped.allowed(full_table_name)

    # Engineers' suggested escaped pattern: must match
    suggested = AllowDenyPattern(
        allow=[r"raw__user-provided_\(upd/upo\)_framework\.draft_base_file_0120$"]
    )
    assert suggested.allowed(full_table_name)

    # Must not accidentally match a similar but different table
    assert not suggested.allowed(
        "raw__user-provided_(upd/upo)_framework.draft_base_file_0120_extra"
    )


def test_default_pattern_ingests_special_char_database() -> None:
    """With the default allow-all pattern, special-char databases are ingested."""
    source = _make_source()
    db_name = "raw__user-provided_(upd/upo)_framework"
    table = _make_table(db_name, "draft_base_file_0120")

    workunits = list(source._gen_table_wu(table=table))

    assert workunits, "Table should be ingested with the default .* pattern"
    urns = [wu.get_urn() for wu in workunits]
    assert any("glue" in urn for urn in urns), (
        "At least one workunit URN should reference the glue platform"
    )
