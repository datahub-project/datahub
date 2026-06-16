"""Tests for Glue ingestion with special characters in database/table names.

Root cause of reported issue: AllowDenyPattern treats allow entries as regexes.
When a user configures database_pattern.allow with a literal database name that
contains regex metacharacters like ( and ), the pattern does not match the
literal name and the database is silently dropped.

Engineering workaround: escape the metacharacters in the recipe, e.g.
  raw__user-provided_\\(upd/upo\\)_framework

Note: / in Glue names is NOT encoded in URNs — this is consistent with how
other DataHub sources (e.g. S3) handle path separators. Literal / in the URN
name field is intentional and expected.
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
    """Tables with special chars in names must produce workunits with the default allow-all pattern."""
    source = _make_source()
    table = _make_table(db_name, table_name)
    workunits = list(source._gen_table_wu(table=table))
    assert workunits, f"No workunits produced for {db_name}.{table_name}"


def test_safe_name_urn_unchanged() -> None:
    """Names with no special chars must appear unmodified in the URN."""
    source = _make_source()
    table = _make_table("safe_db", "safe_table")
    workunits = list(source._gen_table_wu(table=table))
    dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
    assert any("safe_db.safe_table" in urn for urn in dataset_urns)


def test_paren_name_encoded_in_urn() -> None:
    """( and ) in names must be encoded as %28/%29 in the URN."""
    source = _make_source()
    table = _make_table("db_with_(parens)", "tbl")
    workunits = list(source._gen_table_wu(table=table))
    dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
    assert any("%28parens%29" in urn for urn in dataset_urns)


def test_slash_in_name_passes_through_to_urn() -> None:
    """/ in Glue names is left unencoded in the URN, consistent with S3 and other sources."""
    source = _make_source()
    table = _make_table("db/with/slashes", "tbl")
    workunits = list(source._gen_table_wu(table=table))
    dataset_urns = [wu.get_urn() for wu in workunits if "dataset" in wu.get_urn()]
    assert any("db/with/slashes" in urn for urn in dataset_urns)


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
    assert any("glue" in urn for urn in urns)
