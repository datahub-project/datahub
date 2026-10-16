"""The S3 and GCS probes' dataset verdicts against what their sources actually
emit, with moto standing in for S3 and for GCS's S3-compatible endpoint (as
tests/integration/gcs does).

The guarantee pinned here: the probe never under-reports, and it over-reports
only where it says it cannot judge -- a {table} folder in which no object passes
the spec's file rules. That case is checked by listing such a folder, not with a
hand-kept list. The one naming gap is declared too: a table folder whose name
ingestion cannot parse is emitted under a file inside it, and the probe warns.
"""

from typing import Dict, List, Mapping, Tuple

import pytest

from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.gcs.gcs_source import (
    GCSSource,
    GCSSourceConfig,
    equivalent_s3_path_specs,
)
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.source import S3Source
from tests.test_helpers.object_store_probe_helpers import ObjectStorePlatform


def _s3_file_rules(recipe: Dict[str, object]) -> Tuple[List[PathSpec], bool]:
    config = DataLakeSourceConfig.model_validate(recipe)
    return config.path_specs, config.use_s3_content_type


def _gcs_file_rules(recipe: Dict[str, object]) -> Tuple[List[PathSpec], bool]:
    # Ingestion matches GCS keys with the s3:// specs it hands S3Source, and
    # never sets use_s3_content_type.
    config = GCSSourceConfig.model_validate(recipe)
    return equivalent_s3_path_specs(config.path_specs), False


S3 = ObjectStorePlatform(
    source_type="s3",
    scheme="s3",
    source_class=S3Source,
    parse_config=DataLakeSourceConfig.model_validate,
    credentials={
        "aws_config": {
            "aws_access_key_id": "id",
            "aws_secret_access_key": "secret",
            "aws_region": "us-east-1",
        }
    },
    file_rules=_s3_file_rules,
    sets_content_type=True,
    empty_cases=frozenset({"extensionless_without_content_type"}),
)
GCS = ObjectStorePlatform(
    source_type="gcs",
    scheme="gs",
    source_class=GCSSource,
    parse_config=GCSSourceConfig.model_validate,
    credentials={
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"}
    },
    file_rules=_gcs_file_rules,
    sets_content_type=False,
    patches=[("datahub.ingestion.source.gcs.gcs_source.GCS_ENDPOINT_URL", None)],
    empty_cases=frozenset({"extensionless"}),
)

S3_CASES: Dict[str, Dict[str, object]] = {
    "templated_with_tables_filter": {
        "path_specs": [
            {
                "include": "s3://my-bucket/data/{table}/*/*.csv",
                "tables_filter_pattern": {"deny": ["^users$"]},
            }
        ]
    },
    "autodetected_partitions": {
        "path_specs": [{"include": "s3://my-bucket/data/{table}"}]
    },
    "simple_with_exclude": {
        "path_specs": [
            {"include": "s3://my-bucket/raw/*.csv", "exclude": ["**/tmp_*.csv"]}
        ]
    },
    "extensionless_without_content_type": {
        "path_specs": [{"include": "s3://my-bucket/no_ext/*"}]
    },
    "extensionless_with_content_type": {
        "path_specs": [{"include": "s3://my-bucket/no_ext/*"}],
        "use_s3_content_type": True,
    },
    "bucket_wildcard": {
        "path_specs": [{"include": "s3://my-bucket*/data/{table}/*/*.csv"}]
    },
    # A bucket the wildcard globs but cannot parse: ingestion applies
    # tables_filter_pattern to each file's name too, which drops this table.
    "bucket_wildcard_with_tables_filter": {
        "path_specs": [
            {
                "include": "s3://my-bucket*/data/{table}/*/*.csv",
                "tables_filter_pattern": {"deny": ["^part-"]},
            }
        ]
    },
    "any_including_spec_wins": {
        "path_specs": [
            {
                "include": "s3://my-bucket/data/{table}/*/*.csv",
                "tables_filter_pattern": {"deny": [".*"]},
            },
            {"include": "s3://my-bucket/data/{table}/*/*.csv"},
        ]
    },
}

GCS_CASES: Dict[str, Dict[str, object]] = {
    "templated_with_tables_filter": {
        "path_specs": [
            {
                "include": "gs://my-bucket/data/{table}/*/*.csv",
                "tables_filter_pattern": {"deny": ["^users$"]},
            }
        ]
    },
    "autodetected_partitions": {
        "path_specs": [{"include": "gs://my-bucket/data/{table}"}]
    },
    "simple_with_exclude": {
        "path_specs": [
            {"include": "gs://my-bucket/raw/*.csv", "exclude": ["**/tmp_*.csv"]}
        ]
    },
    "simple_with_gs_exclude": {
        "path_specs": [
            {
                "include": "gs://my-bucket/raw/*.csv",
                "exclude": ["gs://my-bucket/raw/tmp_*.csv"],
            }
        ]
    },
    # GCS never sets use_s3_content_type, so an extensionless file is dropped.
    "extensionless": {"path_specs": [{"include": "gs://my-bucket/no_ext/*"}]},
    "bucket_wildcard": {
        "path_specs": [{"include": "gs://my-bucket*/data/{table}/*/*.csv"}]
    },
    "any_including_spec_wins": {
        "path_specs": [
            {
                "include": "gs://my-bucket/data/{table}/*/*.csv",
                "tables_filter_pattern": {"deny": [".*"]},
            },
            {"include": "gs://my-bucket/data/{table}/*/*.csv"},
        ]
    },
}

PLATFORMS: Dict[str, Tuple[ObjectStorePlatform, Mapping[str, Dict[str, object]]]] = {
    "s3": (S3, S3_CASES),
    "gcs": (GCS, GCS_CASES),
}


@pytest.mark.parametrize(
    "source_type, name",
    [
        pytest.param(source_type, name, id=f"{source_type}-{name}")
        for source_type, (_, cases) in sorted(PLATFORMS.items())
        for name in sorted(cases)
    ],
)
def test_probe_verdicts_match_ingestion(source_type: str, name: str) -> None:
    store, cases = PLATFORMS[source_type]
    with store.storage():
        store.assert_verdicts_match_ingestion(name, store.recipe(cases[name]))


@pytest.mark.parametrize("source_type", sorted(PLATFORMS))
def test_the_declared_gap_is_real_and_detected(source_type: str) -> None:
    # data/docs holds only readme.txt, so the folder-level verdict includes a
    # table that ingestion drops. Fails if the fixture stops exercising the gap.
    store, cases = PLATFORMS[source_type]
    recipe = store.recipe(cases["templated_with_tables_filter"])
    with store.storage():
        probed, _ = store.probed(recipe)
        assert set(probed) - store.ingested(recipe) == {
            store.urn(recipe, f"{store.scheme}://my-bucket/data/docs")
        }


@pytest.mark.parametrize("source_type", sorted(PLATFORMS))
def test_a_table_ingestion_cannot_name_is_attributed_to_its_folder(
    source_type: str,
) -> None:
    # Pins the naming gap: fails if the bucket-wildcard fixture stops hitting it.
    store, cases = PLATFORMS[source_type]
    recipe = store.recipe(cases["bucket_wildcard"])
    with store.storage():
        probed, _ = store.probed(recipe)
        attributed = store.named_after_a_file_in(recipe, store.ingested(recipe), probed)
    assert set(attributed.values()) == {
        f"{store.scheme}://my-bucket/data/events",
        f"{store.scheme}://my-bucket/data/users",
    }
