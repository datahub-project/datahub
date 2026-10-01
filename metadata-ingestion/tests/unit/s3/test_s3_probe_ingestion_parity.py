"""The probe's dataset verdicts against what S3Source actually emits (moto).

The guarantee pinned here: the probe never under-reports, and it over-reports
only where it says it cannot judge -- a {table} folder in which no object passes
the spec's file rules. That case is checked by listing such a folder, not with a
hand-kept list. The one naming gap is declared too: a table folder whose name
ingestion cannot parse is emitted under a file inside it, and the probe warns.
"""

from typing import Dict, Iterator, List, Set, Tuple

import boto3
import pytest
from moto import mock_aws

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.data_lake_common.path_spec_verdict import (
    TEMPLATED_FILE_RULES_WARNING,
    UNPARSED_TABLE_WARNING,
)
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.source import S3Source
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.utilities.urns.dataset_urn import DatasetUrn

AWS: Dict[str, object] = {
    "aws_access_key_id": "id",
    "aws_secret_access_key": "secret",
    "aws_region": "us-east-1",
}
CSV = b"a,b\n1,2\n"
OBJECTS: List[Tuple[str, str, str]] = [  # bucket, key, content type
    ("my-bucket", "data/events/year=2024/part-0.csv", "text/csv"),
    ("my-bucket", "data/users/year=2024/part-0.csv", "text/csv"),
    ("my-bucket", "data/_staging/year=2024/part-0.csv", "text/csv"),
    ("my-bucket", "data/docs/year=2024/readme.txt", "text/plain"),
    ("my-bucket", "raw/a.csv", "text/csv"),
    ("my-bucket", "raw/tmp_b.csv", "text/csv"),
    ("my-bucket", "raw/notes.txt", "text/plain"),
    ("my-bucket", "no_ext/blob", "text/csv"),
    ("my-bucket-2", "data/orders/year=2024/part-0.csv", "text/csv"),
]

CASES: Dict[str, Dict[str, object]] = {
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


@pytest.fixture
def storage() -> Iterator[None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        for bucket in sorted({b for b, _, _ in OBJECTS}):
            client.create_bucket(Bucket=bucket)
        for bucket, key, content_type in OBJECTS:
            client.put_object(
                Bucket=bucket, Key=key, Body=CSV, ContentType=content_type
            )
        yield


def _recipe(case: Dict[str, object]) -> Dict[str, object]:
    return {**case, "aws_config": AWS}


def _urn(config: DataLakeSourceConfig, uri: str) -> str:
    path = uri[len("s3://") :].strip("/")
    if config.convert_urns_to_lowercase:
        path = path.lower()
    return make_dataset_urn_with_platform_instance(
        "s3", path, config.platform_instance, config.env
    )


def _ingested(recipe: Dict[str, object]) -> Set[str]:
    source = S3Source.create(recipe, PipelineContext(run_id="s3-probe-parity"))
    try:
        return {
            str(wu.metadata.entityUrn)
            for wu in source.get_workunits()
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
        }
    finally:
        source.close()


def _probed(recipe: Dict[str, object]) -> Tuple[Dict[str, str], List[str]]:
    """urn -> candidate uri for every name probe filter includes, plus its warnings."""
    config = DataLakeSourceConfig.model_validate(recipe)
    names: Set[str] = set()
    for i, spec in enumerate(config.path_specs):
        if spec.emit_folders_only:
            continue
        listing = run_probe_method(
            "s3", recipe, "datasets", {"path_spec": str(i), "limit": "1000"}
        )
        assert not listing.truncated and not listing.failures
        assert isinstance(listing.result, list)
        names |= {d["name"] for d in listing.result}
    verdicts = check_filters(
        source_type="s3",
        config_dict=recipe,
        kind="Table",
        parent_path=[],
        names=sorted(names),
    )
    included = {_urn(config, r.name): r.name for r in verdicts.results if r.included}
    return included, verdicts.warnings


def _no_allowed_file_under(recipe: Dict[str, object], folder_uri: str) -> bool:
    """The one gap the probe declares: a {table} folder whose files all fail
    the spec's file rules. Listing it proves that is why ingestion skipped it."""
    config = DataLakeSourceConfig.model_validate(recipe)
    bucket, _, prefix = folder_uri[len("s3://") :].partition("/")
    client = boto3.client("s3", region_name="us-east-1")
    keys = [
        o["Key"]
        for o in client.list_objects_v2(Bucket=bucket, Prefix=prefix + "/").get(
            "Contents", []
        )
    ]
    return not any(
        spec.allowed(f"s3://{bucket}/{key}", ignore_ext=config.use_s3_content_type)
        for spec in config.path_specs
        for key in keys
    )


def _named_after_a_file_in(
    recipe: Dict[str, object], ingested: Set[str], probed: Dict[str, str]
) -> Dict[str, str]:
    """ingested urn -> the included table folder it was emitted for, for the
    datasets ingestion named after a file rather than the folder. Each must sit
    under an included folder, and judging that folder alone must warn."""
    attributed: Dict[str, str] = {}
    for urn in ingested - set(probed):
        path = DatasetUrn.from_string(urn).name
        folders = [
            u for u in probed.values() if path.startswith(u[len("s3://") :] + "/")
        ]
        assert len(folders) == 1, f"probe missed {urn}"
        alone = check_filters(
            source_type="s3",
            config_dict=recipe,
            kind="Table",
            parent_path=[],
            names=folders,
        )
        assert UNPARSED_TABLE_WARNING in alone.warnings, (
            f"{urn} was named without a warning"
        )
        attributed[urn] = folders[0]
    return attributed


@pytest.mark.parametrize("name", sorted(CASES))
def test_probe_verdicts_match_ingestion(storage: None, name: str) -> None:
    recipe = _recipe(CASES[name])
    ingested = _ingested(recipe)
    probed, warnings = _probed(recipe)
    renamed = set(_named_after_a_file_in(recipe, ingested, probed).values())
    for urn in set(probed) - ingested:
        if probed[urn] in renamed:
            continue
        assert _no_allowed_file_under(recipe, probed[urn]), (
            f"probe included {probed[urn]}, ingestion did not, and a file there "
            f"passes the spec: a verdict rule disagrees with ingestion"
        )
        assert TEMPLATED_FILE_RULES_WARNING in warnings


def test_the_declared_gap_is_real_and_detected(storage: None) -> None:
    # data/docs holds only readme.txt, so the folder-level verdict includes a
    # table that ingestion drops. Fails if the fixture stops exercising the gap.
    recipe = _recipe(CASES["templated_with_tables_filter"])
    probed, _ = _probed(recipe)
    assert set(probed) - _ingested(recipe) == {
        _urn(DataLakeSourceConfig.model_validate(recipe), "s3://my-bucket/data/docs")
    }


def test_a_table_ingestion_cannot_name_is_attributed_to_its_folder(
    storage: None,
) -> None:
    # Pins the naming gap: fails if the bucket-wildcard fixture stops hitting it.
    recipe = _recipe(CASES["bucket_wildcard"])
    probed, _ = _probed(recipe)
    assert set(_named_after_a_file_in(recipe, _ingested(recipe), probed).values()) == {
        "s3://my-bucket/data/events",
        "s3://my-bucket/data/users",
    }
