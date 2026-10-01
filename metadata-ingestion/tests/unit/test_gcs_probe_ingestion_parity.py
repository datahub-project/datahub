"""The probe's dataset verdicts against what GCSSource actually emits, with
moto standing in for GCS's S3-compatible endpoint (as tests/integration/gcs
does).

The guarantee pinned here, as for S3: the probe never under-reports, and it
over-reports only where it says it cannot judge -- a {table} folder in which no
object passes the spec's file rules. A table folder whose name ingestion cannot
parse is emitted under a file inside it, and the probe warns.
"""

from typing import Dict, Iterator, List, Set, Tuple
from unittest import mock

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
from datahub.ingestion.source.gcs.gcs_source import (
    GCSSource,
    GCSSourceConfig,
    equivalent_s3_path_specs,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.utilities.urns.dataset_urn import DatasetUrn

HMAC: Dict[str, object] = {"hmac_access_id": "id", "hmac_access_secret": "secret"}
CSV = b"a,b\n1,2\n"
OBJECTS: List[Tuple[str, str]] = [  # bucket, key
    ("my-bucket", "data/events/year=2024/part-0.csv"),
    ("my-bucket", "data/users/year=2024/part-0.csv"),
    ("my-bucket", "data/_staging/year=2024/part-0.csv"),
    ("my-bucket", "data/docs/year=2024/readme.txt"),
    ("my-bucket", "raw/a.csv"),
    ("my-bucket", "raw/tmp_b.csv"),
    ("my-bucket", "raw/notes.txt"),
    ("my-bucket", "no_ext/blob"),
    ("my-bucket-2", "data/orders/year=2024/part-0.csv"),
]

CASES: Dict[str, List[Dict[str, object]]] = {
    "templated_with_tables_filter": [
        {
            "include": "gs://my-bucket/data/{table}/*/*.csv",
            "tables_filter_pattern": {"deny": ["^users$"]},
        }
    ],
    "autodetected_partitions": [{"include": "gs://my-bucket/data/{table}"}],
    "simple_with_exclude": [
        {"include": "gs://my-bucket/raw/*.csv", "exclude": ["**/tmp_*.csv"]}
    ],
    "simple_with_gs_exclude": [
        {
            "include": "gs://my-bucket/raw/*.csv",
            "exclude": ["gs://my-bucket/raw/tmp_*.csv"],
        }
    ],
    # GCS never sets use_s3_content_type, so an extensionless file is dropped.
    "extensionless": [{"include": "gs://my-bucket/no_ext/*"}],
    "bucket_wildcard": [{"include": "gs://my-bucket*/data/{table}/*/*.csv"}],
    "any_including_spec_wins": [
        {
            "include": "gs://my-bucket/data/{table}/*/*.csv",
            "tables_filter_pattern": {"deny": [".*"]},
        },
        {"include": "gs://my-bucket/data/{table}/*/*.csv"},
    ],
}


@pytest.fixture
def storage() -> Iterator[None]:
    with (
        mock_aws(),
        mock.patch("datahub.ingestion.source.gcs.gcs_source.GCS_ENDPOINT_URL", None),
    ):
        client = boto3.client("s3", region_name="us-east-1")
        for bucket in sorted({b for b, _ in OBJECTS}):
            client.create_bucket(Bucket=bucket)
        for bucket, key in OBJECTS:
            client.put_object(Bucket=bucket, Key=key, Body=CSV)
        yield


def _recipe(specs: List[Dict[str, object]]) -> Dict[str, object]:
    return {"credential": HMAC, "path_specs": specs}


def _urn(config: GCSSourceConfig, uri: str) -> str:
    path = uri[len("gs://") :].strip("/")
    if config.convert_urns_to_lowercase:
        path = path.lower()
    return make_dataset_urn_with_platform_instance(
        "gcs", path, config.platform_instance, config.env
    )


def _ingested(recipe: Dict[str, object]) -> Set[str]:
    source = GCSSource.create(recipe, PipelineContext(run_id="gcs-probe-parity"))
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
    """urn -> candidate gs:// uri for every name probe filter includes, plus
    its warnings."""
    config = GCSSourceConfig.model_validate(recipe)
    names: Set[str] = set()
    for i in range(len(config.path_specs)):
        listing = run_probe_method(
            "gcs", recipe, "datasets", {"path_spec": str(i), "limit": "1000"}
        )
        assert not listing.truncated and not listing.failures
        assert isinstance(listing.result, list)
        names |= {d["name"] for d in listing.result}
    verdicts = check_filters(
        source_type="gcs",
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
    specs = equivalent_s3_path_specs(GCSSourceConfig.model_validate(recipe).path_specs)
    bucket, _, prefix = folder_uri[len("gs://") :].partition("/")
    client = boto3.client("s3", region_name="us-east-1")
    keys = [
        o["Key"]
        for o in client.list_objects_v2(Bucket=bucket, Prefix=prefix + "/").get(
            "Contents", []
        )
    ]
    return not any(
        spec.allowed(f"s3://{bucket}/{key}") for spec in specs for key in keys
    )


def _named_after_a_file_in(
    ingested: Set[str], probed: Dict[str, str], warnings: List[str]
) -> Dict[str, str]:
    """ingested urn -> the included table folder it was emitted for, for the
    datasets ingestion named after a file rather than the folder."""
    attributed: Dict[str, str] = {}
    for urn in ingested - set(probed):
        path = DatasetUrn.from_string(urn).name
        folders = [
            u for u in probed.values() if path.startswith(u[len("gs://") :] + "/")
        ]
        assert len(folders) == 1, f"probe missed {urn}"
        assert UNPARSED_TABLE_WARNING in warnings, f"{urn} was named without a warning"
        attributed[urn] = folders[0]
    return attributed


@pytest.mark.parametrize("name", sorted(CASES))
def test_probe_verdicts_match_ingestion(storage: None, name: str) -> None:
    recipe = _recipe(CASES[name])
    ingested = _ingested(recipe)
    # Every case but the extensionless one must emit something, or it checks
    # nothing.
    assert bool(ingested) is (name != "extensionless")
    probed, warnings = _probed(recipe)
    renamed = set(_named_after_a_file_in(ingested, probed, warnings).values())
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
        _urn(GCSSourceConfig.model_validate(recipe), "gs://my-bucket/data/docs")
    }


def test_a_table_ingestion_cannot_name_is_attributed_to_its_folder(
    storage: None,
) -> None:
    # Pins the naming gap GCS shares with S3: fails if the bucket-wildcard
    # fixture stops hitting it.
    recipe = _recipe(CASES["bucket_wildcard"])
    probed, warnings = _probed(recipe)
    assert set(_named_after_a_file_in(_ingested(recipe), probed, warnings).values()) == {
        "gs://my-bucket/data/events",
        "gs://my-bucket/data/users",
    }
