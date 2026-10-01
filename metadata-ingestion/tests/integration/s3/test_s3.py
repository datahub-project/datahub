"""Integration test for the S3 (data lake) source against the Floci emulator.

Replaces the previous moto-backed S3 ingest test. The same recipes under
``sources/{shared,s3}`` run against a real S3-compatible emulator and are
compared to the goldens in ``golden-files/s3`` (regenerate with
``--update-golden-files``).

Also covers the S3 source's local-filesystem read path (``test_data_lake_local_ingest``),
which needs no backend. Related tests that don't exercise an S3 backend live
elsewhere: config validation and the S3 API call-pattern test are unit tests in
``tests/unit/s3``; the GCS connector test is in ``tests/integration/gcs``.

Determinism note: the S3 source selects a table's schema-representative file and
its min/max partition by object ``last_modified``. The emulator stamps
``last_modified`` at second resolution and offers no API to set it, so uploading
fast would tie many objects and make that selection vary run-to-run. Seeding one
object per second gives each a distinct, strictly-increasing timestamp, which is
what makes the selection deterministic. Any stable order would do; sorted-walk
order is used because it also keeps the pre-existing goldens valid (a convenience,
not a requirement). The timestamp *values* are non-reproducible and are masked
via ``ignore_paths``.
"""

import json
import os
import pathlib
import time
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Set, Tuple
from unittest import mock

import boto3
import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration

test_resources_dir = pathlib.Path(__file__).parent
ENDPOINT_URL = "http://localhost:14566"
REGION = "us-east-1"
PRIMARY_BUCKET = "my-test-bucket"
SECONDARY_BUCKET = "my-test-bucket-2"
FROZEN_TIME = "2020-04-14 07:00:00"

# Canary: the exact set of objects the primary bucket must contain after seeding.
# Catches drift in the test_data/local_system tree directly, rather than letting a
# stray added/removed file surface only as a confusing golden diff.
EXPECTED_PRIMARY_KEYS = [
    "folder_a/folder_aa/folder_aaa/NPS.7.1.package_data_NPS.6.1_ARCN_Lakes_ChemistryData_v1_csv.csv",
    "folder_a/folder_aa/folder_aaa/chord_progressions_avro.avro",
    "folder_a/folder_aa/folder_aaa/chord_progressions_csv.csv",
    "folder_a/folder_aa/folder_aaa/countries_json.json",
    "folder_a/folder_aa/folder_aaa/food_parquet.parquet",
    "folder_a/folder_aa/folder_aaa/small.csv",
    "folder_a/folder_aa/folder_aaa/wa_fn_usec_hr_employee_attrition_csv.csv",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2019/month=feb/part1.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2019/month=feb/part2.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2019/month=jan/part1.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2019/month=jan/part2.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2020/month=feb/part1.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2020/month=feb/part2.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2020/month=march/part1.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2020/month=march/part2.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2021/month=april/part1.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2021/month=april/part2.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2021/month=march/part1.json",
    "folder_a/folder_aa/folder_aaa/folder_aaaa/pokemon_abilities_yearwise_2021/month=march/part2.json",
    "folder_a/folder_aa/folder_aaa/food_csv/part1.csv",
    "folder_a/folder_aa/folder_aaa/food_csv/part2.csv",
    "folder_a/folder_aa/folder_aaa/food_csv/part3.csv",
    "folder_a/folder_aa/folder_aaa/food_parquet/part1.parquet",
    "folder_a/folder_aa/folder_aaa/food_parquet/part2.parquet",
    "folder_a/folder_aa/folder_aaa/no_extension/small",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2019/month=feb/part1.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2019/month=feb/part2.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2019/month=jan/part1.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2019/month=jan/part2.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2020/month=feb/part1.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2020/month=feb/part2.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2020/month=march/part1.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2020/month=march/part2.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2021/month=april/part1.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2021/month=april/part2.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2021/month=march/part1.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2021/month=march/part2.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2022/month=jan/part3.json",
    "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2022/month=jan/_temporary/dummy.json",
    "folders_only_media/audio/podcast.mp3",
    "folders_only_media/videos/2023/clip.mp4",
    "folders_only_media/videos/2024/clip.mp4",
]

# Only these recipes read from the secondary bucket, and only this subtree:
#   multiple_specs_of_different_buckets -> chord_progressions_csv.csv
#   bucket_wildcard_single_file         -> chord_progressions_avro.avro
#   bucket_wildcard_{allow,as}_table    -> food_csv/*.csv
#   bucket_wildcard_with_nested_table   -> {table}/*.* i.e. food_csv, food_parquet, no_extension
# Seeding just this subtree (instead of the full 42-file tree) keeps the goldens
# identical while cutting ~34s of per-object seeding sleep from the fixture.
SECONDARY_BUCKET_KEYS = [
    "folder_a/folder_aa/folder_aaa/chord_progressions_avro.avro",
    "folder_a/folder_aa/folder_aaa/chord_progressions_csv.csv",
    "folder_a/folder_aa/folder_aaa/food_csv/part1.csv",
    "folder_a/folder_aa/folder_aaa/food_csv/part2.csv",
    "folder_a/folder_aa/folder_aaa/food_csv/part3.csv",
    "folder_a/folder_aa/folder_aaa/food_parquet/part1.parquet",
    "folder_a/folder_aa/folder_aaa/food_parquet/part2.parquet",
    "folder_a/folder_aa/folder_aaa/no_extension/small",
]

# Object time fields that the emulator sets to real upload time (moto poked these
# to deterministic values via its internal backend, which a real emulator can't
# do); masked so only their presence/structure is asserted. Covers the operation
# aspect timestamps and the datasetProperties PATCH that adds /lastModified with
# the object's modification time (aspect.json[i].value.time).
IGNORE_PATHS = [
    r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\[\d+\]\['value'\]\['time'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['(min|max)Partition'\]\['(created|lastModified)Time'\]",
]


def _client(service: Literal["s3"]) -> Any:
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _walk_keys(data_dir: pathlib.Path) -> List[str]:
    keys: List[str] = []
    for root, dirs, files in os.walk(data_dir):
        dirs.sort()
        for file in sorted(files):
            keys.append(os.path.relpath(os.path.join(root, file), data_dir))
    return keys


def _seed_bucket(
    s3: Any,
    bucket: str,
    data_dir: pathlib.Path,
    keys: Optional[List[str]] = None,
) -> List[str]:
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_tagging(
        Bucket=bucket, Tagging={"TagSet": [{"Key": "foo", "Value": "bar"}]}
    )
    # The source selects a table's schema-representative file and its min/max
    # partition by object last_modified (source.py). The emulator stamps
    # last_modified at SECOND resolution with no API to set it, so uploading fast
    # would tie many objects and make that selection nondeterministic. Seeding one
    # object per second gives each a distinct, strictly-increasing timestamp ->
    # deterministic selection. Sorted order is an arbitrary-but-stable choice that
    # also keeps the existing goldens valid (a convenience, not a goal). At ~1.1s
    # per object, adding a file to a fully-seeded bucket costs ~1.1s of fixture
    # setup — hence the secondary bucket only seeds the subtree its recipes read.
    uploaded = _walk_keys(data_dir) if keys is None else keys
    for rel_path in uploaded:
        extra = {"ContentType": "text/csv"} if "." not in rel_path else {}
        s3.upload_file(str(data_dir / rel_path), bucket, rel_path, ExtraArgs=extra)
        s3.put_object_tagging(
            Bucket=bucket,
            Key=rel_path,
            Tagging={"TagSet": [{"Key": "baz", "Value": "bob"}]},
        )
        time.sleep(1.1)  # strictly increasing, tie-free second-resolution times
    return uploaded


def _source_files() -> List[Tuple[str, str]]:
    shared = test_resources_dir / "sources/shared"
    s3 = test_resources_dir / "sources/s3"
    return [(str(shared), p) for p in sorted(os.listdir(shared))] + [
        (str(s3), p) for p in sorted(os.listdir(s3))
    ]


def _descriptive_id(source_tuple: Tuple[str, str]) -> str:
    source_dir, source_file = source_tuple
    return f"{os.path.basename(source_dir)}_{source_file.replace('.json', '')}"


@pytest.fixture(scope="module")
def s3_emulator(docker_compose_runner):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "s3",
        setup_command=["up -d --wait"],
    ):
        data_dir = test_resources_dir / "test_data/local_system"
        s3 = _client("s3")
        uploaded = _seed_bucket(s3, PRIMARY_BUCKET, data_dir)
        assert sorted(uploaded) == sorted(EXPECTED_PRIMARY_KEYS), (
            "test_data/local_system tree drifted from EXPECTED_PRIMARY_KEYS"
        )
        # Secondary bucket: only the subtree its recipes read, seeded after the
        # primary bucket so bucket_wildcard_* tables spanning both buckets keep the
        # cross-bucket ordering the goldens were generated with.
        _seed_bucket(s3, SECONDARY_BUCKET, data_dir, keys=SECONDARY_BUCKET_KEYS)
        # High-cardinality numeric dataset for the profiler, under a prefix no
        # other recipe matches so it doesn't affect their goldens.
        s3.upload_file(
            str(test_resources_dir / "test_data/profiling/measurements.csv"),
            PRIMARY_BUCKET,
            "profiling_input/measurements.csv",
        )
        yield


@pytest.mark.parametrize("source_file_tuple", _source_files(), ids=_descriptive_id)
def test_data_lake_s3_ingest(
    s3_emulator, pytestconfig, source_file_tuple, tmp_path, mock_time
):
    """Ingest each shared/s3 recipe against the emulator and compare to its golden.

    Covers the S3 source feature matrix (schema inference, path specs, partition
    detection/traversal, tags, content-type, folders) — one parametrized case per
    recipe under ``sources/{shared,s3}``.
    """
    source_dir, source_file = source_file_tuple
    with open(os.path.join(source_dir, source_file)) as f:
        source = json.load(f)

    # Point the recipe at the emulator (recipes carry dummy creds already).
    source["config"].setdefault("aws_config", {})["aws_endpoint_url"] = ENDPOINT_URL

    config_dict: Dict[str, Any] = {
        "run_id": source_file,
        "source": source,
        "sink": {"type": "file", "config": {"filename": f"{tmp_path}/{source_file}"}},
    }

    pipeline = Pipeline.create(config_dict)
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{source_file}",
        golden_path=f"{test_resources_dir}/golden-files/s3/golden_mces_{source_file}",
        ignore_paths=IGNORE_PATHS,
    )


def test_data_lake_s3_profiling(s3_emulator, pytestconfig, tmp_path, mock_time):
    """Profile a file read through the S3 client against the emulator.

    Exercises the pure-Python (pyarrow) data-lake profiler end-to-end: numeric
    columns emit min/max/mean/median/stdev, the low-cardinality categorical column
    emits distinct-value frequencies, all in a datasetProfile aspect.
    """
    source = {
        "type": "s3",
        "config": {
            "path_specs": [
                {"include": "s3://my-test-bucket/profiling_input/measurements.csv"}
            ],
            "aws_config": {
                "aws_endpoint_url": ENDPOINT_URL,
                "aws_region": REGION,
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
            },
            "profiling": {
                "enabled": True,
                "include_field_null_count": True,
                "include_field_min_value": True,
                "include_field_max_value": True,
                "include_field_mean_value": True,
                "include_field_median_value": True,
                "include_field_stddev_value": True,
                "include_field_distinct_value_frequencies": True,
                "include_field_sample_values": True,
            },
        },
    }
    output = f"{tmp_path}/s3_profiling_mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "s3-profiling",
            "source": source,
            "sink": {"type": "file", "config": {"filename": output}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=f"{test_resources_dir}/golden-files/s3/golden_mces_s3_profiling.json",
        ignore_paths=IGNORE_PATHS,
    )


def _shared_source_files() -> List[Tuple[str, str]]:
    shared = test_resources_dir / "sources/shared"
    return [(str(shared), p) for p in sorted(os.listdir(shared))]


@pytest.fixture(scope="module")
def touch_local_files():
    # The local-FS ingest reads mtimes for partition ordering; set them
    # deterministically (matching the golden) without needing any backend.
    data_dir = test_resources_dir / "test_data/local_system"
    current_time_sec = datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S").timestamp()
    for root, dirs, files in os.walk(data_dir):
        dirs.sort()
        for file in sorted(files):
            current_time_sec += 10
            os.utime(
                os.path.join(root, file), times=(current_time_sec, current_time_sec)
            )


@pytest.mark.integration
@pytest.mark.parametrize(
    "source_file_tuple", _shared_source_files(), ids=_descriptive_id
)
def test_data_lake_local_ingest(
    touch_local_files, pytestconfig, source_file_tuple, tmp_path, mock_time
):
    """Ingest each shared recipe from the local filesystem (profiling enabled).

    The S3 source's local-file path needs no backend: the ``s3://`` path specs are
    rewritten to the local ``test_data`` tree and the emitted metadata — including
    datasetProfile aspects — is compared to ``golden-files/local``.
    """
    source_dir, source_file = source_file_tuple
    with open(os.path.join(source_dir, source_file)) as f:
        source = json.load(f)

    for path_spec in source["config"]["path_specs"]:
        path_spec["include"] = (
            path_spec["include"]
            .replace(
                "s3://my-test-bucket/", "tests/integration/s3/test_data/local_system/"
            )
            .replace(
                "s3://my-test-bucket-2/", "tests/integration/s3/test_data/local_system/"
            )
        )
    source["config"]["profiling"]["enabled"] = True
    source["config"].pop("aws_config")
    source["config"].pop("use_s3_bucket_tags", None)
    source["config"].pop("use_s3_object_tags", None)

    config_dict = {
        "run_id": source_file,
        "source": source,
        "sink": {"type": "file", "config": {"filename": f"{tmp_path}/{source_file}"}},
    }

    pipeline = Pipeline.create(config_dict)
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{source_file}",
        golden_path=f"{test_resources_dir}/golden-files/local/golden_mces_{source_file}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\[\d+\]\['value'\]\['time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.schema.SchemaMetadata'\]\['fields'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['size_in_bytes'\]",
        ],
    )


# Probe tests. They share this module's emulator fixture rather than starting a
# second one, and compare against the goldens above: those are real ingestion
# output for the same recipes against the same emulator.


def _probe_source(source_file_tuple: Tuple[str, str]) -> Dict[str, Any]:
    source_dir, source_file = source_file_tuple
    with open(os.path.join(source_dir, source_file)) as f:
        config: Dict[str, Any] = json.load(f)["config"]
    config.setdefault("aws_config", {})["aws_endpoint_url"] = ENDPOINT_URL
    return config


def _golden(source_file: str) -> List[Dict[str, Any]]:
    with open(test_resources_dir / f"golden-files/s3/golden_mces_{source_file}") as f:
        return json.load(f)


def _probe_included_tables(config: Dict[str, Any]) -> Dict[str, str]:
    """urn -> s3 uri, for every dataset candidate probe filter includes."""
    parsed = DataLakeSourceConfig.model_validate(config)
    names: Set[str] = set()
    for i, spec in enumerate(parsed.path_specs):
        if spec.emit_folders_only:
            continue
        listing = run_probe_method(
            "s3", config, "datasets", {"path_spec": str(i), "limit": "1000"}
        )
        assert not listing.truncated and not listing.failures
        assert isinstance(listing.result, list)
        names |= {d["name"] for d in listing.result}
    verdicts = check_filters(
        source_type="s3",
        config_dict=config,
        kind="Table",
        parent_path=[],
        names=sorted(names),
    )
    out: Dict[str, str] = {}
    for r in verdicts.results:
        if r.included:
            path = r.name[len("s3://") :].strip("/")
            if parsed.convert_urns_to_lowercase:
                path = path.lower()
            urn = make_dataset_urn_with_platform_instance(
                "s3", path, parsed.platform_instance, parsed.env
            )
            out[urn] = r.name
    return out


def _no_allowed_file_under(config: Dict[str, Any], folder_uri: str) -> bool:
    """The gap the probe declares: a {table} folder whose files all fail the
    spec's file rules. Listing it shows that is why ingestion skipped it."""
    parsed = DataLakeSourceConfig.model_validate(config)
    bucket, _, prefix = folder_uri[len("s3://") :].partition("/")
    pages = (
        _client("s3")
        .get_paginator("list_objects_v2")
        .paginate(Bucket=bucket, Prefix=f"{prefix}/" if prefix else "")
    )
    keys = [o["Key"] for page in pages for o in page.get("Contents", [])]
    return not any(
        spec.allowed(f"s3://{bucket}/{key}", ignore_ext=parsed.use_s3_content_type)
        for spec in parsed.path_specs
        for key in keys
    )


def _dataset_source_files() -> List[Tuple[str, str]]:
    return [
        t
        for t in _source_files()
        if not any(s.get("emit_folders_only") for s in _probe_source(t)["path_specs"])
    ]


@pytest.mark.parametrize(
    "source_file_tuple", _dataset_source_files(), ids=_descriptive_id
)
def test_probe_datasets_match_real_ingestion(
    s3_emulator: None, source_file_tuple: Tuple[str, str]
) -> None:
    """probe run datasets + probe filter, against the emulator and recipe the
    golden was ingested from: never fewer datasets, and extra ones only where no
    file under the table folder passes the spec."""
    config = _probe_source(source_file_tuple)
    golden = {
        e["entityUrn"]
        for e in _golden(source_file_tuple[1])
        if e.get("entityType") == "dataset"
        and e.get("aspectName") == "datasetProperties"
    }
    probed = _probe_included_tables(config)
    assert golden <= set(probed), f"probe missed {golden - set(probed)}"
    for urn in set(probed) - golden:
        assert _no_allowed_file_under(config, probed[urn]), probed[urn]


def test_probe_folders_match_real_folders_only_ingestion(s3_emulator: None) -> None:
    config = _probe_source(
        (str(test_resources_dir / "sources/s3"), "folders_only.json")
    )
    listing = run_probe_method("s3", config, "path_spec_folders", {"limit": "1000"})
    assert isinstance(listing.result, list) and listing.result
    verdicts = check_filters(
        source_type="s3",
        config_dict=config,
        kind="Folder",
        parent_path=[],
        names=listing.result,
    )
    probed = {r.name[len("s3://") :] for r in verdicts.results if r.included}
    folders = {
        e["aspect"]["json"]["customProperties"]["folder_abs_path"]
        for e in _golden("folders_only.json")
        if e.get("aspectName") == "containerProperties"
        and "folder_abs_path" in e["aspect"]["json"].get("customProperties", {})
    }
    leaves = {f for f in folders if not any(o.startswith(f + "/") for o in folders)}
    assert probed == leaves


def test_probe_buckets_and_tags_against_the_emulator(s3_emulator: None) -> None:
    config = _probe_source((str(test_resources_dir / "sources/s3"), "single_file.json"))
    buckets = run_probe_method("s3", config, "buckets", {}).result
    assert isinstance(buckets, list)
    assert {PRIMARY_BUCKET, SECONDARY_BUCKET} <= set(buckets)
    tags = run_probe_method(
        "s3",
        {**config, "use_s3_bucket_tags": True, "use_s3_object_tags": True},
        "tags",
        {"bucket": PRIMARY_BUCKET, "key": EXPECTED_PRIMARY_KEYS[0]},
    ).result
    assert tags == {"bucket": ["foo:bar"], "object": ["baz:bob"]}


def test_probe_issues_only_metadata_operations_against_the_emulator(
    s3_emulator: None,
) -> None:
    seen: List[str] = []
    real_client = boto3.session.Session.client

    def recording_client(self: Any, *args: Any, **kwargs: Any) -> Any:
        client = real_client(self, *args, **kwargs)
        client.meta.events.register(
            "before-call.s3", lambda model, **_: seen.append(model.name)
        )
        return client

    config = _probe_source(
        (
            str(test_resources_dir / "sources/s3"),
            "bucket_wildcard_with_nested_table.json",
        )
    )
    with mock.patch.object(boto3.session.Session, "client", recording_client):
        for command, kwargs in [
            ("buckets", {}),
            ("folders", {"bucket": PRIMARY_BUCKET}),
            ("objects", {"bucket": PRIMARY_BUCKET, "limit": "50"}),
            ("datasets", {}),
        ]:
            run_probe_method("s3", config, command, dict(kwargs))
    assert seen and set(seen) <= {"ListBuckets", "ListObjectsV2"}
