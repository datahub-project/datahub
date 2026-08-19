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
from typing import Any, Dict, List, Literal, Optional, Tuple

import boto3
import pytest

from datahub.ingestion.run.pipeline import Pipeline
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
