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
from typing import Any, Dict, List, Literal, Tuple

import boto3
import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration_batch_2

test_resources_dir = pathlib.Path(__file__).parent
ENDPOINT_URL = "http://localhost:14566"
REGION = "us-east-1"
BUCKET_NAMES = ["my-test-bucket", "my-test-bucket-2"]
FROZEN_TIME = "2020-04-14 07:00:00"

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


def _seed_bucket(s3: Any, bucket: str, data_dir: pathlib.Path) -> None:
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_tagging(
        Bucket=bucket, Tagging={"TagSet": [{"Key": "foo", "Value": "bar"}]}
    )
    # The source selects a table's schema-representative file and its min/max
    # partition by object last_modified (source.py). The emulator stamps
    # last_modified at SECOND resolution with no API to set it, so uploading fast
    # would tie many objects and make that selection nondeterministic. Seeding one
    # object per second gives each a distinct, strictly-increasing timestamp ->
    # deterministic selection. The sorted-walk order is an arbitrary-but-stable
    # choice that also keeps the existing goldens valid (a convenience, not a goal).
    for root, dirs, files in os.walk(data_dir):
        dirs.sort()
        for file in sorted(files):
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, data_dir)
            extra = {"ContentType": "text/csv"} if "." not in rel_path else {}
            s3.upload_file(full_path, bucket, rel_path, ExtraArgs=extra)
            s3.put_object_tagging(
                Bucket=bucket,
                Key=rel_path,
                Tagging={"TagSet": [{"Key": "baz", "Value": "bob"}]},
            )
            time.sleep(1.1)  # strictly increasing, tie-free second-resolution times


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
        for bucket in BUCKET_NAMES:
            _seed_bucket(s3, bucket, data_dir)
        # High-cardinality numeric dataset for the profiler, under a prefix no
        # other recipe matches so it doesn't affect their goldens.
        s3.upload_file(
            str(test_resources_dir / "test_data/profiling/measurements.csv"),
            "my-test-bucket",
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
