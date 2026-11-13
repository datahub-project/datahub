"""Integration test for the Azure Blob Storage (ABS) source.

Runs against the Floci Azure emulator (``floci/floci-az``) via the standard
``docker_compose_runner`` fixture. The source's native ``BlobServiceClient`` is
pointed at the emulator through an (Azurite-compatible) connection string — see
the ``connection_string`` override added to ``AzureConnectionConfig``. Blobs are
seeded with the Azure SDK (reusing the S3 suite's data tree) and ingestion runs
a full ``Pipeline``; output is compared to golden files (regenerate with
``--update-golden-files``).

This is net-new integration coverage: the ABS source previously had only
unit tests with a mocked SDK client and no golden.
"""

import pathlib
import time
from typing import Any, Dict

import pytest
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration

test_resources_dir = pathlib.Path(__file__).parent
# S3 suite's data tree is reused so ABS structure mirrors the other object stores.
s3_data_dir = test_resources_dir.parent / "s3/test_data/local_system"

ACCOUNT = "devstoreaccount1"
CONTAINER = "test-container"
# Well-known Azurite dev account key; floci-az is Azurite-compatible. The
# BlobEndpoint override routes the SDK at the emulator's host port.
DEV_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)
CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    f"AccountName={ACCOUNT};AccountKey={DEV_KEY};"
    f"BlobEndpoint=http://127.0.0.1:14577/{ACCOUNT};"
)
BLOB_BASE = f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}"

IGNORE_PATHS = [
    r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\[\d+\]\['value'\]\['time'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['(min|max)Partition'\]\['(created|lastModified)Time'\]",
    # Blob/container properties carry per-run etag + modification time.
    r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['(blob|container)_(last_modified|etag|creation_time)'\]",
]

# Curated multi-format files seeded under data/ for schema-inference coverage.
DATA_FILES = {
    "data/small.csv": "folder_a/folder_aa/folder_aaa/small.csv",
    "data/countries_json.json": "folder_a/folder_aa/folder_aaa/countries_json.json",
    "data/food_parquet.parquet": "folder_a/folder_aa/folder_aaa/food_parquet.parquet",
    "data/chord_progressions_avro.avro": "folder_a/folder_aa/folder_aaa/chord_progressions_avro.avro",
}
# A high-cardinality numeric dataset profiled on its own so the profiler emits
# real min/max/mean/median/stddev (for the numeric columns) alongside the
# distinct-value-frequency stats (for the low-cardinality categorical column).
PROFILE_BLOB = "profile/measurements.csv"
PROFILE_SOURCE = test_resources_dir.parent / "s3/test_data/profiling/measurements.csv"
# Two partitions seeded with a >1s gap so the (second-resolution) emulator gives
# them a strict order and the max-partition selection is deterministic.
PARTITION_FILES = [
    "sales/year=2021/month=jan/part1.json",
    "sales/year=2021/month=feb/part1.json",
]
PARTITION_SOURCE = "folder_a/folder_aa/folder_aaa/pokemon_abilities_json/year=2019/month=jan/part1.json"
# Media tree for the folders-only recipe.
MEDIA_BLOBS = ["media/audio/podcast.mp3", "media/videos/2024/clip.mp4"]


def _svc() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(CONNECTION_STRING)


def _seed() -> None:
    cc = _svc().get_container_client(CONTAINER)
    try:
        cc.create_container()
    except ResourceExistsError:
        pass  # already exists on a reused emulator

    def upload(name: str, src_rel: str) -> None:
        with open(s3_data_dir / src_rel, "rb") as fh:
            cc.upload_blob(name=name, data=fh, overwrite=True)

    for dest, src in DATA_FILES.items():
        upload(dest, src)
    with open(PROFILE_SOURCE, "rb") as fh:
        cc.upload_blob(name=PROFILE_BLOB, data=fh, overwrite=True)
    for blob in MEDIA_BLOBS:
        upload(blob, "folder_a/folder_aa/folder_aaa/small.csv")
    for part in PARTITION_FILES:
        upload(part, PARTITION_SOURCE)
        time.sleep(1.1)  # strict, tie-free ordering for max-partition selection


def _azure_config(**overrides: Any) -> Dict[str, Any]:
    return {
        "account_name": ACCOUNT,
        "container_name": CONTAINER,
        "connection_string": CONNECTION_STRING,
        **overrides,
    }


def _run(config: Dict[str, Any], tmp_path: pathlib.Path, out: str) -> str:
    output = f"{tmp_path}/{out}"
    pipeline = Pipeline.create(
        {
            "run_id": "abs-test",
            "source": {"type": "abs", "config": config},
            "sink": {"type": "file", "config": {"filename": output}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return output


def _check(pytestconfig: Any, output: str, golden: str) -> None:
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / golden,
        ignore_paths=IGNORE_PATHS,
    )


@pytest.fixture(scope="module")
def abs_emulator(docker_compose_runner):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "abs",
        setup_command=["up -d --wait"],
    ):
        _seed()
        yield


@pytest.mark.integration
def test_abs_folder(abs_emulator, pytestconfig, tmp_path, mock_time):
    """Multi-format schema inference (csv/json/parquet/avro) over a blob folder."""
    config = {
        "path_specs": [{"include": f"{BLOB_BASE}/data/*.*"}],
        "azure_config": _azure_config(),
    }
    _check(
        pytestconfig,
        _run(config, tmp_path, "abs_folder_mces.json"),
        "abs_folder_mces_golden.json",
    )


@pytest.mark.integration
def test_abs_single_file(abs_emulator, pytestconfig, tmp_path, mock_time):
    """Ingest a single explicitly-addressed blob as one dataset."""
    config = {
        "path_specs": [{"include": f"{BLOB_BASE}/data/small.csv"}],
        "azure_config": _azure_config(),
    }
    _check(
        pytestconfig,
        _run(config, tmp_path, "abs_single_file_mces.json"),
        "abs_single_file_mces_golden.json",
    )


@pytest.mark.integration
def test_abs_partitioned(abs_emulator, pytestconfig, tmp_path, mock_time):
    """Partitioned folder → one table with {partition} templating from blob paths."""
    config = {
        "env": "UAT",
        "path_specs": [
            {
                "include": f"{BLOB_BASE}/{{table}}/{{partition[0]}}/{{partition[1]}}/*.*",
                "table_name": "{table}",
            }
        ],
        "azure_config": _azure_config(),
    }
    _check(
        pytestconfig,
        _run(config, tmp_path, "abs_partitioned_mces.json"),
        "abs_partitioned_mces_golden.json",
    )


@pytest.mark.integration
def test_abs_folders_only(abs_emulator, pytestconfig, tmp_path, mock_time):
    """emit_folders_only → container/folder entities without file datasets."""
    config = {
        "path_specs": [
            {"include": f"{BLOB_BASE}/media/*/*/", "emit_folders_only": True}
        ],
        "azure_config": _azure_config(),
    }
    _check(
        pytestconfig,
        _run(config, tmp_path, "abs_folders_only_mces.json"),
        "abs_folders_only_mces_golden.json",
    )


@pytest.mark.integration
def test_abs_blob_properties(abs_emulator, pytestconfig, tmp_path, mock_time):
    """Surface container + blob properties as DataHub properties.

    use_abs_blob_tags is intentionally NOT set: floci-az's get_blob_tags API
    returns a response the Azure SDK fails to decode (emulator fidelity gap); the
    blob-tags path is covered by the unit test
    tests/unit/abs/test_abs_source.py::test_get_abs_tags_emits_blob_tags.
    """
    config = {
        "path_specs": [{"include": f"{BLOB_BASE}/data/*.*"}],
        "azure_config": _azure_config(),
        "use_abs_container_properties": True,
        "use_abs_blob_properties": True,
    }
    _check(
        pytestconfig,
        _run(config, tmp_path, "abs_blob_properties_mces.json"),
        "abs_blob_properties_mces_golden.json",
    )


@pytest.mark.integration
def test_abs_profiling(abs_emulator, pytestconfig, tmp_path, mock_time):
    """Profile a blob read through the injected BlobServiceClient against the emulator.

    Exercises the pure-Python (pyarrow) data-lake profiler: numeric columns emit
    min/max/mean/median/stdev, the low-cardinality categorical column emits
    distinct-value frequencies, all in a datasetProfile aspect.
    """
    config = {
        "path_specs": [{"include": f"{BLOB_BASE}/{PROFILE_BLOB}"}],
        "azure_config": _azure_config(),
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
    }
    _check(
        pytestconfig,
        _run(config, tmp_path, "abs_profiling_mces.json"),
        "abs_profiling_mces_golden.json",
    )
