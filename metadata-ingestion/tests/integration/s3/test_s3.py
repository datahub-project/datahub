import json
import logging
import os
from datetime import datetime

import moto.s3
import pytest
from boto3.session import Session
from moto import mock_s3
from pydantic import ValidationError

from datahub.ingestion.run.pipeline import Pipeline, PipelineContext
from datahub.ingestion.source.s3.source import S3Source
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"

FILE_LIST_FOR_VALIDATION = [
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
]


@pytest.fixture(scope="module", autouse=True)
def bucket_names():
    return ["my-test-bucket", "my-test-bucket-2"]


@pytest.fixture(scope="module", autouse=True)
def s3():
    with mock_s3():
        conn = Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        yield conn


@pytest.fixture(scope="module", autouse=True)
def s3_resource(s3):
    with mock_s3():
        conn = s3.resource("s3")
        yield conn


@pytest.fixture(scope="module", autouse=True)
def s3_client(s3):
    with mock_s3():
        conn = s3.client("s3")
        yield conn


@pytest.fixture(scope="module", autouse=True)
def s3_populate(pytestconfig, s3_resource, s3_client, bucket_names):
    for bucket_name in bucket_names:
        logging.info(f"Populating s3 bucket: {bucket_name}")
        s3_resource.create_bucket(Bucket=bucket_name)
        bkt = s3_resource.Bucket(bucket_name)
        bkt.Tagging().put(Tagging={"TagSet": [{"Key": "foo", "Value": "bar"}]})
        test_resources_dir = (
            pytestconfig.rootpath / "tests/integration/s3/test_data/local_system/"
        )

        current_time_sec = datetime.strptime(
            FROZEN_TIME, "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        file_list = []
        for root, _dirs, files in os.walk(test_resources_dir):
            _dirs.sort()
            for file in sorted(files):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, test_resources_dir)
                file_list.append(rel_path)
                bkt.upload_file(
                    full_path,
                    rel_path,  # Set content type for `no_extension/small` file to text/csv
                    ExtraArgs=(
                        {"ContentType": "text/csv"} if "." not in rel_path else {}
                    ),
                )
                s3_client.put_object_tagging(
                    Bucket=bucket_name,
                    Key=rel_path,
                    Tagging={"TagSet": [{"Key": "baz", "Value": "bob"}]},
                )
                key = (
                    moto.s3.models.s3_backends["123456789012"]["global"]
                    .buckets[bucket_name]
                    .keys[rel_path]
                )
                current_time_sec += 10
                key.last_modified = datetime.fromtimestamp(current_time_sec)

        # This is used to make sure the list of files are the same in the test as locally
        assert file_list == FILE_LIST_FOR_VALIDATION
    yield


@pytest.fixture(scope="module", autouse=True)
def touch_local_files(pytestconfig):
    test_resources_dir = (
        pytestconfig.rootpath / "tests/integration/s3/test_data/local_system/"
    )
    current_time_sec = datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S").timestamp()

    for root, _dirs, files in os.walk(test_resources_dir):
        _dirs.sort()
        for file in sorted(files):
            current_time_sec += 10
            full_path = os.path.join(root, file)
            os.utime(full_path, times=(current_time_sec, current_time_sec))


SHARED_SOURCE_FILES_PATH = "./tests/integration/s3/sources/shared"
shared_source_files = [
    (SHARED_SOURCE_FILES_PATH, p) for p in os.listdir(SHARED_SOURCE_FILES_PATH)
]

S3_SOURCE_FILES_PATH = "./tests/integration/s3/sources/s3"
s3_source_files = [(S3_SOURCE_FILES_PATH, p) for p in os.listdir(S3_SOURCE_FILES_PATH)]


@pytest.mark.integration
@pytest.mark.parametrize("source_file_tuple", shared_source_files + s3_source_files)
def test_data_lake_s3_ingest(
    pytestconfig, s3_populate, source_file_tuple, tmp_path, mock_time
):
    source_dir, source_file = source_file_tuple
    test_resources_dir = pytestconfig.rootpath / "tests/integration/s3/"

    f = open(os.path.join(source_dir, source_file))
    source = json.load(f)

    config_dict = {}
    config_dict["source"] = source
    config_dict["sink"] = {
        "type": "file",
        "config": {
            "filename": f"{tmp_path}/{source_file}",
        },
    }

    config_dict["run_id"] = source_file

    pipeline = Pipeline.create(config_dict)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{source_file}",
        golden_path=f"{test_resources_dir}/golden-files/s3/golden_mces_{source_file}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
        ],
    )


@pytest.mark.integration
@pytest.mark.parametrize("source_file_tuple", shared_source_files)
def test_data_lake_local_ingest(
    pytestconfig, touch_local_files, source_file_tuple, tmp_path, mock_time
):
    source_dir, source_file = source_file_tuple
    test_resources_dir = pytestconfig.rootpath / "tests/integration/s3/"
    f = open(os.path.join(source_dir, source_file))
    source = json.load(f)

    config_dict = {}
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
    config_dict["source"] = source
    config_dict["sink"] = {
        "type": "file",
        "config": {
            "filename": f"{tmp_path}/{source_file}",
        },
    }

    config_dict["run_id"] = source_file

    pipeline = Pipeline.create(config_dict)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{source_file}",
        golden_path=f"{test_resources_dir}/golden-files/local/golden_mces_{source_file}",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\[\d+\]\['value'\]\['time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
            # root[41]['aspect']['json']['fieldProfiles'][0]['sampleValues'][0]
            r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
            #        "root[0]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][2]['com.linkedin.pegasus2avro.schema.SchemaMetadata']['fields'][4]"
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.schema.SchemaMetadata'\]\['fields'\]",
            #    "root[0]['proposedSnapshot']['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot']['aspects'][1]['com.linkedin.pegasus2avro.dataset.DatasetProperties']['customProperties']['size_in_bytes']"
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['size_in_bytes'\]",
        ],
    )


def test_data_lake_incorrect_config_raises_error(tmp_path, mock_time):
    ctx = PipelineContext(run_id="test-s3")

    # Baseline: valid config
    source: dict = {
        "path_spec": {"include": "a/b/c/d/{table}.*", "table_name": "{table}"}
    }
    s3 = S3Source.create(source, ctx)
    assert s3.source_config.platform == "file"

    # Case 1 : named variable in table name is not present in include
    source = {"path_spec": {"include": "a/b/c/d/{table}.*", "table_name": "{table1}"}}
    with pytest.raises(ValidationError, match="table_name"):
        S3Source.create(source, ctx)

    # Case 2 : named variable in exclude is not allowed
    source = {
        "path_spec": {
            "include": "a/b/c/d/{table}/*.*",
            "exclude": ["a/b/c/d/a-{exclude}/**"],
        },
    }
    with pytest.raises(ValidationError, match=r"exclude.*named variable"):
        S3Source.create(source, ctx)

    # Case 3 : unsupported file type not allowed
    source = {
        "path_spec": {
            "include": "a/b/c/d/{table}/*.hd5",
        }
    }
    with pytest.raises(ValidationError, match="file type"):
        S3Source.create(source, ctx)

    # Case 4 : ** in include not allowed
    source = {
        "path_spec": {
            "include": "a/b/c/d/**/*.*",
        },
    }
    with pytest.raises(ValidationError, match=r"\*\*"):
        S3Source.create(source, ctx)
