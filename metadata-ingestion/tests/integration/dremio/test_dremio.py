import json
import os
import subprocess
from typing import Dict

import boto3
import pytest
import requests
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2023-10-15 07:00:00"
MINIO_PORT = 9000
MYSQL_PORT = 3306

# Dremio server credentials
DREMIO_HOST = "http://localhost:9047"
DREMIO_USERNAME = "admin"
DREMIO_PASSWORD = "2310Admin1234!@"
MINIO_S3_ENDPOINT = "minio:9000"
AWS_ACCESS_KEY = "miniouser"
AWS_SECRET_KEY = "miniopassword"
AWS_ROOT_PATH = "/warehouse"


def is_minio_up(container_name: str) -> bool:
    """A cheap way to figure out if postgres is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '1 Online'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


def is_mysql_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mysql is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '/usr/sbin/mysqld: ready for connections.' | grep {port}"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


def install_mysql_client(container_name: str) -> None:
    """
    This is bit hacky to install mysql-client and connect mysql to start-mysql in container
    """

    command = f'docker exec --user root {container_name} sh -c  "apt-get update && apt-get install -y mysql-client && /usr/bin/mysql -h test_mysql -u root -prootpwd123"'
    ret = subprocess.run(command, shell=True, stdout=subprocess.DEVNULL)
    assert ret.returncode == 0


def create_spaces_and_folders(headers):
    """
    Create spaces and folders in Dremio
    """
    url = f"{DREMIO_HOST}/api/v3/catalog"

    # Create Space
    payload = {"entityType": "space", "name": "space"}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create space: {response.text}"

    # Create Folder inside Space
    json_data = {"entityType": "folder", "path": ["space", "test_folder"]}
    response = requests.post(url, headers=headers, data=json.dumps(json_data))
    assert response.status_code == 200, f"Failed to create folder: {response.text}"


def create_sample_source(headers):
    url = f"{DREMIO_HOST}/api/v3/catalog"

    payload = {
        "entityType": "source",
        "config": {
            "externalBucketList": ["samples.dremio.com"],
            "credentialType": "NONE",
            "secure": False,
            "propertyList": [],
        },
        "name": "Samples",
        "accelerationRefreshPeriod": 3600000,
        "accelerationGracePeriod": 10800000,
        "accelerationNeverRefresh": True,
        "accelerationNeverExpire": True,
        "accelerationActivePolicyType": "PERIOD",
        "accelerationRefreshSchedule": "0 0 8 * * *",
        "accelerationRefreshOnDataChanges": False,
        "type": "S3",
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add dataset: {response.text}"


def create_s3_source(headers):
    url = f"{DREMIO_HOST}/api/v3/catalog"

    payload = {
        "entityType": "source",
        "name": "s3",
        "config": {
            "credentialType": "ACCESS_KEY",
            "accessKey": AWS_ACCESS_KEY,
            "accessSecret": AWS_SECRET_KEY,
            "secure": False,
            "externalBucketList": ["warehouse"],
            "enableAsync": True,
            "enableFileStatusCheck": True,
            "rootPath": "/",
            "defaultCtasFormat": "ICEBERG",
            "propertyList": [
                {"name": "fs.s3a.access.key", "value": AWS_ACCESS_KEY},
                {"name": "fs.s3a.secret.key", "value": AWS_SECRET_KEY},
                {
                    "name": "fs.s3a.aws.credentials.provider",
                    "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                },
                {"name": "fs.s3a.endpoint", "value": MINIO_S3_ENDPOINT},
                {"name": "fs.s3a.path.style.access", "value": "True"},
                {"name": "dremio.s3.compat", "value": "True"},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "False"},
            ],
            "compatibilityMode": True,
            "whitelistedBuckets": [],
            "isCachingEnabled": True,
            "maxCacheSpacePct": 100,
        },
        "accelerationRefreshPeriod": 3600000,
        "accelerationGracePeriod": 10800000,
        "accelerationActivePolicyType": "PERIOD",
        "accelerationRefreshSchedule": "0 0 8 * * *",
        "accelerationRefreshOnDataChanges": False,
        "metadataPolicy": {
            "deleteUnavailableDatasets": True,
            "autoPromoteDatasets": False,
            "namesRefreshMs": 3600000,
            "datasetRefreshAfterMs": 3600000,
            "datasetExpireAfterMs": 10800000,
            "authTTLMs": 86400000,
            "datasetUpdateMode": "PREFETCH_QUERIED",
        },
        "type": "S3",
        "accessControlList": {"userControls": [], "roleControls": []},
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add s3 datasource: {response.text}"


def create_mysql_source(headers):
    url = f"{DREMIO_HOST}/api/v3/catalog"

    payload = {
        "entityType": "source",
        "config": {
            "username": "root",
            "password": "rootpwd123",
            "hostname": "test_mysql",
            "port": MYSQL_PORT,
            "database": "",
            "authenticationType": "MASTER",
            "netWriteTimeout": 60,
            "fetchSize": 200,
            "maxIdleConns": 8,
            "idleTimeSec": 60,
        },
        "name": "mysql",
        "accelerationRefreshPeriod": 3600000,
        "accelerationGracePeriod": 10800000,
        "accelerationActivePolicyType": "PERIOD",
        "accelerationRefreshSchedule": "0 0 8 * * *",
        "accelerationRefreshOnDataChanges": False,
        "metadataPolicy": {
            "deleteUnavailableDatasets": True,
            "namesRefreshMs": 3600000,
            "datasetRefreshAfterMs": 3600000,
            "datasetExpireAfterMs": 10800000,
            "authTTLMs": 86400000,
            "datasetUpdateMode": "PREFETCH_QUERIED",
        },
        "type": "MYSQL",
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, (
        f"Failed to add mysql datasource: {response.text}"
    )


def upload_dataset(headers):
    url = f"{DREMIO_HOST}/api/v3/catalog/dremio%3A%2Fs3%2Fwarehouse"
    payload = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        "path": [
            "s3",
            "warehouse",
        ],
        "format": {"type": "Parquet"},
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add dataset: {response.text}"

    url = f"{DREMIO_HOST}/api/v3/catalog/dremio%3A%2FSamples%2Fsamples.dremio.com%2FNYC-weather.csv"

    payload = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        "path": [
            "Samples",
            "samples.dremio.com",
            "NYC-weather.csv",
        ],
        "format": {
            "fieldDelimiter": ",",
            "quote": '"',
            "comment": "#",
            "lineDelimiter": "\r\n",
            "escape": '"',
            "extractHeader": False,
            "trimHeader": True,
            "skipFirstLine": False,
            "type": "Text",
        },
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add dataset: {response.text}"

    url = f"{DREMIO_HOST}/api/v3/catalog/dremio%3A%2FSamples%2Fsamples.dremio.com%2FDremio%20University%2Foracle-departments.xlsx"

    payload = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        "path": [
            "Samples",
            "samples.dremio.com",
            "Dremio University",
            "oracle-departments.xlsx",
        ],
        "format": {"extractHeader": True, "hasMergedCells": False, "type": "Excel"},
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add dataset: {response.text}"

    url = f"{DREMIO_HOST}/api/v3/catalog/dremio%3A%2FSamples%2Fsamples.dremio.com%2FDremio%20University%2Fgoogleplaystore.csv"

    payload = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        "path": [
            "Samples",
            "samples.dremio.com",
            "Dremio University",
            "googleplaystore.csv",
        ],
        "format": {
            "fieldDelimiter": ",",
            "quote": '"',
            "comment": "#",
            "lineDelimiter": "\r\n",
            "escape": '"',
            "extractHeader": False,
            "trimHeader": True,
            "skipFirstLine": False,
            "type": "Text",
        },
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add dataset: {response.text}"

    url = f"{DREMIO_HOST}/api/v3/catalog/dremio%3A%2FSamples%2Fsamples.dremio.com%2Ftpcds_sf1000%2Fcatalog_page%2F1ab266d5-18eb-4780-711d-0fa337fa6c00%2F0_0_0.parquet"
    payload = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        "path": [
            "Samples",
            "samples.dremio.com",
            "tpcds_sf1000",
            "catalog_page",
            "1ab266d5-18eb-4780-711d-0fa337fa6c00",
            "0_0_0.parquet",
        ],
        "format": {"type": "Parquet"},
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to add dataset: {response.text}"


def create_view(headers):
    # from s3
    url = f"{DREMIO_HOST}/api/v3/catalog"
    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "test_folder", "raw"],
        "sql": "SELECT * FROM s3.warehouse",
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"

    url = f"{DREMIO_HOST}/api/v3/catalog"
    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "warehouse"],
        "sql": 'SELECT * from Samples."samples.dremio.com"."NYC-weather.csv"',
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"

    # from mysql
    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "test_folder", "customers"],
        "sql": "SELECT * FROM mysql.northwind.customers",
        "sqlContext": ["mysql", "northwind"],
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"

    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "test_folder", "orders"],
        "sql": "SELECT * FROM mysql.northwind.orders",
        "sqlContext": ["mysql", "northwind"],
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"

    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "test_folder", "metadata_aspect"],
        "sql": "SELECT * FROM mysql.metagalaxy.metadata_aspect",
        "sqlContext": ["mysql", "metagalaxy"],
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"

    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "test_folder", "metadata_index"],
        "sql": "SELECT * FROM mysql.metagalaxy.metadata_index",
        "sqlContext": ["mysql", "metagalaxy"],
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"

    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": ["space", "test_folder", "metadata_index_view"],
        "sql": "SELECT * FROM mysql.metagalaxy.metadata_index_view",
        "sqlContext": ["mysql", "metagalaxy"],
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    assert response.status_code == 200, f"Failed to create view: {response.text}"


def dremio_header():
    """
    Get Dremio authentication token
    """
    url = f"{DREMIO_HOST}/apiv2/login"
    headers = {"Content-Type": "application/json"}
    payload = {"userName": DREMIO_USERNAME, "password": DREMIO_PASSWORD}

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()  # Raise exception if request failed

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"_dremio{response.json()['token']}",
    }
    return headers


@pytest.fixture(scope="module")
def dremio_setup():
    headers = dremio_header()
    create_sample_source(headers)
    create_s3_source(headers)
    create_mysql_source(headers)
    create_spaces_and_folders(headers)
    upload_dataset(headers)
    create_view(headers)


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/dremio"


@pytest.fixture(scope="module")
def mock_dremio_service(docker_compose_runner, pytestconfig, test_resources_dir):
    # Spin up Dremio and MinIO (for mock S3) services using Docker Compose.
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "dremio"
    ) as docker_services:
        wait_for_port(docker_services, "dremio", 9047, timeout=120)
        wait_for_port(
            docker_services,
            "minio",
            MINIO_PORT,
            timeout=120,
            checker=lambda: is_minio_up("minio"),
        )
        wait_for_port(
            docker_services,
            "test_mysql",
            MYSQL_PORT,
            timeout=120,
            checker=lambda: is_mysql_up("test_mysql", MYSQL_PORT),
        )

        # Ensure the admin and data setup scripts have the right permissions
        subprocess.run(
            ["chmod", "+x", f"{test_resources_dir}/setup_dremio_admin.sh"], check=True
        )

        # Run the setup_dremio_admin.sh script
        admin_setup_cmd = f"{test_resources_dir}/setup_dremio_admin.sh"
        subprocess.run(admin_setup_cmd, shell=True, check=True)

        install_mysql_client("dremio")
        yield docker_compose_runner


@pytest.fixture(scope="module", autouse=True)
def s3_bkt(mock_dremio_service):
    s3 = boto3.resource(
        "s3",
        endpoint_url=f"http://localhost:{MINIO_PORT}",
        aws_access_key_id="miniouser",
        aws_secret_access_key="miniopassword",
    )
    bkt = s3.Bucket("warehouse")
    bkt.create()
    return bkt


@pytest.fixture(scope="module", autouse=True)
def populate_minio(pytestconfig, s3_bkt):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dremio/test_data/"

    for root, _dirs, files in os.walk(test_resources_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, test_resources_dir)
            s3_bkt.upload_file(full_path, rel_path)
    yield


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_dremio_ingest(
    test_resources_dir,
    dremio_setup,
    pytestconfig,
    tmp_path,
):
    # Run the metadata ingestion pipeline with specific output file
    config_file = (test_resources_dir / "dremio_to_file.yml").resolve()
    output_path = tmp_path / "dremio_mces.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "dremio_mces_golden.json",
        ignore_paths=[],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_dremio_platform_instance_urns(
    test_resources_dir,
    dremio_setup,
    pytestconfig,
    tmp_path,
):
    config_file = (
        test_resources_dir / "dremio_platform_instance_to_file.yml"
    ).resolve()
    output_path = tmp_path / "dremio_mces.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    with output_path.open() as f:
        content = f.read()
        # Skip if file is empty or just contains brackets
        if not content or content.strip() in ("[]", "[", "]"):
            pytest.fail(f"Output file is empty or invalid: {content}")

    try:
        # Try to load as JSON Lines first
        mces = []
        for line in content.splitlines():
            line = line.strip()
            if line and line not in ("[", "]"):  # Skip empty lines and bare brackets
                mce = json.loads(line)
                mces.append(mce)
    except json.JSONDecodeError:
        # If that fails, try loading as a single JSON array
        try:
            mces = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Failed to parse file content: {content}")
            raise e

    # Verify MCEs
    assert len(mces) > 0, "No MCEs found in output file"

    # Verify the platform instances
    for mce in mces:
        if "entityType" not in mce:
            continue

        # Check dataset URN structure
        if mce["entityType"] == "dataset" and "entityUrn" in mce:
            assert "test-platform.dremio" in mce["entityUrn"], (
                f"Platform instance missing in dataset URN: {mce['entityUrn']}"
            )

        # Check aspects for both datasets and containers
        if "aspectName" in mce:
            # Check dataPlatformInstance aspect
            if mce["aspectName"] == "dataPlatformInstance":
                aspect = mce["aspect"]
                if not isinstance(aspect, Dict) or "json" not in aspect:
                    continue

                aspect_json = aspect["json"]
                if not isinstance(aspect_json, Dict):
                    continue

                if "instance" not in aspect_json:
                    continue

                instance = aspect_json["instance"]
                expected_instance = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:dremio,test-platform)"
                assert instance == expected_instance, (
                    f"Invalid platform instance format: {instance}"
                )

    # Verify against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "dremio_platform_instance_mces_golden.json",
        ignore_paths=[],
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_dremio_schema_filter(
    test_resources_dir,
    dremio_setup,
    pytestconfig,
    tmp_path,
):
    config_file = (test_resources_dir / "dremio_schema_filter_to_file.yml").resolve()
    output_path = tmp_path / "dremio_mces.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "dremio_schema_filter_mces_golden.json",
        ignore_paths=[],
    )
