import json
import logging
import random
import re

import numpy as np
import pytest
from ravendb import DocumentStore

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

from datahub.ingestion.run.pipeline import Pipeline
from ravendb.documents.commands.crud import PutDocumentCommand, GetDocumentsCommand

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


DB_RECIPE_FILE = "ravendb_to_file_db.yml"
RAVENDB_PORT = 8080
TESTDB_NAME = "testdb"
CONTAINER_NAME = "testravendb"
CONTAINER_IP = None


# ignore timestamps and random changing values triggered by container start
IGNORE_KEYS = [
    "runId",
    "lastCollectionIndexingTime",
    "lastDatabaseEtag",
    "lastDocEtag",
    "lastObserved",
    "databaseChangeVector",
    "externalUrl",
    "sizeOnDisk",
    "tempBuffersSizeOnDisk",
    "indexes"
]


def get_container_ip(container_name=CONTAINER_NAME):
    if CONTAINER_IP:
        return CONTAINER_IP
    else:
        import time
        import docker

        client = docker.from_env()
        try:
            container = client.containers.get(container_name)
            ip = container.attrs["NetworkSettings"]["IPAddress"]
            logging.debug(
                f"Container with name {container_name} found. IP: {ip}"
            )
            time.sleep(60)
            set_container_ip(ip)
            return ip
        except docker.errors.NotFound:
            logging.debug(f"Container with name '{container_name}' not found.")
            raise docker.errors.NotFound


def set_container_ip(value):
    global CONTAINER_IP
    logging.debug(f"Setting container ip to: {value}")
    CONTAINER_IP = value


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/ravendb"


def is_container_running(container_name: str) -> bool:
    """Returns true if the status of the container with the given name is 'Running'"""
    import time
    import docker

    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        logging.debug(
            f"Container with name {container_name} found. Status: {container.status}"
        )
        time.sleep(60)
        set_container_ip(container.attrs["NetworkSettings"]["IPAddress"])
        return container.status == "running"
    except docker.errors.NotFound:
        logging.debug(f"Container with name '{container_name}' not found.")
        return False


@pytest.fixture(scope="module")
def ravendb_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    logging.debug("Start RavenDB runner")
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "ravendb"
    ) as docker_services:
        wait_for_port(
            docker_services,
            CONTAINER_NAME,
            RAVENDB_PORT,
            timeout=500,
            checker=lambda: is_container_running(CONTAINER_NAME),
        )
        yield docker_services


def load_document_store():
    logging.debug(f"Loading document store of database '{TESTDB_NAME}'")
    container_ip = get_container_ip()
    store = DocumentStore(
        f"http://{container_ip}:{RAVENDB_PORT}", TESTDB_NAME
    )  # RAVEN_DATABASE)
    store.initialize()
    return store


def remove_database():
    from ravendb.serverwide.operations.common import DeleteDatabaseOperation

    logging.debug("Deleting databases")
    store = load_document_store()
    store.maintenance.server.send(
        DeleteDatabaseOperation(database_name=TESTDB_NAME, hard_delete=True)
    )


def prepare_database():
    """
        Fills the database with test items, if the test items are not yet in the collections.
    """
    from ravendb.documents.indexes.definitions import IndexDefinition
    from ravendb.documents.operations.indexes import (
        GetIndexOperation,
        PutIndexesOperation,
    )

    logging.debug("Preparing databases")
    store = load_document_store()
    request_executor = store.get_request_executor()

    def assert_entries_set():
        command = GetDocumentsCommand.from_single_id("testing/art1")
        request_executor.execute_command(command)
        try:
            return command.result.results[0]["@metadata"]["@id"] == "testing/art1"
        except AttributeError:
            return False

    if assert_entries_set():
        logging.debug("Skipping task: Database is already prepared")
        return

    for i in range(5):
        put_command1 = PutDocumentCommand(
            key=f"testing/toy{i}",
            change_vector=None,
            document={
                "Name": f"test_toy_{i}",
                "Price": str(np.around(random.uniform(1, 100), 2)),
                "Category": "Toy",
                "Brand": "Fisher Price",
                "@metadata": {
                    "Raven-Python-Type": "Products",
                    "@collection": "Products",
                },
            },
        )
        request_executor.execute_command(put_command1)
        put_command2 = PutDocumentCommand(
            key=f"testing/art{i}",
            change_vector=None,
            document={
                "Name": f"test_art_{i}",
                "Price": str(np.around(random.uniform(1, 100), 2)),
                "Category": "Image",
                "Size": "A4",
                "Shipping": True,
                "@metadata": {
                    "Raven-Python-Type": "Products",
                    "@collection": "Products",
                },
            },
        )
        request_executor.execute_command(put_command2)
        logging.debug(f"Successfull iteration {i} of inserts.")

    # create index
    index = IndexDefinition()
    index.name = "Products/Search"

    index.maps = (
        "from p in docs.Products "
        + "select new { "
        + "   Name = p.Name, "
        + "   Category = p.Category,"
        + "   Id = p.DocumentId "
        + "}"
    )
    request_executor.execute_command(
        PutIndexesOperation(index).get_command(store.conventions)
    )

    # assert entries set
    command = GetDocumentsCommand.from_single_id("testing/art1")
    request_executor.execute_command(command)
    assert assert_entries_set()
    command = GetIndexOperation(index.name).get_command(store.conventions)
    request_executor.execute_command(command)
    assert command.result != None


def run_pipeline(tmp_path):
    logging.debug(f"Run database with container id {get_container_ip()}")
    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(
        {
            "run_id": "ravendb-test",
            "source": {
                "type": "ravendb",
                "config": {
                    "connect_uri": f"http://{get_container_ip()}:8080",
                    "collection_pattern":
                    {
                        'allow': [".*"],
                        'deny': ["@.*"],
                        'ignoreCase': True
                    },
                    "schema_sampling_size": 200,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/ravendb_mces.json"
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    pipeline.pretty_print_summary()


def test_ravendb_ingest_without_collections(
    ravendb_runner, test_resources_dir, tmp_path, pytestconfig
):
    run_pipeline(tmp_path)

    golden_path = test_resources_dir / "ravendb_mces_no_collections_golden.json"
    output_path = tmp_path / "ravendb_mces.json"
    check_golden_file(output_path, golden_path, pytestconfig)


def test_ravendb_ingest_with_db(
    ravendb_runner, test_resources_dir, tmp_path, pytestconfig
):
    # Set up database
    prepare_database()
    run_pipeline(tmp_path)

    golden_path = test_resources_dir / "ravendb_mces_with_db_golden.json"
    output_path = tmp_path / "ravendb_mces.json"
    check_golden_file(output_path, golden_path, pytestconfig)


def test_ravendb_ingest_without_documentstore(
    ravendb_runner, test_resources_dir, tmp_path, pytestconfig
):
    remove_database()
    run_pipeline(tmp_path)

    golden_path = test_resources_dir / "ravendb_mces_no_documentstore_golden.json"
    output_path = tmp_path / "ravendb_mces.json"
    check_golden_file(output_path, golden_path, pytestconfig)


def check_golden_file(output_file_path, golden_file_path, pytestconfig):
    """
    Check mce output file against golden file ignoring the keys in IGNORE_KEYS array
    since they are run dependent.
    """

    def find_key_chains(d, target_key, current_chain=None, key_chains=None):
        if current_chain is None:
            current_chain = []
        if key_chains is None:
            key_chains = []

        if isinstance(d, dict):
            for k, v in d.items():
                if k == target_key:
                    key_chains.append(current_chain + [k])
                find_key_chains(v, target_key, current_chain + [k], key_chains)
        elif isinstance(d, list):
            for i, v in enumerate(d):
                find_key_chains(v, target_key, current_chain + [i], key_chains)

        return key_chains

    def construct_key_regex(keys_list, escape=True):
        regex_list = []

        for keys in keys_list:
            s = "root[XX]"
            for key in keys:
                if isinstance(key, int):
                    replace = f"[{str(key)}][XX]"
                else:
                    replace = f"['{str(key)}'][XX]"
                s = s.replace("[XX]", replace)
            s = s.replace("[XX]", "")
            if escape:
                regex_string = re.escape(s)
                regex_string = re.sub(r"\d+", r"\\d+", regex_string)
                # print(regex_string)
                regex_list.append(regex_string)
            else:
                regex_list.append(s)
        return list(set(regex_list))

    with open(str(golden_file_path), "r") as f:
        golden = json.load(f)
    ignore_key_paths = []
    for key in IGNORE_KEYS:
        keys = find_key_chains(golden, key)
        ignore_key_paths.extend(construct_key_regex(keys))

    logging.debug("Ignoring attributes during check:")
    [logging.debug(key) for key in ignore_key_paths]

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file_path,
        golden_path=golden_file_path,
        ignore_paths=ignore_key_paths,
    )
