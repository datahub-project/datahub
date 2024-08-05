import datetime
import json
import logging
import os
import pathlib
import signal
import subprocess
import time
from typing import Iterator

import pytest
import yaml
from datahub.api.entities.dataset.dataset import (
    Dataset,
    SchemaFieldSpecification,
    SchemaSpecification,
)
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from tests.integrations_service_utils import (
    bootstrap_action,
    rollback_action,
    start_action,
    stop_action,
    wait_until_action_has_processed_event,
)
from tests.utils import (
    get_gms_url,
    get_integrations_service_url,
    wait_for_healthcheck_util,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(scope="session")
def graph_client() -> DataHubGraph:
    return DataHubGraph(config=DatahubClientConfig(server=get_gms_url()))


def kill_action_processes(action_urn=None):
    """
    Note : this is a temporary solution that only works with local deployments.
    This function will be replaced with a more robust solution in the future if needed.
    """
    # Find processes
    try:
        output = subprocess.check_output(["ps", "aux"], universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ps command: {e}")
        return

    pids = []
    for line in output.splitlines():
        if "action_runner.py" in line:
            if action_urn is None or action_urn in line:
                parts = line.split()
                if len(parts) > 1:
                    pids.append(int(parts[1]))

    if not pids:
        message = (
            f"No processes found for action URN: {action_urn}"
            if action_urn
            else "No action_runner.py processes found"
        )
        print(message)
        return

    # Kill processes
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
            print(f"Killed process {pid}")
        except ProcessLookupError:
            print(f"Process {pid} not found")
        except PermissionError:
            print(f"Permission denied to kill process {pid}")

    message = (
        f"All processes for action URN {action_urn} have been terminated"
        if action_urn
        else "All action_runner.py processes have been terminated"
    )
    print(message)


def cleanup(graph_client: DataHubGraph, urns: list, test_resources_dir: str):
    for urn in urns:
        graph_client.delete_entity(urn, hard=True)
    actions_gql = (pathlib.Path(test_resources_dir) / "actions.gql").read_text()
    all_actions = graph_client.execute_graphql(
        query=actions_gql, operation_name="listActions"
    )
    logger.debug(f"Got actions: {all_actions}")
    try:
        for action in all_actions["listActionPipelines"]["actionPipelines"]:
            action_urn = action["urn"]
            try:
                stop_action(action_urn, get_integrations_service_url())
            except Exception as e:
                logger.error(f"Error stopping action: {e}")
            # ensure all action processes that have been spawned are stopped
            # action processes are named like actions/action_runner.py
            # /tmp/datahub/recipes/urn:li:dataHubAction:1721600759.json --port
            # .. 8080
            try:
                kill_action_processes(action_urn)
            except Exception as e:
                logger.error(f"Error killing action processes: {e}")
            graph_client.delete_entity(action_urn, hard=True)
    except Exception as e:
        logger.error(f"Error deleting action: {e}")
    try:
        kill_action_processes()
    except Exception as e:
        logger.error(f"Error killing action processes: {e}")


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/propagation/"


@pytest.fixture(scope="module")
def test_action_urn():
    return f"urn:li:dataHubAction:{int(time.time())}"


@pytest.fixture(scope="module")
def create_test_data(
    graph_client: DataHubGraph, test_resources_dir, test_action_urn
) -> Iterator[None]:
    # Create datasets
    dataset_urns = [make_dataset_urn("snowflake", f"table_foo_{i}") for i in range(2)]
    downstream_dataset_urn = dataset_urns[1]
    upstream_dataset_urn = dataset_urns[0]
    downstream_schema_field_urns = [
        make_schema_field_urn(downstream_dataset_urn, "column_1"),
        make_schema_field_urn(downstream_dataset_urn, "column_2"),
    ]
    upstream_schema_field_urns = [
        make_schema_field_urn(upstream_dataset_urn, "column_1"),
        make_schema_field_urn(upstream_dataset_urn, "column_2"),
    ]

    action_urn = test_action_urn
    recipe_file = test_resources_dir / "doc_propagation_action_recipe.yaml"

    with open(recipe_file, "r") as f:
        recipe_json_str = json.dumps(yaml.safe_load(f))

    cleanup(
        graph_client,
        dataset_urns
        + downstream_schema_field_urns
        + upstream_schema_field_urns
        + [action_urn],
        test_resources_dir,
    )
    wait_for_writes_to_sync()

    for i, dataset_urn in enumerate(dataset_urns):
        dataset = Dataset(
            id=f"table_foo_{i}",
            platform="snowflake",
            name=f"table_foo_{i}",
            description=f"this is table foo {i}",
            subtype=None,
            subtypes=None,
            downstreams=None,
            properties=None,
            urn=dataset_urn,
            schema=SchemaSpecification(
                file=None,
                fields=[
                    SchemaFieldSpecification(
                        id="column_1",
                        type="string",
                        description="this is column 1" if i == 0 else None,
                        urn=make_schema_field_urn(dataset_urn, "column_1"),
                    ),
                    SchemaFieldSpecification(
                        id="column_2",
                        type="string",
                        urn=make_schema_field_urn(dataset_urn, "column_2"),
                        description="this is column 2" if i == 0 else None,
                    ),
                ],
            ),
        )
        for mcp in dataset.generate_mcp():
            graph_client.emit_mcp(mcp)

    # Emit lineage
    downstream_dataset_urn = dataset_urns[1]
    upstream_dataset_urn = dataset_urns[0]
    downstream_schema_field_urns = [
        make_schema_field_urn(downstream_dataset_urn, "column_1"),
        make_schema_field_urn(downstream_dataset_urn, "column_2"),
    ]
    upstream_schema_field_urns = [
        make_schema_field_urn(upstream_dataset_urn, "column_1"),
        make_schema_field_urn(upstream_dataset_urn, "column_2"),
    ]

    upstream_lineage_aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_dataset_urn,
                type="COPY",
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                upstreams=[upstream_schema_field_urn],
                downstreams=[downstream_schema_field_urn],
            )
            for upstream_schema_field_urn, downstream_schema_field_urn in zip(
                upstream_schema_field_urns, downstream_schema_field_urns
            )
        ],
    )
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=downstream_dataset_urn,
            aspect=upstream_lineage_aspect,
        )
    )

    # Provision the action
    from datahub.metadata.schema_classes import (
        DataHubActionConfigClass,
        DataHubActionInfoClass,
    )

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=action_urn,
            aspect=DataHubActionInfoClass(
                name="test_docs_propagation",
                type="datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction",
                config=DataHubActionConfigClass(
                    recipe=recipe_json_str,
                ),
            ),
        )
    )
    yield
    cleanup(
        graph_client,
        dataset_urns + downstream_schema_field_urns + upstream_schema_field_urns,
        test_resources_dir,
    )


def test_main_loop(
    graph_client: DataHubGraph, create_test_data, test_action_urn
) -> None:
    # First test bootstrap
    docs_propagation_bootstrap(graph_client, create_test_data, test_action_urn)

    # Then test rollback
    docs_propagation_rollback(graph_client, create_test_data, test_action_urn)

    # Finally test live
    docs_propagation_live(graph_client, create_test_data, test_action_urn)

    # Test rollback again
    docs_propagation_rollback(graph_client, create_test_data, test_action_urn)


def docs_propagation_bootstrap(
    graph_client: DataHubGraph, create_test_data, test_action_urn
) -> None:
    # Wait for the writes to sync

    wait_for_writes_to_sync()

    bootstrap_action(
        test_action_urn, get_integrations_service_url(), wait_for_completion=True
    )

    upstream_dataset_urn = make_dataset_urn("snowflake", "table_foo_0")
    upstream_schema_field_urn = make_schema_field_urn(upstream_dataset_urn, "column_1")

    downstream_schema_field_urn = make_schema_field_urn(
        make_dataset_urn("snowflake", "table_foo_1"), "column_1"
    )

    documentation_aspect = graph_client.get_aspect(
        downstream_schema_field_urn, DocumentationClass
    )
    assert documentation_aspect is not None
    if documentation_aspect:
        first_element = documentation_aspect.documentations[0]
        assert first_element.documentation == "this is column 1"
        assert first_element.attribution
        assert first_element.attribution.sourceDetail
        assert (
            first_element.attribution.sourceDetail.get("origin")
            == upstream_schema_field_urn
        )
        assert first_element.attribution.sourceDetail.get("via") is None
        assert first_element.attribution.sourceDetail.get("propagated") == "true"
        assert first_element.attribution.actor == "urn:li:corpuser:__datahub_system"


def docs_propagation_live(
    graph_client: DataHubGraph,
    create_test_data,
    test_action_urn,
) -> None:
    start_action(
        test_action_urn, get_integrations_service_url(), wait_for_completion=True
    )
    wait_for_writes_to_sync()

    try:
        # edit the description of the upstream schema field
        upstream_dataset_urn = make_dataset_urn("snowflake", "table_foo_0")
        upstream_schema_field_urn = make_schema_field_urn(
            upstream_dataset_urn, "column_1"
        )

        audit_timestamp = datetime.datetime.now(datetime.timezone.utc)
        # get human readable timestamp
        human_readable_timestamp = audit_timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
        editable_schema_metadata_aspect = EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="column_1",
                    description="this is the updated description as of {}".format(
                        human_readable_timestamp
                    ),
                ),
            ],
            created=AuditStampClass(
                time=int(audit_timestamp.timestamp() * 1000),
                actor="urn:li:corpuser:sdas@acryl.io",
            ),
            lastModified=AuditStampClass(
                time=int(audit_timestamp.timestamp() * 1000),
                actor="urn:li:corpuser:sdas@acryl.io",
            ),
        )
        assert editable_schema_metadata_aspect.validate()
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=upstream_dataset_urn,
                aspect=editable_schema_metadata_aspect,
            )
        )

        wait_until_action_has_processed_event(
            test_action_urn, get_integrations_service_url(), audit_timestamp
        )
        wait_for_writes_to_sync()

        # get the downstream schema field and check if the description has been
        # propagated
        downstream_schema_field_urn = make_schema_field_urn(
            make_dataset_urn("snowflake", "table_foo_1"), "column_1"
        )

        documentation_aspect = graph_client.get_aspect(
            downstream_schema_field_urn, DocumentationClass
        )
        assert documentation_aspect is not None
        if documentation_aspect and documentation_aspect.documentations:
            first_element = documentation_aspect.documentations[0]
            assert (
                first_element.documentation
                == "this is the updated description as of {}".format(
                    human_readable_timestamp
                )
            )
            assert first_element.attribution
            assert first_element.attribution.sourceDetail
            assert (
                first_element.attribution.sourceDetail.get("origin")
                == upstream_schema_field_urn
            )
            assert first_element.attribution.sourceDetail.get("via") is None
            assert first_element.attribution.sourceDetail.get("propagated") == "true"
            assert first_element.attribution.actor == "urn:li:corpuser:__datahub_system"
            # assert first_element.attribution.source ==
            # "urn:li:corpuser:__system"
    finally:
        stop_action(test_action_urn, get_integrations_service_url())


def docs_propagation_rollback(
    graph_client: DataHubGraph, create_test_data, test_action_urn
) -> None:
    wait_for_writes_to_sync()
    rollback_action(
        test_action_urn,
        get_integrations_service_url(),
        wait_for_completion=True,
    )
    # get the downstream schema field and check if the description has been
    # propagated
    downstream_schema_field_urn = make_schema_field_urn(
        make_dataset_urn("snowflake", "table_foo_1"), "column_1"
    )

    documentation_aspect = graph_client.get_aspect(
        downstream_schema_field_urn, DocumentationClass
    )
    assert documentation_aspect is not None
    try:
        assert documentation_aspect.documentations == []
    except Exception:
        breakpoint()
        raise
