import logging
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import pytest
import tenacity
from jinja2 import Template
from pydantic import BaseModel

import datahub.metadata.schema_classes as models
from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.utilities.urns.urn import Urn
from tests.utils import (
    delete_urns_from_file,
    get_gms_url,
    ingest_file_via_rest,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

DELETE_AFTER_TEST = os.getenv("DELETE_AFTER_TEST", "false").lower() == "true"


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="create_test_data"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()


def sanitize_field(field: models.SchemaFieldClass) -> models.SchemaFieldClass:
    if field.fieldPath.startswith("[version=2.0]"):
        field.fieldPath = field.fieldPath.split(".")[-1]

    return field


def sanitize(event: Any) -> Any:
    if isinstance(event, MetadataChangeProposalWrapper):
        if event.aspectName == "schemaMetadata":
            assert isinstance(event.aspect, models.SchemaMetadataClass)
            schema_metadata: models.SchemaMetadataClass = event.aspect
            schema_metadata.fields = [
                sanitize_field(field) for field in schema_metadata.fields
            ]

    return event


def generate_temp_yaml(template_path: Path, output_path: Path, test_id: str):
    # Load the YAML template
    with open(template_path, "r") as file:
        template_content = file.read()

    # Render the template with Jinja2
    template = Template(template_content)
    rendered_yaml = template.render(test_id=test_id)

    # Write the rendered YAML to a temporary file
    with open(output_path, "w") as file:
        file.write(rendered_yaml)

    return output_path


class ActionTestEnv(BaseModel):
    class Config:
        allow_extra = True

    DATAHUB_ACTIONS_DOC_PROPAGATION_MAX_PROPAGATION_FANOUT: int


@pytest.fixture(scope="module")
def action_env_vars(pytestconfig) -> ActionTestEnv:
    common_test_resources_dir = Path(pytestconfig.rootdir) / "tests" / "resources"
    env_file = common_test_resources_dir / "actions.env"
    # validate the env file exists
    assert env_file.exists()
    # read the env file, ignore comments and empty lines and convert to dict
    env_vars = {}
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                env_vars[key] = value

    return ActionTestEnv.parse_obj(env_vars)


@pytest.fixture(scope="function")
def test_id():
    return f"test_{uuid.uuid4().hex[:8]}"


def create_test_data(filename: str, template_path: Path, test_id: str) -> List[str]:
    def get_urns_from_mcp(mcp: MetadataChangeProposalWrapper) -> List[str]:
        assert mcp.entityUrn
        urns = [mcp.entityUrn]
        if mcp.aspectName == "schemaMetadata":
            dataset_urn = mcp.entityUrn
            assert isinstance(mcp.aspect, models.SchemaMetadataClass)
            schema_metadata: models.SchemaMetadataClass = mcp.aspect
            for field in schema_metadata.fields:
                field_urn = make_schema_field_urn(dataset_urn, field.fieldPath)
                urns.append(field_urn)
        return urns

    # Generate temporary YAML file
    temp_yaml_path = template_path.parent / f"temp_{template_path.name}_{test_id}.yaml"
    generate_temp_yaml(template_path, temp_yaml_path, test_id)

    mcps = []
    all_urns = []
    for dataset in Dataset.from_yaml(file=str(temp_yaml_path)):
        mcps.extend([sanitize(event) for event in dataset.generate_mcp()])

    file_emitter = FileEmitter(filename)
    for mcp in mcps:
        all_urns.extend(get_urns_from_mcp(mcp))
        file_emitter.emit(mcp)

    file_emitter.close()

    # Clean up the temporary YAML file
    temp_yaml_path.unlink()

    return list(set(all_urns))


@pytest.fixture(scope="module", autouse=False)
def root_dir(pytestconfig):
    return pytestconfig.rootdir


@pytest.fixture(scope="module", autouse=False)
def test_resources_dir(root_dir):
    return Path(root_dir) / "tests" / "actions" / "doc_propagation" / "resources"


@pytest.fixture(scope="function")
def ingest_cleanup_data(ingest_cleanup_data_function):
    """
    This fixture is a wrapper around ingest_cleanup_data_function() that yields
    the urns to make default usage easier.
    """
    with ingest_cleanup_data_function() as urns:
        # Convert the generator to a list to ensure it is fully consumed
        yield urns


@pytest.fixture(scope="module", autouse=False)
def graph():
    graph: DataHubGraph = DataHubGraph(config=DatahubClientConfig(server=get_gms_url()))
    yield graph


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(scope="function")
def test_data(tmp_path, test_resources_dir, test_id):
    filename = tmp_path / f"test_data_{test_id}.json"
    return create_test_data(str(filename), test_resources_dir, test_id)


@pytest.fixture(scope="function")
def dataset_depth_map(test_id):
    return {
        0: f"urn:li:dataset:(urn:li:dataPlatform:events,{test_id}.ClickEvent,PROD)",
        1: f"urn:li:dataset:(urn:li:dataPlatform:hive,{test_id}.user.clicks,PROD)",
        2: f"urn:li:dataset:(urn:li:dataPlatform:hive,{test_id}.user.clicks_2,PROD)",
        3: f"urn:li:dataset:(urn:li:dataPlatform:hive,{test_id}.user.clicks_3,PROD)",
        4: f"urn:li:dataset:(urn:li:dataPlatform:hive,{test_id}.user.clicks_4,PROD)",
        5: f"urn:li:dataset:(urn:li:dataPlatform:hive,{test_id}.user.clicks_5,PROD)",
        6: f"urn:li:dataset:(urn:li:dataPlatform:hive,{test_id}.user.clicks_6,PROD)",
    }


@pytest.fixture(scope="function")
def ingest_cleanup_data_function(request, test_resources_dir, graph, test_id):
    @contextmanager
    def _ingest_cleanup_data(template_file="datasets_template.yaml"):
        new_file, filename = tempfile.mkstemp(suffix=f"_{test_id}.json")
        try:
            template_path = Path(test_resources_dir) / template_file
            all_urns = create_test_data(filename, template_path, test_id)
            print(
                f"Ingesting datasets test data for test_id: {test_id} using template: {template_file}"
            )
            ingest_file_via_rest(filename)
            yield all_urns
        finally:
            if DELETE_AFTER_TEST:
                print(f"Removing test data for test_id: {test_id}")
                delete_urns_from_file(filename)
                for urn in all_urns:
                    graph.delete_entity(urn, hard=True)
                wait_for_writes_to_sync()
            os.remove(filename)

    return _ingest_cleanup_data


@pytest.fixture(scope="function")
def large_fanout_graph_function(graph: DataHubGraph):
    @contextmanager
    def _large_fanout_graph(
        test_id: str, max_fanout: int
    ) -> Iterator[Tuple[str, List[str]]]:
        max_index = max_fanout + 1
        all_urns = []
        dataset_base_name = f"large_fanout_dataset_{test_id}"
        try:
            delete_prior_to_running = False
            if delete_prior_to_running:
                for i in range(1, max_index + 1):
                    dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:hive,{dataset_base_name}_{i},PROD)"
                    graph.delete_entity(dataset_urn, hard=True)
                graph.delete_entity(
                    f"urn:li:dataset:(urn:li:dataPlatform:events,{dataset_base_name}_0,PROD)",
                    hard=True,
                )
                graph.delete_entity(
                    f"urn:li:dataset:(urn:li:dataPlatform:events,{dataset_base_name}_1,PROD)",
                    hard=True,
                )
                wait_for_writes_to_sync()

            dataset_1 = f"urn:li:dataset:(urn:li:dataPlatform:events,{dataset_base_name}_0,PROD)"
            schema_metadata_1 = models.SchemaMetadataClass(
                schemaName="large_fanout_dataset_0",
                platform="urn:li:dataPlatform:events",
                version=0,
                hash="",
                platformSchema=models.OtherSchemaClass(rawSchema=""),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="ip",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        description="This is the description",
                        nativeDataType="string",
                    )
                ],
            )
            graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_1, aspect=schema_metadata_1
                )
            )
            all_urns.append(dataset_1)

            total_fanout = max_index
            for i in range(1, total_fanout + 1):
                dataset_i = f"urn:li:dataset:(urn:li:dataPlatform:hive,{dataset_base_name}_{i},PROD)"
                schema_metadata_i = models.SchemaMetadataClass(
                    schemaName=f"large_fanout_dataset_{i}",
                    platform="urn:li:dataPlatform:hive",
                    version=0,
                    hash="",
                    platformSchema=models.OtherSchemaClass(rawSchema=""),
                    fields=[
                        models.SchemaFieldClass(
                            fieldPath="ip",
                            type=models.SchemaFieldDataTypeClass(
                                type=models.StringTypeClass()
                            ),
                            nativeDataType="string",
                        )
                    ],
                )
                upstreams = models.UpstreamLineageClass(
                    upstreams=[
                        models.UpstreamClass(
                            dataset=dataset_1,
                            type=models.DatasetLineageTypeClass.COPY,
                        )
                    ],
                    fineGrainedLineages=[
                        models.FineGrainedLineageClass(
                            upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[
                                f"urn:li:schemaField:({dataset_1},ip)",
                            ],
                            downstreams=[
                                f"urn:li:schemaField:({dataset_i},ip)",
                            ],
                        )
                    ],
                )
                for mcp in MetadataChangeProposalWrapper.construct_many(
                    entityUrn=dataset_i,
                    aspects=[
                        schema_metadata_i,
                        upstreams,
                    ],
                ):
                    graph.emit(mcp)
                all_urns.append(dataset_i)

            wait_for_writes_to_sync()
            yield (dataset_1, all_urns)
        finally:
            if DELETE_AFTER_TEST:
                for urn in all_urns:
                    graph.delete_entity(urn, hard=True)

    return _large_fanout_graph


def add_col_col_lineage(
    graph, test_id: str, depth: int, dataset_depth_map: Dict[int, str]
):
    field_path = "ip"

    field_pairs = []
    for current_depth in range(depth):
        upstream_dataset = dataset_depth_map[current_depth]
        downstream_dataset = dataset_depth_map[current_depth + 1]
        downstream_field = f"urn:li:schemaField:({downstream_dataset},{field_path})"
        upstream_field = f"urn:li:schemaField:({upstream_dataset},{field_path})"
        upstreams = graph.get_aspect(downstream_dataset, models.UpstreamLineageClass)
        upstreams.fineGrainedLineages = [
            models.FineGrainedLineageClass(
                upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[upstream_field],
                downstreams=[downstream_field],
            )
        ]
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=downstream_dataset, aspect=upstreams
            )
        )
        field_pairs.append((downstream_field, upstream_field))
    wait_for_writes_to_sync()
    return field_pairs


def add_col_col_cycle_lineage(
    graph, test_id: str, dataset_depth_map: Dict[int, str], cycle: List[int]
):
    field_path = "ip"

    lineage_pairs = [(cycle[i], cycle[i + 1]) for i in range(len(cycle) - 1)]

    field_pairs = []

    for src, dest in lineage_pairs:
        upstream_dataset = dataset_depth_map[src]
        downstream_dataset = dataset_depth_map[dest]
        downstream_field = f"urn:li:schemaField:({downstream_dataset},{field_path})"
        upstream_field = f"urn:li:schemaField:({upstream_dataset},{field_path})"
        upstreams = graph.get_aspect(downstream_dataset, models.UpstreamLineageClass)
        upstreams.fineGrainedLineages = [
            models.FineGrainedLineageClass(
                upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[upstream_field],
                downstreams=[downstream_field],
            )
        ]
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=downstream_dataset, aspect=upstreams
            )
        )
        field_pairs.append((downstream_field, upstream_field))
    wait_for_writes_to_sync()
    return field_pairs


def add_field_description(f1, description, graph):
    urn = Urn.from_string(f1)
    dataset_urn = urn.entity_ids[0]
    schema_metadata = graph.get_aspect(dataset_urn, models.SchemaMetadataClass)
    field = next(f for f in schema_metadata.fields if f.fieldPath == urn.entity_ids[1])
    field.description = description
    graph.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=schema_metadata)
    )
    wait_for_writes_to_sync()


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, max=10),
    stop=tenacity.stop_after_delay(60),
)
def check_propagated_description(downstream_field, description, graph):
    documentation = graph.get_aspect(downstream_field, models.DocumentationClass)
    assert any(doc.documentation == description for doc in documentation.documentations)


def ensure_no_propagated_description(graph, schema_field):
    documentation = graph.get_aspect(schema_field, models.DocumentationClass)
    assert documentation is None or not documentation.documentations


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_col_col_propagation_depth_1(
    ingest_cleanup_data, graph, test_id, dataset_depth_map
):
    downstream_field, upstream_field = add_col_col_lineage(
        graph, depth=1, test_id=test_id, dataset_depth_map=dataset_depth_map
    )[0]
    add_field_description(upstream_field, "This is the new description", graph)
    check_propagated_description(downstream_field, "This is the new description", graph)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_col_col_propagation_depth_6(
    ingest_cleanup_data, graph, test_id, dataset_depth_map
):
    field_pairs = add_col_col_lineage(
        graph, depth=6, test_id=test_id, dataset_depth_map=dataset_depth_map
    )
    upstream_field = field_pairs[0][1]
    add_field_description(
        upstream_field, f"This is the new description {test_id}", graph
    )
    for downstream_field, _ in field_pairs[:-1]:
        check_propagated_description(
            downstream_field, f"This is the new description {test_id}", graph
        )

    # last hop should NOT be propagated
    last_downstream_field = f"urn:li:schemaField:({dataset_depth_map[6]},ip)"
    ensure_no_propagated_description(graph, last_downstream_field)
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    # now check upstream propagation
    add_field_description(
        last_downstream_field, f"This is the new upstream description {test_id}", graph
    )
    propagated_upstream_field = f"urn:li:schemaField:({dataset_depth_map[1]},ip)"
    check_propagated_description(
        propagated_upstream_field,
        f"This is the new upstream description {test_id}",
        graph,
    )
    # propagation depth will prevent the last hop from being propagated
    ensure_no_propagated_description(graph, upstream_field)
    # also check that the previously propagated descriptions (for downstream
    # fields) are still there
    for index in [1, 2, 3, 4]:
        check_propagated_description(
            f"urn:li:schemaField:({dataset_depth_map[index]},ip)",
            description=f"This is the new description {test_id}",
            graph=graph,
        )


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_col_col_propagation_cycles(
    ingest_cleanup_data_function, graph, test_id, dataset_depth_map
):
    custom_template = "datasets_for_cycles_template.yaml"
    with ingest_cleanup_data_function(custom_template) as urns:
        [u for u in urns]
        click_dataset_urn = dataset_depth_map[0]
        hive_dataset_urn = dataset_depth_map[1]
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=click_dataset_urn,
                aspect=models.SiblingsClass(siblings=[hive_dataset_urn], primary=True),
            )
        )
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=hive_dataset_urn,
                aspect=models.SiblingsClass(
                    siblings=[click_dataset_urn], primary=False
                ),
            )
        )
        # create field level lineage
        add_col_col_cycle_lineage(
            graph,
            test_id=test_id,
            dataset_depth_map=dataset_depth_map,
            cycle=[1, 2, 3, 4, 1],
        )
        wait_for_writes_to_sync()
        field = f"urn:li:schemaField:({click_dataset_urn},ip)"
        add_field_description(
            f1=field, description=f"This is the new description {test_id}", graph=graph
        )
        for index in [1, 2, 3, 4]:
            check_propagated_description(
                f"urn:li:schemaField:({dataset_depth_map[index]},ip)",
                description=f"This is the new description {test_id}",
                graph=graph,
            )
        # make sure the original field does not have a propagated description
        ensure_no_propagated_description(
            graph, f"urn:li:schemaField:({click_dataset_urn},ip)"
        )


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_col_col_propagation_large_fanout(
    large_fanout_graph_function, test_id: str, action_env_vars: ActionTestEnv, graph
):
    default_max_fanout = (
        action_env_vars.DATAHUB_ACTIONS_DOC_PROPAGATION_MAX_PROPAGATION_FANOUT
    )
    with large_fanout_graph_function(test_id, default_max_fanout) as (
        dataset_1,
        all_urns,
    ):
        new_description = f"This is the new description + {int(time.time())}"
        # we change the description of the first field
        editable_schema_metadata = models.EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                models.EditableSchemaFieldInfoClass(
                    fieldPath="ip",
                    description=new_description,
                )
            ]
        )
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_1, aspect=editable_schema_metadata
            )
        )
        wait_for_writes_to_sync()
        # now we check that the description has been propagated to all the
        # downstream fields
        num_fields_with_propagated_description = 0
        num_fields_missing_descriptions = 0
        for i in range(1, default_max_fanout + 2):
            downstream_field = f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,large_fanout_dataset_{test_id}_{i},PROD),ip)"

            try:
                check_propagated_description(downstream_field, new_description, graph)
                num_fields_with_propagated_description += 1
            except tenacity.RetryError:
                logger.error(
                    f"Field {downstream_field} does not have the propagated description"
                )
                num_fields_missing_descriptions += 1
        logger.warning(
            f"Number of fields with propagated description: {num_fields_with_propagated_description}"
        )
        logger.warning(
            f"Number of fields missing description: {num_fields_missing_descriptions}"
        )
        assert num_fields_missing_descriptions == 1
        assert num_fields_with_propagated_description == default_max_fanout
        # fanout is 1000, so the last field should not have the propagated description
