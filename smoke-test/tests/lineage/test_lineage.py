import logging
import time
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import networkx as nx
import pytest
from pydantic import BaseModel, validator

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EdgeClass,
    FineGrainedLineageClass as FineGrainedLineage,
    FineGrainedLineageDownstreamTypeClass as FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamTypeClass as FineGrainedLineageUpstreamType,
    OtherSchemaClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn
from tests.utils import ingest_file_via_rest, wait_for_writes_to_sync

logger = logging.getLogger(__name__)


class DeleteAgent:
    def delete_entity(self, urn: str) -> None:
        pass


class DataHubGraphDeleteAgent(DeleteAgent):
    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def delete_entity(self, urn: str) -> None:
        self.graph.delete_entity(urn, hard=True)


class DataHubConsoleDeleteAgent(DeleteAgent):
    def delete_entity(self, urn: str) -> None:
        print(f"Would delete {urn}")


class DataHubConsoleEmitter:
    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None:
        print(mcp)


INFINITE_HOPS: int = -1


def ingest_tableau_cll_via_rest(auth_session) -> None:
    ingest_file_via_rest(
        auth_session,
        "tests/lineage/tableau_cll_mcps.json",
    )


def search_across_lineage(
    graph: DataHubGraph,
    main_entity: str,
    hops: int = INFINITE_HOPS,
    direction: str = "UPSTREAM",
    convert_schema_fields_to_datasets: bool = True,
):
    def _explain_sal_result(result: dict) -> str:
        explain = ""
        entities = [
            x["entity"]["urn"] for x in result["searchAcrossLineage"]["searchResults"]
        ]
        number_of_results = len(entities)
        explain += f"Number of results: {number_of_results}\n"
        explain += "Entities: "
        try:
            for e in entities:
                explain += f"\t{e.replace('urn:li:', '')}\n"
            for entity in entities:
                paths = [
                    x["paths"][0]["path"]
                    for x in result["searchAcrossLineage"]["searchResults"]
                    if x["entity"]["urn"] == entity
                ]
                explain += f"Paths for entity {entity}: "
                for path in paths:
                    explain += (
                        "\t"
                        + " -> ".join(
                            [
                                x["urn"]
                                .replace("urn:li:schemaField", "field")
                                .replace("urn:li:dataset", "dataset")
                                .replace("urn:li:dataPlatform", "platform")
                                for x in path
                            ]
                        )
                        + "\n"
                    )
        except Exception:
            # breakpoint()
            pass
        return explain

    variable: dict[str, Any] = {
        "input": (
            {
                "urn": main_entity,
                "query": "*",
                "direction": direction,
                "searchFlags": {
                    "groupingSpec": {
                        "groupingCriteria": [
                            {
                                "baseEntityType": "SCHEMA_FIELD",
                                "groupingEntityType": "DATASET",
                            },
                        ]
                    },
                    "skipCache": True,
                },
            }
            if convert_schema_fields_to_datasets
            else {
                "urn": main_entity,
                "query": "*",
                "direction": direction,
                "searchFlags": {
                    "skipCache": True,
                },
            }
        )
    }
    if hops != INFINITE_HOPS:
        variable["input"].update(
            {
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "degree",
                                "condition": "EQUAL",
                                "values": ["{}".format(hops)],
                                "negated": False,
                            }
                        ]
                    }
                ]
            }
        )
    result = graph.execute_graphql(
        """
        query($input: SearchAcrossLineageInput!) {
            searchAcrossLineage(input: $input)
            {
                searchResults {
                entity {
                    urn
                }
                paths {
                    path {
                    urn
                    }
                }
                }
            }
        }
        """,
        variables=variable,
    )
    print(f"Query -> Entity {main_entity} with hops {hops} and direction {direction}")
    print(result)
    print(_explain_sal_result(result))
    return result


class Direction(Enum):
    UPSTREAM = "UPSTREAM"
    DOWNSTREAM = "DOWNSTREAM"

    def opposite(self):
        if self == Direction.UPSTREAM:
            return Direction.DOWNSTREAM
        else:
            return Direction.UPSTREAM


class Path(BaseModel):
    path: List[str]

    def add_node(self, node: str) -> None:
        self.path.append(node)

    def __hash__(self) -> int:
        return ".".join(self.path).__hash__()


class LineageExpectation(BaseModel):
    direction: Direction
    main_entity: str
    hops: int
    impacted_entities: Dict[str, List[Path]]


class ImpactQuery(BaseModel):
    main_entity: str
    hops: int
    direction: Direction
    upconvert_schema_fields_to_datasets: bool

    def __hash__(self) -> int:
        raw_string = (
            f"{self.main_entity}{self.hops}{self.direction}"
            + f"{self.upconvert_schema_fields_to_datasets}"
        )
        return raw_string.__hash__()


class ScenarioExpectation:
    """
    This class stores the expectations for the lineage of a scenario. It is used
    to store the pre-materialized expectations for all datasets and schema
    fields across all hops and directions possible. This makes it easy to check
    that the results of a lineage query match the expectations.
    """

    def __init__(self):
        self._graph = nx.DiGraph()

    def __simplify(self, urn_or_list: Union[str, List[str]]) -> str:
        if isinstance(urn_or_list, list):
            return ",".join([self.__simplify(x) for x in urn_or_list])
        else:
            return (
                urn_or_list.replace("urn:li:schemaField", "F")
                .replace("urn:li:dataset", "D")
                .replace("urn:li:dataPlatform", "P")
                .replace("urn:li:query", "Q")
            )

    def extend_impacted_entities(
        self,
        direction: Direction,
        parent_entity: str,
        child_entity: str,
        path_extension: Optional[List[str]] = None,
    ) -> None:
        via_node = path_extension[0] if path_extension else None
        if via_node:
            self._graph.add_edge(parent_entity, child_entity, via=via_node)
        else:
            self._graph.add_edge(parent_entity, child_entity)

    def generate_query_expectation_pairs(
        self, max_hops: int
    ) -> Iterable[Tuple[ImpactQuery, LineageExpectation]]:
        upconvert_options = [
            True
        ]  # TODO: Add False once search-across-lineage supports returning schema fields
        for main_entity in self._graph.nodes():
            for direction in [Direction.UPSTREAM, Direction.DOWNSTREAM]:
                for upconvert_schema_fields_to_datasets in upconvert_options:
                    possible_hops = [h for h in range(1, max_hops)] + [INFINITE_HOPS]
                    for hops in possible_hops:
                        query = ImpactQuery(
                            main_entity=main_entity,
                            hops=hops,
                            direction=direction,
                            upconvert_schema_fields_to_datasets=upconvert_schema_fields_to_datasets,
                        )
                        yield query, self.get_expectation_for_query(query)

    def get_expectation_for_query(self, query: ImpactQuery) -> LineageExpectation:
        graph_to_walk = (
            self._graph
            if query.direction == Direction.DOWNSTREAM
            else self._graph.reverse()
        )
        entity_paths = nx.shortest_path(graph_to_walk, source=query.main_entity)
        lineage_expectation = LineageExpectation(
            direction=query.direction,
            main_entity=query.main_entity,
            hops=query.hops,
            impacted_entities={},
        )
        for entity, paths in entity_paths.items():
            if entity == query.main_entity:
                continue
            if query.hops != INFINITE_HOPS and len(paths) != (
                query.hops + 1
            ):  # +1 because the path includes the main entity
                print(
                    f"Skipping {entity} because it is less than or more than {query.hops} hops away"
                )
                continue
            path_graph = nx.path_graph(paths)
            expanded_path: List[str] = []
            via_entity = None
            for ea in path_graph.edges():
                expanded_path.append(ea[0])
                if "via" in graph_to_walk.edges[ea[0], ea[1]]:
                    via_entity = graph_to_walk.edges[ea[0], ea[1]]["via"]
                    expanded_path.append(via_entity)
            if via_entity and not via_entity.startswith(
                "urn:li:query"
            ):  # Transient nodes like queries are not included as impacted entities
                if via_entity not in lineage_expectation.impacted_entities:
                    lineage_expectation.impacted_entities[via_entity] = []
                via_path = Path(path=[x for x in expanded_path])
                if via_path not in lineage_expectation.impacted_entities[via_entity]:
                    lineage_expectation.impacted_entities[via_entity].append(
                        Path(path=[x for x in expanded_path])
                    )

            expanded_path.append(paths[-1])
            if entity not in lineage_expectation.impacted_entities:
                lineage_expectation.impacted_entities[entity] = []
            lineage_expectation.impacted_entities[entity].append(
                Path(path=expanded_path)
            )

        if query.upconvert_schema_fields_to_datasets:
            entries_to_add: Dict[str, List[Path]] = {}
            entries_to_remove = []
            for impacted_entity in lineage_expectation.impacted_entities:
                if impacted_entity.startswith("urn:li:schemaField"):
                    impacted_dataset_entity = Urn.create_from_string(
                        impacted_entity
                    ).entity_ids[0]
                    if impacted_dataset_entity in entries_to_add:
                        entries_to_add[impacted_dataset_entity].extend(
                            lineage_expectation.impacted_entities[impacted_entity]
                        )
                    else:
                        entries_to_add[impacted_dataset_entity] = (
                            lineage_expectation.impacted_entities[impacted_entity]
                        )
                    entries_to_remove.append(impacted_entity)
            for impacted_entity in entries_to_remove:
                del lineage_expectation.impacted_entities[impacted_entity]
            lineage_expectation.impacted_entities.update(entries_to_add)
        return lineage_expectation


class Scenario(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    class LineageStyle(Enum):
        DATASET_QUERY_DATASET = "DATASET_QUERY_DATASET"
        DATASET_JOB_DATASET = "DATASET_JOB_DATASET"

    lineage_style: LineageStyle
    default_platform: str = "mysql"
    default_transformation_platform: str = "airflow"
    hop_platform_map: Dict[int, str] = {}
    hop_transformation_map: Dict[int, str] = {}
    num_hops: int = 1
    default_datasets_at_each_hop: int = 2
    default_dataset_fanin: int = 2  # Number of datasets that feed into a transformation
    default_column_fanin: int = 2  # Number of columns that feed into a transformation
    default_dataset_fanout: int = (
        1  # Number of datasets that a transformation feeds into
    )
    default_column_fanout: int = 1  # Number of columns that a transformation feeds into
    # num_upstream_datasets: int = 2
    # num_downstream_datasets: int = 1
    default_dataset_prefix: str = "librarydb."
    hop_dataset_prefix_map: Dict[int, str] = {}
    query_id: str = "guid-guid-guid"
    query_string: str = "SELECT * FROM foo"
    transformation_job: str = "job1"
    transformation_flow: str = "flow1"
    _generated_urns: Set[str] = set()
    expectations: Optional[ScenarioExpectation] = None

    @validator("expectations", pre=True, always=True)
    def expectations_validator(
        cls, v: Optional[ScenarioExpectation]
    ) -> ScenarioExpectation:
        if v is None:
            return ScenarioExpectation()
        else:
            return v

    def get_column_name(self, column_index: int) -> str:
        return f"column_{column_index}"

    def set_upstream_dataset_prefix(self, dataset):
        self.upstream_dataset_prefix = dataset

    def set_downstream_dataset_prefix(self, dataset):
        self.downstream_dataset_prefix = dataset

    def set_transformation_query(self, query: str) -> None:
        self.transformation_query = query

    def set_transformation_job(self, job: str) -> None:
        self.transformation_job = job

    def set_transformation_flow(self, flow: str) -> None:
        self.transformation_flow = flow

    def get_transformation_job_urn(self, hop_index: int) -> str:
        return builder.make_data_job_urn(
            orchestrator=self.default_transformation_platform,
            flow_id=f"layer_{hop_index}_{self.transformation_flow}",
            job_id=self.transformation_job,
            cluster="PROD",
        )

    def get_transformation_query_urn(self, hop_index: int = 0) -> str:
        return f"urn:li:query:{self.query_id}_{hop_index}"  # TODO - add hop index to query id

    def get_transformation_flow_urn(self, hop_index: int) -> str:
        return builder.make_data_flow_urn(
            orchestrator=self.default_transformation_platform,
            flow_id=f"layer_{hop_index}_{self.transformation_flow}",
            cluster="PROD",
        )

    def get_upstream_dataset_urns(self, hop_index: int) -> List[str]:
        return [
            self.get_dataset_urn(hop_index=hop_index, index=i)
            for i in range(self.default_dataset_fanin)
        ]

    def get_dataset_urn(self, hop_index: int, index: int) -> str:
        platform = self.hop_platform_map.get(hop_index, self.default_platform)
        prefix = self.hop_dataset_prefix_map.get(
            index, f"{self.default_dataset_prefix}layer_{hop_index}."
        )
        return builder.make_dataset_urn(platform, f"{prefix}{index}")

    def get_column_urn(
        self, hop_index: int, dataset_index: int, column_index: int = 0
    ) -> str:
        return builder.make_schema_field_urn(
            self.get_dataset_urn(hop_index, dataset_index),
            self.get_column_name(column_index),
        )

    def get_upstream_column_urn(
        self, hop_index: int, dataset_index: int, column_index: int = 0
    ) -> str:
        return builder.make_schema_field_urn(
            self.get_dataset_urn(hop_index, dataset_index),
            self.get_column_name(column_index),
        )

    def get_downstream_column_urn(
        self, hop_index: int, dataset_index: int, column_index: int = 0
    ) -> str:
        return builder.make_schema_field_urn(
            self.get_dataset_urn(hop_index + 1, dataset_index),
            self.get_column_name(column_index),
        )

    def get_downstream_dataset_urns(self, hop_index: int) -> List[str]:
        return [
            self.get_dataset_urn(hop_index + 1, i)
            for i in range(self.default_dataset_fanout)
        ]

    def get_lineage_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        for hop_index in range(0, self.num_hops):
            yield from self.get_lineage_mcps_for_hop(hop_index)

    def get_lineage_mcps_for_hop(
        self, hop_index: int
    ) -> Iterable[MetadataChangeProposalWrapper]:
        assert self.expectations is not None
        if self.lineage_style == Scenario.LineageStyle.DATASET_JOB_DATASET:
            fine_grained_lineage = FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    self.get_upstream_column_urn(hop_index, dataset_index, 0)
                    for dataset_index in range(self.default_dataset_fanin)
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[
                    self.get_downstream_column_urn(hop_index, dataset_index, 0)
                    for dataset_index in range(self.default_dataset_fanout)
                ],
            )
            datajob_io = DataJobInputOutputClass(
                inputDatasets=self.get_upstream_dataset_urns(hop_index),
                outputDatasets=self.get_downstream_dataset_urns(hop_index),
                inputDatajobs=[],  # not supporting job -> job lineage for now
                fineGrainedLineages=[fine_grained_lineage],
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=self.get_transformation_job_urn(hop_index),
                aspect=datajob_io,
            )

            # Add field level expectations
            for upstream_field_urn in fine_grained_lineage.upstreams or []:
                for downstream_field_urn in fine_grained_lineage.downstreams or []:
                    self.expectations.extend_impacted_entities(
                        Direction.DOWNSTREAM,
                        upstream_field_urn,
                        downstream_field_urn,
                        path_extension=[
                            self.get_transformation_job_urn(hop_index),
                            downstream_field_urn,
                        ],
                    )

            # Add table level expectations
            for upstream_dataset_urn in datajob_io.inputDatasets:
                # No path extension, because we don't use via nodes for dataset -> dataset edges
                self.expectations.extend_impacted_entities(
                    Direction.DOWNSTREAM,
                    upstream_dataset_urn,
                    self.get_transformation_job_urn(hop_index),
                )
            for downstream_dataset_urn in datajob_io.outputDatasets:
                self.expectations.extend_impacted_entities(
                    Direction.DOWNSTREAM,
                    self.get_transformation_job_urn(hop_index),
                    downstream_dataset_urn,
                )

        if self.lineage_style == Scenario.LineageStyle.DATASET_QUERY_DATASET:
            # we emit upstream lineage from the downstream dataset
            for downstream_dataset_index in range(self.default_dataset_fanout):
                mcp_entity_urn = self.get_dataset_urn(
                    hop_index + 1, downstream_dataset_index
                )
                fine_grained_lineages = [
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=[
                            self.get_upstream_column_urn(
                                hop_index, d_i, upstream_col_index
                            )
                            for d_i in range(self.default_dataset_fanin)
                        ],
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[
                            self.get_downstream_column_urn(
                                hop_index,
                                downstream_dataset_index,
                                downstream_col_index,
                            )
                            for downstream_col_index in range(
                                self.default_column_fanout
                            )
                        ],
                        query=self.get_transformation_query_urn(hop_index),
                    )
                    for upstream_col_index in range(self.default_column_fanin)
                ]
                upstream_lineage = UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=self.get_dataset_urn(hop_index, i),
                            type=DatasetLineageTypeClass.TRANSFORMED,
                            query=self.get_transformation_query_urn(hop_index),
                        )
                        for i in range(self.default_dataset_fanin)
                    ],
                    fineGrainedLineages=fine_grained_lineages,
                )
                for fine_grained_lineage in fine_grained_lineages:
                    # Add field level expectations
                    for upstream_field_urn in fine_grained_lineage.upstreams or []:
                        for downstream_field_urn in (
                            fine_grained_lineage.downstreams or []
                        ):
                            self.expectations.extend_impacted_entities(
                                Direction.DOWNSTREAM,
                                upstream_field_urn,
                                downstream_field_urn,
                                path_extension=[
                                    self.get_transformation_query_urn(hop_index),
                                    downstream_field_urn,
                                ],
                            )

                # Add table level expectations
                for upstream_dataset in upstream_lineage.upstreams:
                    self.expectations.extend_impacted_entities(
                        Direction.DOWNSTREAM,
                        upstream_dataset.dataset,
                        mcp_entity_urn,
                        path_extension=[
                            self.get_transformation_query_urn(hop_index),
                            mcp_entity_urn,
                        ],
                    )

                yield MetadataChangeProposalWrapper(
                    entityUrn=mcp_entity_urn,
                    aspect=upstream_lineage,
                )

    def get_entity_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        for hop_index in range(
            0, self.num_hops + 1
        ):  # we generate entities with last hop inclusive
            for mcp in self.get_entity_mcps_for_hop(hop_index):
                assert mcp.entityUrn
                self._generated_urns.add(mcp.entityUrn)
                yield mcp

    def get_entity_mcps_for_hop(
        self, hop_index: int
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if self.lineage_style == Scenario.LineageStyle.DATASET_JOB_DATASET:
            # Construct the DataJobInfo aspect with the job -> flow lineage.
            dataflow_urn = self.get_transformation_flow_urn(hop_index)

            dataflow_info = DataFlowInfoClass(
                name=self.transformation_flow.title() + " Flow"
            )

            dataflow_info_mcp = MetadataChangeProposalWrapper(
                entityUrn=dataflow_urn,
                aspect=dataflow_info,
            )
            yield dataflow_info_mcp

            datajob_info = DataJobInfoClass(
                name=self.transformation_job.title() + " Job",
                type="AIRFLOW",
                flowUrn=dataflow_urn,
            )

            # Construct a MetadataChangeProposalWrapper object with the DataJobInfo aspect.
            # NOTE: This will overwrite all of the existing dataJobInfo aspect information associated with this job.
            datajob_info_mcp = MetadataChangeProposalWrapper(
                entityUrn=self.get_transformation_job_urn(hop_index),
                aspect=datajob_info,
            )
            yield datajob_info_mcp

        if self.lineage_style == Scenario.LineageStyle.DATASET_QUERY_DATASET:
            query_urn = self.get_transformation_query_urn(hop_index=hop_index)

            fake_auditstamp = AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub",
            )

            query_properties = QueryPropertiesClass(
                statement=QueryStatementClass(
                    value=self.query_string,
                    language=QueryLanguageClass.SQL,
                ),
                source=QuerySourceClass.SYSTEM,
                created=fake_auditstamp,
                lastModified=fake_auditstamp,
            )

            query_info_mcp = MetadataChangeProposalWrapper(
                entityUrn=query_urn,
                aspect=query_properties,
            )
            yield query_info_mcp
        # Generate schema and properties mcps for all datasets
        for dataset_index in range(self.default_datasets_at_each_hop):
            dataset_urn = DatasetUrn.from_string(
                self.get_dataset_urn(hop_index, dataset_index)
            )
            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=str(dataset_urn),
                aspects=[
                    SchemaMetadataClass(
                        schemaName=str(dataset_urn),
                        platform=builder.make_data_platform_urn(dataset_urn.platform),
                        version=0,
                        hash="",
                        platformSchema=OtherSchemaClass(rawSchema=""),
                        fields=[
                            SchemaFieldClass(
                                fieldPath=self.get_column_name(i),
                                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                                nativeDataType="string",
                            )
                            for i in range(self.default_column_fanin)
                        ],
                    ),
                    DatasetPropertiesClass(
                        name=dataset_urn.name,
                    ),
                ],
            )

    def cleanup(self, delete_agent: DeleteAgent) -> None:
        """Delete all entities created by this scenario."""
        for urn in self._generated_urns:
            delete_agent.delete_entity(urn)

    def test_expectation(self, graph: DataHubGraph) -> bool:
        print("Testing expectation...")
        assert self.expectations is not None
        try:
            for hop_index in range(self.num_hops):
                for dataset_urn in self.get_upstream_dataset_urns(hop_index):
                    assert graph.exists(dataset_urn) is True
                for dataset_urn in self.get_downstream_dataset_urns(hop_index):
                    assert graph.exists(dataset_urn) is True

            if self.lineage_style == Scenario.LineageStyle.DATASET_JOB_DATASET:
                assert graph.exists(self.get_transformation_job_urn(hop_index)) is True
                assert graph.exists(self.get_transformation_flow_urn(hop_index)) is True

            if self.lineage_style == Scenario.LineageStyle.DATASET_QUERY_DATASET:
                assert (
                    graph.exists(self.get_transformation_query_urn(hop_index)) is True
                )

            wait_for_writes_to_sync()  # Wait for the graph to update
            # We would like to check that lineage is correct for all datasets and schema fields for all values of hops and for all directions of lineage exploration
            # Since we already have expectations stored for all datasets and schema_fields, we can just check that the results match the expectations

            for (
                query,
                expectation,
            ) in self.expectations.generate_query_expectation_pairs(self.num_hops):
                impacted_entities_expectation = set(
                    [x for x in expectation.impacted_entities.keys()]
                )
                if len(impacted_entities_expectation) == 0:
                    continue
                result = search_across_lineage(
                    graph,
                    query.main_entity,
                    query.hops,
                    query.direction.value,
                    query.upconvert_schema_fields_to_datasets,
                )
                impacted_entities = set(
                    [
                        x["entity"]["urn"]
                        for x in result["searchAcrossLineage"]["searchResults"]
                    ]
                )
                try:
                    assert impacted_entities == impacted_entities_expectation, (
                        f"Expected impacted entities to be {impacted_entities_expectation}, found {impacted_entities}"
                    )
                except Exception:
                    # breakpoint()
                    raise
                search_results = result["searchAcrossLineage"]["searchResults"]
                for impacted_entity in impacted_entities:
                    # breakpoint()
                    impacted_entity_paths: List[Path] = []
                    # breakpoint()
                    entity_paths_response = [
                        x["paths"]
                        for x in search_results
                        if x["entity"]["urn"] == impacted_entity
                    ]
                    for path_response in entity_paths_response:
                        for p in path_response:
                            q = p["path"]
                            impacted_entity_paths.append(
                                Path(path=[x["urn"] for x in q])
                            )
                    # if len(impacted_entity_paths) > 1:
                    #     breakpoint()
                    try:
                        assert len(impacted_entity_paths) == len(
                            expectation.impacted_entities[impacted_entity]
                        ), (
                            f"Expected length of impacted entity paths to be {len(expectation.impacted_entities[impacted_entity])}, found {len(impacted_entity_paths)}"
                        )
                        assert set(impacted_entity_paths) == set(
                            expectation.impacted_entities[impacted_entity]
                        ), (
                            f"Expected impacted entity paths to be {expectation.impacted_entities[impacted_entity]}, found {impacted_entity_paths}"
                        )
                    except Exception:
                        # breakpoint()
                        raise
                    # for i in range(len(impacted_entity_paths)):
                    #     assert impacted_entity_paths[i].path == expectation.impacted_entities[impacted_entity][i].path, f"Expected impacted entity paths to be {expectation.impacted_entities[impacted_entity][i].path}, found {impacted_entity_paths[i].path}"
            print("Test passed!")
            return True
        except AssertionError as e:
            print("Test failed!")
            raise e
            return False


# @tenacity.retry(
#     stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
# )
@pytest.mark.parametrize(
    "lineage_style",
    [
        Scenario.LineageStyle.DATASET_QUERY_DATASET,
        Scenario.LineageStyle.DATASET_JOB_DATASET,
    ],
)
@pytest.mark.parametrize(
    "graph_level",
    [
        1,
        2,
        3,
        # TODO - convert this to range of 1 to 10 to make sure we can handle large graphs
    ],
)
def test_lineage_via_node(
    graph_client: DataHubGraph, lineage_style: Scenario.LineageStyle, graph_level: int
) -> None:
    scenario: Scenario = Scenario(
        hop_platform_map={0: "mysql", 1: "snowflake"},
        lineage_style=lineage_style,
        num_hops=graph_level,
        default_dataset_prefix=f"{lineage_style.value}.",
    )

    # Create an emitter to the GMS REST API.
    emitter = graph_client
    # emitter = DataHubConsoleEmitter()

    # Emit metadata!
    for mcp in scenario.get_entity_mcps():
        emitter.emit_mcp(mcp)

    for mcps in scenario.get_lineage_mcps():
        emitter.emit_mcp(mcps)

    wait_for_writes_to_sync()
    try:
        scenario.test_expectation(graph_client)
    finally:
        scenario.cleanup(DataHubGraphDeleteAgent(graph_client))


@pytest.fixture(scope="module")
def chart_urn_fixture():
    return "urn:li:chart:(tableau,2241f3d6-df8d-b515-9c0c-f5e5b347b26e)"


@pytest.fixture(scope="module")
def intermediates_fixture():
    return [
        "urn:li:dataset:(urn:li:dataPlatform:tableau,6bd53e72-9fe4-ea86-3d23-14b826c13fa5,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,1c5653d6-c448-0850-108b-5c78aeaf6b51,PROD)",
    ]


@pytest.fixture(scope="module")
def destination_urn_fixture():
    return "urn:li:dataset:(urn:li:dataPlatform:external,sales target %28us%29.xlsx.sheet1,PROD)"


@pytest.fixture(scope="module", autouse=False)
def ingest_multipath_metadata(
    graph_client: DataHubGraph,
    chart_urn_fixture,
    intermediates_fixture,
    destination_urn_fixture,
):
    fake_auditstamp = AuditStampClass(
        time=int(time.time() * 1000),
        actor="urn:li:corpuser:datahub",
    )

    chart_urn = chart_urn_fixture
    intermediates = intermediates_fixture
    destination_urn = destination_urn_fixture
    for mcp in MetadataChangeProposalWrapper.construct_many(
        entityUrn=destination_urn,
        aspects=[
            DatasetPropertiesClass(
                name="sales target (us).xlsx.sheet1",
            ),
        ],
    ):
        graph_client.emit_mcp(mcp)

    for intermediate in intermediates:
        for mcp in MetadataChangeProposalWrapper.construct_many(
            entityUrn=intermediate,
            aspects=[
                DatasetPropertiesClass(
                    name="intermediate",
                ),
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=destination_urn,
                            type="TRANSFORMED",
                        )
                    ]
                ),
            ],
        ):
            graph_client.emit_mcp(mcp)

    for mcp in MetadataChangeProposalWrapper.construct_many(
        entityUrn=chart_urn,
        aspects=[
            ChartInfoClass(
                title="chart",
                description="chart",
                lastModified=ChangeAuditStampsClass(created=fake_auditstamp),
                inputEdges=[
                    EdgeClass(
                        destinationUrn=intermediate_entity,
                        sourceUrn=chart_urn,
                    )
                    for intermediate_entity in intermediates
                ],
            )
        ],
    ):
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()
    yield
    for urn in [chart_urn] + intermediates + [destination_urn]:
        graph_client.delete_entity(urn, hard=True)
    wait_for_writes_to_sync()


# TODO: Reenable once fixed
# def test_simple_lineage_multiple_paths(
#     graph_client: DataHubGraph,
#     ingest_multipath_metadata,
#     chart_urn_fixture,
#     intermediates_fixture,
#     destination_urn_fixture,
# ):
#     chart_urn = chart_urn_fixture
#     intermediates = intermediates_fixture
#     destination_urn = destination_urn_fixture
#     results = search_across_lineage(
#         graph_client,
#         chart_urn,
#         direction="UPSTREAM",
#         convert_schema_fields_to_datasets=True,
#     )
#     assert destination_urn in [
#         x["entity"]["urn"] for x in results["searchAcrossLineage"]["searchResults"]
#     ]
#     for search_result in results["searchAcrossLineage"]["searchResults"]:
#         if search_result["entity"]["urn"] == destination_urn:
#             assert (
#                 len(search_result["paths"]) == 2
#             )  # 2 paths from the chart to the dataset
#             for path in search_result["paths"]:
#                 assert len(path["path"]) == 3
#                 assert path["path"][-1]["urn"] == destination_urn
#                 assert path["path"][0]["urn"] == chart_urn
#                 assert path["path"][1]["urn"] in intermediates
