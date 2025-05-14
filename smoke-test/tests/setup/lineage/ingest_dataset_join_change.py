from typing import List

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    UpstreamClass,
)
from tests.setup.lineage.constants import (
    DATASET_ENTITY_TYPE,
    SNOWFLAKE_DATA_PLATFORM,
    TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
    TIMESTAMP_MILLIS_ONE_DAY_AGO,
)
from tests.setup.lineage.helper_classes import Dataset, Field
from tests.setup.lineage.utils import (
    create_node,
    create_upstream_edge,
    create_upstream_mcp,
    emit_mcps,
)

# Constants for Case 3
GDP_DATASET_ID = "economic_data.gdp"
GDP_DATASET_URN = make_dataset_urn(
    platform=SNOWFLAKE_DATA_PLATFORM, name=GDP_DATASET_ID
)
GDP_DATASET = Dataset(
    id=GDP_DATASET_ID,
    platform=SNOWFLAKE_DATA_PLATFORM,
    schema_metadata=[
        Field(name="country", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(name="year", type=SchemaFieldDataTypeClass(type=NumberTypeClass())),
        Field(name="gdp_value", type=SchemaFieldDataTypeClass(type=NumberTypeClass())),
        Field(
            name="net_factor_income_value",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
    ],
)

FACTOR_INCOME_DATASET_ID = "economic_data.factor_income"
FACTOR_INCOME_DATASET_URN = make_dataset_urn(
    platform=SNOWFLAKE_DATA_PLATFORM, name=FACTOR_INCOME_DATASET_ID
)
FACTOR_INCOME_DATASET = Dataset(
    id=FACTOR_INCOME_DATASET_ID,
    platform=SNOWFLAKE_DATA_PLATFORM,
    schema_metadata=[
        Field(name="country", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(name="year", type=SchemaFieldDataTypeClass(type=NumberTypeClass())),
        Field(
            name="net_factor_income_value",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
    ],
)

GNP_DATASET_ID = "economic_data.gnp"
GNP_DATASET_URN = make_dataset_urn(
    platform=SNOWFLAKE_DATA_PLATFORM, name=GNP_DATASET_ID
)
GNP_DATASET = Dataset(
    id=GNP_DATASET_ID,
    platform=SNOWFLAKE_DATA_PLATFORM,
    schema_metadata=[
        Field(name="country", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(name="year", type=SchemaFieldDataTypeClass(type=NumberTypeClass())),
        Field(name="gnp_value", type=SchemaFieldDataTypeClass(type=NumberTypeClass())),
    ],
)


def ingest_dataset_join_change(graph_client: DataHubGraph) -> None:
    # Case 3. gnp has two upstreams originally (gdp and factor_income), but later factor_income is removed.
    emit_mcps(graph_client, create_node(GDP_DATASET))
    emit_mcps(graph_client, create_node(FACTOR_INCOME_DATASET))
    emit_mcps(graph_client, create_node(GNP_DATASET))
    d3_d1_edge: UpstreamClass = create_upstream_edge(
        GDP_DATASET_URN,
        TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
        TIMESTAMP_MILLIS_ONE_DAY_AGO,
    )
    d3_d2_edge: UpstreamClass = create_upstream_edge(
        FACTOR_INCOME_DATASET_URN,
        TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
        TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
    )
    case_3_upstreams: List[UpstreamClass] = [
        d3_d1_edge,
        d3_d2_edge,
    ]
    case_3_mcp = create_upstream_mcp(
        DATASET_ENTITY_TYPE,
        GNP_DATASET_URN,
        case_3_upstreams,
        TIMESTAMP_MILLIS_ONE_DAY_AGO,
        run_id="gdp_gnp",
    )
    graph_client.emit_mcp(case_3_mcp)


def get_dataset_join_change_urns() -> List[str]:
    return [
        GNP_DATASET_URN,
        GDP_DATASET_URN,
        FACTOR_INCOME_DATASET_URN,
    ]
