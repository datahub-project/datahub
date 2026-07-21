"""Register GraphQL and gRPC services for the Service Catalog demo (scene 6).

Proves the Service/api/serviceContract model is protocol-agnostic: the same
typed shape covers REST (register_rest_services.py), **GraphQL** (SDL contract,
GRAPHQL_OPERATION operations), and **gRPC** (protobuf contract, GRPC_METHOD
operations). Each service carries its whole contract as a ``serviceContract``
(GRAPHQL_SDL / GRPC_PROTO) and composes its operations as ``api`` entities with
typed input/output.

    export DATAHUB_GMS_URL=http://localhost:8080
    python register_graphql_grpc_services.py
"""

from __future__ import annotations

import logging
from typing import List

from datahub.api.entities.agent.api import (
    API_SUBTYPE_GRAPHQL_OPERATION,
    API_SUBTYPE_GRPC_METHOD,
    Api,
    ApiParam,
)
from datahub.api.entities.common.large_string import make_large_string
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    ServiceDefinitionClass,
    ServiceDefinitionFormatClass,
    ServiceLifecycleClass,
    ServicePropertiesClass,
    StatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------- #
# GraphQL service — graph-api
# --------------------------------------------------------------------------- #
GRAPH_API_ID = "graph-api"
GRAPH_API_SDL = """\
\"\"\"The commerce GraphQL API.\"\"\"
type Query {
  \"\"\"Look up orders for a customer.\"\"\"
  orders(customerId: ID!, first: Int = 20): [Order!]!
}

type Mutation {
  \"\"\"Place a new trade order.\"\"\"
  placeOrder(input: PlaceOrderInput!): Order!
}

input PlaceOrderInput {
  symbol: String!
  quantity: Float!
  side: Side!
  currencyPair: String
}

type Order {
  orderId: ID!
  status: OrderStatus!
  notionalAmount: Float!
  currencyPair: String!
}

enum Side { BUY SELL }
enum OrderStatus { NEW FILLED CANCELLED }
"""

GRAPH_API_OPS: List[Api] = [
    Api(
        id=f"{GRAPH_API_ID}.query.orders",
        name="Query.orders",
        subtypes=[API_SUBTYPE_GRAPHQL_OPERATION],
        description="Look up orders for a customer.",
        platform="graphql",
        parameters=[
            ApiParam(name="customerId", data_type="ID", required=True),
            ApiParam(name="first", data_type="Int", required=False),
        ],
        returns=[ApiParam(name="orders", data_type="array<Order>", required=True)],
    ),
    Api(
        id=f"{GRAPH_API_ID}.mutation.placeOrder",
        name="Mutation.placeOrder",
        subtypes=[API_SUBTYPE_GRAPHQL_OPERATION],
        description="Place a new trade order.",
        platform="graphql",
        parameters=[
            ApiParam(name="symbol", data_type="String", required=True),
            ApiParam(name="quantity", data_type="Float", required=True),
            ApiParam(name="side", data_type="Side", required=True),
            ApiParam(name="currencyPair", data_type="String", required=False),
        ],
        returns=[
            ApiParam(name="orderId", data_type="ID", required=True),
            ApiParam(name="status", data_type="OrderStatus", required=True),
            ApiParam(name="notionalAmount", data_type="Float", required=True),
        ],
    ),
]

# --------------------------------------------------------------------------- #
# gRPC service — pricing-grpc
# --------------------------------------------------------------------------- #
PRICING_GRPC_ID = "pricing-grpc"
PRICING_PROTO = """\
syntax = "proto3";

package pricing.v1;

// Real-time instrument pricing.
service PricingService {
  // Get the current price for an instrument.
  rpc GetPrice(GetPriceRequest) returns (GetPriceResponse);
}

message GetPriceRequest {
  string symbol = 1;
  string currency_pair = 2;
}

message GetPriceResponse {
  string symbol = 1;
  double bid = 2;
  double ask = 3;
  int64 as_of_epoch_millis = 4;
}
"""

PRICING_GRPC_OPS: List[Api] = [
    Api(
        id=f"{PRICING_GRPC_ID}.PricingService.GetPrice",
        name="PricingService.GetPrice",
        subtypes=[API_SUBTYPE_GRPC_METHOD],
        description="Get the current price for an instrument.",
        platform="grpc",
        parameters=[
            ApiParam(name="symbol", data_type="string", required=True),
            ApiParam(name="currency_pair", data_type="string", required=False),
        ],
        returns=[
            ApiParam(name="symbol", data_type="string", required=True),
            ApiParam(name="bid", data_type="double", required=True),
            ApiParam(name="ask", data_type="double", required=True),
            ApiParam(name="as_of_epoch_millis", data_type="int64", required=True),
        ],
    ),
]


def _register(
    service_id: str,
    display_name: str,
    description: str,
    subtype: str,
    platform: str,
    contract_format: str,
    contract_text: str,
    ops: List[Api],
) -> None:
    graph = get_default_graph()
    api_urns = [op.emit(graph) for op in ops]
    urn = f"urn:li:service:{service_id}"
    aspects = [
        ServicePropertiesClass(
            displayName=display_name,
            description=description,
            apis=api_urns or None,
            lifecycle=ServiceLifecycleClass.PRODUCTION,
        ),
        ServiceDefinitionClass(
            format=contract_format,
            rawSpec=make_large_string(contract_text),
        ),
        SubTypesClass(typeNames=[subtype]),
        DataPlatformInstanceClass(platform=make_data_platform_urn(platform)),
        StatusClass(removed=False),
    ]
    for aspect in aspects:
        graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))
    logger.info("Registered Service %s composing %d API(s)", urn, len(api_urns))


def main() -> None:
    _register(
        GRAPH_API_ID,
        "Commerce GraphQL API",
        "GraphQL API for orders and pricing.",
        "GRAPHQL",
        "graphql",
        ServiceDefinitionFormatClass.GRAPHQL_SDL,
        GRAPH_API_SDL,
        GRAPH_API_OPS,
    )
    _register(
        PRICING_GRPC_ID,
        "Pricing gRPC Service",
        "gRPC service for real-time instrument pricing.",
        "GRPC",
        "grpc",
        ServiceDefinitionFormatClass.GRPC_PROTO,
        PRICING_PROTO,
        PRICING_GRPC_OPS,
    )


if __name__ == "__main__":
    main()
