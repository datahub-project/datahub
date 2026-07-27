"""Register a REST API into DataHub as a ``Service`` with an OpenAPI contract.

This is the canonical ``serviceContract`` demo: a ``Service`` (subtype
``REST_API``) carries the **whole OpenAPI document** in a ``serviceContract``
aspect (``format = OPENAPI``), and the individual operations are emitted as
first-class ``api`` entities (subtype ``REST_ENDPOINT``) linked back via
``ServiceProperties.apis`` (the ``ServiceComposesApi`` relationship).

The operation-level ``api`` entities are the *parsed projection* of the OpenAPI
doc; the ``serviceContract`` blob is the *source of truth* (round-trips
``components``/``servers`` and anything the per-operation signatures drop).

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080   # GMS, not the frontend
    python register_rest_services.py

    # View at (frontend):  http://localhost:9002/service/urn:li:service:order-entry-api
    # The Contract tab renders the OpenAPI YAML below.
"""

from __future__ import annotations

import logging
from typing import List

from datahub.api.entities.agent.api import API_SUBTYPE_REST_ENDPOINT, Api, ApiParam
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

SERVICE_ID = "order-entry-api"
PLATFORM = "openapi"
REST_API_SUBTYPE = "REST_API"
DOCS_URL = "https://api.example.com/docs"

# The whole OpenAPI document — the source of truth stored on serviceContract.
ORDER_ENTRY_OPENAPI = """\
openapi: 3.1.0
info:
  title: Order Entry API
  version: 1.4.0
  description: Place, look up, and cancel trade orders.
servers:
  - url: https://api.example.com/v1
paths:
  /orders:
    post:
      operationId: placeOrder
      summary: Place a new trade order
      requestBody:
        required: true
        content:
          application/json:
            schema: { $ref: "#/components/schemas/PlaceOrderRequest" }
      responses:
        "201":
          description: The created order
          content:
            application/json:
              schema: { $ref: "#/components/schemas/Order" }
  /orders/{orderId}:
    get:
      operationId: getOrderById
      summary: Fetch an order by ID
      parameters:
        - name: orderId
          in: path
          required: true
          schema: { type: string }
        - name: includeLineItems
          in: query
          required: false
          schema: { type: boolean }
      responses:
        "200":
          description: The order
          content:
            application/json:
              schema: { $ref: "#/components/schemas/Order" }
    delete:
      operationId: deleteOrder
      summary: Cancel an order
      parameters:
        - name: orderId
          in: path
          required: true
          schema: { type: string }
      responses:
        "204":
          description: Cancelled
components:
  schemas:
    PlaceOrderRequest:
      type: object
      properties:
        symbol: { type: string }
        quantity: { type: number }
        side: { type: string, enum: [BUY, SELL] }
        currencyPair: { type: string }
    Order:
      type: object
      properties:
        orderId: { type: string }
        status: { type: string, enum: [NEW, FILLED, CANCELLED] }
        notionalAmount: { type: number }
        currencyPair: { type: string }
        lineItems:
          type: array
          items: { $ref: "#/components/schemas/LineItem" }
    LineItem:
      type: object
      properties:
        symbol: { type: string }
        quantity: { type: number }
        side: { type: string, enum: [BUY, SELL] }
"""

# The operations, as first-class api(REST_ENDPOINT) entities. Ids are minted via
# Api.rest_id so they resolve identically on every run and match the OpenAPI
# importer's ids for the same endpoints.
ORDER_ENTRY_APIS: List[Api] = [
    Api(
        id=Api.rest_id(SERVICE_ID, "POST", "/orders"),
        name="POST /orders",
        subtypes=[API_SUBTYPE_REST_ENDPOINT],
        method="POST",
        path="/orders",
        description="Place a new trade order.",
        platform=PLATFORM,
        external_url=f"{DOCS_URL}#operation/placeOrder",
        parameters=[
            ApiParam(
                name="symbol",
                data_type="string",
                required=True,
                description="Instrument symbol.",
            ),
            ApiParam(name="quantity", data_type="number", required=True),
            ApiParam(
                name="side",
                data_type="string",
                required=True,
                description="BUY or SELL.",
            ),
            ApiParam(name="currencyPair", data_type="string", required=False),
        ],
        returns=[
            ApiParam(name="orderId", data_type="string", required=True),
            ApiParam(
                name="status",
                data_type="string",
                required=True,
                description="NEW, FILLED, or CANCELLED.",
            ),
            ApiParam(name="notionalAmount", data_type="number", required=True),
            ApiParam(name="currencyPair", data_type="string", required=True),
        ],
    ),
    Api(
        id=Api.rest_id(SERVICE_ID, "GET", "/orders/{orderId}"),
        name="GET /orders/{orderId}",
        subtypes=[API_SUBTYPE_REST_ENDPOINT],
        method="GET",
        path="/orders/{orderId}",
        description="Fetch an order by ID.",
        platform=PLATFORM,
        external_url=f"{DOCS_URL}#operation/getOrderById",
        parameters=[
            ApiParam(
                name="orderId",
                data_type="string",
                required=True,
                description="The order identifier.",
            ),
            ApiParam(
                name="includeLineItems",
                data_type="boolean",
                required=False,
                description="Include line items in the response.",
            ),
        ],
        returns=[
            ApiParam(name="orderId", data_type="string", required=True),
            ApiParam(
                name="status",
                data_type="string",
                required=True,
                description="NEW, FILLED, or CANCELLED.",
            ),
            ApiParam(name="notionalAmount", data_type="number", required=True),
            ApiParam(name="currencyPair", data_type="string", required=True),
            ApiParam(name="lineItems", data_type="array<LineItem>", required=False),
        ],
    ),
    Api(
        id=Api.rest_id(SERVICE_ID, "DELETE", "/orders/{orderId}"),
        name="DELETE /orders/{orderId}",
        subtypes=[API_SUBTYPE_REST_ENDPOINT],
        method="DELETE",
        path="/orders/{orderId}",
        description="Cancel an order.",
        platform=PLATFORM,
        external_url=f"{DOCS_URL}#operation/deleteOrder",
        parameters=[
            ApiParam(
                name="orderId",
                data_type="string",
                required=True,
                description="The order identifier.",
            ),
        ],
        # 204 No Content -> no output fields (modeled honestly, not conflated).
        returns=[],
    ),
]


def main() -> None:
    graph = get_default_graph()

    api_urns = [api.emit(graph) for api in ORDER_ENTRY_APIS]
    urn = f"urn:li:service:{SERVICE_ID}"

    # Preserve sourceRepository if a previous run (register_repositories.py) wired
    # the SourcedFrom edge — this write owns displayName/description/apis/lifecycle,
    # but must not null out the repo-wiring field it doesn't set here.
    existing = graph.get_aspect(urn, ServicePropertiesClass)
    source_repository = existing.sourceRepository if existing else None

    aspects = [
        ServicePropertiesClass(
            displayName="Order Entry API",
            description="Place, look up, and cancel trade orders.",
            apis=api_urns or None,
            lifecycle=ServiceLifecycleClass.PRODUCTION,
            sourceRepository=source_repository,
        ),
        ServiceDefinitionClass(
            format=ServiceDefinitionFormatClass.OPENAPI,
            rawSpec=make_large_string(ORDER_ENTRY_OPENAPI),
            version="1.4.0",
            externalUrl=DOCS_URL,
        ),
        SubTypesClass(typeNames=[REST_API_SUBTYPE]),
        DataPlatformInstanceClass(platform=make_data_platform_urn(PLATFORM)),
        StatusClass(removed=False),
    ]
    for aspect in aspects:
        graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))

    logger.info("Registered Service %s composing %d API(s)", urn, len(api_urns))
    for api_urn in api_urns:
        logger.info("  %s", api_urn)


if __name__ == "__main__":
    main()
