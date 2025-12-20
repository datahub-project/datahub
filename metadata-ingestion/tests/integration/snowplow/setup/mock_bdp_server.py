#!/usr/bin/env python3
"""
Mock Snowplow BDP API Server for local development and testing.

This Flask server mimics the Snowplow BDP Console API endpoints, serving
mock responses from fixture files. Useful for:
- Local development without BDP account
- Integration testing without live API calls
- Testing error scenarios and edge cases

Usage:
    python mock_bdp_server.py --port 8081

    # In another terminal:
    export SNOWPLOW_ORG_ID="test-org-uuid"
    export SNOWPLOW_API_KEY_ID="test-key-id"
    export SNOWPLOW_API_KEY="test-secret"
    datahub ingest -c test_recipe.yml
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Tuple, TypeVar, Union

from flask import Flask, Response, jsonify, request

# Type variable for route handler functions
F = TypeVar("F", bound=Callable[..., Any])

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def typed_route(rule: str, **options: Any) -> Callable[[F], F]:
    """
    Typed wrapper for Flask's app.route decorator.

    This wrapper preserves function type annotations for mypy compatibility
    with disallow_untyped_decorators enabled.
    """

    def decorator(func: F) -> F:
        app.add_url_rule(rule, view_func=func, **options)
        return func

    return decorator


# Load fixtures
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(filename: str) -> Any:
    """Load a fixture file (can be dict, list, or other JSON types)."""
    with open(FIXTURES_DIR / filename) as f:
        return json.load(f)


# Mock JWT tokens (simple mapping for testing)
MOCK_TOKENS: Dict[str, str] = {"test-key-id:test-secret": "mock_jwt_token_12345"}


@typed_route("/organizations/<org_id>/credentials/v3/token", methods=["GET"])
def get_token(org_id: str) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for JWT token generation.

    In real BDP API, this is a GET request that returns a JWT token
    based on API key authentication via headers.
    """
    # Check authentication headers
    api_key_id = request.headers.get("X-Api-Key-Id")
    api_key = request.headers.get("X-Api-Key")

    if not api_key_id or not api_key:
        logger.warning("Missing authentication headers")
        return jsonify({"error": "Missing authentication headers"}), 401

    # Generate mock token
    token_key = f"{api_key_id}:{api_key}"
    token = MOCK_TOKENS.get(token_key, f"mock_token_{api_key_id}")

    # Token expires in 1 hour (mimic real API)
    expires_at = (datetime.now() + timedelta(hours=1)).isoformat()

    logger.info(f"Generated token for org_id={org_id}, api_key_id={api_key_id}")

    return jsonify({"accessToken": token, "expiresAt": expires_at})


@typed_route("/organizations/<org_id>/data-structures/v1", methods=["GET"])
def get_data_structures(org_id: str) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for listing data structures (schemas).

    Supports query parameters:
    - filter: Filter by schema type (event, entity)
    - vendor: Filter by vendor
    - limit: Pagination limit
    - offset: Pagination offset
    """
    # Check authentication (JWT token)
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        logger.warning("Missing or invalid Authorization header")
        return jsonify({"error": "Unauthorized"}), 401

    # Load mock data
    try:
        data = load_fixture("data_structures_with_ownership.json")
    except FileNotFoundError:
        logger.error("Fixture file not found: data_structures_with_ownership.json")
        return jsonify({"error": "Mock data not found"}), 500

    # Apply filters (data is now direct array, not wrapped)
    schema_type = request.args.get("filter")
    vendor = request.args.get("vendor")

    filtered_data = data  # Direct array now

    if schema_type:
        filtered_data = [
            ds for ds in filtered_data if ds["meta"]["schemaType"] == schema_type
        ]
        logger.info(
            f"Filtered by schemaType={schema_type}, found {len(filtered_data)} results"
        )

    if vendor:
        filtered_data = [ds for ds in filtered_data if ds["vendor"] == vendor]
        logger.info(f"Filtered by vendor={vendor}, found {len(filtered_data)} results")

    # Pagination
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))

    paginated_data = filtered_data[offset : offset + limit]

    logger.info(f"Returning {len(paginated_data)} data structures for org_id={org_id}")

    # Return direct array (per Swagger spec)
    return jsonify(paginated_data)


@typed_route(
    "/organizations/<org_id>/data-structures/v1/<schema_hash>", methods=["GET"]
)
def get_data_structure_by_hash(
    org_id: str, schema_hash: str
) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for getting a specific data structure by hash.
    """
    # Check authentication
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401

    # Load mock data (direct array now)
    data = load_fixture("data_structures_with_ownership.json")

    # Find by hash
    for ds in data:  # Direct array now
        if ds["hash"] == schema_hash:
            logger.info(f"Found data structure with hash={schema_hash}")
            return jsonify(ds)

    logger.warning(f"Data structure not found: hash={schema_hash}")
    return jsonify({"error": "Data structure not found"}), 404


@typed_route("/organizations/<org_id>/data-products/v2", methods=["GET"])
def get_data_products(org_id: str) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for listing data products.

    API Reference: https://docs.snowplow.io/docs/data-product-studio/data-products/api/

    Note: Data products use v2 API and wrapped response format with data, includes, and errors fields.
    This differs from v1 and follows the same pattern as event specifications.
    """
    # Check authentication
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401

    # Load mock data
    try:
        response_data = load_fixture("data_products_response.json")
    except FileNotFoundError:
        logger.error("Fixture file not found: data_products_response.json")
        return jsonify({"error": "Mock data not found"}), 500

    logger.info(
        f"Returning {len(response_data.get('data', []))} data products for org_id={org_id}"
    )

    # Return wrapped response (data, includes, errors)
    return jsonify(response_data)


@typed_route("/organizations/<org_id>/users", methods=["GET"])
def get_users(org_id: str) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for listing users in organization.

    Used for resolving initiatorId to user emails in ownership tracking.
    """
    # Check authentication
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401

    # Mock users (matching test data)
    users = [
        {
            "id": "user1",
            "email": "ryan@company.com",
            "name": "Ryan Smith",
            "displayName": "Ryan S.",
        },
        {
            "id": "user2",
            "email": "jane@company.com",
            "name": "Jane Doe",
            "displayName": "Jane D.",
        },
        {
            "id": "user3",
            "email": "alice@company.com",
            "name": "Alice Johnson",
            "displayName": "Alice J.",
        },
        {
            "id": "user4",
            "email": "bob@company.com",
            "name": "Bob Wilson",
            "displayName": "Bob W.",
        },
    ]

    logger.info(f"Returning {len(users)} users for org_id={org_id}")

    # Return direct array (per Swagger spec)
    return jsonify(users)


@typed_route("/organizations/<org_id>/event-specs/v1", methods=["GET"])
def get_event_specifications(org_id: str) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for listing event specifications.

    API Reference: https://docs.snowplow.io/docs/data-product-studio/event-specifications/api/

    Note: Event specifications use wrapped response format with data, includes, and errors fields.
    This differs from data structures which return direct arrays.
    """
    # Check authentication
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401

    # Load mock data
    try:
        response_data = load_fixture("event_specifications_response.json")
    except FileNotFoundError:
        logger.error("Fixture file not found: event_specifications_response.json")
        return jsonify({"error": "Mock data not found"}), 500

    logger.info(
        f"Returning {len(response_data.get('data', []))} event specifications for org_id={org_id}"
    )

    # Return wrapped response (data, includes, errors)
    return jsonify(response_data)


@typed_route("/organizations/<org_id>/tracking-scenarios/v1", methods=["GET"])
def get_tracking_scenarios(org_id: str) -> Union[Response, Tuple[Response, int]]:
    """
    Mock endpoint for listing tracking scenarios.

    API Reference: https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-tracking-scenarios/api/

    Note: Tracking scenarios use wrapped response format with data, includes, and errors fields.
    This differs from data structures which return direct arrays.
    """
    # Check authentication
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401

    # Load mock data
    try:
        response_data = load_fixture("tracking_scenarios_response.json")
    except FileNotFoundError:
        logger.error("Fixture file not found: tracking_scenarios_response.json")
        return jsonify({"error": "Mock data not found"}), 500

    logger.info(
        f"Returning {len(response_data.get('data', []))} tracking scenarios for org_id={org_id}"
    )

    # Return wrapped response (data, includes, errors)
    return jsonify(response_data)


@typed_route("/health", methods=["GET"])
def health() -> Response:
    """Health check endpoint."""
    return jsonify(
        {
            "status": "healthy",
            "service": "mock-snowplow-bdp-api",
            "timestamp": datetime.now().isoformat(),
        }
    )


@typed_route("/", methods=["GET"])
def index() -> Response:
    """Root endpoint with API documentation."""
    return jsonify(
        {
            "name": "Mock Snowplow BDP API Server",
            "version": "1.0.0",
            "endpoints": {
                "token": "GET /organizations/{orgId}/credentials/v3/token",
                "data_structures": "GET /organizations/{orgId}/data-structures/v1",
                "data_structure": "GET /organizations/{orgId}/data-structures/v1/{hash}",
                "data_products": "GET /organizations/{orgId}/data-products/v1",
                "health": "GET /health",
            },
            "fixtures": {"data_structures": "data_structures_with_ownership.json"},
            "authentication": {
                "method": "API Key → JWT Token",
                "headers": {"X-Api-Key-Id": "test-key-id", "X-Api-Key": "test-secret"},
            },
        }
    )


def main():
    """Run the mock server."""
    import argparse

    parser = argparse.ArgumentParser(description="Mock Snowplow BDP API Server")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8081, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()

    logger.info(f"Starting Mock Snowplow BDP API Server on {args.host}:{args.port}")
    logger.info(f"Fixtures directory: {FIXTURES_DIR}")
    logger.info(f"API docs: http://{args.host}:{args.port}/")
    logger.info(f"Health check: http://{args.host}:{args.port}/health")

    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
