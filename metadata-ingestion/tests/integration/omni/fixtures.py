"""Shared fixture data and fake API client for Omni integration tests.

The fake client covers:
  - 2 connections (Snowflake prod + staging)
  - 2 models (1 SHARED with topics, 1 WORKBOOK referencing the shared model)
  - 2 topics from the shared model: orders (2 views) + customers (1 view)
  - 3 physical tables: ORDERS, ORDER_ITEMS, CUSTOMERS
  - 2 documents: 1 published dashboard with 2 tiles, 1 workbook-only
  - 1 folder: Finance
"""

from typing import Any, Dict, Iterator, List

# ---- Connections ----------------------------------------------------------

CONNECTIONS = [
    {
        "id": "conn-snow-prod",
        "name": "Snowflake Prod",
        "dialect": "snowflake",
        "database": "ANALYTICS_PROD",
        "scope": "organization",
        "deleted": False,
    },
    {
        "id": "conn-snow-staging",
        "name": "Snowflake Staging",
        "dialect": "snowflake",
        "database": "ANALYTICS_STAGING",
        "scope": "organization",
        "deleted": False,
    },
]

# ---- Models ---------------------------------------------------------------

MODELS = [
    {
        "id": "shared-model-1",
        "name": "Analytics Shared Model",
        "modelKind": "SHARED",
        "connectionId": "conn-snow-prod",
        "baseModelId": None,
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": "2024-06-01T00:00:00Z",
        "deletedAt": None,
    },
    {
        "id": "workbook-model-1",
        "name": "Sales Workbook",
        "modelKind": "WORKBOOK",
        "connectionId": "conn-snow-prod",
        "baseModelId": "shared-model-1",
        "createdAt": "2024-03-01T00:00:00Z",
        "updatedAt": "2024-06-15T00:00:00Z",
        "deletedAt": None,
    },
]

# ---- Model YAMLs ----------------------------------------------------------

MODEL_YAML_SHARED = {
    "files": {
        "topics/orders.topic": """\
type: topic
name: orders
base_view_name: orders
""",
        "topics/customers.topic": """\
type: topic
name: customers
base_view_name: customers
""",
        "views/orders.view": """\
type: view
name: orders
schema: PUBLIC
table_name: ORDERS
dimensions:
  - name: order_id
    sql_type: NUMBER
  - name: customer_id
    sql_type: NUMBER
  - name: created_at
    sql_type: TIMESTAMP
measures:
  total_revenue:
    sql_type: NUMBER
""",
        "views/customers.view": """\
type: view
name: customers
schema: PUBLIC
table_name: CUSTOMERS
dimensions:
  - name: customer_id
    sql_type: NUMBER
  - name: name
    sql_type: STRING
  - name: email
    sql_type: STRING
measures:
  lifetime_value:
    sql_type: NUMBER
""",
    }
}

MODEL_YAML_WORKBOOK: Dict[str, Any] = {"files": {}}

# ---- Topics ---------------------------------------------------------------

TOPIC_ORDERS = {
    "id": "topic-orders",
    "views": [
        {
            "name": "orders",
            "schema": "PUBLIC",
            "table_name": "ORDERS",
            "dimensions": [
                {"field_name": "order_id", "sql_type": "NUMBER"},
                {"field_name": "customer_id", "sql_type": "NUMBER"},
                {"field_name": "created_at", "sql_type": "TIMESTAMP"},
            ],
            "measures": [
                {
                    "field_name": "total_revenue",
                    "sql_type": "NUMBER",
                    "display_sql": "SUM(amount)",
                }
            ],
        },
        {
            "name": "order_items",
            "schema": "PUBLIC",
            "table_name": "ORDER_ITEMS",
            "dimensions": [
                {"field_name": "item_id", "sql_type": "NUMBER"},
                {"field_name": "product_id", "sql_type": "NUMBER"},
                {"field_name": "quantity", "sql_type": "NUMBER"},
            ],
            "measures": [],
        },
    ],
}

TOPIC_CUSTOMERS = {
    "id": "topic-customers",
    "views": [
        {
            "name": "customers",
            "schema": "PUBLIC",
            "table_name": "CUSTOMERS",
            "dimensions": [
                {"field_name": "customer_id", "sql_type": "NUMBER"},
                {"field_name": "name", "sql_type": "STRING"},
                {"field_name": "email", "sql_type": "STRING"},
            ],
            "measures": [
                {
                    "field_name": "lifetime_value",
                    "sql_type": "NUMBER",
                    "display_sql": "SUM(order_amount)",
                }
            ],
        }
    ],
}

# ---- Folders --------------------------------------------------------------

FOLDERS = [
    {
        "id": "folder-1",
        "name": "Finance",
        "path": "/Finance",
        "scope": "shared",
        "owner": {"id": "user-admin", "name": "Admin User"},
        "url": "https://acme.omniapp.co/folders/folder-1",
    }
]

# ---- Documents ------------------------------------------------------------

DOCUMENTS = [
    {
        "identifier": "doc-dashboard-1",
        "name": "Sales Dashboard",
        "hasDashboard": True,
        "connectionId": "conn-snow-prod",
        "scope": "shared",
        "deleted": False,
        "folder": {"id": "folder-1", "name": "Finance", "path": "/Finance"},
        "owner": {"id": "user-admin", "name": "Admin User"},
        "url": "https://acme.omniapp.co/dashboards/doc-dashboard-1",
        "updatedAt": "2024-06-15T12:00:00Z",
        "labels": [{"name": "finance"}, {"name": "executive"}],
    },
    {
        "identifier": "doc-workbook-1",
        "name": "Orders Analysis",
        "hasDashboard": False,
        "connectionId": "conn-snow-prod",
        "scope": "personal",
        "deleted": False,
        "folder": {},
        "owner": {"id": "user-analyst", "name": "Data Analyst"},
        "url": "https://acme.omniapp.co/workbooks/doc-workbook-1",
        "updatedAt": "2024-06-10T08:00:00Z",
        "labels": [],
    },
]

# ---- Dashboard document ---------------------------------------------------

DASHBOARD_DOC_DASHBOARD_1 = {
    "modelId": "shared-model-1",
    "queryPresentations": [
        {
            "id": "tile-revenue",
            "name": "Revenue by Month",
            "topicName": "orders",
            "query": {
                "fields": ["orders.created_at", "orders.total_revenue"],
                "modelId": "shared-model-1",
            },
        },
        {
            "id": "tile-customers",
            "name": "Top Customers",
            "topicName": "customers",
            "query": {
                "fields": ["customers.customer_id", "customers.lifetime_value"],
                "modelId": "shared-model-1",
            },
        },
    ],
}

# ---- Document queries -----------------------------------------------------

QUERIES_DASHBOARD_1: List[Dict[str, Any]] = []

QUERIES_WORKBOOK_1 = [
    {
        "query": {
            "modelId": "shared-model-1",
            "fields": ["orders.order_id", "orders.total_revenue"],
        }
    }
]

# ---- Fake client ----------------------------------------------------------


class FakeOmniClientFull:
    """Comprehensive fake Omni API client returning deterministic fixture data.

    Provides full coverage of all code paths in OmniSource including:
      - Connection discovery
      - Model + YAML parsing → topic names
      - Topic API fetch → views + fields
      - Folder ingestion
      - Document list + dashboard payload + queries
      - Fine-grained lineage fields
    """

    def list_connections(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
        return list(CONNECTIONS)

    def list_models(self, page_size: int = 50) -> Iterator[Dict[str, Any]]:
        yield from MODELS

    def get_model_yaml(self, model_id: str) -> Dict[str, Any]:
        if model_id == "shared-model-1":
            return MODEL_YAML_SHARED
        return MODEL_YAML_WORKBOOK

    def get_topic(self, model_id: str, topic_name: str) -> Dict[str, Any]:
        if model_id != "shared-model-1":
            raise RuntimeError(f"404 Not Found: topic {topic_name} not in {model_id}")
        if topic_name == "orders":
            return TOPIC_ORDERS
        if topic_name == "customers":
            return TOPIC_CUSTOMERS
        raise RuntimeError(f"404 Not Found: unknown topic {topic_name}")

    def list_folders(self, page_size: int = 50) -> Iterator[Dict[str, Any]]:
        yield from FOLDERS

    def list_documents(
        self, page_size: int = 50, include_deleted: bool = False
    ) -> Iterator[Dict[str, Any]]:
        yield from DOCUMENTS

    def get_dashboard_document(self, document_id: str) -> Dict[str, Any]:
        if document_id == "doc-dashboard-1":
            return DASHBOARD_DOC_DASHBOARD_1
        return {}

    def get_document_queries(self, document_id: str) -> List[Dict[str, Any]]:
        if document_id == "doc-dashboard-1":
            return QUERIES_DASHBOARD_1
        if document_id == "doc-workbook-1":
            return QUERIES_WORKBOOK_1
        return []

    def test_connection(self) -> bool:
        return True


class FakeOmniClientConnectionFail:
    """Fake client that simulates a failed API authentication."""

    def test_connection(self) -> bool:
        raise RuntimeError("401 Unauthorized: Invalid API key")
