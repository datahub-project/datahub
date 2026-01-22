#!/usr/bin/env python3
"""
Populate BigLake catalog with test Iceberg tables using PyIceberg (no PySpark).

This is a simpler alternative to populate_biglake_test_data.py that avoids
PySpark version compatibility issues.

Prerequisites:
  - PyIceberg installed: pip install "pyiceberg[gcs]"
  - PyArrow installed: pip install pyarrow
  - Google Cloud credentials configured

Usage:
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
  export GCP_PROJECT_ID=your-project-id
  export GCS_WAREHOUSE_BUCKET=your-bucket-name

  python scripts/populate_biglake_simple.py
"""

import os
import sys
import traceback
from datetime import datetime

import pyarrow as pa
from google.auth import default
from google.auth.transport.requests import Request
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    FloatType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


def get_env_or_exit(var_name: str) -> str:
    """Get environment variable or exit with error."""
    value = os.getenv(var_name)
    if not value:
        print(f"ERROR: {var_name} environment variable not set")
        print(f"Set it with: export {var_name}=your-value")
        sys.exit(1)
    return value


def get_google_access_token() -> str:
    """Get Google Cloud access token from credentials."""

    credentials, project = default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    return credentials.token


def create_catalog(project_id: str, warehouse_bucket: str):
    """Create PyIceberg catalog for BigLake."""
    print("Connecting to BigLake catalog...")

    # Get access token for authentication
    print("  Generating Google Cloud access token...")
    access_token = get_google_access_token()
    print("  ✅ Access token generated")

    catalog_properties = {
        "type": "rest",
        "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        "warehouse": f"gs://{warehouse_bucket}",
        "token": access_token,  # Use token-based auth instead of google auth
        "gcs.project-id": project_id,
        "header.x-goog-user-project": project_id,
        "header.X-Iceberg-Access-Delegation": "",
    }

    try:
        catalog = load_catalog("biglake", **catalog_properties)
        print("✅ Connected to BigLake")
        print(f"   URI: {catalog_properties['uri']}")
        print(f"   Warehouse: {catalog_properties['warehouse']}")
        print(f"   Project: {project_id}")
        print()
        return catalog
    except Exception as e:
        print(f"❌ Failed to connect to BigLake: {e}")
        print("\nTroubleshooting:")
        print("  - Ensure BigLake API is enabled")
        print("  - Check service account has BigLake permissions")
        print("  - Verify GCS bucket exists and is accessible")
        sys.exit(1)


def create_taxis_table(catalog, namespace: str = "nyc"):
    """Create NYC taxis sample table (from DataHub integration tests)."""
    print(f"Creating table: {namespace}.taxis")

    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace(namespace)
        print(f"  Created namespace: {namespace}")
    except Exception:
        print(f"  Namespace {namespace} already exists")

    # Define schema (from DataHub tests)
    schema = Schema(
        NestedField(1, "vendor_id", LongType(), required=False),
        NestedField(2, "trip_date", TimestampType(), required=False),
        NestedField(3, "trip_id", LongType(), required=False),
        NestedField(4, "trip_distance", FloatType(), required=False),
        NestedField(5, "fare_amount", DoubleType(), required=False),
        NestedField(6, "store_and_fwd_flag", StringType(), required=False),
    )

    # Partition by trip_date (day granularity)
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=2,  # trip_date field
            field_id=1000,
            transform=DayTransform(),
            name="trip_date_day",
        )
    )

    # Create or replace table
    table_name = f"{namespace}.taxis"
    try:
        # Try to drop if exists
        try:
            catalog.drop_table(table_name)
            print(f"  Dropped existing table: {table_name}")
        except Exception:
            pass

        # Create new table
        table = catalog.create_table(
            identifier=table_name, schema=schema, partition_spec=partition_spec
        )
        print(f"  Created table: {table_name}")

        # Sample data (from DataHub tests)
        # Create PyArrow table with explicit schema matching Iceberg schema
        data = pa.table(
            {
                "vendor_id": pa.array([1, 2, 2, 1, 3], type=pa.int64()),
                "trip_date": pa.array(
                    [
                        datetime(2000, 1, 1, 12, 0),
                        datetime(2000, 1, 2, 12, 0),
                        datetime(2000, 1, 3, 12, 0),
                        datetime(2000, 1, 4, 12, 0),
                        datetime(2000, 1, 4, 12, 0),
                    ],
                    type=pa.timestamp("us"),  # Microsecond precision
                ),
                "trip_id": pa.array(
                    [1000371, 1000372, 1000373, 1000374, 1000375], type=pa.int64()
                ),
                "trip_distance": pa.array([1.8, 2.5, 0.9, 8.4, 0.0], type=pa.float32()),
                "fare_amount": pa.array(
                    [15.32, 22.15, 9.01, 42.13, 0.0], type=pa.float64()
                ),
                "store_and_fwd_flag": pa.array(
                    ["N", "N", "N", "Y", "Y"], type=pa.string()
                ),
            }
        )

        # Append data
        table.append(data)
        print(f"✅ Table created: {table_name} (5 rows)")
        print()

    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        raise


def create_customers_table(catalog, namespace: str = "retail"):
    """Create customers sample table."""
    print(f"Creating table: {namespace}.customers")

    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace(namespace)
        print(f"  Created namespace: {namespace}")
    except Exception:
        print(f"  Namespace {namespace} already exists")

    # Define schema (all fields nullable for simplicity)
    schema = Schema(
        NestedField(1, "customer_id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "email", StringType(), required=False),
        NestedField(4, "signup_date", TimestampType(), required=False),
        NestedField(5, "status", StringType(), required=False),
    )

    # Create table
    table_name = f"{namespace}.customers"
    try:
        # Try to drop if exists
        try:
            catalog.drop_table(table_name)
            print(f"  Dropped existing table: {table_name}")
        except Exception:
            pass

        # Create new table
        table = catalog.create_table(identifier=table_name, schema=schema)
        print(f"  Created table: {table_name}")

        # Sample data with explicit types
        data = pa.table(
            {
                "customer_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                "name": pa.array(
                    [
                        "Alice Smith",
                        "Bob Johnson",
                        "Carol White",
                        "David Brown",
                        "Eve Davis",
                    ],
                    type=pa.string(),
                ),
                "email": pa.array(
                    [
                        "alice@example.com",
                        "bob@example.com",
                        "carol@example.com",
                        "david@example.com",
                        "eve@example.com",
                    ],
                    type=pa.string(),
                ),
                "signup_date": pa.array(
                    [
                        datetime(2023, 1, 15),
                        datetime(2023, 2, 20),
                        datetime(2023, 3, 10),
                        datetime(2023, 4, 5),
                        datetime(2023, 5, 12),
                    ],
                    type=pa.timestamp("us"),
                ),
                "status": pa.array(
                    ["active", "active", "inactive", "active", "active"],
                    type=pa.string(),
                ),
            }
        )

        # Append data
        table.append(data)
        print(f"✅ Table created: {table_name} (5 rows)")
        print()

    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        raise


def create_orders_table(catalog, namespace: str = "retail"):
    """Create orders sample table."""
    print(f"Creating table: {namespace}.orders")

    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace(namespace)
        print(f"  Created namespace: {namespace}")
    except Exception:
        print(f"  Namespace {namespace} already exists")

    # Define schema (all fields nullable for simplicity)
    schema = Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "customer_id", LongType(), required=False),
        NestedField(3, "order_date", TimestampType(), required=False),
        NestedField(4, "amount", DoubleType(), required=False),
        NestedField(5, "status", StringType(), required=False),
    )

    # Partition by order_date
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=3,  # order_date field
            field_id=1000,
            transform=DayTransform(),
            name="order_date_day",
        )
    )

    # Create table
    table_name = f"{namespace}.orders"
    try:
        # Try to drop if exists
        try:
            catalog.drop_table(table_name)
            print(f"  Dropped existing table: {table_name}")
        except Exception:
            pass

        # Create new table
        table = catalog.create_table(
            identifier=table_name, schema=schema, partition_spec=partition_spec
        )
        print(f"  Created table: {table_name}")

        # Sample data with explicit types
        data = pa.table(
            {
                "order_id": pa.array(
                    [1001, 1002, 1003, 1004, 1005, 1006, 1007], type=pa.int64()
                ),
                "customer_id": pa.array([1, 2, 1, 3, 4, 5, 2], type=pa.int64()),
                "order_date": pa.array(
                    [
                        datetime(2023, 6, 1, 10, 30),
                        datetime(2023, 6, 2, 14, 15),
                        datetime(2023, 6, 3, 9, 45),
                        datetime(2023, 6, 4, 16, 20),
                        datetime(2023, 6, 5, 11, 10),
                        datetime(2023, 6, 6, 13, 55),
                        datetime(2023, 6, 7, 15, 30),
                    ],
                    type=pa.timestamp("us"),
                ),
                "amount": pa.array(
                    [125.50, 89.99, 210.00, 45.75, 156.25, 78.50, 312.00],
                    type=pa.float64(),
                ),
                "status": pa.array(
                    [
                        "completed",
                        "completed",
                        "completed",
                        "cancelled",
                        "completed",
                        "pending",
                        "completed",
                    ],
                    type=pa.string(),
                ),
            }
        )

        # Append data
        table.append(data)
        print(f"✅ Table created: {table_name} (7 rows)")
        print()

    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        raise


def verify_tables(catalog):
    """Verify created tables."""
    print("=" * 70)
    print("Verifying created tables")
    print("=" * 70)
    print()

    for namespace in ["nyc", "retail"]:
        try:
            tables = catalog.list_tables(namespace)
            print(f"Namespace: {namespace}")
            for table in tables:
                print(f"  - {table}")
            print()
        except Exception as e:
            print(f"Namespace {namespace}: {e}")
            print()


def main():
    """Main function to populate BigLake with test data."""
    print("=" * 70)
    print("BigLake Test Data Population (PyIceberg)")
    print("=" * 70)
    print()

    # Get environment variables
    creds_file = get_env_or_exit("GOOGLE_APPLICATION_CREDENTIALS")
    project_id = get_env_or_exit("GCP_PROJECT_ID")
    warehouse_bucket = get_env_or_exit("GCS_WAREHOUSE_BUCKET")

    print("Configuration:")
    print(f"  Credentials: {creds_file}")
    print(f"  Project ID: {project_id}")
    print(f"  Warehouse: gs://{warehouse_bucket}")
    print()

    # Create catalog
    catalog = create_catalog(project_id, warehouse_bucket)

    # Create tables
    try:
        print("Creating sample tables...")
        print()

        # NYC taxis table (from DataHub tests)
        create_taxis_table(catalog, namespace="nyc")

        # Retail tables
        create_customers_table(catalog, namespace="retail")
        create_orders_table(catalog, namespace="retail")

        # Verify
        verify_tables(catalog)

        print("=" * 70)
        print("✅ SUCCESS: Test data populated!")
        print("=" * 70)
        print()
        print("Next steps:")
        print("  1. Run DataHub ingestion:")
        print("     cd metadata-ingestion")
        print("     source venv/bin/activate")
        print("     datahub ingest -c recipes/biglake_cli_console.yml")
        print()
        print("  2. Verify tables appear in DataHub UI:")
        print("     http://localhost:9002")
        print()

    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
