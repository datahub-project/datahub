#!/usr/bin/env python3
"""
Setup test catalogs and data in HMS 3.x for catalog functionality testing.

This script creates:
1. Multiple catalogs (hive, spark_catalog, iceberg_catalog)
2. Databases in each catalog (some with same names to test namespace isolation)
3. Tables with various configurations

Usage:
    python3 setup-catalogs.py [--host HOST] [--port PORT]
"""

import argparse
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

MAX_RETRIES = 30
RETRY_DELAY = 2


def connect_to_hms(host: str = "localhost", port: int = 9084) -> Tuple[Any, Any]:
    """Connect to HMS with retry logic."""
    from pymetastore.hive_metastore import ThriftHiveMetastore
    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TSocket, TTransport

    for attempt in range(MAX_RETRIES):
        try:
            socket = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(socket)
            transport.open()

            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ThriftHiveMetastore.Client(protocol)

            # Test connection
            client.get_all_databases()
            return client, transport

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  Connection attempt {attempt + 1} failed: {e}")
                print(f"  Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(
                    f"Failed to connect after {MAX_RETRIES} attempts: {e}"
                ) from e

    return None, None


def create_catalog(client: Any, name: str, description: str, location: str) -> bool:
    """Create a catalog (HMS 3.x only) using CreateCatalogRequest."""
    from pymetastore.hive_metastore.ttypes import Catalog, CreateCatalogRequest

    try:
        catalog = Catalog(
            name=name,
            description=description,
            locationUri=location,
        )
        request = CreateCatalogRequest(catalog=catalog)
        client.create_catalog(request)
        print(f"  ✓ Created catalog: {name}")
        return True
    except Exception as e:
        if "AlreadyExists" in str(type(e).__name__) or "AlreadyExists" in str(e):
            print(f"  - Catalog {name} already exists")
            return True
        else:
            print(f"  ✗ Failed to create catalog {name}: {e}")
            return False


def create_database(
    client: Any, db_name: str, description: str, catalog_name: str = "hive"
) -> bool:
    """Create a database in the specified catalog."""
    from pymetastore.hive_metastore.ttypes import Database

    try:
        db = Database(
            name=db_name,
            description=description,
            catalogName=catalog_name,
            locationUri=f"/opt/hive/data/warehouse/{catalog_name}/{db_name}.db",
        )
        client.create_database(db)
        print(f"  ✓ Created database: {catalog_name}.{db_name}")
        return True
    except Exception as e:
        if "AlreadyExists" in str(type(e).__name__) or "AlreadyExists" in str(e):
            print(f"  - Database {catalog_name}.{db_name} already exists")
            return True
        else:
            print(f"  ✗ Failed to create {catalog_name}.{db_name}: {e}")
            return False


def create_table(
    client: Any,
    db_name: str,
    table_name: str,
    columns: List[Tuple[str, str, str]],
    catalog_name: str = "hive",
    partition_keys: Optional[List[Tuple[str, str, str]]] = None,
    table_type: str = "MANAGED_TABLE",
    table_properties: Optional[Dict[str, str]] = None,
) -> bool:
    """Create a table in the specified database."""
    from pymetastore.hive_metastore.ttypes import (
        FieldSchema,
        SerDeInfo,
        StorageDescriptor,
        Table,
    )

    try:
        cols = [
            FieldSchema(
                name=col[0], type=col[1], comment=col[2] if len(col) > 2 else ""
            )
            for col in columns
        ]

        partition_cols = []
        if partition_keys:
            partition_cols = [
                FieldSchema(
                    name=pk[0], type=pk[1], comment=pk[2] if len(pk) > 2 else ""
                )
                for pk in partition_keys
            ]

        sd = StorageDescriptor(
            cols=cols,
            location=f"/opt/hive/data/warehouse/{catalog_name}/{db_name}.db/{table_name}",
            inputFormat="org.apache.hadoop.mapred.TextInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            compressed=False,
            numBuckets=-1,
            serdeInfo=SerDeInfo(
                name=table_name,
                serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                parameters={"serialization.format": "1"},
            ),
            bucketCols=[],
            sortCols=[],
            parameters={},
            skewedInfo=None,
            storedAsSubDirectories=False,
        )

        table = Table(
            tableName=table_name,
            dbName=db_name,
            owner="hive",
            createTime=int(time.time()),
            lastAccessTime=int(time.time()),
            retention=0,
            sd=sd,
            partitionKeys=partition_cols,
            parameters=table_properties or {},
            viewOriginalText=None,
            viewExpandedText=None,
            tableType=table_type,
            privileges=None,
            temporary=False,
            rewriteEnabled=False,
            catName=catalog_name,
        )

        client.create_table(table)
        print(f"  ✓ Created table: {catalog_name}.{db_name}.{table_name}")
        return True
    except Exception as e:
        if "AlreadyExists" in str(type(e).__name__) or "AlreadyExists" in str(e):
            print(f"  - Table {catalog_name}.{db_name}.{table_name} already exists")
            return True
        else:
            print(f"  ✗ Failed to create {catalog_name}.{db_name}.{table_name}: {e}")
            return False


def run_setup(host: str = "localhost", port: int = 9084) -> None:
    """
    Run the setup with configurable host and port.

    This function is called from the test fixture to set up test data.
    """
    print("=" * 60)
    print(f"Setting up HMS 3.x Catalog Test Data (connecting to {host}:{port})")
    print("=" * 60)

    # Connect to HMS
    print("\n1. Connecting to HMS 3.x...")
    client, transport = connect_to_hms(host=host, port=port)
    if not client:
        raise Exception("Failed to connect to HMS")
    print("  ✓ Connected successfully")

    # Check HMS version/catalogs
    print("\n2. Checking HMS catalog support...")
    try:
        response = client.get_catalogs()
        catalogs = list(response.names) if response.names else []
        print(f"  ✓ Existing catalogs: {catalogs}")
    except Exception as e:
        print(f"  ⚠ Could not list catalogs: {e}")
        catalogs = []

    # Create additional catalogs
    print("\n3. Creating test catalogs...")
    create_catalog(
        client,
        "spark_catalog",
        "Spark default catalog for Delta/Iceberg tables",
        "/opt/hive/data/warehouse/spark_catalog",
    )
    create_catalog(
        client,
        "iceberg_catalog",
        "Iceberg-specific catalog",
        "/opt/hive/data/warehouse/iceberg_catalog",
    )

    # Create databases in 'hive' catalog (default)
    print("\n4. Creating databases in 'hive' catalog...")
    create_database(client, "test_db", "Test database in hive catalog", "hive")
    create_database(client, "analytics_db", "Analytics database in hive", "hive")

    # Create databases in 'spark_catalog'
    print("\n5. Creating databases in 'spark_catalog'...")
    create_database(
        client, "test_db", "Test database in spark_catalog", "spark_catalog"
    )
    create_database(client, "delta_db", "Delta Lake database", "spark_catalog")

    # Create databases in 'iceberg_catalog'
    print("\n6. Creating databases in 'iceberg_catalog'...")
    create_database(
        client, "test_db", "Test database in iceberg_catalog", "iceberg_catalog"
    )

    # Create tables in 'hive.test_db'
    print("\n7. Creating tables in 'hive.test_db'...")
    create_table(
        client,
        "test_db",
        "users",
        [
            ("id", "bigint", "User ID"),
            ("name", "string", "User name"),
            ("email", "string", "User email"),
            ("created_at", "timestamp", "Creation timestamp"),
        ],
        catalog_name="hive",
    )
    create_table(
        client,
        "test_db",
        "events",
        [
            ("event_id", "string", "Event ID"),
            ("user_id", "bigint", "User ID"),
            ("event_type", "string", "Event type"),
            ("event_data", "string", "Event data JSON"),
        ],
        catalog_name="hive",
        partition_keys=[("event_date", "string", "Event date partition")],
    )

    # Create tables in 'spark_catalog.test_db' (namespace isolation test)
    print(
        "\n8. Creating tables in 'spark_catalog.test_db' (namespace isolation test)..."
    )
    create_table(
        client,
        "test_db",
        "users",  # Same name as hive.test_db.users - different schema!
        [
            ("user_id", "bigint", "Spark user ID"),
            ("username", "string", "Spark username"),
            ("active", "boolean", "Is active"),
        ],
        catalog_name="spark_catalog",
        table_properties={"spark.table.version": "2.0"},
    )

    # Create tables in 'iceberg_catalog.test_db'
    print("\n9. Creating tables in 'iceberg_catalog.test_db'...")
    create_table(
        client,
        "test_db",
        "transactions",
        [
            ("txn_id", "string", "Transaction ID"),
            ("amount", "decimal(18,2)", "Transaction amount"),
            ("status", "string", "Transaction status"),
        ],
        catalog_name="iceberg_catalog",
        table_type="EXTERNAL_TABLE",
        table_properties={"table_type": "ICEBERG"},
    )

    # Verify setup
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    response = client.get_catalogs()
    catalogs = list(response.names) if response.names else []
    print(f"\nCatalogs: {catalogs}")

    for catalog in catalogs:
        try:
            # Get databases for this catalog
            dbs = client.get_databases(f"@{catalog}#")
            print(f"\n{catalog}:")
            for db in dbs:
                tables = client.get_all_tables(db)
                print(f"  {db}: {tables}")
        except Exception as e:
            print(f"  Error listing {catalog}: {e}")

    transport.close()

    print("\n" + "=" * 60)
    print("✅ HMS 3.x catalog test data setup complete!")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Setup HMS 3.x catalog test data")
    parser.add_argument(
        "--host",
        default="localhost",
        help="HMS hostname (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9084,
        help="HMS port (default: 9084)",
    )
    args = parser.parse_args()

    try:
        run_setup(host=args.host, port=args.port)
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
