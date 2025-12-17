#!/usr/bin/env python3
"""
Setup test data in Kerberized Hive Metastore.

This script creates test databases and tables via the HMS Thrift API
with Kerberos SASL authentication. It's designed to run in the
kerberos-client container after kinit has been performed.

Usage:
    kinit -kt /keytabs/testuser.keytab testuser@TEST.LOCAL
    python3 /setup-test-data.py
"""

import sys
import time

# Retry connection with exponential backoff
MAX_RETRIES = 10
RETRY_DELAY = 5


def connect_to_hms():
    """Connect to Kerberized HMS with retry logic."""
    from pyhive.sasl_compat import PureSASLClient
    from pymetastore.hive_metastore import ThriftHiveMetastore
    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TSocket
    from thrift_sasl import TSaslClientTransport

    for attempt in range(MAX_RETRIES):
        try:
            socket = TSocket.TSocket("hive-metastore", 9083)

            def sasl_factory():
                return PureSASLClient(
                    "hive-metastore", service="hive", mechanism="GSSAPI"
                )

            transport = TSaslClientTransport(sasl_factory, "GSSAPI", socket)
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


def create_database(client, name, description):
    """Create a database if it doesn't exist."""
    from pymetastore.hive_metastore.ttypes import Database

    try:
        db = Database(name=name, description=description)
        client.create_database(db)
        print(f"  ✓ Created database: {name}")
        return True
    except Exception as e:
        if "AlreadyExists" in str(type(e).__name__) or "AlreadyExists" in str(e):
            print(f"  - Database {name} already exists")
            return True
        else:
            print(f"  ✗ Failed to create {name}: {e}")
            return False


def create_table(
    client,
    db_name,
    table_name,
    columns,
    table_type="MANAGED_TABLE",
    partition_keys=None,
    table_properties=None,
    storage_format=None,
    location=None,
):
    """Create a table with the specified columns.

    Args:
        client: HMS Thrift client
        db_name: Database name
        table_name: Table name
        columns: List of (name, type) or (name, type, comment) tuples
        table_type: MANAGED_TABLE, EXTERNAL_TABLE, etc.
        partition_keys: List of (name, type) tuples for partition columns
        table_properties: Dict of table properties
        storage_format: "ORC", "PARQUET", or None for text
        location: Custom storage location (e.g., s3://bucket/path, hdfs://path)
    """
    from pymetastore.hive_metastore.ttypes import (
        FieldSchema,
        SerDeInfo,
        StorageDescriptor,
        Table,
    )

    try:
        cols = [
            FieldSchema(name=c[0], type=c[1], comment=c[2] if len(c) > 2 else "")
            for c in columns
        ]
        part_cols = []
        if partition_keys:
            part_cols = [
                FieldSchema(name=p[0], type=p[1], comment="") for p in partition_keys
            ]

        # Set storage format
        if storage_format == "ORC":
            input_fmt = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
            output_fmt = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
            serde_lib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        elif storage_format == "PARQUET":
            input_fmt = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            output_fmt = (
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            )
            serde_lib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        else:
            input_fmt = "org.apache.hadoop.mapred.TextInputFormat"
            output_fmt = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            serde_lib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

        # Use custom location if provided, otherwise default warehouse path
        storage_location = location or f"/user/hive/warehouse/{db_name}.db/{table_name}"

        sd = StorageDescriptor(
            cols=cols,
            location=storage_location,
            inputFormat=input_fmt,
            outputFormat=output_fmt,
            serdeInfo=SerDeInfo(serializationLib=serde_lib),
        )

        table = Table(
            tableName=table_name,
            dbName=db_name,
            owner="testuser",
            sd=sd,
            tableType=table_type,
            partitionKeys=part_cols if part_cols else None,
            parameters=table_properties,
        )

        client.create_table(table)
        print(f"  ✓ Created table: {db_name}.{table_name}")
        if location:
            print(f"    Location: {location}")
        return True
    except Exception as e:
        if "AlreadyExists" in str(type(e).__name__) or "AlreadyExists" in str(e):
            print(f"  - Table {db_name}.{table_name} already exists")
            return True
        else:
            print(f"  ✗ Failed to create {db_name}.{table_name}: {e}")
            return False


def main():
    print("=" * 60)
    print("Setting up Test Data in Kerberized HMS")
    print("=" * 60)

    # Connect to HMS
    print("\n1. Connecting to Kerberized HMS...")
    client, transport = connect_to_hms()
    if not client:
        print("Failed to connect to HMS")
        sys.exit(1)
    print("  ✓ Connected successfully")

    # Create databases
    print("\n2. Creating databases...")
    create_database(client, "db1", "Primary test database with complex types")
    create_database(client, "db2", "Secondary database for multi-db testing")
    create_database(client, "db_filtered", "Database for filter pattern testing")

    # Create tables with complex types in db1
    # These match the tables in hive_setup.sql for parity with non-Kerberized tests
    print("\n3. Creating tables in db1...")

    # Partitioned pokes table (matches hive_setup.sql)
    create_table(
        client,
        "db1",
        "pokes",
        [("foo", "int"), ("bar", "string")],
        partition_keys=[("baz", "string")],
    )

    # Table with underscore prefix (edge case for name parsing)
    create_table(
        client,
        "db1",
        "_test_table_underscore",
        [
            ("foo", "int"),
            ("bar", "string"),
        ],
    )

    # Struct type (matches hive_setup.sql)
    create_table(
        client,
        "db1",
        "struct_test",
        [
            ("property_id", "int"),
            ("service", "struct<type:string,provider:array<int>>"),
        ],
    )

    # Array of structs with table properties (matches hive_setup.sql)
    create_table(
        client,
        "db1",
        "array_struct_test",
        [
            ("property_id", "int", "id of property"),
            (
                "service",
                "array<struct<type:string,provider:array<int>>>",
                "service types and providers",
            ),
        ],
        table_properties={
            "comment": "This table has array of structs",
            "another.comment": "This table has no partitions",
        },
    )

    # Nested struct with VARCHAR (matches hive_setup.sql)
    create_table(
        client,
        "db1",
        "nested_struct_test",
        [
            ("property_id", "int"),
            (
                "service",
                "struct<type:string,provider:struct<name:varchar(50),id:tinyint>>",
            ),
        ],
    )

    # Union type with 5 types stored as ORC (matches hive_setup.sql)
    create_table(
        client,
        "db1",
        "union_test",
        [
            (
                "foo",
                "uniontype<int,double,array<string>,struct<a:int,b:string>,struct<c:int,d:double>>",
            )
        ],
        storage_format="ORC",
    )

    # Map type (matches hive_setup.sql: KeyValue not key_value)
    create_table(
        client,
        "db1",
        "map_test",
        [
            ("KeyValue", "string"),
            ("RecordId", "map<int,string>"),
        ],
    )

    # All primitive types (additional test coverage)
    create_table(
        client,
        "db1",
        "all_types_test",
        [
            ("col_tinyint", "tinyint", "1-byte signed integer"),
            ("col_smallint", "smallint", "2-byte signed integer"),
            ("col_int", "int", "4-byte signed integer"),
            ("col_bigint", "bigint", "8-byte signed integer"),
            ("col_float", "float", "Single precision"),
            ("col_double", "double", "Double precision"),
            ("col_decimal", "decimal(10,2)", "Decimal with precision"),
            ("col_string", "string", "String type"),
            ("col_varchar", "varchar(100)", "Variable-length string"),
            ("col_char", "char(10)", "Fixed-length string"),
            ("col_boolean", "boolean", "Boolean type"),
            ("col_date", "date", "Date type"),
            ("col_timestamp", "timestamp", "Timestamp type"),
            ("col_binary", "binary", "Binary type"),
        ],
    )

    # Create tables in db2 (matches hive_setup.sql)
    print("\n4. Creating tables in db2...")
    create_table(
        client,
        "db2",
        "pokes",
        [
            ("foo", "int"),
            ("bar", "string"),
        ],
    )

    # Create table in db_filtered (for filter pattern testing)
    print("\n5. Creating tables in db_filtered...")
    create_table(
        client,
        "db_filtered",
        "should_be_filtered",
        [
            ("id", "int"),
        ],
    )

    # Create tables with external storage locations for lineage testing
    # NOTE: Using HDFS paths for all tables since the test environment only has HDFS configured.
    # In production, these would be S3/Azure/GCS paths. The storage lineage feature
    # handles all these storage platforms the same way - by parsing the location URL.
    print("\n6. Creating storage lineage test tables...")

    # Create a database for lineage testing
    create_database(client, "db_lineage", "Database for storage lineage testing")

    # Events table (simulating S3-like path structure)
    create_table(
        client,
        "db_lineage",
        "events_raw",
        [
            ("event_id", "string", "Unique event identifier"),
            ("event_type", "string", "Type of event"),
            ("event_timestamp", "timestamp", "When the event occurred"),
            ("user_id", "bigint", "User who triggered the event"),
            ("payload", "string", "Event payload JSON"),
        ],
        table_type="EXTERNAL_TABLE",
        storage_format="PARQUET",
        location="hdfs://namenode:8020/data/events/raw/2024",
        table_properties={
            "comment": "External table for events - lineage testing",
            "EXTERNAL": "TRUE",
        },
    )

    # Logs table with partitions
    create_table(
        client,
        "db_lineage",
        "logs_partitioned",
        [
            ("log_id", "string"),
            ("log_level", "string"),
            ("message", "string"),
            ("timestamp", "timestamp"),
        ],
        table_type="EXTERNAL_TABLE",
        partition_keys=[("year", "int"), ("month", "int"), ("day", "int")],
        storage_format="PARQUET",
        location="hdfs://namenode:8020/data/logs/application",
        table_properties={
            "comment": "Partitioned external table for logs - lineage testing",
            "EXTERNAL": "TRUE",
        },
    )

    # Transactions table (ORC format)
    create_table(
        client,
        "db_lineage",
        "transactions",
        [
            ("transaction_id", "string"),
            ("amount", "decimal(18,2)"),
            ("currency", "string"),
            ("status", "string"),
            ("created_at", "timestamp"),
        ],
        table_type="EXTERNAL_TABLE",
        storage_format="ORC",
        location="hdfs://namenode:8020/data/warehouse/transactions",
        table_properties={
            "comment": "External table for transactions - lineage testing",
            "EXTERNAL": "TRUE",
            "orc.compress": "SNAPPY",
        },
    )

    # Customer data table
    create_table(
        client,
        "db_lineage",
        "customer_data",
        [
            ("customer_id", "bigint"),
            ("name", "string"),
            ("email", "string"),
            ("created_date", "date"),
        ],
        table_type="EXTERNAL_TABLE",
        storage_format="PARQUET",
        location="hdfs://namenode:8020/data/customers/current",
        table_properties={
            "comment": "External table for customers - lineage testing",
            "EXTERNAL": "TRUE",
        },
    )

    # Metrics table with map type
    create_table(
        client,
        "db_lineage",
        "metrics_hourly",
        [
            ("metric_name", "string"),
            ("metric_value", "double"),
            ("dimensions", "map<string,string>"),
            ("collected_at", "timestamp"),
        ],
        table_type="EXTERNAL_TABLE",
        storage_format="PARQUET",
        location="hdfs://namenode:8020/data/metrics/streaming/hourly",
        table_properties={
            "comment": "External table for metrics - lineage testing",
            "EXTERNAL": "TRUE",
        },
    )

    # Note about views
    print("\n7. Note about views...")
    print("  ⚠ Views (struct_test_view_materialized, array_struct_test_view,")
    print("    array_struct_test_view_multiline) cannot be created via HMS Thrift API.")
    print("    Views require HiveServer2 and CREATE VIEW statements.")
    print("    For Kerberized HMS-only testing, views are not applicable.")

    # Verification
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    databases = client.get_all_databases()
    print(f"\nDatabases: {databases}")

    for db in ["db1", "db2", "db_filtered", "db_lineage"]:
        if db in databases:
            tables = client.get_all_tables(db)
            print(f"\n{db} tables ({len(tables)}): {tables}")

    # Show complex type details
    print("\n" + "-" * 40)
    print("Complex Type Samples:")
    print("-" * 40)
    for tbl in [
        "struct_test",
        "array_struct_test",
        "nested_struct_test",
        "map_test",
        "union_test",
    ]:
        try:
            t = client.get_table("db1", tbl)
            cols = [(c.name, c.type) for c in t.sd.cols]
            print(f"  {tbl}: {cols}")
        except Exception:
            pass

    # Show partitioned table
    print("\n" + "-" * 40)
    print("Partitioned Tables:")
    print("-" * 40)
    try:
        t = client.get_table("db1", "pokes")
        parts = [(p.name, p.type) for p in (t.partitionKeys or [])]
        print(f"  db1.pokes partition keys: {parts}")
    except Exception as e:
        print(f"  Failed to get pokes: {e}")

    # Show table properties
    print("\n" + "-" * 40)
    print("Table Properties:")
    print("-" * 40)
    try:
        t = client.get_table("db1", "array_struct_test")
        props = t.parameters or {}
        print(f"  db1.array_struct_test: {dict(props)}")
    except Exception:
        pass

    # Show storage lineage tables with their locations
    print("\n" + "-" * 40)
    print("Storage Lineage Tables (External Locations):")
    print("-" * 40)
    lineage_tables = [
        "events_raw",
        "logs_partitioned",
        "transactions",
        "customer_data",
        "metrics_hourly",
    ]
    for tbl in lineage_tables:
        try:
            t = client.get_table("db_lineage", tbl)
            location = t.sd.location if t.sd else "N/A"
            cols = [(c.name, c.type) for c in t.sd.cols] if t.sd else []
            print(f"  {tbl}:")
            print(f"    Location: {location}")
            print(f"    Columns: {cols}")
        except Exception as e:
            print(f"  {tbl}: Failed to retrieve - {e}")

    # Test HMS 3.x Catalog Support
    print("\n" + "-" * 40)
    print("HMS 3.x Catalog Support Test:")
    print("-" * 40)
    test_catalog_support(client)

    transport.close()

    print("\n" + "=" * 60)
    print("✅ Test data setup complete!")
    print("=" * 60)


def test_catalog_support(client):
    """
    Test HMS 3.x catalog support.

    This function tests whether the HMS server supports the Catalog API
    introduced in HMS 3.0. It gracefully handles both HMS 2.x and 3.x servers.
    """
    print("  Testing HMS 3.x catalog API...")

    # Test get_catalogs() - only available in HMS 3.x
    try:
        response = client.get_catalogs()
        catalogs = list(response.names) if response.names else []
        print(f"  ✓ HMS 3.x detected! Catalogs: {catalogs}")

        # Test catalog-aware database listing
        if catalogs:
            for catalog in catalogs[:2]:  # Test first 2 catalogs
                try:
                    from pymetastore.hive_metastore.ttypes import GetDatabasesRequest

                    req = GetDatabasesRequest(catalogName=catalog)
                    result = client.get_databases(req)
                    dbs = (
                        list(result.databases)
                        if hasattr(result, "databases")
                        else list(result)
                        if result
                        else []
                    )
                    print(
                        f"    Catalog '{catalog}' has {len(dbs)} databases: {dbs[:5]}..."
                    )
                except Exception as e:
                    print(f"    Failed to list databases in catalog '{catalog}': {e}")

        # Test catalog-aware table listing
        if catalogs and catalogs[0]:
            try:
                from pymetastore.hive_metastore.ttypes import GetTableRequest

                # Try to get a table with catalog context
                catalog = catalogs[0]
                req = GetTableRequest(catName=catalog, dbName="db1", tblName="pokes")
                response = client.get_table_req(req)
                if response.table:
                    print("    ✓ Successfully retrieved table with catalog context")
            except Exception as e:
                print(f"    Catalog-aware table retrieval: {e}")

    except Exception as e:
        # HMS 2.x doesn't have catalog support
        print(f"  ℹ HMS 2.x detected (no catalog support): {type(e).__name__}")
        print("    This is normal for Hive Metastore versions prior to 3.0")
        print("    The connector will use standard APIs for metadata extraction")


if __name__ == "__main__":
    main()
