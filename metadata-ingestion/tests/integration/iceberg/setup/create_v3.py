import sys

from pyspark.sql import SparkSession


def main(table_name: str) -> None:
    spark = SparkSession.builder.getOrCreate()

    # DDL is required instead of DataFrame.saveAsTable because only DDL can set
    # the format-version table property.
    namespace = table_name.split(".")[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    # The table is created without data on purpose: the catalog rejects snapshots
    # without first-row-id (Iceberg V3 row-lineage), which the Iceberg 1.8.1 runtime
    # bundled with the Spark image cannot produce yet. Data-level V3 behavior is
    # covered by the unit tests. This still exercises a real Java-written V3 table
    # metadata file end-to-end through the catalog and the connector.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            vendor_id BIGINT,
            trip_id BIGINT,
            trip_distance FLOAT,
            fare_amount DOUBLE,
            store_and_fwd_flag STRING
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='3')
        """
    )

    # Some Iceberg write paths flip format-version back to 2
    # (https://github.com/apache/iceberg/issues/12510); fail fast instead of
    # silently testing a V2 table.
    properties = {
        row[0]: row[1]
        for row in spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    }
    assert properties.get("format-version") == "3", (
        f"Expected format-version=3, got {properties.get('format-version')}"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Missing required parameter <table_name>")
    main(sys.argv[1])
