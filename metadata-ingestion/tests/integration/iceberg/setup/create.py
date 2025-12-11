# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def main(table_name: str) -> None:
    spark = SparkSession.builder.getOrCreate()

    schema = StructType(
        [
            StructField("vendor_id", LongType(), True),
            StructField("trip_date", TimestampType(), True),
            StructField("trip_id", LongType(), True),
            StructField("trip_distance", FloatType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
        ]
    )

    data = [
        (1, datetime(2000, 1, 1, 12, 0), 1000371, 1.8, 15.32, "N"),
        (2, datetime(2000, 1, 2, 12, 0), 1000372, 2.5, 22.15, "N"),
        (2, datetime(2000, 1, 3, 12, 0), 1000373, 0.9, 9.01, "N"),
        (1, datetime(2000, 1, 4, 12, 0), 1000374, 8.4, 42.13, "Y"),
        # Following entry will test profiling values at 0
        (3, datetime(2000, 1, 4, 12, 0), 1000375, 0.0, 0.0, "Y"),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.partitionBy("trip_date").saveAsTable(table_name)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Missing required parameter <table_name>")
    main(sys.argv[1])
