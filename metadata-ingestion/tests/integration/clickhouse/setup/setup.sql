CREATE DATABASE db1;

CREATE TABLE db1.test_data_types
(
    `col_Array` Array(String),
    `col_Bool` Bool COMMENT 'https://github.com/ClickHouse/ClickHouse/pull/31072',
    `col_Date` Date,
    `col_Date32` Date32 COMMENT 'this type was added in ClickHouse v21.9',
    `col_DateTime` DateTime,
    `col_DatetimeTZ` DateTime('Europe/Berlin'),
    `col_DateTime32` DateTime,
    `col_DateTime64` DateTime64(3),
    `col_DateTime64TZ` DateTime64(2, 'Europe/Berlin'),
    `col_Decimal` Decimal(2, 1),
    `col_Decimal128` Decimal(38, 2),
    `col_Decimal256` Decimal(76, 3),
    `col_Decimal32` Decimal(9, 4),
    `col_Decimal64` Decimal(18, 5),
    `col_Enum` Enum8('hello' = 1, 'world' = 2),
    `col_Enum16` Enum16('hello' = 1, 'world' = 2),
    `col_Enum8` Enum8('hello' = 1, 'world' = 2),
    `col_FixedString` FixedString(128),
    `col_Float32` Float32,
    `col_Float64` Float64,
    `col_IPv4` IPv4,
    `col_IPv6` IPv6,
    `col_Int128` Int128,
    `col_Int16` Int16,
    `col_Int256` Int256,
    `col_Int32` Int32,
    `col_Int64` Int64,
    `col_Int8` Int8,
    `col_Map` Map(String, Nullable(UInt64)),
    `col_String` String,
    `col_Tuple` Tuple(UInt8, Array(String)),
    `col_UInt128` UInt128,
    `col_UInt16` UInt16,
    `col_UInt256` UInt256,
    `col_UInt32` UInt32,
    `col_UInt64` UInt64,
    `col_UInt8` UInt8,
    `col_UUID` UUID
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
COMMENT 'This table has basic types';


CREATE TABLE db1.test_nested_data_types
(
    `col_ArrayArrayInt` Array(Array(Int8)) COMMENT 'this is a comment',
    `col_LowCardinality` LowCardinality(String),
    `col_AggregateFunction` AggregateFunction(avg, Float64),
    `col_SimpleAggregateFunction` SimpleAggregateFunction(max, Decimal(38, 7)),
    `col_Nested.c1` Array(UInt32),
    `col_Nested.c2` Array(UInt64),
    `col_Nested.c3.c4` Array(UInt128),
    `col_Nullable` Nullable(Int8),
    `col_Array_Nullable_String` Array(Nullable(String)),
    `col_LowCardinality_Nullable_String` LowCardinality(Nullable(String))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
COMMENT 'This table has nested types';


CREATE DICTIONARY db1.test_dict
(
    `col_Int64` Int64,
    `col_String` String
)
PRIMARY KEY col_Int64
SOURCE(CLICKHOUSE(DB 'db1' TABLE 'test_data_types'))
LAYOUT(DIRECT());


CREATE VIEW db1.test_view
(
    `col_String` String
) AS
SELECT dictGetOrDefault('db1.test_dict', 'col_String', toUInt64(123), 'na') AS col_String;


CREATE TABLE db1.mv_target_table
(
    `col_DateTime` DateTime,
    `col_Int64` Int64,
    `col_Float64` Float64,
    `col_Decimal64` Decimal(18, 5),
    `col_String` Nullable(String)
)
ENGINE = MergeTree
ORDER BY col_Int64
SETTINGS index_granularity = 8192
COMMENT 'This is target table for materialized view';


-- https://clickhouse.com/docs/en/sql-reference/table-functions/generate/#generaterandom
INSERT INTO db1.mv_target_table
SELECT *
  FROM generateRandom('col_DateTime DateTime, col_Int64 Int64, col_Float64 Float64, col_Decimal64 Decimal(18, 5), col_String Nullable(String)',
                      5 -- random_seed
                     )
 LIMIT 10;


CREATE MATERIALIZED VIEW db1.mv_with_target_table TO db1.mv_target_table
(
    `col_DateTime` DateTime,
    `col_Int64` Int64,
    `col_Float64` Float64,
    `col_Decimal64` Decimal(18, 5),
    `col_String` String
) AS
SELECT
    col_DateTime,
    col_Int64,
    col_Float64,
    col_Decimal64,
    col_String
FROM db1.test_data_types;


CREATE MATERIALIZED VIEW db1.mv_without_target_table
(
    `col_ArrayArrayInt` Array(Array(Int8)),
    `col_LowCardinality` LowCardinality(String),
    `col_Nullable` Nullable(Int8),
    `col_Array_Nullable_String` Array(Nullable(String)),
    `col_LowCardinality_Nullable_String` LowCardinality(Nullable(String))
)
ENGINE = MergeTree
PRIMARY KEY tuple()
ORDER BY tuple()
SETTINGS index_granularity = 8192 AS
SELECT
    col_ArrayArrayInt,
    col_LowCardinality,
    col_Nullable,
    col_Array_Nullable_String,
    col_LowCardinality_Nullable_String
FROM db1.test_nested_data_types;
