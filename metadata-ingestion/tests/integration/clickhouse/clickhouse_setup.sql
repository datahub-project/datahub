CREATE DATABASE schema_example;
CREATE TABLE IF NOT EXISTS schema_example.example_table
(
    column_date Date,
    column_int8 Int8,
    column_TINYINT TINYINT,
    column_BOOL BOOL,
    column_BOOLEAN BOOLEAN,
    column_INT1 INT1,
    column_Int16 Int16,
    column_SMALLINT SMALLINT,
    -- available in docs but not valid for engine
--     column_INT2 INT2,
    column_Int32 Int32,
    column_INT INT,
    -- available in docs but not valid for engine
--     column_INT4 INT4,
    column_INTEGER INTEGER,
    column_Int64 Int64,
    column_BIGINT BIGINT,
    column_Int128 Int128,
    column_Int256 Int256,
    column_UInt8 UInt8,
    column_UInt16 UInt16,
    column_UInt32 UInt32,
    column_UInt64 UInt64,
    column_UInt128 UInt128,
    column_UInt256 UInt256,


    column_Float32 Float32,
    column_FLOAT FLOAT,
    column_Float64 Float64,
    column_DOUBLE DOUBLE,

    column_DecimalPS Decimal(5, 4),
    column_Decimal32S Decimal32(2),
    column_Decimal64S Decimal64(2),
    column_Decimal128S Decimal128(2),
    column_Decimal256S Decimal256(2),

    column_String String,
    column_LONGTEXT LONGTEXT,
    column_MEDIUMTEXT MEDIUMTEXT,
    column_TINYTEXT TINYTEXT,
    column_TEXT TEXT,
    column_LONGBLOB LONGBLOB,
    column_MEDIUMBLOB MEDIUMBLOB,
    column_TINYBLOB TINYBLOB,
    column_BLOB BLOB,
    column_VARCHAR VARCHAR,
    column_CHAR CHAR,
    column_FixedString FixedString(4),
    column_Date Date,
    column_Date32 Date32,
    column_Datetime Datetime,
    column_DatetimeTZ Datetime('Europe/Berlin'),
    column_DateTime64 DateTime64(2),
    column_DateTime64TZ DateTime64(2, 'Europe/Berlin'),
    column_Enum Enum('hello' = 1, 'world' = 2),
    column_Enum8 Enum8('hello2' = 3, 'world2' = 4),
    column_Enum16 Enum16('hello3' = 5, 'world3' = 6),
    column_LowCardinality LowCardinality(String),
    column_Array Array(String),
    column_ArrayArrayInt Array(Array(Int)),
    column_AggregateFunction AggregateFunction(uniq, UInt64),
    column_nested Nested
    (
        nested_column_int Int,
        nested_column_datetime DateTime
    ),
    column_tuple Tuple(String, Int),
    column_nullable_int Nullable(Int),
    column_nullable_array Array(Nullable(UInt8)),
    column_nullable_tuple Tuple(UInt8, Nullable(String)),
    column_ipv4 IPv4,
    column_ipv6 IPv6,
    column_double_precision DOUBLE PRECISION,
    column_char_large_object CHAR LARGE OBJECT,
    column_char_varying CHAR VARYING,
    column_character_large_object CHARACTER LARGE OBJECT,
    column_character_varying CHARACTER VARYING,
    column_nchar_large_object NCHAR LARGE OBJECT,
    column_nchar_varying NCHAR VARYING,
    column_national_character_large_object NATIONAL CHARACTER LARGE OBJECT,
    column_national_character_varying NATIONAL CHARACTER VARYING,
    column_national_char_varying NATIONAL CHAR VARYING,
    column_national_character NATIONAL CHARACTER,
    column_national_char NATIONAL CHAR,
    column_binary_large_object BINARY LARGE OBJECT,
    column_binary_varying BINARY VARYING,
    -- geo - skipped as marked as experimental
--     column_Point Point,
--     column_Ring Ring,
--     column_Polygon Polygon,
--     column_MultiPolygon MultiPolygon,

    column_map Map(String, UInt64),
    column_simple_aggregate_function SimpleAggregateFunction(sum, Double)
    )
ENGINE = MergeTree(column_date, intHash32(column_int8), (intHash32(column_int8), column_Int16), 8192);

CREATE VIEW schema_example.example_view AS
    SELECT
        column_date,
        column_int8,
        column_Int16
    FROM schema_example.example_table;

CREATE MATERIALIZED VIEW schema_example.example_view_materialized
    ENGINE = MergeTree(column_date, intHash32(column_int8), (intHash32(column_int8), column_Int16), 8192)
    AS SELECT
        column_date,
        column_int8,
        column_Int16
    FROM schema_example.example_table;
