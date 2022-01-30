CREATE KEYSPACE IF NOT EXISTS keyspace_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE keyspace_test;

-- https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cql_data_types_c.html
CREATE TABLE IF NOT EXISTS table_example
(
    example_column_ascii     ascii,
    example_column_bigint    bigint,
    example_column_blob      blob,
    example_column_boolean   boolean,
    example_column_date      date,
    example_column_decimal   decimal,
    example_column_double    double,
    example_column_float     float,
    example_column_frozen    frozen<set<int>>,
    example_column_inet      inet,
    example_column_int       int,
    example_column_list      list<int>,
    example_column_map       map<int,text>,
    example_column_set       set<int>,
    example_column_smallint  smallint,
    example_column_text      text STATIC,
    example_column_time      time,
    example_column_timestamp timestamp,
    example_column_timeuuid  timeuuid,
    example_column_tinyint   tinyint,
    example_column_tuple     tuple<int, int>,
    example_column_uuid      uuid,
    example_column_varchar   varchar,
    example_column_varint    varint,
    primary key ((example_column_ascii, example_column_bigint), example_column_blob, example_column_boolean)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS keyspace_test.view_example AS
SELECT example_column_ascii,
       example_column_bigint,
       example_column_blob,
       example_column_boolean,
       example_column_date
FROM keyspace_test.table_example
WHERE example_column_ascii IS NOT NULL
  AND example_column_bigint IS NOT NULL
  AND example_column_blob IS NOT NULL
  AND example_column_boolean IS NOT NULL
PRIMARY KEY ( example_column_ascii, example_column_bigint, example_column_blob, example_column_boolean );

CREATE TABLE IF NOT EXISTS table_example_counter
(
    example_column_int       int,
    example_column_counter counter,
    PRIMARY KEY (example_column_int)
);

CREATE TYPE IF NOT EXISTS udt_address (
    street text,
    city text
);

CREATE TABLE IF NOT EXISTS table_example_with_udts (
    name text PRIMARY KEY,
    addresses map<text, frozen<udt_address>>
);
