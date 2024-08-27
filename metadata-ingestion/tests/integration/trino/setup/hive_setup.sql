CREATE DATABASE IF NOT EXISTS db1;
CREATE DATABASE IF NOT EXISTS db2;
-- Setup a "pokes" example table.
CREATE TABLE IF NOT EXISTS db1.pokes (foo INT, bar STRING) PARTITIONED BY (baz STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE db1.pokes PARTITION (baz='dummy');

CREATE TABLE IF NOT EXISTS db2.pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE db2.pokes;

-- Setup a table with a special character.
CREATE TABLE IF NOT EXISTS db1.`_test_table_underscore` (foo INT, bar STRING);

-- Create tables with struct and array types.
-- From https://stackoverflow.com/questions/57491644/correct-usage-of-a-struct-in-hive.
CREATE TABLE IF NOT EXISTS db1.struct_test
(
 property_id INT,
 service STRUCT<
                type: STRING
               ,provider: ARRAY<INT>
               >
);

CREATE TABLE IF NOT EXISTS db1.array_struct_test
(
 property_id INT COMMENT 'id of property',
 service array<STRUCT<
                type: STRING
               ,provider: ARRAY<INT>
               >> COMMENT 'service types and providers'
)  TBLPROPERTIES ('comment' = 'This table has array of structs', 'another.comment' = 'This table has no partitions');;

WITH
test_data as (
    SELECT 989 property_id, array(NAMED_STRUCT('type','Cleaning','provider', ARRAY(587, 887)),
                      NAMED_STRUCT('type','Pricing','provider', ARRAY(932))
                      ) AS service
                      --, array(4,5,5) AS ratings
)
INSERT INTO TABLE db1.array_struct_test
select * from test_data;

CREATE MATERIALIZED VIEW db1.struct_test_view_materialized as select * from db1.struct_test;
CREATE VIEW db1.array_struct_test_view as select * from db1.array_struct_test;

CREATE TABLE IF NOT EXISTS db1.nested_struct_test
(
 property_id INT,
 service STRUCT<
                type: STRING
               ,provider: STRUCT<name:VARCHAR(50), id:TINYINT>
               >
);

CREATE TABLE db1.union_test(
    foo UNIONTYPE<int, double, array<string>, struct<a:int,b:string>>
) STORED AS ORC ;

CREATE TABLE db1.map_test(KeyValue String, RecordId map<int,string>); 

CREATE TABLE db1.classification_test(id STRING, name STRING, email STRING, gender STRING, age INT);

INSERT INTO
    db1.classification_test
VALUES
    ("1", "Foo Bar", "foo@bar.com", "M", 21),
    ("2", "John Doe", "john.doe@example.com", "M", 30),
    ("3", "Jane Doe", "jane.doe@abc.com", "F", 27);