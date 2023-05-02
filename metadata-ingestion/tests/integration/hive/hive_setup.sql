CREATE DATABASE IF NOT EXISTS db1;
CREATE DATABASE IF NOT EXISTS db2;
-- Setup a "pokes" example table.
CREATE TABLE IF NOT EXISTS db1.pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE db1.pokes;

CREATE TABLE IF NOT EXISTS db2.pokes (foo INT, bar STRING, CONSTRAINT pk_1173723383_1683022998392_0 primary key(foo) DISABLE NOVALIDATE NORELY);
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
 property_id INT,
 service array<STRUCT<
                type: STRING
               ,provider: ARRAY<INT>
               >>
);

WITH
test_data as (
    SELECT 989 property_id, array(NAMED_STRUCT('type','Cleaning','provider', ARRAY(587, 887)),
                      NAMED_STRUCT('type','Pricing','provider', ARRAY(932))
                      ) AS service
)
INSERT INTO TABLE db1.array_struct_test
select * from test_data;

CREATE TABLE IF NOT EXISTS db1.nested_struct_test
(
 property_id INT,
 service STRUCT<
                type: STRING
               ,provider: STRUCT<name:VARCHAR(50), id:TINYINT>
               >
);

CREATE TABLE db1.union_test(
    foo UNIONTYPE<int, double, array<string>, struct<a:int,b:string>, struct<c:int,d:double>>
);

CREATE TABLE db1.map_test(
    KeyValue String, 
    RecordId map<int,string>
); 