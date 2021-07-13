
-- Setup a "pokes" example table.
CREATE TABLE pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;

-- Setup a table with a special character.
CREATE TABLE `_test_table_underscore` (foo INT, bar STRING);

-- Create tables with struct and array types.
-- From https://stackoverflow.com/questions/57491644/correct-usage-of-a-struct-in-hive.
CREATE TABLE struct_test
(
 property_id INT,
 service STRUCT<
                type: STRING
               ,provider: ARRAY<INT>
               >
);

CREATE TABLE array_struct_test
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
INSERT INTO TABLE array_struct_test
select * from test_data;
