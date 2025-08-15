
CREATE KEYSPACE cass_test_1 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE KEYSPACE cass_test_2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE cass_test_1.information (
    person_id int PRIMARY KEY,
    last_updated timestamp,
    details text
);

CREATE TABLE cass_test_1.people (
    person_id int PRIMARY KEY,
    name text,
    email text
);

CREATE TABLE cass_test_2.tasks (
    task_id int PRIMARY KEY,
    last_updated timestamp,
    details text,
    status text
);

CREATE MATERIALIZED VIEW cass_test_2.task_status AS
SELECT
    task_id,
    status
FROM cass_test_2.tasks
WHERE status IS NOT NULL AND task_id IS NOT NULL
PRIMARY KEY (task_id, status);

-- Create Keyspace with comments
CREATE KEYSPACE IF NOT EXISTS example_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use Keyspace
USE example_keyspace;

-- Table with non-counter column types
CREATE TABLE IF NOT EXISTS all_data_types (
    id uuid PRIMARY KEY,
    ascii_column ascii,
    bigint_column bigint,
    blob_column blob,
    boolean_column boolean,
    date_column date,
    decimal_column decimal,
    double_column double,
    float_column float,
    inet_column inet,
    int_column int,
    list_column list<text>,
    map_column map<text, text>,
    set_column set<text>,
    smallint_column smallint,
    text_column text,
    time_column time,
    timestamp_column timestamp,
    timeuuid_column timeuuid,
    tinyint_column tinyint,
    tuple_column tuple<int, text>,
    uuid_column uuid,
    varchar_column varchar,
    varint_column varint,
    frozen_map_column frozen<map<text, text>>,
    frozen_list_column frozen<list<text>>,
    frozen_set_column frozen<set<text>>
) WITH COMMENT = 'Table containing all supported Cassandra data types, excluding counters';

-- Separate table for counters
CREATE TABLE IF NOT EXISTS counter_table (
    id uuid PRIMARY KEY,
    counter_column counter
) WITH COMMENT = 'Separate table containing only counter column';

-- Sample view
CREATE MATERIALIZED VIEW IF NOT EXISTS example_view_1 AS
    SELECT id, ascii_column, bigint_column
    FROM all_data_types
    WHERE id IS NOT NULL AND ascii_column IS NOT NULL
    PRIMARY KEY (id, ascii_column) WITH COMMENT = 'Example view definition with id and ascii_column';

CREATE MATERIALIZED VIEW IF NOT EXISTS example_view_2 AS
    SELECT id, ascii_column, float_column
    FROM all_data_types
    WHERE id IS NOT NULL AND ascii_column IS NOT NULL
    PRIMARY KEY (id, ascii_column) WITH COMMENT = 'Example view definition with id and ascii_column';

-- Table created for profilling
CREATE TABLE IF NOT EXISTS shopping_cart (
userid text PRIMARY KEY,
item_count int,
last_update_timestamp timestamp
);

-- Insert some data
INSERT INTO shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('9876', 2, '2024-11-01T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('1234', 5, '2024-11-02T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('1235', 100, '2024-11-03T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('1236', 50, '2024-11-04T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('1237', 75, '2024-11-05T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, last_update_timestamp)
VALUES ('1238', '2024-11-06T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, last_update_timestamp)
VALUES ('1239', '2024-11-07T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, last_update_timestamp)
VALUES ('1240', '2024-11-08T00:00:00.000+0000');

INSERT INTO shopping_cart
(userid, last_update_timestamp)
VALUES ('1241', '2024-11-09T00:00:00.000+0000');