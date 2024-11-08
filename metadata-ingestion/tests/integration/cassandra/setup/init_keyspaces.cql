
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
