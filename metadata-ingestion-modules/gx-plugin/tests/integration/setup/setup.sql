-- 2 tables at least with common column names
-- at least 1 table with partitioning column
-- at least one agg column

CREATE TABLE foo1 (
    id INTEGER NOT NULL,
    name VARCHAR (50) NOT NULL,
    category VARCHAR (50) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE foo2 (
    id INTEGER NOT NULL,
    name VARCHAR (50) NOT NULL,
    start_date DATE,
    start_year INTEGER,
    score NUMERIC(2),
    PRIMARY KEY (id)
);

CREATE TABLE "FOO3" (
    id INTEGER NOT NULL,
    name VARCHAR (50) NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO foo1 (id, name, category) VALUES (1, 'name1', 'catA');
INSERT INTO foo1 (id, name, category) VALUES (2, 'name2', 'catA');
INSERT INTO foo1 (id, name, category) VALUES (3, 'name3', 'catB');
INSERT INTO foo1 (id, name, category) VALUES (4, 'name4', 'catB');
INSERT INTO foo1 (id, name, category) VALUES (5, 'name5', 'catA');


INSERT INTO foo2 (id, name, start_date, start_year, score) VALUES (1, 'name1', TO_DATE('2021-12-21','YYYY-MM-DD'), 2021, 10);
INSERT INTO foo2 (id, name, start_date, start_year, score) VALUES (2, 'name2', TO_DATE('2021-12-22','YYYY-MM-DD'), 2021, 7);
INSERT INTO foo2 (id, name, start_date, start_year, score) VALUES (3, 'name3', TO_DATE('2021-12-21','YYYY-MM-DD'), 2021, 8);
INSERT INTO foo2 (id, name, start_date, start_year, score) VALUES (4, 'name4', TO_DATE('2021-12-22','YYYY-MM-DD'), 2021, 9);
INSERT INTO foo2 (id, name, start_date, start_year, score) VALUES (5, 'name5', TO_DATE('2021-12-23','YYYY-MM-DD'), 2021, 6);

INSERT INTO "FOO3" (id, name) VALUES (1, 'name');