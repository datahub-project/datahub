

-- Create a Top-k projection
CREATE TABLE readings (meter_id INT, reading_date TIMESTAMP, reading_value FLOAT);
CREATE PROJECTION readings_topk (meter_id, recent_date, recent_value) AS SELECT meter_id, reading_date, reading_value FROM readings LIMIT 5 OVER (PARTITION BY meter_id ORDER BY reading_date DESC);

-- Create a live agg projs
CREATE TABLE clicks(user_id IDENTITY(1,1), page_id INTEGER, click_time TIMESTAMP NOT NULL);
CREATE PROJECTION clicks_agg AS SELECT page_id, click_time::DATE click_date, COUNT(*) num_clicks FROM clicks GROUP BY page_id, click_time::DATE;


-- Create a VIEW
CREATE VIEW sampleview AS SELECT SUM(annual_income), customer_state
FROM public.customer_dimension
WHERE customer_key IN (SELECT customer_key FROM store.store_sales_fact)
GROUP BY customer_state ORDER BY customer_state ASC;

-- Step 1: Create library
\set libfile '\''/opt/sqlalchemy-vertica-dialect'/python/TransformFunctions.py\''
CREATE LIBRARY TransformFunctions AS '/opt/vertica/sdk/examples/python/TransformFunctions.py' LANGUAGE 'Python';


-- Step 2: Create functions
CREATE TRANSFORM FUNCTION tokenize AS NAME 'StringTokenizerFactory' LIBRARY TransformFunctions;
CREATE TRANSFORM FUNCTION topk AS NAME 'TopKPerPartitionFactory' LIBRARY TransformFunctions;

CREATE TABLE phrases (phrase VARCHAR(128));
COPY phrases FROM STDIN;
Word
The quick brown fox jumped over the lazy dog
\.

SELECT tokenize(phrase) OVER () FROM phrases;

-- Create a temp table

-- CREATE TEMPORARY TABLE sampletemp (a int, b int) ON COMMIT PRESERVE ROWS;
-- INSERT INTO sampletemp VALUES(1,2);

-- Create partition key
-- ALTER TABLE store.store_orders_fact PARTITION BY date_ordered::DATE GROUP BY DATE_TRUNC('month', (date_ordered)::DATE);
-- SELECT PARTITION_TABLE('store.store_orders_fact');
-- CREATE PROJECTION ytd_orders AS SELECT * FROM store.store_orders_fact ORDER BY date_ordered
--     ON PARTITION RANGE BETWEEN date_trunc('year',now())::date AND NULL;




SELECT start_refresh();