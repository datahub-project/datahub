# how to load data into Hive Server (example from https://github.com/big-data-europe/docker-hive0)
`docker-compose exec hive-server bash`   
`> /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000`  
`>> CREATE TABLE pokes (foo INT, bar STRING);`  
`>> LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;  `