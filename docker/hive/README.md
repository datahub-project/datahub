# About this Docker-compose.yml
Based on this `[docker-hive](https://github.com/big-data-europe/docker-hive)` repo, and make some modification with `docker-compose.yml`

1. In `line 47`, the port of `prestodb` will be conflict with one of docker containers of datahub, change it to `7081:8080` shoud fix the problem

2. add `networks` in the end of file to make it same as what datahub is using

```
networks:
 default:
 name: datahub_network
```
# Seed a Hive table
```
$ docker-compose exec hive-server bash
  # /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
  > CREATE TABLE pokes (foo INT, bar STRING);
  > LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
  > SELECT * FROM pokes;
```
We create a `pokes` table with two columns: foo and bar. 

# Run it
```
docker-compose up
```
Wait for related containers successfully up and running
