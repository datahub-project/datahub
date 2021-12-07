# Introduction to Scheduling Metadata Ingestion

Given a recipe file `/home/ubuntu/datahub_ingest/mysql_to_datahub.yml`.
```
source:
  type: mysql
  config:
    # Coordinates
    host_port: localhost:3306
    database: dbname

    # Credentials
    username: root
    password: example

sink:
 type: datahub-rest 
 config:
  server: http://localhost:8080
```

We can do ingestion of our metadata using [DataHub CLI](../../docs/cli.md) as follows

```
datahub ingest -c /home/ubuntu/datahub_ingest/mysql_to_datahub.yml
```

This will ingest metadata from the `mysql` source which is configured in the recipe file. This does ingestion once. As the source system changes we would like to have the changes reflected in DataHub. To do this someone will need to re-run the ingestion command using a recipe file. 

An alternate to running the command manually we can schedule the ingestion to run on a regular basis. In this section we give some examples of how scheduling ingestion of metadata into DataHub can be done.