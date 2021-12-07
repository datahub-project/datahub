# Introduction to Scheduling Metadata Ingestion

Using [DataHub CLI](../../docs/cli.md) we can do ingestion of our metadata given a recipe file `/home/ubuntu/datahub_ingest/mysql_to_datahub.yml`.

```
datahub ingest -c /home/ubuntu/datahub_ingest/mysql_to_datahub.yml
```

This will ingest whatever source is configured in `mysql_to_datahub.yml` recipe file. This does ingestion once. As the source system changes we would like to have the changes reflected in DataHub. To do this someone will need to re-run the ingestion command using a recipe file. 

An alternate to running the command manually we can schedule the ingestion to run on a regular basis. In this section we give some examples of how scheduling ingestion of metadata into DataHub can be done.