# Using Cron

To follow this guide you need to use [DataHub CLI](../../docs/cli.md).

Assume you have a recipe file `/home/ubuntu/datahub_ingest/mysql_to_datahub.yml` on your machine
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

You can use crontab to schedule ingestion to run five minutes after midnight, every day. Read through [crontab docs](https://man7.org/linux/man-pages/man5/crontab.5.html) for more options related to scheduling.

```
5 0 * * * datahub ingest -c /home/ubuntu/datahub_ingest/mysql_to_datahub.yml
```
