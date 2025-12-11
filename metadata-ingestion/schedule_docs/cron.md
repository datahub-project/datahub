<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

# Using Cron

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

We can use crontab to schedule ingestion to run five minutes after midnight, every day using [DataHub CLI](../../docs/cli.md).

```
5 0 * * * datahub ingest -c /home/ubuntu/datahub_ingest/mysql_to_datahub.yml
```

Read through [crontab docs](https://man7.org/linux/man-pages/man5/crontab.5.html) for more options related to scheduling.
