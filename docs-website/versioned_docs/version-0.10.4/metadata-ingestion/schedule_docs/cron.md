---
title: Using Cron
slug: /metadata-ingestion/schedule_docs/cron
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/schedule_docs/cron.md
---

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
