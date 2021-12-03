# Scheduling Ingestion using Cron

To follow this guide you need to use [DataHub CLI](../../docs/cli.md).

Assume you have a receipe file `/home/ubuntu/example.yml` on your machine
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
  type: file
  config:
    filename: ./path/to/mce/file.json
```

You can use crontab to schedule ingestion to run five minutes after midnight, every day. Read through [crontab docs](https://man7.org/linux/man-pages/man5/crontab.5.html) for more options related to scheduling.

```
5 0 * * * datahub ingest -c /home/ubuntu/example.yml
```
