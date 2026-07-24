### Overview

The `informix` module ingests metadata from IBM Informix into DataHub via JDBC. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### JDBC Driver

This source connects through the IBM Informix JDBC driver, which is not bundled with `acryl-datahub` because it is proprietary. Provide it in one of two ways:

1. **Bring your own jars** (recommended for air-gapped or license-restricted environments): set `driver_jar_paths` to explicit paths for the `com.ibm.informix:jdbc` jar and the `org.mongodb:bson` jar it depends on. No download is attempted when this is set.
2. **Auto-download**: set `accept_ibm_jdbc_license: true` to have the source download and checksum-verify the driver from Maven Central at runtime, caching it under `~/.datahub/jars/informix` (override with `driver_cache_dir`). Pin exact versions with `jdbc_driver_version` / `bson_version` if needed.

:::caution

Auto-download requires accepting the [IBM Informix JDBC Driver Software License Agreement](http://www-03.ibm.com/software/sla/sladb.nsf/doclookup/CA4476C0AF8346EC852579290012D218?OpenDocument). Setting `accept_ibm_jdbc_license: true` is your confirmation that you accept these terms; DataHub does not redistribute the driver.

:::

#### Permissions

The connecting user needs `SELECT` on the Informix system catalog to enumerate metadata:

- `systables`, `syscolumns` — tables, views, and columns.
- `sysconstraints`, `sysindexes` — primary key detection.

No access to user table data is required; this module does not profile or sample rows.
