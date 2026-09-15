### Overview

The `db2` module ingests metadata from Db2 into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Permissions

The user requires `SELECT` privileges on the system schema and tables:

- Db2 for LUW: `SYSCAT.*`
- Db2 for z/OS: `SYSIBM.*`
- Db2 for IBM i (AS/400): `QSYS2.*`

Additionally, when profiling is enabled, the user will need `SELECT` privileges on the tables to be profiled.

#### Authentication and TLS/SSL

Authentication is done by default via a Db2 username and password over a non-encrypted connection.

Other authentication methods as well as TLS/SSL may be configured using the `uri_args` config option, e.g.

```yaml
uri_args:
  Security: SSL
```
