### Permissions

In order to execute this source the user needs `SELECT` privileges on the system schema and tables:

- Db2 for LUW: `SYSCAT.*`
- Db2 for z/OS: `SYSIBM.*`
- Db2 for IBM i (AS/400): `QSYS2.*`

Additionally, when profiling is enabled, the user will need `SELECT` privileges on the tables to be profiled.

### Authentication and TLS/SSL

Authentication is done by default via a Db2 username and password over a non-encrypted connection.

Other authentication methods as well as TLS/SSL may be configured using the `uri_args` config option, e.g.

```yaml
uri_args:
  Security: SSL
```

### Db2 Platform Support

This source has been tested against:

- Db2 for LUW using the default CLI driver (already included when installing the `acryl-datahub[db2]` package)
- Db2 for IBM i (AS/400) using the IBM i Access ODBC Driver (not included and so must be installed separately by the end user; see [Using the IBM i Access ODBC Driver](#using-the-ibm-i-access-odbc-driver) below)

This source also includes nominal support for:

- Db2 for IBM i (AS/400) using the CLI driver ([requires Db2 Connect](https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information))
- Db2 for z/OS using the CLI driver ([requires Db2 Connect](https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information))
