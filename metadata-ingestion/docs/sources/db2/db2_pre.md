### Prerequisites

In order to execute this source the user needs `SELECT` privileges on the system schemas:

- Db2 for LUW: `SYSCAT`
- Db2 for z/OS: `SYSIBM`
- Db2 for i (AS400): `QSYS2`

Additionally, when profiling is enabled, the user will need `SELECT` privileges on the tables to be profiled.

### Db2 Platforms

This source has been tested against Db2 for LUW. Additionally, it includes nominal support for Db2 for z/OS and Db2 for i (AS400).

### Authentication and TLS/SSL

Authentication is done via a Db2 user and password over a non-encrypted connection.

Other authentication methods as well as TLS/SSL may be configured by overriding the `connect_uri` config option.
