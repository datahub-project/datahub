<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

## Integration Details

The implementation uses the [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana). The SQLAlchemy Dialect for SAP HANA is an open-source project hosted at GitHub that is actively maintained by SAP SE, and is not part of a licensed SAP HANA edition or option. It is provided under the terms of the project license. Please notice that sqlalchemy-hana isn't an official SAP product and isn't covered by SAP support.

## Compatibility

Under the hood, [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana) uses the SAP HANA Python Driver hdbcli. Therefore it is compatible with HANA or HANA express versions since HANA SPS 2.
