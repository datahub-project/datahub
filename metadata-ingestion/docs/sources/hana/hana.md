### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Integration Details

The implementation uses the [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana). The SQLAlchemy Dialect for SAP HANA is an open-source project hosted at GitHub that is actively maintained by SAP SE, and is not part of a licensed SAP HANA edition or option. It is provided under the terms of the project license. Please notice that sqlalchemy-hana isn't an official SAP product and isn't covered by SAP support.

#### Compatibility

Under the hood, [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana) uses the SAP HANA Python Driver hdbcli. Therefore it is compatible with HANA or HANA express versions since HANA SPS 2.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
