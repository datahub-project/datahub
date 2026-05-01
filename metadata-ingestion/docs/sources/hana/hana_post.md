### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

Under the hood, [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana) uses the SAP HANA Python Driver hdbcli. Therefore it is compatible with HANA or HANA express versions since HANA SPS 2.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
