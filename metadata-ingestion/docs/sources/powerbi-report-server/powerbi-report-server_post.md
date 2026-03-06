### Limitations

- Ingestion is focused on report-server catalog assets and does not model all report-server object types as first-class DataHub entities.
- Upstream dataset/datasource lineage is limited compared with cloud Power BI ingestion.
- Metadata quality depends on permissions granted to the report-server user.

### Troubleshooting

- **Unauthorized responses**: verify the configured account has access to the report server and target folders.
- **Missing reports**: confirm report types are supported and the crawler scope includes the expected catalog paths.
- **Connection issues**: validate Report Server URL reachability and authentication settings from the ingestion runtime.
