### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Common Configurations

##### Data Lake Mode — Ingest Structured Files

Ingest CSV, Parquet, and Excel files from specific SharePoint sites:

```yaml
source:
  type: sharepoint
  config:
    auth:
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
    site:
      hostname: "myorg.sharepoint.com"
      site_pattern:
        allow:
          - "/sites/DataEngineering"
          - "/sites/Analytics"
      library_pattern:
        deny:
          - "^Style Library$"
          - "^SiteAssets$"
    mode: data_lake
    file_types:
      - csv
      - parquet
      - xlsx
    stateful_ingestion:
      enabled: true
```

##### Document Mode — Ingest Pages and Documents

Ingest SharePoint site pages as Document entities for semantic search:

```yaml
source:
  type: sharepoint
  config:
    auth:
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
    site:
      hostname: "myorg.sharepoint.com"
    mode: document
    ingest_pages: true
    ingest_files: true
    stateful_ingestion:
      enabled: true
```

##### Skip Personal OneDrive Sites

By default, personal OneDrive sites (paths starting with `/personal/`) are excluded. To include them:

```yaml
site:
  hostname: "myorg.sharepoint.com"
  skip_personal_sites: false
```

##### Certificate Authentication (Production)

Use a certificate instead of a client secret for production workloads:

```yaml
auth:
  tenant_id: "${AZURE_TENANT_ID}"
  client_id: "${AZURE_CLIENT_ID}"
  certificate_path: "/path/to/private-key.pem"
```

### Limitations

Module behavior is constrained by the Microsoft Graph API, SharePoint permissions, and the metadata exposed by each site.

#### Known Limitations

- **Schema inference**: Only CSV, TSV, JSON, JSONL, Parquet, Avro, and Excel files support schema inference. PDF files are ingested as Datasets but without schema.
- **Large files**: Schema inference downloads files into memory. Very large files may cause memory pressure; consider setting `max_files` to limit scope.
- **SharePoint Server (on-premises)**: Not supported. Only SharePoint Online (Microsoft 365) via the Graph API is supported.
- **Graph API rate limits**: The connector retries on HTTP 429 and 401, but sustained high-volume ingestion may hit tenant-level throttling.
- **Canvas-only pages**: The `get_page_html` endpoint requires the SharePoint Pages API (available in most tenants). Sites that do not expose the canvas layout API will return empty content.

### Troubleshooting

**`Failed to acquire token: invalid_client`**

- Verify `tenant_id`, `client_id`, and `client_secret` (or `certificate_path`) are correct
- Ensure the app registration has the `Sites.Read.All` and `Files.Read.All` **Application** permissions (not Delegated) and that admin consent has been granted

**`403 Forbidden` on site or file requests**

- Confirm admin consent was granted for the Graph API permissions
- Check that no Conditional Access policy blocks service principal access from the ingestion host

**Empty schema for CSV or Parquet files**

- Verify the file is accessible via its `@microsoft.graph.downloadUrl` (not expired)
- Check that `file_types` includes the extension you expect (e.g. `csv`)
- Files larger than available memory will fail silently during schema inference; reduce `max_files` and re-run

**No pages emitted in document mode**

- Confirm `ingest_pages: true` is set
- Verify the service principal has `Sites.Read.All` permission and that the SharePoint tenant enables the Pages API
- Check ingestion logs for `SharePointClientError` on the pages endpoint

**Personal sites unexpectedly excluded**

- `skip_personal_sites` defaults to `true`. Set it to `false` under `site:` to include personal OneDrive sites
