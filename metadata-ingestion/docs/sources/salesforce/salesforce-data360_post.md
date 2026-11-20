### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- Data Lake Objects, data streams and DLO-to-DMO mappings are not ingested yet.
- DMOs are named by their API name only, so a DMO in a non-default dataspace shares its URN with the same DMO in other dataspaces. Use `dataspace` to choose which dataspace relationships are read from.
- The DMO list endpoint takes about 0.2 seconds per DMO, so a run with `include_disabled_dmos: true` on a new org takes several minutes.

### Troubleshooting

If no DMOs are ingested, check that Data 360 is provisioned in the org and that the ingestion user has a Data Cloud permission set.
