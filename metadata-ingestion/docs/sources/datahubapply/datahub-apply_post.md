### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features. This module focuses on applying metadata updates rather than extracting metadata from external systems.

### Limitations

- This module does not discover source metadata; it only applies configured updates to existing DataHub entities.
- Incorrect URNs or selectors can lead to partial updates or no-ops.

### Troubleshooting

- Validate target URNs and entity existence before running large apply jobs.
- Start with a small scoped recipe to verify permissions and expected update behavior.
- Review ingestion logs for validation or authorization errors returned by DataHub APIs.
