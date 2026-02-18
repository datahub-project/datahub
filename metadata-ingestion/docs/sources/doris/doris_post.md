## Migration from MySQL Connector

If you were previously ingesting Doris using the MySQL connector, switch to the dedicated Doris connector for better support:

**Configuration changes:**

- Change `type: mysql` → `type: doris`
- Change port: `3306` → `9030`

**Important:** Dataset URNs will change from `platform:mysql` to `platform:doris`. This creates new entities in DataHub. Enable stateful ingestion with `remove_stale_metadata: true` to automatically clean up old MySQL-based entities.
