# Apache Doris Connector Documentation

## File Organization

### User Documentation

- **`doris_pre.md`** - User-facing prerequisites and setup guide
- **`doris_recipe.yml`** - Example recipe configuration
- **`README.md`** (this file) - Documentation overview

## Quick Links

- [Apache Doris Official Site](https://doris.apache.org/)
- [Doris MySQL Compatibility](https://doris.apache.org/docs/query-data/mysql-compatibility/)
- [Doris System Tables](https://doris.apache.org/docs/admin-manual/system-tables/)

## Implementation Notes

The Apache Doris connector extends the MySQL connector since Doris uses the MySQL protocol. Key implementation details:

1. **Port**: Default 9030 (not 3306)
2. **Stored Procedures**: Not supported (fields hidden from docs, `information_schema.ROUTINES` is always empty)
3. **System Tables**: Most `information_schema` tables work, but `ROUTINES` is a compatibility stub
4. **Data Types**: Full support for Doris-specific types:
   - `HLL` (HyperLogLog) → Mapped to BytesTypeClass
   - `BITMAP` → Mapped to BytesTypeClass
   - `ARRAY` → Mapped to ArrayTypeClass
   - `JSONB` → Mapped to RecordTypeClass
   - `QUANTILE_STATE` → Mapped to BytesTypeClass
5. **Type Registration**: All types are registered with SQLAlchemy and properly mapped to DataHub types

## Testing

Integration tests are located in `tests/integration/doris/`. See the test README for details on running tests.

## Contributing

When updating the Doris connector:

1. Update `doris_pre.md` for user-facing changes
2. Add/update type registrations in `doris.py` if new Doris types are added
3. Update unit tests (`test_doris_source.py`) for new types
4. Update integration tests if behavior changes
5. Regenerate golden files if output format changes
