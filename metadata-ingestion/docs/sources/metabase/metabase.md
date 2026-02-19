Metabase databases will be mapped to a DataHub platform based on the engine listed in the
[api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response. This mapping can be
customized by using the `engine_platform_map` config option. For example, to map databases using the `athena` engine to
the underlying datasets in the `glue` platform, the following snippet can be used:

```yml
engine_platform_map:
  athena: glue
```

DataHub will try to determine database name from Metabase [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database)
payload. However, the name can be overridden from `database_alias_map` for a given database connected to Metabase.

If several platform instances with the same platform (e.g. from several distinct clickhouse clusters) are present in DataHub,
the mapping between database id in Metabase and platform instance in DataHub may be configured with the following map:

```yml
database_id_to_instance_map:
  "42": platform_instance_in_datahub
```

The key in this map must be string, not integer although Metabase API provides `id` as number.
If `database_id_to_instance_map` is not specified, `platform_instance_map` is used for platform instance mapping. If none of the above are specified, platform instance is not used when constructing `urn` when searching for dataset relations.

If needed it is possible to exclude collections from other users by setting the following configuration:

```yaml
exclude_other_user_collections: true
```

### Excluding Personal Collections

When using an admin API key, the `exclude_other_user_collections` option may not work as expected because API keys don't have a "personal collection" to compare against. In this case, you can use `exclude_personal_collections` to reliably filter out all personal collections:

```yaml
exclude_personal_collections: true
```

This option identifies personal collections by checking for the `personal_owner_id` field in the Metabase API response, which is more reliable when using admin-level API keys. It filters both dashboards and cards from personal collections.

| Option                           | Use Case                                                                     |
| -------------------------------- | ---------------------------------------------------------------------------- |
| `exclude_other_user_collections` | When authenticating with username/password as a regular user                 |
| `exclude_personal_collections`   | When using an admin API key and you want to exclude all personal collections |

## Compatibility

Metabase version [v0.48.3](https://www.metabase.com/start/oss/)
