Metabase databases will be mapped to a DataHub platform based on the engine listed in the [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response.
This mapping can be customized by using the `engine_platform_map` config option.
For example, to map databases using the `athena` engine to the underlying datasets in the `glue` platform, the following option can be used:
```yml
  engine_platform_map:
    athena: glue
```

DataHub will try to determine database name from Metabase [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response.
However, the name can be overridden from `database_alias_map` for a given database connected to Metabase.
For example, to map databases using the `postgres` platform, the following option can be used:
```yml
  database_alias_map:
    postgres:
      "a1b2c3": "my_database"
      "d4e5f6": "my_other_database"
```

## Compatibility

Metabase version [v0.41.2](https://www.metabase.com/start/oss/)
