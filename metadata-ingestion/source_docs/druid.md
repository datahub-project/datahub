# Druid

To install this plugin, run `pip install 'acryl-datahub[druid]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

**Note** It is important to define a explicitly define deny schema pattern for internal druid databases (lookup & sys)
if adding a schema pattern otherwise the crawler may crash before processing relevant databases.
This deny pattern is defined by default but is overriden by user-submitted configurations

```yml
source:
  type: druid
  config:
    # Point to broker address
    host_port: localhost:8082
    schema_pattern:
      deny:
        - "^(lookup|sys).*"
    # options is same as above
```
