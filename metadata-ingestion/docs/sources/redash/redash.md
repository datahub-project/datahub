<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

Note! The integration can use an SQL parser to try to parse the tables the chart depends on. This parsing is disabled by default,
but can be enabled by setting `parse_table_names_from_sql: true`. The parser is based on the [`sqlglot`](https://pypi.org/project/sqlglot/) package.
