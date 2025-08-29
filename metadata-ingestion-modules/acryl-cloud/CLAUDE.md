# CLAUDE.md

- When using `polars`
  - We should try to avoid using the map_elements call with a lambda function, as it will be a performance bottlenece since it processes each row individually instead of leveraging Polars' vectorized operations.
