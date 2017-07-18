This directory contains configurations for both ETL and non-ETL jobs. At launch the backend service loads all `.job` files at the root directory level and runsx or schedules jobs accordingly. The syntax of job files are identical to Java's properties file, with the added support for resolving environmental variables using the `${var_name}` syntax.

Example job files are placed in the [templates](templates) directory. Make sure to change them to match your environment.
