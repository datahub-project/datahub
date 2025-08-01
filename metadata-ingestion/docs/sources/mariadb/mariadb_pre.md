### Prerequisites

In order to execute this source the user credentials needs the following privileges

- `grant select on DATABASE.* to 'USERNAME'@'%'`
- `grant show view on DATABASE.* to 'USERNAME'@'%'`

For stored procedure ingestion (enabled by default), additional privileges are required:

- `grant select on information_schema.ROUTINES to 'USERNAME'@'%'`
- `grant execute on DATABASE.* to 'USERNAME'@'%'` (optional, for SHOW CREATE PROCEDURE)

`select` is required to see the table structure as well as for profiling.
`select` on information_schema.ROUTINES is required to discover stored procedures.
`execute` privilege allows for better procedure definition extraction but is optional.
