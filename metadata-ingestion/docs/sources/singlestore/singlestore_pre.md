### Prerequisites

In order to execute this source the user credentials needs the following privileges

- `grant select on DATABASE.* to 'USERNAME'@'%'`
- `grant show view on DATABASE.* to 'USERNAME'@'%'`
- `grant show routine on DATABASE.* to 'USERNAME'@'%'`

`SELECT` is required to see the table structure as well as for profiling.
