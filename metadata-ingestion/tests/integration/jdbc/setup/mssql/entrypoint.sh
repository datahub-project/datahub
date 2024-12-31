#!/bin/bash
/opt/mssql/bin/sqlservr &
sleep 30
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Password123!" -i /setup/init.sql
tail -f /dev/null