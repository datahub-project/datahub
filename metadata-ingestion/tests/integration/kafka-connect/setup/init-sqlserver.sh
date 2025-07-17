#!/bin/bash

echo "Starting SQL Server in background..."
/opt/mssql/bin/sqlservr &
SQL_PID=$!

echo "Waiting for SQL Server to start..."
sleep 45

# Wait for SQL Server to be ready to accept connections
echo "Testing SQL Server connection..."
for i in {1..30}; do
    if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Password123!" -C -Q "SELECT 1" > /dev/null 2>&1; then
        echo "SQL Server is ready!"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "SQL Server failed to start within timeout"
        exit 1
    fi
    
    echo "Attempt $i/30: SQL Server not ready yet..."
    sleep 3
done

echo "Running SQL Server setup script..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Password123!" -C -i /usr/src/app/sqlserver-setup.sql

if [ $? -eq 0 ]; then
    echo "SQL Server setup completed successfully!"
else
    echo "SQL Server setup failed!"
    exit 1
fi

# Keep container running
echo "SQL Server initialization complete. Container is ready."
wait $SQL_PID