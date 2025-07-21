-- SQL Server setup script for Kafka Connect integration tests

CREATE DATABASE TestDB;
GO

USE TestDB;
GO

-- Note: dbo schema already exists by default, so we don't need to create it

CREATE TABLE dbo.test_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(50) NOT NULL,
    email NVARCHAR(100),
    created_date DATETIME2 DEFAULT GETDATE()
);
GO

INSERT INTO dbo.test_table (name, email) VALUES
    ('John Doe', 'john.doe@example.com'),
    ('Jane Smith', 'jane.smith@example.com'),
    ('Bob Johnson', 'bob.johnson@example.com');
GO

-- Enable SQL Server Agent (required for CDC)
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'Agent XPs', 1;
RECONFIGURE;
GO

-- Enable CDC on the database (requires Developer/Enterprise/Standard edition)
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on the test table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'test_table',
    @role_name = NULL;
GO

PRINT 'SQL Server test database setup completed successfully';