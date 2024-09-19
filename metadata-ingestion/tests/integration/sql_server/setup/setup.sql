CREATE DATABASE NewData;
GO
USE NewData;
GO
CREATE TABLE ProductsNew (ID int, ProductName nvarchar(max), Price money);
GO
CREATE SCHEMA FooNew;
GO
CREATE TABLE FooNew.ItemsNew (ID int, ItemName nvarchar(max), Price smallmoney);
GO
CREATE TABLE FooNew.PersonsNew (
    ID int NOT NULL PRIMARY KEY,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int
);

CREATE DATABASE DemoData;
GO
USE DemoData;
GO
CREATE TABLE Products (ID int, ProductName nvarchar(max));
GO
CREATE SCHEMA Foo;
GO
CREATE TABLE Foo.Items (ID int, ItemName nvarchar(max));
GO
CREATE TABLE Foo.Persons (
    ID int NOT NULL PRIMARY KEY,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int
);
GO
CREATE TABLE Foo.SalesReason 
   (
      TempID int NOT NULL,
      SomeId UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
      Name nvarchar(50)
      , CONSTRAINT PK_TempSales PRIMARY KEY NONCLUSTERED (TempID)
      , CONSTRAINT FK_TempSales_SalesReason FOREIGN KEY (TempID)
        REFERENCES Foo.Persons (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
   )
;
GO
CREATE PROCEDURE [Foo].[Proc.With.SpecialChar] @ID INT
AS
    SELECT @ID AS ThatDB;
GO

GO
EXEC sys.sp_addextendedproperty
@name = N'MS_Description',
@value = N'Description for table Items of schema Foo.',
@level0type = N'SCHEMA', @level0name = 'Foo',
@level1type = N'TABLE',  @level1name = 'Items';
GO

GO
EXEC sys.sp_addextendedproperty
@name = N'MS_Description',
@value = N'Description for column LastName of table Persons of schema Foo.',
@level0type = N'SCHEMA', @level0name = 'Foo',
@level1type = N'TABLE', @level1name = 'Persons',
@level2type = N'COLUMN',@level2name = 'LastName';
GO
USE msdb ;
GO
EXEC dbo.sp_add_job
    @job_name = N'Weekly Demo Data Backup' ;
GO
EXEC sp_add_jobstep
    @job_name = N'Weekly Demo Data Backup',
    @step_name = N'Set database to read only',
    @database_name = N'DemoData',
    @subsystem = N'TSQL',
    @command = N'ALTER DATABASE DemoData SET READ_ONLY',
    @retry_attempts = 5,
    @retry_interval = 5 ;
GO
EXEC dbo.sp_add_schedule
    @schedule_name = N'RunOnce',
    @freq_type = 1,
    @active_start_time = 233000 ;
GO
EXEC sp_attach_schedule
   @job_name = N'Weekly Demo Data Backup',
   @schedule_name = N'RunOnce';
GO
EXEC dbo.sp_add_jobserver
    @job_name = N'Weekly Demo Data Backup'
GO

CREATE DATABASE LINEAGEDB_SLAVE;
GO
USE LINEAGEDB_SLAVE;
GO
CREATE TABLE [table_for_synonym] (id VARCHAR(MAX));
GO

CREATE DATABASE LINEAGEDB_MASTER;
GO
USE LINEAGEDB_MASTER;
GO
CREATE SCHEMA schema_with_lineage;
GO
CREATE TABLE [schema_with_lineage].[table_number_one] (id VARCHAR(MAX));
GO
CREATE TABLE [schema_with_lineage].[table_number_two] (is_updated BIT);
GO
CREATE VIEW [schema_with_lineage].[view_of_table_number_two] AS SELECT is_updated FROM [schema_with_lineage].[table_number_two];
GO
CREATE TABLE [schema_with_lineage].[table_number_three] (order_number INT, weight VARCHAR(MAX));
GO
CREATE SYNONYM [schema_with_lineage].[ghost_table] FOR [LINEAGEDB_SLAVE].[dbo].[table_for_synonym];
GO
CREATE PROCEDURE [schema_with_lineage].[procedure_number_one]
AS
BEGIN
	DECLARE
		@t1 VARCHAR(MAX)
	SELECT @t1 = [id] FROM [schema_with_lineage].[table_number_one];
END;
GO       
CREATE PROCEDURE [schema_with_lineage].[procedure_number_two]
AS
BEGIN
	DECLARE @t1 INT = 1
	UPDATE [schema_with_lineage].[view_of_table_number_two] SET [is_updated] = CASE
                                                                                   WHEN @t1 = 1 THEN 1
		                                                                           ELSE 0 
		                                                                       END;
END;      
GO
CREATE PROCEDURE [schema_with_lineage].[procedure_number_three]
AS
BEGIN
	IF (SELECT order_number FROM [schema_with_lineage].[table_number_three]) > 10
		INSERT INTO [schema_with_lineage].[table_number_three]([weight]) VALUES ('high')
	ELSE
		INSERT INTO [schema_with_lineage].[table_number_three]([weight]) VALUES ('low')
END;
GO 
CREATE PROCEDURE [schema_with_lineage].[procedure_number_four]
AS
BEGIN
	DECLARE @t1 VARCHAR(MAX);
	SELECT @t1 = [id] FROM [schema_with_lineage].[ghost_table];
	INSERT into [schema_with_lineage].[ghost_table] values (UPPER(@t1));
	
END;