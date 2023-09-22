CREATE DATABASE NewData;
GO
USE NewData;
GO
CREATE TABLE ProductsNew (ID int, ProductName nvarchar(max));
GO
CREATE SCHEMA FooNew;
GO
CREATE TABLE FooNew.ItemsNew (ID int, ItemName nvarchar(max));
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
CREATE PROCEDURE Foo.DBs @ID INT
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
