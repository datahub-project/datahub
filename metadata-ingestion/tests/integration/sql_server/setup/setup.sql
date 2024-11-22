DROP DATABASE IF EXISTS NewData;
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
GO
CREATE VIEW FooNew.View1 AS
SELECT LastName, FirstName
FROM FooNew.PersonsNew
WHERE Age > 18
GO

DROP DATABASE IF EXISTS DemoData;
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
CREATE VIEW Foo.PersonsView AS SELECT * FROM Foo.Persons;
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
DROP PROCEDURE IF EXISTS [Foo].[Proc.With.SpecialChar];
GO
CREATE PROCEDURE [Foo].[Proc.With.SpecialChar] @ID INT
AS
    SELECT @ID AS ThatDB;
GO

DROP PROCEDURE IF EXISTS [Foo].[NewProc];
GO
CREATE PROCEDURE [Foo].[NewProc]
    AS
    BEGIN
        --insert into items table from salesreason table
        insert into Foo.Items (ID, ItemName)
        SELECT TempID, Name
        FROM Foo.SalesReason;


       IF OBJECT_ID('Foo.age_dist', 'U') IS NULL
       BEGIN
            -- Create and populate if table doesn't exist
            SELECT Age, COUNT(*) as Count
            INTO Foo.age_dist
            FROM Foo.Persons
            GROUP BY Age
        END
        ELSE
        BEGIN
            -- Update existing table
            TRUNCATE TABLE Foo.age_dist;

            INSERT INTO Foo.age_dist (Age, Count)
            SELECT Age, COUNT(*) as Count
            FROM Foo.Persons
            GROUP BY Age
        END

        SELECT ID, Age INTO #TEMPTABLE FROM NewData.FooNew.PersonsNew
        
        UPDATE DemoData.Foo.Persons
        SET Age = t.Age
        FROM DemoData.Foo.Persons p
        JOIN #TEMPTABLE t ON p.ID = t.ID

    END
GO

EXEC Foo.NewProc
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