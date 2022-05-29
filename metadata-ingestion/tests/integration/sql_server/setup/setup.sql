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
      Name nvarchar(50)
      , CONSTRAINT PK_TempSales PRIMARY KEY NONCLUSTERED (TempID)
      , CONSTRAINT FK_TempSales_SalesReason FOREIGN KEY (TempID)
        REFERENCES Foo.Persons (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
   )
;
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