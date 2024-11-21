CREATE PROCEDURE [Foo].[NewProc] @ID INT
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
    END