-- SPDX-License-Identifier: Apache-2.0
--
-- This file is unmodified from its original version developed by Acryl Data, Inc.,
-- and is now included as part of a repository maintained by the National Digital Twin Programme.
-- All support, maintenance and further development of this code is now the responsibility
-- of the National Digital Twin Programme.

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

        SELECT * INTO #TempTable FROM NewData.FooNew.PersonsNew
        
        UPDATE DemoData.Foo.Persons
        SET Age = t.Age
        FROM DemoData.Foo.Persons p
        JOIN #TempTable t ON p.ID = t.ID

    END