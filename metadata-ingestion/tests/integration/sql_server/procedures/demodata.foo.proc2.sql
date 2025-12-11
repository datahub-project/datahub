-- SPDX-License-Identifier: Apache-2.0
--
-- This file is unmodified from its original version developed by Acryl Data, Inc.,
-- and is now included as part of a repository maintained by the National Digital Twin Programme.
-- All support, maintenance and further development of this code is now the responsibility
-- of the National Digital Twin Programme.

CREATE PROCEDURE [foo].[proc2]
    AS
    BEGIN
        --insert into items table from salesreason table
        insert into foo.items (id, itemame)
        SELECT tempid, name
        FROM foo.salesreason;


       IF OBJECT_ID('foo.age_dist', 'U') IS NULL

       BEGIN
            -- Create and populate if table doesn't exist
            SELECT age, COUNT(*) as count
            INTO foo.age_dist
            FROM foo.persons
            GROUP BY age
        END
        ELSE
        BEGIN
            -- Update existing table
            TRUNCATE TABLE foo.age_dist;

            INSERT INTO foo.age_dist (age, count)
            SELECT age, COUNT(*) as count
            FROM foo.persons
            GROUP BY age
        END

        SELECT * INTO #temptable FROM newdata.foonew.personsnew
        
        UPDATE demodata.foo.persons
        SET age = t.age
        FROM demodata.foo.persons p
        JOIN #temptable t ON p.ID = t.ID
    END