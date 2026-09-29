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