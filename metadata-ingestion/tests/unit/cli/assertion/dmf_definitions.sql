
-- Start of Assertion 5c32eef47bd763fece7d21c7cbf6c659

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__5c32eef47bd763fece7d21c7cbf6c659 (ARGT TABLE(col_date DATE))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:5c32eef47bd763fece7d21c7cbf6c659 of type volume'
            AS
            $$
            select case when metric <= 1000 then 1 else 0 end from (select count(*) as metric from TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES )
            $$;
            
-- End of Assertion 5c32eef47bd763fece7d21c7cbf6c659

-- Start of Assertion 04be4145bd8de10bed3dfcb0cee57842

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__04be4145bd8de10bed3dfcb0cee57842 (ARGT TABLE(col_date DATE))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:04be4145bd8de10bed3dfcb0cee57842 of type field'
            AS
            $$
            select case when metric=0 then 1 else 0 end from (select $1 as metric from (select count(*)
        from TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES where col_date is null))
            $$;
            
-- End of Assertion 04be4145bd8de10bed3dfcb0cee57842

-- Start of Assertion 4dfda3a192a48f5b5b733986c145e8db

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__4dfda3a192a48f5b5b733986c145e8db (ARGT TABLE(value INT))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:4dfda3a192a48f5b5b733986c145e8db of type field'
            AS
            $$
            select case when metric <= 0 then 1 else 0 end from (select sum($1) as metric from (select case when value between 0 and 10 then 0 else 1 end
        from TEST_DB.PUBLIC.TEST_ASSERTIONS where value is not null))
            $$;
            
-- End of Assertion 4dfda3a192a48f5b5b733986c145e8db

-- Start of Assertion 2d4f2da1e661ffba0c69db7e6c75c0fb

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__2d4f2da1e661ffba0c69db7e6c75c0fb (ARGT TABLE(value INT))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:2d4f2da1e661ffba0c69db7e6c75c0fb of type sql'
            AS
            $$
            select case when metric=5 then 1 else 0 end from (select $1 as metric from (select mode(value) from test_db.public.test_assertions))
            $$;
            
-- End of Assertion 2d4f2da1e661ffba0c69db7e6c75c0fb
