
-- Start of Assertion 025cce4dd4123c0f007908011a9c64d7

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__025cce4dd4123c0f007908011a9c64d7 (ARGT TABLE(col_date DATE))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:025cce4dd4123c0f007908011a9c64d7 of type freshness'
            AS
            $$
            select case when metric <= 3600 then 1 else 0 end from (select timediff(
                second,
                max(col_timestamp::TIMESTAMP_LTZ),
                SNOWFLAKE.CORE.DATA_METRIC_SCHEDULED_TIME()
            )  as metric from TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES )
            $$;
            
-- End of Assertion 025cce4dd4123c0f007908011a9c64d7

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

-- Start of Assertion b065942d2bca8a4dbe90cc3ec2d9ca9f

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__b065942d2bca8a4dbe90cc3ec2d9ca9f (ARGT TABLE(quantity FLOAT))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:b065942d2bca8a4dbe90cc3ec2d9ca9f of type field'
            AS
            $$
            select case when metric <= 0 then 1 else 0 end from (select sum($1) as metric from (select case when quantity between 0 and 10 then 0 else 1 end
        from TEST_DB.PUBLIC.PURCHASE_EVENT where quantity is not null))
            $$;
            
-- End of Assertion b065942d2bca8a4dbe90cc3ec2d9ca9f

-- Start of Assertion 170dbd53f28eedbbaba52ebbf189f6b1

            CREATE or REPLACE DATA METRIC FUNCTION
            test_db.datahub_dmfs.datahub__170dbd53f28eedbbaba52ebbf189f6b1 (ARGT TABLE(quantity FLOAT))
            RETURNS NUMBER
            COMMENT = 'Created via DataHub for assertion urn:li:assertion:170dbd53f28eedbbaba52ebbf189f6b1 of type sql'
            AS
            $$
            select case when metric=5 then 1 else 0 end from (select $1 as metric from (select mode(quantity) from test_db.public.purchase_event))
            $$;
            
-- End of Assertion 170dbd53f28eedbbaba52ebbf189f6b1
