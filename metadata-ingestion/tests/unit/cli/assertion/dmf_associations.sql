
-- Start of Assertion 025cce4dd4123c0f007908011a9c64d7

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'USING CRON 0 * * * * UTC';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__025cce4dd4123c0f007908011a9c64d7 ON (col_date);
            
-- End of Assertion 025cce4dd4123c0f007908011a9c64d7

-- Start of Assertion 5c32eef47bd763fece7d21c7cbf6c659

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'USING CRON 0 * * * * UTC';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__5c32eef47bd763fece7d21c7cbf6c659 ON (col_date);
            
-- End of Assertion 5c32eef47bd763fece7d21c7cbf6c659

-- Start of Assertion 04be4145bd8de10bed3dfcb0cee57842

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'USING CRON 0 * * * * UTC';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__04be4145bd8de10bed3dfcb0cee57842 ON (col_date);
            
-- End of Assertion 04be4145bd8de10bed3dfcb0cee57842

-- Start of Assertion b065942d2bca8a4dbe90cc3ec2d9ca9f

            ALTER TABLE TEST_DB.PUBLIC.PURCHASE_EVENT SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.PURCHASE_EVENT ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__b065942d2bca8a4dbe90cc3ec2d9ca9f ON (quantity);
            
-- End of Assertion b065942d2bca8a4dbe90cc3ec2d9ca9f

-- Start of Assertion 170dbd53f28eedbbaba52ebbf189f6b1

            ALTER TABLE TEST_DB.PUBLIC.PURCHASE_EVENT SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.PURCHASE_EVENT ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__170dbd53f28eedbbaba52ebbf189f6b1 ON (quantity);
            
-- End of Assertion 170dbd53f28eedbbaba52ebbf189f6b1
