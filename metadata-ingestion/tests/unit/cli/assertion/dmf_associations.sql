
-- Start of Assertion 5c32eef47bd763fece7d21c7cbf6c659

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__5c32eef47bd763fece7d21c7cbf6c659 ON (col_date);
            
-- End of Assertion 5c32eef47bd763fece7d21c7cbf6c659

-- Start of Assertion 04be4145bd8de10bed3dfcb0cee57842

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS_ALL_TIMES ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__04be4145bd8de10bed3dfcb0cee57842 ON (col_date);
            
-- End of Assertion 04be4145bd8de10bed3dfcb0cee57842

-- Start of Assertion 4dfda3a192a48f5b5b733986c145e8db

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__4dfda3a192a48f5b5b733986c145e8db ON (value);
            
-- End of Assertion 4dfda3a192a48f5b5b733986c145e8db

-- Start of Assertion 2d4f2da1e661ffba0c69db7e6c75c0fb

            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
            ALTER TABLE TEST_DB.PUBLIC.TEST_ASSERTIONS ADD DATA METRIC FUNCTION test_db.datahub_dmfs.datahub__2d4f2da1e661ffba0c69db7e6c75c0fb ON (value);
            
-- End of Assertion 2d4f2da1e661ffba0c69db7e6c75c0fb
