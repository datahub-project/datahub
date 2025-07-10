import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_queries import (
    _build_access_history_database_filter_condition,
)


class TestBuildAccessHistoryDatabaseFilterCondition:
    @pytest.mark.parametrize(
        "database_pattern,additional_database_names,expected",
        [
            pytest.param(
                None,
                None,
                "TRUE",
                id="no_pattern_no_additional_dbs",
            ),
            pytest.param(
                AllowDenyPattern(),
                None,
                "TRUE",
                id="empty_pattern_no_additional_dbs",
            ),
            pytest.param(
                None,
                [],
                "TRUE",
                id="no_pattern_empty_additional_dbs",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*')))",
                id="allow_pattern_only",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP')))",
                id="deny_pattern_only",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*"], deny=[".*_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*') AND (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP')))",
                id="allow_and_deny_patterns",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*", "DEV_.*"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'DEV_.*')))",
                id="multiple_allow_patterns",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*_TEMP", ".*_STAGING"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP' AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_STAGING'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP' AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_STAGING'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP' AND SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_STAGING')))",
                id="multiple_deny_patterns",
            ),
            pytest.param(
                None,
                ["DB1", "DB2"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB2')))",
                id="multiple_additional_database_names",
            ),
            pytest.param(
                None,
                ["SPECIAL_DB"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'SPECIAL_DB')))",
                id="single_additional_database_name",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*"]),
                ["DB1", "DB2"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*') AND (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB2')))",
                id="pattern_with_additional_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*", "DEV_.*"], deny=[".*_TEMP"]),
                ["SPECIAL_DB"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP') AND (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'DEV_.*') AND (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP') AND (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'SPECIAL_DB')))",
                id="complex_pattern_with_additional_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=[".*"]),
                None,
                "TRUE",
                id="default_allow_pattern_ignored",
            ),
            pytest.param(
                AllowDenyPattern(allow=[".*"], deny=[".*_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP')))",
                id="default_allow_pattern_with_deny_pattern",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD'_.*"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD''_.*'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD''_.*'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD''_.*')))",
                id="sql_injection_protection_allow_pattern",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*'_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*''_TEMP'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*''_TEMP'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*''_TEMP')))",
                id="sql_injection_protection_deny_pattern",
            ),
            pytest.param(
                None,
                ["DB'1", "DB'2"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''2'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''2'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB''1' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB''2')))",
                id="sql_injection_protection_additional_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=[]),
                None,
                "TRUE",
                id="empty_allow_pattern_list",
            ),
            pytest.param(
                AllowDenyPattern(deny=[]),
                None,
                "TRUE",
                id="empty_deny_pattern_list",
            ),
            pytest.param(
                AllowDenyPattern(allow=[], deny=[]),
                None,
                "TRUE",
                id="both_empty_pattern_lists",
            ),
        ],
    )
    def test_build_access_history_database_filter_condition(
        self, database_pattern, additional_database_names, expected
    ):
        """Test the _build_access_history_database_filter_condition function with various inputs."""
        result = _build_access_history_database_filter_condition(
            database_pattern, additional_database_names
        )
        assert result == expected
