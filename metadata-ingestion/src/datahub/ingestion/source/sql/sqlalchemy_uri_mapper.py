from collections import OrderedDict
from typing import Callable, Dict, Optional, Tuple


def _platform_alchemy_uri_tester_gen(
    platform: str, opt_starts_with: Optional[str] = None
) -> Tuple[str, Callable[[str], bool]]:
    return platform, lambda x: x.startswith(opt_starts_with or platform)


PLATFORM_TO_SQLALCHEMY_URI_TESTER_MAP: Dict[str, Callable[[str], bool]] = OrderedDict(
    [
        _platform_alchemy_uri_tester_gen("athena", "awsathena"),
        _platform_alchemy_uri_tester_gen("bigquery"),
        _platform_alchemy_uri_tester_gen("clickhouse"),
        _platform_alchemy_uri_tester_gen("druid"),
        _platform_alchemy_uri_tester_gen("hana"),
        _platform_alchemy_uri_tester_gen("hive"),
        _platform_alchemy_uri_tester_gen("mongodb"),
        _platform_alchemy_uri_tester_gen("mssql"),
        _platform_alchemy_uri_tester_gen("mysql"),
        _platform_alchemy_uri_tester_gen("oracle"),
        _platform_alchemy_uri_tester_gen("pinot"),
        _platform_alchemy_uri_tester_gen("presto"),
        (
            "redshift",
            lambda x: (
                x.startswith(("jdbc:postgres:", "postgresql"))
                and x.find("redshift.amazonaws") > 0
            )
            or x.startswith("redshift"),
        ),
        # Don't move this before redshift.
        _platform_alchemy_uri_tester_gen("postgres", "postgresql"),
        _platform_alchemy_uri_tester_gen("snowflake"),
        _platform_alchemy_uri_tester_gen("sqlite"),
        _platform_alchemy_uri_tester_gen("trino"),
        _platform_alchemy_uri_tester_gen("vertica"),
    ]
)


def get_platform_from_sqlalchemy_uri(sqlalchemy_uri: str) -> str:
    for platform, tester in PLATFORM_TO_SQLALCHEMY_URI_TESTER_MAP.items():
        if tester(sqlalchemy_uri):
            return platform
    return "external"
