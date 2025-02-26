from io import StringIO

import yaml

from datahub.sdk.search_client import FilterDsl as F
from datahub.sdk.search_filters import Filter, load_filters


def test_filters_simple() -> None:
    yaml_dict = {"platform": ["snowflake", "bigquery"]}
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.platform(["snowflake", "bigquery"])


def test_filters_and() -> None:
    yaml_dict = {
        "and": [
            {"env": ["PROD"]},
            {"platform": ["snowflake", "bigquery"]},
        ]
    }
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.and_(
        F.env("PROD"),
        F.platform(["snowflake", "bigquery"]),
    )


def test_filters_complex() -> None:
    yaml_dict = yaml.safe_load(
        StringIO("""\
and:
  - env: [PROD]
  - or:
    - platform: [ snowflake, bigquery ]
    - and:
      - platform: [postgres]
      - domain: [urn:li:domain:analytics]
""")
    )
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.and_(
        F.env("PROD"),
        F.or_(
            F.platform(["snowflake", "bigquery"]),
            F.and_(
                F.platform("postgres"),
                F.domain("urn:li:domain:analytics"),
            ),
        ),
    )
    assert filter_obj.compile()
