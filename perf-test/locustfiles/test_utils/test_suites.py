import time

from test_utils.mock_data_helper import default_mock_data


def flatten(l):
    return [item for sublist in l for item in sublist]


# Legacy hardcoded URNs for backwards compatibility with specific test scenarios
MIRO_URNS = [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.bing_ads.ad_groups,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.salesforce_marketing_cloud.bounce,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,production.shared.user,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.masterdata.account,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,production.finance.ht_arr_retention,PROD)"
]


def get_random_mock_urns(count: int = 100) -> list[str]:
    """
    Generate a list of random URNs from the mock data dataset.

    This provides diverse URN selection to avoid cache pollution during load tests.
    Each call generates a fresh set of random URNs spanning the full ~1M table dataset.

    Args:
        count: Number of random URNs to generate (default: 100)

    Returns:
        List of URN strings
    """
    return [default_mock_data.get_random_urn() for _ in range(count)]

# Generate diverse mock URNs for lineage tests (avoids cache pollution)
# Using 100 random URNs gives better coverage than 5 hardcoded ones
_MOCK_LINEAGE_URNS = get_random_mock_urns(100)

LINEAGE = [
    {
        "query_name": "getLineageCounts",
        "variables": [
            {"test_name": "count #{}".format(idx),
             "urn": urn,
             "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)
        ]
    },
    {
        "query_name": "searchAcrossLineage",
        "inputs": flatten([
            # downstreams
            [{"test_name": "search #{} (↓1)".format(idx),
              "urn": urn,
              "direction": "DOWNSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)],
            [{"test_name": "search #{} (↓2)".format(idx),
              "urn": urn,
              "direction": "DOWNSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["2"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)],
            [{"test_name": "search #{} (↓3+)".format(idx),
              "urn": urn,
              "direction": "DOWNSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1", "2", "3+"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)],
            # upstreams
            [{"test_name": "search #{} (↑1)".format(idx),
              "urn": urn,
              "direction": "UPSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)],
            [{"test_name": "search #{} (↑2)".format(idx),
              "urn": urn,
              "direction": "UPSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["2"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)],
            [{"test_name": "search #{} (↑3+)".format(idx),
              "urn": urn,
              "direction": "UPSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1", "2", "3+"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(_MOCK_LINEAGE_URNS)],
        ])
    }
]

# Legacy LINEAGE using hardcoded Miro URNs - kept for backwards compatibility
# Use LINEAGE (above) for modern tests against mock data
LINEAGE_MIRO = [
    {
        "query_name": "getLineageCounts",
        "variables": [
            {"test_name": "count #{}".format(idx),
             "urn": urn,
             "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)
        ]
    },
    {
        "query_name": "searchAcrossLineage",
        "inputs": flatten([
            # downstreams
            [{"test_name": "search #{} (↓1)".format(idx),
              "urn": urn,
              "direction": "DOWNSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)],
            [{"test_name": "search #{} (↓2)".format(idx),
              "urn": urn,
              "direction": "DOWNSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["2"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)],
            [{"test_name": "search #{} (↓3+)".format(idx),
              "urn": urn,
              "direction": "DOWNSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1", "2", "3+"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)],
            # upstreams
            [{"test_name": "search #{} (↑1)".format(idx),
              "urn": urn,
              "direction": "UPSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)],
            [{"test_name": "search #{} (↑2)".format(idx),
              "urn": urn,
              "direction": "UPSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["2"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)],
            [{"test_name": "search #{} (↑3+)".format(idx),
              "urn": urn,
              "direction": "UPSTREAM",
              "orFilters": [{"and": [{"field": "degree", "condition": "EQUAL", "values": ["1", "2", "3+"], "negated":False}]}],
              "endTimeMillis": int(time.time() * 1000)} for idx, urn in enumerate(MIRO_URNS)],
        ])
    }
]

SEARCH_ACROSS_ENTITIES = [
    {
        "query_name": "searchAcrossEntities",
        "inputs": [
            # Disabled Cache
            {"test_name": "*", "query": "*"},
            {"test_name": "customer", "query": "customer"},
            {"test_name": "orders", "query": "orders"},
            {"test_name": "log events", "query": "log events"},
            {"test_name": "account history", "query": "account history"},

            {"test_name": "* (cntrl)", "query": "*"},
            {"test_name": "customer (cntrl)", "query": "customer"},
            {"test_name": "orders (cntrl)", "query": "orders"},
            {"test_name": "log events (cntrl)", "query": "log events"},
            {"test_name": "account history (cntrl)", "query": "account history"},
            # Disabled Cache Single Entity
            # {"test_name": "* DATASET", "query": "*", "types": ["DATASET"]},
            # {"test_name": "customer DATASET", "query": "customer", "types": ["DATASET"]},
            # {"test_name": "orders DATASET", "query": "orders", "types": ["DATASET"]},
            # {"test_name": "log events DATASET", "query": "log events", "types": ["DATASET"]},
            # {"test_name": "account history DATASET", "query": "account history", "types": ["DATASET"]},

            # Enabled Cache
            {"test_name": "* (cache)", "query": "*", "searchFlags": {"skipCache": False, "fulltext": True}},
            {"test_name": "customer (cache)", "query": "customer", "searchFlags": {"skipCache": False, "fulltext": True}},
            {"test_name": "orders (cache)", "query": "orders", "searchFlags": {"skipCache": False, "fulltext": True}},
            {"test_name": "log events (cache)", "query": "log events", "searchFlags": {"skipCache": False, "fulltext": True}},
            {"test_name": "account history (cache)", "query": "account history", "searchFlags": {"skipCache": False, "fulltext": True}},
            # Enabled Cache Single Entity
            # {"test_name": "* DATASET (cache)", "query": "*", "types": ["DATASET"], "searchFlags": {"skipCache": False, "fulltext": True}},
            # {"test_name": "customer DATASET (cache)", "query": "customer", "types": ["DATASET"], "searchFlags": {"skipCache": False, "fulltext": True}},
            # {"test_name": "orders DATASET (cache)", "query": "orders", "types": ["DATASET"], "searchFlags": {"skipCache": False, "fulltext": True}},
            # {"test_name": "log events DATASET (cache)", "query": "log events", "types": ["DATASET"], "searchFlags": {"skipCache": False, "fulltext": True}},
            # {"test_name": "account history DATASET (cache)", "query": "account history", "types": ["DATASET"], "searchFlags": {"skipCache": False, "fulltext": True}},
        ]
    }
]