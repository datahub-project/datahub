import time


def flatten(l):
    return [item for sublist in l for item in sublist]


MIRO_URNS = [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.bing_ads.ad_groups,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.salesforce_marketing_cloud.bounce,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,production.shared.user,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.masterdata.account,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,production.finance.ht_arr_retention,PROD)"
]

LINEAGE = [
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