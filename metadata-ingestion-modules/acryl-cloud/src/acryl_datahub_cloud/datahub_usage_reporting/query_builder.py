from typing import Dict


class QueryBuilder:
    @staticmethod
    def get_soft_deleted_entities_query() -> Dict:
        return {
            "sort": [{"urn": {"order": "asc"}}],
        }

    @staticmethod
    def get_query_entities_query() -> Dict:
        return {
            "sort": [{"urn": {"order": "asc"}}],
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must_not": [
                                {"term": {"source": "MANUAL"}},
                            ]
                        }
                    }
                }
            },
        }

    @staticmethod
    def get_upstreams_query() -> Dict:
        return {
            "sort": [{"destination.urn": {"order": "asc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"destination.entityType": ["dataset"]}},
                        {"terms": {"source.entityType": ["dataset"]}},
                    ]
                }
            },
        }

    @staticmethod
    def get_dashboard_usage_query(days: int) -> Dict:
        return {
            "sort": [{"urn": {"order": "asc"}}],
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": f"now-{days}d",
                                            "lt": "now/d",
                                        }
                                    }
                                },
                                {"term": {"isExploded": False}},
                            ]
                        }
                    }
                }
            },
        }

    @staticmethod
    def get_dataset_usage_query(days: int) -> Dict:
        return {
            "sort": [{"urn": {"order": "asc"}}],
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": f"now-{days}d/d",
                                            "lt": "now/d",
                                        }
                                    }
                                },
                                {"term": {"isExploded": False}},
                                {"range": {"totalSqlQueries": {"gt": 0}}},
                            ]
                        }
                    }
                }
            },
        }

    @staticmethod
    def get_dataset_write_usage_raw_query(days: int) -> Dict:
        return {
            "sort": [{"urn": {"order": "asc"}}, {"@timestamp": {"order": "asc"}}],
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {"gte": f"now-{days}d/d", "lte": "now/d"}
                            }
                        },
                        {"terms": {"operationType": ["INSERT", "UPDATE", "CREATE"]}},
                    ]
                }
            },
            "_source": {
                "includes": ["urn", "@timestamp"],
            },
        }

    @staticmethod
    def get_dataset_write_usage_composite_query(days: int) -> Dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {"gte": f"now-{days}d/d", "lte": "now/d"}
                            }
                        },
                        {"terms": {"operationType": ["INSERT", "UPDATE", "CREATE"]}},
                    ]
                }
            },
            "aggs": {
                "urn_count": {
                    "composite": {
                        "sources": [
                            {"dataset_operationaspect_v1": {"terms": {"field": "urn"}}}
                        ]
                    }
                }
            },
        }

    @staticmethod
    def get_query_usage_query(days: int) -> Dict:
        return {
            "sort": [{"urn": {"order": "asc"}}],
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": f"now-{days}d/d",
                                            "lt": "now/d",
                                        }
                                    }
                                },
                                {"term": {"isExploded": False}},
                            ]
                        }
                    }
                }
            },
        }
