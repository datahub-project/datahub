import json
import logging
import re
from typing import Any, Dict, List, Tuple

import pydantic
import pytest

from datahub.ingestion.source.elastic_search import (
    CollapseUrns,
    ElasticsearchSourceConfig,
    ElasticToSchemaFieldConverter,
    collapse_urn,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField

logger = logging.getLogger(__name__)


def test_elasticsearch_throws_error_wrong_operation_config():
    with pytest.raises(pydantic.ValidationError):
        ElasticsearchSourceConfig.parse_obj(
            {
                "profiling": {
                    "enabled": True,
                    "operation_config": {
                        "lower_freq_profile_enabled": True,
                    },
                }
            }
        )


def assert_field_paths_are_unique(fields: List[SchemaField]) -> None:
    fields_paths = [f.fieldPath for f in fields if re.match(".*[^]]$", f.fieldPath)]

    if fields_paths:
        assert len(fields_paths) == len(set(fields_paths))


def assret_field_paths_match(
    fields: List[SchemaField], expected_field_paths: List[str]
) -> None:
    logger.debug('FieldPaths=\n"' + '",\n"'.join(f.fieldPath for f in fields) + '"')
    assert len(fields) == len(expected_field_paths)
    for f, efp in zip(fields, expected_field_paths):
        assert f.fieldPath == efp
    assert_field_paths_are_unique(fields)


# NOTE: Currently this is the list of all elastic indices that datahub uses for reasonable coverage.
# Simplify these later to just have enough coverage.
schema_test_cases: Dict[str, Tuple[str, List[str]]] = {
    ".ds-datahub_usage_event-000001": (
        """{
    "@timestamp": {
        "type": "date"
    },
    "actorUrn": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "browserId": {
        "type": "keyword"
    },
    "corp_user_name": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "corp_user_username": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "dataset_name": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "dataset_platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "date": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "entityType": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "entityUrn": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "hash": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "height": {
        "type": "long"
    },
    "index": {
        "type": "long"
    },
    "moduleId": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "path": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "prevPathname": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "query": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "renderId": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "renderType": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "scenarioType": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "search": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "timestamp": {
        "type": "date"
    },
    "title": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "total": {
        "type": "long"
    },
    "type": {
        "type": "keyword"
    },
    "url": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "userAgent": {
        "type": "keyword"
    },
    "width": {
        "type": "long"
    }
}""",
        [
            "[version=2.0].[type=date].@timestamp",
            "[version=2.0].[type=text].actorUrn",
            "[version=2.0].[type=keyword].browserId",
            "[version=2.0].[type=text].corp_user_name",
            "[version=2.0].[type=text].corp_user_username",
            "[version=2.0].[type=text].dataset_name",
            "[version=2.0].[type=text].dataset_platform",
            "[version=2.0].[type=text].date",
            "[version=2.0].[type=text].entityType",
            "[version=2.0].[type=text].entityUrn",
            "[version=2.0].[type=text].hash",
            "[version=2.0].[type=long].height",
            "[version=2.0].[type=long].index",
            "[version=2.0].[type=text].moduleId",
            "[version=2.0].[type=text].path",
            "[version=2.0].[type=text].prevPathname",
            "[version=2.0].[type=text].query",
            "[version=2.0].[type=text].renderId",
            "[version=2.0].[type=text].renderType",
            "[version=2.0].[type=text].scenarioType",
            "[version=2.0].[type=text].search",
            "[version=2.0].[type=date].timestamp",
            "[version=2.0].[type=text].title",
            "[version=2.0].[type=long].total",
            "[version=2.0].[type=keyword].type",
            "[version=2.0].[type=text].url",
            "[version=2.0].[type=keyword].userAgent",
            "[version=2.0].[type=long].width",
        ],
    ),
    "chartindex_v2": (
        """{
    "access": {
        "type": "keyword",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedDescription": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "glossaryTerms": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "queryType": {
        "type": "keyword",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "title": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "tool": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "type": {
        "type": "keyword",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].access",
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=keyword].editedDescription",
            "[version=2.0].[type=text].glossaryTerms",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=keyword].queryType",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].title",
            "[version=2.0].[type=keyword].tool",
            "[version=2.0].[type=keyword].type",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "corpgroupindex_v2": (
        """{
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "displayName": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=keyword].displayName",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "corpuserindex_v2": (
        """{
    "active": {
        "type": "boolean"
    },
    "email": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "fullName": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasTags": {
        "type": "boolean"
    },
    "ldap": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "managerLdap": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "skills": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "status": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "teams": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "title": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=boolean].active",
            "[version=2.0].[type=keyword].email",
            "[version=2.0].[type=keyword].fullName",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].ldap",
            "[version=2.0].[type=text].managerLdap",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=keyword].skills",
            "[version=2.0].[type=keyword].status",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].teams",
            "[version=2.0].[type=keyword].title",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "dashboardindex_v2": (
        """{
    "access": {
        "type": "keyword",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedDescription": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "glossaryTerms": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "title": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "tool": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].access",
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=keyword].editedDescription",
            "[version=2.0].[type=text].glossaryTerms",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].title",
            "[version=2.0].[type=keyword].tool",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "dataflowindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "cluster": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedDescription": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "flowId": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "glossaryTerms": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "orchestrator": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "project": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].cluster",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=keyword].editedDescription",
            "[version=2.0].[type=keyword].flowId",
            "[version=2.0].[type=text].glossaryTerms",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=keyword].orchestrator",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=keyword].project",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "datahubpolicyindex_v2": (
        """{
    "urn": {
        "type": "keyword"
    }
}""",
        ["[version=2.0].[type=keyword].urn"],
    ),
    "datahubretentionindex_v2": (
        """{
    "urn": {
        "type": "keyword"
    }
}""",
        ["[version=2.0].[type=keyword].urn"],
    ),
    "datajobindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "dataFlow": {
        "type": "text",
        "fields": {
            "ngram": {
                "type": "text",
                "analyzer": "partial_urn_component"
            }
        },
        "analyzer": "urn_component"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedDescription": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "glossaryTerms": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "inputs": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "jobId": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "numInputDatasets": {
        "type": "long"
    },
    "numOutputDatasets": {
        "type": "long"
    },
    "outputs": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=text].dataFlow",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=keyword].editedDescription",
            "[version=2.0].[type=text].glossaryTerms",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=text].inputs",
            "[version=2.0].[type=keyword].jobId",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=long].numInputDatasets",
            "[version=2.0].[type=long].numOutputDatasets",
            "[version=2.0].[type=text].outputs",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "dataplatformindex_v2": (
        """{
    "urn": {
        "type": "keyword"
    }
}""",
        ["[version=2.0].[type=keyword].urn"],
    ),
    "dataprocessindex_v2": (
        """{
    "hasOwners": {
        "type": "boolean"
    },
    "inputs": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "numInputDatasets": {
        "type": "long"
    },
    "numOutputDatasets": {
        "type": "long"
    },
    "orchestrator": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "origin": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "outputs": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=text].inputs",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=long].numInputDatasets",
            "[version=2.0].[type=long].numOutputDatasets",
            "[version=2.0].[type=keyword].orchestrator",
            "[version=2.0].[type=keyword].origin",
            "[version=2.0].[type=text].outputs",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "dataset_datasetprofileaspect_v1": (
        """{
    "@timestamp": {
        "type": "date"
    },
    "event": {
        "type": "object",
        "enabled": false
    },
    "eventGranularity": {
        "type": "keyword"
    },
    "isExploded": {
        "type": "boolean"
    },
    "messageId": {
        "type": "keyword"
    },
    "systemMetadata": {
        "type": "object",
        "enabled": false
    },
    "timestampMillis": {
        "type": "date"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=date].@timestamp",
            "[version=2.0].[type=object].event",
            "[version=2.0].[type=keyword].eventGranularity",
            "[version=2.0].[type=boolean].isExploded",
            "[version=2.0].[type=keyword].messageId",
            "[version=2.0].[type=object].systemMetadata",
            "[version=2.0].[type=date].timestampMillis",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "dataset_datasetusagestatisticsaspect_v1": (
        """{
    "@timestamp": {
        "type": "date"
    },
    "event": {
        "type": "object",
        "enabled": false
    },
    "eventGranularity": {
        "type": "keyword"
    },
    "fieldCounts": {
        "properties": {
            "count": {
                "type": "integer"
            },
            "fieldPath": {
                "type": "keyword"
            }
        }
    },
    "isExploded": {
        "type": "boolean"
    },
    "messageId": {
        "type": "keyword"
    },
    "systemMetadata": {
        "type": "object",
        "enabled": false
    },
    "timestampMillis": {
        "type": "date"
    },
    "topSqlQueries": {
        "type": "keyword"
    },
    "totalSqlQueries": {
        "type": "integer"
    },
    "uniqueUserCount": {
        "type": "integer"
    },
    "urn": {
        "type": "keyword"
    },
    "userCounts": {
        "properties": {
            "count": {
                "type": "integer"
            },
            "user": {
                "type": "keyword"
            },
            "userEmail": {
                "type": "keyword"
            }
        }
    }
}""",
        [
            "[version=2.0].[type=date].@timestamp",
            "[version=2.0].[type=object].event",
            "[version=2.0].[type=keyword].eventGranularity",
            "[version=2.0].[type=properties].fieldCounts",
            "[version=2.0].[type=properties].fieldCounts.[type=integer].count",
            "[version=2.0].[type=properties].fieldCounts.[type=keyword].fieldPath",
            "[version=2.0].[type=boolean].isExploded",
            "[version=2.0].[type=keyword].messageId",
            "[version=2.0].[type=object].systemMetadata",
            "[version=2.0].[type=date].timestampMillis",
            "[version=2.0].[type=keyword].topSqlQueries",
            "[version=2.0].[type=integer].totalSqlQueries",
            "[version=2.0].[type=integer].uniqueUserCount",
            "[version=2.0].[type=keyword].urn",
            "[version=2.0].[type=properties].userCounts",
            "[version=2.0].[type=properties].userCounts.[type=integer].count",
            "[version=2.0].[type=properties].userCounts.[type=keyword].user",
            "[version=2.0].[type=properties].userCounts.[type=keyword].userEmail",
        ],
    ),
    "datasetindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "deprecated": {
        "type": "boolean"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedDescription": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedFieldDescriptions": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "editedFieldGlossaryTerms": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "editedFieldTags": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "fieldDescriptions": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "fieldGlossaryTerms": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "fieldPaths": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "fieldTags": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "glossaryTerms": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "materialized": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "origin": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "keyword": {
                "type": "keyword"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "typeNames": {
        "type": "keyword",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "upstreams": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=keyword].editedDescription",
            "[version=2.0].[type=keyword].editedFieldDescriptions",
            "[version=2.0].[type=text].editedFieldGlossaryTerms",
            "[version=2.0].[type=text].editedFieldTags",
            "[version=2.0].[type=keyword].fieldDescriptions",
            "[version=2.0].[type=text].fieldGlossaryTerms",
            "[version=2.0].[type=keyword].fieldPaths",
            "[version=2.0].[type=text].fieldTags",
            "[version=2.0].[type=text].glossaryTerms",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=boolean].materialized",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=keyword].origin",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].typeNames",
            "[version=2.0].[type=text].upstreams",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "glossarynodeindex_v2": (
        """{
    "definition": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].definition",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "glossarytermindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "definition": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasRelatedTerms": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "isRelatedTerms": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "sourceRef": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "termSource": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].definition",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=text].hasRelatedTerms",
            "[version=2.0].[type=text].isRelatedTerms",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=keyword].sourceRef",
            "[version=2.0].[type=keyword].termSource",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "ilm-history-2-000001": (
        """{
    "@timestamp": {
        "type": "date",
        "format": "epoch_millis"
    },
    "error_details": {
        "type": "text"
    },
    "index": {
        "type": "keyword"
    },
    "index_age": {
        "type": "long"
    },
    "policy": {
        "type": "keyword"
    },
    "state": {
        "dynamic": "true",
        "properties": {
            "action": {
                "type": "keyword"
            },
            "action_time": {
                "type": "date",
                "format": "epoch_millis"
            },
            "creation_date": {
                "type": "date",
                "format": "epoch_millis"
            },
            "failed_step": {
                "type": "keyword"
            },
            "is_auto-retryable_error": {
                "type": "keyword"
            },
            "phase": {
                "type": "keyword"
            },
            "phase_definition": {
                "type": "text"
            },
            "phase_time": {
                "type": "date",
                "format": "epoch_millis"
            },
            "step": {
                "type": "keyword"
            },
            "step_info": {
                "type": "text"
            },
            "step_time": {
                "type": "date",
                "format": "epoch_millis"
            }
        }
    },
    "success": {
        "type": "boolean"
    }
}""",
        [
            "[version=2.0].[type=date].@timestamp",
            "[version=2.0].[type=text].error_details",
            "[version=2.0].[type=keyword].index",
            "[version=2.0].[type=long].index_age",
            "[version=2.0].[type=keyword].policy",
            "[version=2.0].[type=properties].state",
            "[version=2.0].[type=properties].state.[type=keyword].action",
            "[version=2.0].[type=properties].state.[type=date].action_time",
            "[version=2.0].[type=properties].state.[type=date].creation_date",
            "[version=2.0].[type=properties].state.[type=keyword].failed_step",
            "[version=2.0].[type=properties].state.[type=keyword].is_auto-retryable_error",
            "[version=2.0].[type=properties].state.[type=keyword].phase",
            "[version=2.0].[type=properties].state.[type=text].phase_definition",
            "[version=2.0].[type=properties].state.[type=date].phase_time",
            "[version=2.0].[type=properties].state.[type=keyword].step",
            "[version=2.0].[type=properties].state.[type=text].step_info",
            "[version=2.0].[type=properties].state.[type=date].step_time",
            "[version=2.0].[type=boolean].success",
        ],
    ),
    "mlfeatureindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "deprecated": {
        "type": "boolean"
    },
    "featureNamespace": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=keyword].featureNamespace",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "mlfeaturetableindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "deprecated": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "mlmodeldeploymentindex_v2": (
        """{
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "deprecated": {
        "type": "boolean"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "origin": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "keyword": {
                "type": "keyword"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=keyword].origin",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "mlmodelgroupindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "deprecated": {
        "type": "boolean"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "origin": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=keyword].origin",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "mlmodelindex_v2": (
        """{
    "browsePaths": {
        "type": "text",
        "fields": {
            "length": {
                "type": "token_count",
                "analyzer": "slash_pattern"
            }
        },
        "analyzer": "browse_path_hierarchy",
        "fielddata": true
    },
    "customProperties": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "deprecated": {
        "type": "boolean"
    },
    "description": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasDescription": {
        "type": "boolean"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "origin": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "keyword": {
                "type": "keyword"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "type": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=text].browsePaths",
            "[version=2.0].[type=keyword].customProperties",
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=keyword].description",
            "[version=2.0].[type=boolean].hasDescription",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=keyword].origin",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].type",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "mlprimarykeyindex_v2": (
        """{
    "deprecated": {
        "type": "boolean"
    },
    "featureNamespace": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "hasOwners": {
        "type": "boolean"
    },
    "hasTags": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "platform": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "tags": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        },
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=boolean].deprecated",
            "[version=2.0].[type=keyword].featureNamespace",
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=boolean].hasTags",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=text].platform",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=text].tags",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "schemafieldindex_v2": (
        """{
    "fieldPath": {
        "type": "keyword",
        "normalizer": "keyword_normalizer"
    },
    "parent": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].fieldPath",
            "[version=2.0].[type=text].parent",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "system_metadata_service_v1": (
        """{
    "aspect": {
        "type": "keyword"
    },
    "lastUpdated": {
        "type": "long"
    },
    "registryName": {
        "type": "keyword"
    },
    "registryVersion": {
        "type": "keyword"
    },
    "runId": {
        "type": "keyword"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=keyword].aspect",
            "[version=2.0].[type=long].lastUpdated",
            "[version=2.0].[type=keyword].registryName",
            "[version=2.0].[type=keyword].registryVersion",
            "[version=2.0].[type=keyword].runId",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
    "tagindex_v2": (
        """{
    "hasOwners": {
        "type": "boolean"
    },
    "name": {
        "type": "keyword",
        "fields": {
            "delimited": {
                "type": "text",
                "analyzer": "word_delimited"
            },
            "ngram": {
                "type": "text",
                "analyzer": "partial"
            }
        },
        "normalizer": "keyword_normalizer"
    },
    "owners": {
        "type": "text",
        "analyzer": "urn_component"
    },
    "removed": {
        "type": "boolean"
    },
    "urn": {
        "type": "keyword"
    }
}""",
        [
            "[version=2.0].[type=boolean].hasOwners",
            "[version=2.0].[type=keyword].name",
            "[version=2.0].[type=text].owners",
            "[version=2.0].[type=boolean].removed",
            "[version=2.0].[type=keyword].urn",
        ],
    ),
}


@pytest.mark.parametrize(
    "schema, expected_field_paths",
    schema_test_cases.values(),
    ids=schema_test_cases.keys(),
)
def test_elastic_search_schema_conversion(
    schema: str, expected_field_paths: List[str]
) -> None:
    schema_dict: Dict[str, Any] = json.loads(schema)
    mappings: Dict[str, Any] = {"properties": schema_dict}
    actual_fields = list(ElasticToSchemaFieldConverter.get_schema_fields(mappings))
    assret_field_paths_match(actual_fields, expected_field_paths)


def test_no_properties_in_mappings_schema() -> None:
    fields = list(ElasticToSchemaFieldConverter.get_schema_fields({}))
    assert fields == []


def test_host_port_parsing() -> None:
    """ensure we handle different styles of host_port specifications correctly"""
    examples = [
        "http://localhost:9200",
        "https://localhost:9200",
        "localhost:9300",
        "localhost",
        "http://localhost:3400",
        "192.168.0.1",
        "192.168.0.1:9200",
        "http://192.168.2.1",
        "https://192.168.0.1:9300",
        "https://192.168.0.1/",
    ]
    bad_examples = ["localhost:abcd", "htttp://localhost:1234", "localhost:9200//"]
    for example in examples:
        config_dict = {"host": example}
        config = ElasticsearchSourceConfig.parse_obj(config_dict)
        assert config.host == example

    for bad_example in bad_examples:
        config_dict = {"host": bad_example}

        with pytest.raises(pydantic.ValidationError):
            ElasticsearchSourceConfig.parse_obj(config_dict)


def test_collapse_urns() -> None:
    assert (
        collapse_urn(
            urn="urn:li:dataset:(urn:li:dataPlatform:elasticsearch,platform1.prefix_datahub_usage_event-000059,PROD)",
            collapse_urns=CollapseUrns(
                urns_suffix_regex=[
                    "-\\d+$",
                ]
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,platform1.prefix_datahub_usage_event,PROD)"
    )

    assert (
        collapse_urn(
            urn="urn:li:dataset:(urn:li:dataPlatform:elasticsearch,platform1.prefix_datahub_usage_event-2023.01.11,PROD)",
            collapse_urns=CollapseUrns(
                urns_suffix_regex=[
                    "-\\d{4}\\.\\d{2}\\.\\d{2}",
                ]
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,platform1.prefix_datahub_usage_event,PROD)"
    )
