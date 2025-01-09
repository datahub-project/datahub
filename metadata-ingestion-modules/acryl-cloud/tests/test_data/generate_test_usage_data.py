import json
import random
import time
import uuid
from typing import Any, Dict, List


def generate_test_datasets(num_datasets: int) -> List[Dict[str, Any]]:
    datasets = []
    current_time_millis = int(time.time() * 1000)

    for i in range(num_datasets):
        dataset = {
            "urn": f"urn:li:dataset:(urn:li:dataPlatform:bigquery,mydb.my_schema_my_table_{i},PROD)",
            "name": f"my_table_{i}",
            "removed:": False,
            "lastModifiedAt": current_time_millis
            - (random.randint(1, 30) * (60 * 60 * 1000 * 24)),
        }

        event = {
            "_source": dataset,
        }

        datasets.append(event)

    return datasets


def generate_test_data(num_events: int, num_users: int) -> List[Dict[str, Any]]:
    current_time_millis = int(time.time() * 1000)
    events = []

    for i in range(num_events):
        num_queries = random.randint(1, 500)
        top_sql_queries = [
            f"select * from my_table_{i}_query_{j}" for j in range(num_queries % 10)
        ]
        user_counts = [
            {
                "count": random.randint(1, int(num_queries)),
                "userEmail": f"user{j}@acryl.io",
                "user": f"urn:li:corpuser:user{j}",
            }
            for j in range(random.randint(1, 5))
        ]

        event_source = {
            "urn": f"urn:li:dataset:(urn:li:dataPlatform:bigquery,mydb.my_schema_my_table_{i},PROD)",
            "@timestamp": current_time_millis,
            "timestampMillis": current_time_millis,
            "runId": str(uuid.uuid4()),
            "eventGranularity": str({"multiple": 1, "unit": "DAY"}),
            "partitionSpec": {"partition": "FULL_TABLE_SNAPSHOT"},
            "isExploded": False,
            "event": {
                "timestampMillis": current_time_millis,
                "totalSqlQueries": num_queries,
                "topSqlQueries": top_sql_queries,
                "eventGranularity": {"multiple": 1, "unit": "DAY"},
                "fieldCounts": [],
                "userCounts": user_counts,
                "partitionSpec": {
                    "type": "FULL_TABLE",
                    "partition": "FULL_TABLE_SNAPSHOT",
                },
                "uniqueUserCount": num_users,
            },
            "systemMetadata": {
                "pipelineName": f"urn:li:dataHubIngestionSource:{uuid.uuid4()}",
                "lastRunId": "no-run-id-provided",
                "runId": str(uuid.uuid4()),
                "lastObserved": current_time_millis + i * 1000,
            },
            "totalSqlQueries": num_queries,
            "topSqlQueries": top_sql_queries,
            "partition": "FULL_TABLE_SNAPSHOT",
            "uniqueUserCount": num_users,
        }

        event = {
            "_source": event_source,
        }

        events.append(event)

    return events


num_datasets = 10000000
# Generate 10 test events with 5 topSqlQueries and 3 users each
name = "test_dataset_usage_huge"

test_datasets = generate_test_datasets(num_datasets)
json.dump(test_datasets, open(f"{name}_datasets.json", "w"), indent=4)

test_events = generate_test_data(5000, num_datasets)
json.dump(test_events, open(f"{name}_datasetusages.json", "w"), indent=4)
