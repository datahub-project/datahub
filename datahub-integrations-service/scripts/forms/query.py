import time
from typing import Any
import click
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from fake_form_data import FormSnapshotColumns
from datahub.ingestion.graph.client import get_default_graph

class AnalyticsService:
    
    def query(self, entity_urn: str, params: dict[str, Any]) -> list[str]:
        raise NotImplementedError



class IntegrationsService(AnalyticsService):

    def __init__(self, host: str = "http://localhost", port: int = 9003):
        self.host = host
        self.port = port
        self.route = "/private/analytics"

    def query(self, entity_urn: str, params: dict[str, Any]) -> list[str]:
        url = f"{self.host}:{self.port}{self.route}/query"
        query_params = {
            "entity_urn": entity_urn,
            "format": "tuple",
        }
        json_params = {
            k: v for k, v in params.items() if k not in ["sql_query_fragment"]
        }
        if 'sql_query_fragment' in params:
            query_params['sql_query_fragment'] = params['sql_query_fragment']
        response = requests.post(url, params=query_params, json=json_params if json_params else None)
        logger.info(f"Querying {url} with {query_params} and params {json_params}")
        if response.ok:
            decoded_lines = []
            for line in response.iter_lines():
                if line:
                    decoded_lines.append(line.decode('utf-8'))
                    # print(decoded_line)  # Process each line of the response here
            return decoded_lines
        else:
            print(f"Request failed: {response.status_code}")


class DataHubGraphQL(AnalyticsService):

    def __init__(self, host: str = "http://localhost", port: int = 9002):
        self.graph = get_default_graph()
        self.graphql_query = """
        query Foobar($input: FormAnalyticsInput!){
  formAnalytics(input:$input) {
    header
    table {
      row
    }
  }
}"""

    def query(self, entity_urn: str, params: dict[str, Any]) -> list[str]:

        response = self.graph.execute_graphql(self.graphql_query, {
            "input": {
                "query_string": params["sql_query_fragment"] if "sql_query_fragment" in params else None,
            }
        })
        # logger.info(f"Response from GraphQL: {response}")
        logger.info(f"Header = {response['formAnalytics']['header']}")
        rows: list[str] = []
        for row in response['formAnalytics']['table']:
            # logger.info(f"Row = {row['row']}")
            rows.append(",".join(row['row']))
        return rows
        # return [",".join(response['formAnalytics']['table']['header'])] + [",".join(row['row']) for row in response['formAnalytics']['table']]



def owner_earliest_assigned_date(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": None if "owner" not in params else {
            "andFilters": [
                {
                    "column": "",
                    "value": params["owner"],
                }
            ]
        },
        "aggregation": {
            "groupBy": [FormSnapshotColumns.assignee_urn.value],
            "aggregations": [
                {
                    "column": FormSnapshotColumns.form_assigned_date.value,
                    "type": "MIN",
                    "alias": "earliest_assignment_date",
                }
            ],
        },
    }

def owner_latest_assigned_date(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": None if "owner" not in params else {
            "andFilters": [
                {
                    "column": FormSnapshotColumns.assignee_urn.value,
                    "value": params["owner"],
                }
            ]
        },
        "aggregation": {
            "groupBy": [FormSnapshotColumns.assignee_urn.value],
            "aggregations": [
                {
                    "column": FormSnapshotColumns.form_assigned_date.value,
                    "type": "MAX",
                    "alias": "latest_assignment_date",
                }
            ],
        },
    }

def owner_by_status(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": None if not params else {
            "andFilters": [
                {
                    "column": k,
                    "value": v,
                } for k, v in params.items()
            ]
        },
        "aggregation": {
            "groupBy": [FormSnapshotColumns.assignee_urn.value, FormSnapshotColumns.form_status.value],
            "aggregations": [
                {
                    "column": "1",
                    "type": "COUNT",
                    "alias": "count",
                }
            ],
        },
    }


def dataset_by_status_and_domain(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": None if not params else {
            "andFilters": [
                {
                    "column": k,
                    "value": v,
                } for k, v in params.items()
            ]
        },
        "aggregation": {
            "groupBy": [FormSnapshotColumns.domain.value, FormSnapshotColumns.subdomain.value, FormSnapshotColumns.question_id.value, FormSnapshotColumns.question_status.value],
            "aggregations": [
                {
                    "column": "1",
                    "type": "COUNT",
                    "alias": "count",
                }
            ],
        },
    }

def verified_datasets_by_domain(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": None if not params else {
            "andFilters": [
                {
                    "column": k,
                    "value": v,
                } for k, v in params.items()
            ]
        },
        "aggregation": {
            "groupBy": [FormSnapshotColumns.domain.value, FormSnapshotColumns.subdomain.value],
            "aggregations": [
                {
                    "column": FormSnapshotColumns.form_completed_date.value,
                    "type": "COUNT",
                    "alias": "count",
                }
            ],
        },
    }

def verified_datasets_by_owner(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": None if not params else {
            "andFilters": [
                {
                    "column": k,
                    "value": v,
                } for k, v in params.items()
            ]
        },
        "aggregation": {
            "groupBy": [FormSnapshotColumns.assignee_urn.value],
            "aggregations": [
                {
                    "column": FormSnapshotColumns.asset_urn.value,
                    "type": "COUNT",
                    "alias": "count",
                }
            ],
        },
    }

def verified_datasets(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": ["dataset", "dataset_verified_date", "owner", "domain", "subdomain", "dataset_status"],
        "filter": {
            "andFilters": [
                {
                    "column": k,
                    "value": v,
                } for k, v in (params or {}).items()
            ] + [{
                "column": "dataset_verified_date",
                "condition": ">",
                "value": "2021-01-01",
            }]
        }
    }

def verified_datasets_count(params: dict[str, str]) -> dict[str, Any]:
    return {
        "project": [],
        "filter": {
            "andFilters": [
                {
                    "column": k,
                    "value": v,
                } for k, v in (params or {}).items()
            ] + [{
                "column": FormSnapshotColumns.form_completed_date.value,
                "condition": "IS NOT",
                "value": "NULL",
            }]
        },
        "aggregation": {
            "groupBy": [],
            "aggregations": [
                {
                    "column": "DISTINCT dataset",
                    "type": "COUNT",
                    "alias": "verified_datasets_count",
                }
            ],
        },
    }

def total_datasets_count(params: dict[str, str]) -> dict[str, Any]:
    return {
        "sql_query_fragment": "SELECT COUNT(DISTINCT dataset) from {{table}}"
    }

query_registry = {
    "owner_earliest_assigned_date": owner_earliest_assigned_date,
    "owner_latest_assigned_date": owner_latest_assigned_date,
    "owner_by_status": owner_by_status,
    "dataset_by_status_and_domain": dataset_by_status_and_domain,
    "verified_datasets_by_domain": verified_datasets_by_domain,
    "verified_datasets_by_owner": verified_datasets_by_owner,
    "verified_datasets": verified_datasets,
    "verified_datasets_count": verified_datasets_count,
    "total_datasets_count": total_datasets_count,
}


def execute_query(query_params, dataset_urn, analytics_service: AnalyticsService):
    """
    Function to execute a single query and return the result.
    """
    start_time = time.time()
    logger.info(f"Querying {dataset_urn} with params {query_params}")
    result = analytics_service.query(dataset_urn, query_params)
    end_time = time.time()
    return query_params, result, end_time - start_time

def resolve_query(query, params) -> dict[str, Any]:
    """
    Function to resolve a query from the registry.
    """
    if query in query_registry:
        return query_registry[query](params)
    elif query.endswith(".sql"):
        with open(query, "r") as f:
            sql_query_fragment = f.read()
            for k, v in params.items():
                sql_query_fragment = sql_query_fragment.replace(f"{{{{{k}}}}}", v)
            return {
                "sql_query_fragment": sql_query_fragment,
            }
    else:
        raise ValueError(f"Query {query} not found")

@click.command()
@click.option('--gql', is_flag=True, help='Use GraphQL instead of REST for querying', default=False)
@click.option('--query', multiple=True, prompt='Your query', help=f"The query to execute. Choose from {','.join([k for k in query_registry.keys()])}")
@click.argument('args', nargs=-1)
def main(gql, query, args):
    integrations_service = IntegrationsService()
    graphql_service = DataHubGraphQL()
    params = dict(zip(args[::2], args[1::2]))
    # Example usage
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:datahub,local_parquet,PROD)"
    queries = {q: resolve_query(q, params) for q in query}
    logger.info(f"Executing queries {queries} on dataset {dataset_urn}")
    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
        future_to_query = {}
        future_to_query = {executor.submit(execute_query, q_value, dataset_urn, integrations_service if not gql else graphql_service): q_name for q_name, q_value in queries.items()}
        # Processing results as they complete
        results = {}
        for future in as_completed(future_to_query):
            query = future_to_query[future]
            try:
                query_value, result, elapsed_time = future.result()
                results[query] = {
                    "result": result,
                    "elapsed_time": elapsed_time
                }
            except Exception as exc:
                logger.error(f"{query} generated an exception: {exc}")

        # Print results
        for query, result in results.items():
            click.echo(f"Results for query {query}:")
            for line in result['result']:
                click.echo(line)
            click.echo("----------------")
        
        # Print elapsed time
        for query, result in results.items():
            click.echo(f"Elapsed time for query {query}: {result['elapsed_time']} seconds")
            click.echo("----------------")


if __name__ == "__main__":
    main()
