"""Test DAG for @asset decorator - simple case without explicit URI.

This demonstrates the @asset decorator which automatically:
1. Creates an Asset with URI = function name ("decorated_asset_producer")
2. Creates a DAG with dag_id = function name
3. Creates a task that produces this asset as an outlet

DataHub should capture this as:
urn:li:dataset:(urn:li:dataPlatform:airflow,decorated_asset_producer,PROD)
"""

from airflow.sdk import asset


@asset(schedule=None)
def decorated_asset_producer():
    """Produce a simple asset using function name as the asset identifier."""
    return "data produced by decorated asset"
