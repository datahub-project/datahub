import logging

from datahub.metadata.urns import DatasetUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_test_datasets(client: DataHubClient) -> None:
    # Input datasets
    raw_website_logs = Dataset(
        platform="snowflake",
        name="raw_website_traffic_logs",
        display_name="Raw Website Traffic Logs",
        description="Unprocessed web traffic data for analysis",
        schema=[
            ("log_id", "string"),
            ("timestamp", "date"),
            ("user_id", "string"),
            ("page_url", "string"),
            ("session_duration", "integer"),
            ("traffic_source", "string"),
        ],
    )
    client.entities.upsert(raw_website_logs)

    # Processed datasets
    processed_user_metrics = Dataset(
        platform="snowflake",
        name="processed_user_engagement_metrics",
        display_name="Processed User Engagement Metrics",
        description="Processed and aggregated user engagement data",
        schema=[
            ("metric_id", "string"),
            ("analysis_date", "date"),
            ("user_segment", "string"),
            ("page_category", "string"),
            ("avg_session_duration", "decimal"),
            ("engagement_score", "decimal"),
        ],
    )
    client.entities.upsert(processed_user_metrics)

    # Dimension dataset
    user_segments_dimension = Dataset(
        platform="snowflake",
        name="user_segments_dimension",
        display_name="User Segments Dimension",
        description="Predefined user segmentation reference data",
        schema=[
            ("segment_id", "string"),
            ("segment_name", "string"),
            ("segment_criteria", "string"),
            ("priority_level", "decimal"),
        ],
    )
    client.entities.upsert(user_segments_dimension)

    # Final report dataset
    user_engagement_summary = Dataset(
        platform="snowflake",
        name="user_engagement_summary_report",
        display_name="User Engagement Summary Report",
        description="Final aggregated user engagement summary report",
        schema=[
            ("report_period", "date"),
            ("user_segment", "string"),
            ("total_engagement_hours", "decimal"),
            ("average_engagement_score", "decimal"),
        ],
    )
    client.entities.upsert(user_engagement_summary)


def test_lineage_connections(
    client: DataHubClient, lineage_client: LineageClient
) -> None:
    # get datasets from client
    raw_website_traffic_log = client.entities.get(
        DatasetUrn(name="raw_website_traffic_logs", platform="snowflake")
    )
    processed_user_engagement_metrics = client.entities.get(
        DatasetUrn(name="processed_user_engagement_metrics", platform="snowflake")
    )
    user_segments_dimension = client.entities.get(
        DatasetUrn(name="user_segments_dimension", platform="snowflake")
    )

    # 1. Basic table-level transform lineage (no column mapping)
    lineage_client.add_dataset_transform_lineage(
        upstream=raw_website_traffic_log.urn,
        downstream=processed_user_engagement_metrics.urn,
    )

    # 2. Transform lineage with column mapping
    column_mapping = {
        "metric_id": ["log_id"],
        "analysis_date": ["timestamp"],
        "avg_session_duration": ["session_duration"],
    }
    lineage_client.add_dataset_transform_lineage(
        upstream=raw_website_traffic_log.urn,
        downstream=processed_user_engagement_metrics.urn,
        column_lineage=column_mapping,
    )

    # 3. Copy lineage with auto_strict column matching
    lineage_client.add_dataset_copy_lineage(
        upstream=raw_website_traffic_log.urn,
        downstream=processed_user_engagement_metrics.urn,
        column_lineage="auto_strict",
    )

    # 4. Copy lineage with auto_fuzzy column matching
    lineage_client.add_dataset_copy_lineage(
        upstream=user_segments_dimension.urn,
        downstream=processed_user_engagement_metrics.urn,
        column_lineage="auto_fuzzy",
    )

    # 5. Copy lineage with no column lineage (table-level only)
    lineage_client.add_dataset_copy_lineage(
        upstream=raw_website_traffic_log.urn,
        downstream=processed_user_engagement_metrics.urn,
        column_lineage=None,
    )

    # 6. SQL-based lineage
    sql_query = """
    CREATE TABLE user_engagement_summary_report AS
    SELECT 
        analysis_date AS report_period,
        s.segment_name AS user_segment,
        SUM(avg_session_duration) AS total_engagement_hours,
        AVG(engagement_score) AS average_engagement_score
    FROM processed_user_engagement_metrics m
    JOIN user_segments_dimension s ON m.user_segment = s.segment_id
    GROUP BY 1, 2
    """
    lineage_client.add_dataset_lineage_from_sql(
        query_text=sql_query, platform="snowflake"
    )


if __name__ == "__main__":
    client = DataHubClient.from_env()
    lineage_client = LineageClient(client=client)

    # Create test datasets
    create_test_datasets(client)

    # Test lineage connections
    test_lineage_connections(client, lineage_client)
