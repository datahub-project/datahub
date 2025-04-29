#!/usr/bin/env python3
"""
DataHub Lineage Demonstration Script
Showcases lineage capabilities using a sales data processing example.
"""

import logging

from datahub.sdk.dataset import Dataset
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_lineage_datasets(client: DataHubClient) -> dict:
    """
    Create datasets for lineage demonstration.

    Args:
        client: DataHub client for creating datasets

    Returns:
        Dictionary of created datasets
    """
    # Raw source data from sales system
    raw_sales = Dataset(
        platform="snowflake",
        name="raw_sales_data",
        display_name="Raw Sales Data",
        description="Unprocessed sales data from source systems",
        schema=[
            ("sale_id", "string"),
            ("sale_date", "date"),
            ("customer_id", "string"),
            ("product_code", "string"),
            ("quantity", "integer"),
            ("unit_price", "decimal"),
        ],
    )
    client.entities.upsert(raw_sales)

    # Cleaned sales data after initial transformations
    cleaned_sales = Dataset(
        platform="snowflake",
        name="cleaned_sales_data",
        display_name="Cleaned Sales Data",
        description="Sales data after initial cleaning and standardization",
        schema=[
            ("sale_id", "string"),
            ("transaction_date", "date"),
            ("customer_id", "string"),
            ("product_id", "string"),
            ("quantity", "integer"),
            ("sale_amount", "decimal"),
        ],
    )
    client.entities.upsert(cleaned_sales)

    # Transformed sales data with business calculations
    transformed_sales = Dataset(
        platform="snowflake",
        name="transformed_sales_data",
        display_name="Transformed Sales Data",
        description="Sales data with advanced calculations and derived metrics",
        schema=[
            ("sale_id", "string"),
            ("sale_date", "date"),
            ("customer_id", "string"),
            ("product_id", "string"),
            ("revenue", "decimal"),
            ("profit_margin", "decimal"),
        ],
    )
    client.entities.upsert(transformed_sales)

    # Product dimension for enriching sales data
    product_dimension = Dataset(
        platform="snowflake",
        name="product_dimension",
        display_name="Product Dimension",
        description="Product reference data for enriching sales information",
        schema=[
            ("product_id", "string"),
            ("product_name", "string"),
            ("category", "string"),
            ("cost", "decimal"),
        ],
    )
    client.entities.upsert(product_dimension)

    # Final sales summary report
    sales_summary = Dataset(
        platform="snowflake",
        name="sales_summary_report",
        display_name="Sales Summary Report",
        description="Aggregated sales report with key business metrics",
        schema=[
            ("report_date", "date"),
            ("category", "string"),
            ("total_revenue", "decimal"),
            ("total_profit", "decimal"),
        ],
    )
    client.entities.upsert(sales_summary)

    return {
        "raw": raw_sales,
        "cleaned": cleaned_sales,
        "transformed": transformed_sales,
        "product": product_dimension,
        "report": sales_summary,
    }


def create_lineage_connections(lineage_client: LineageClient, datasets: dict) -> None:
    """
    Establish lineage connections between datasets.

    Args:
        lineage_client: DataHub lineage client
        datasets: Dictionary of created datasets
    """
    # Raw to Cleaned: Basic table-level lineage
    lineage_client.add_dataset_transform_lineage(
        upstream=datasets["raw"].urn,
        downstream=datasets["cleaned"].urn,
        query_text="Basic data cleaning and standardization",
    )

    # Cleaned to Transformed: Column-level lineage
    column_mapping = {
        "sale_id": ["sale_id"],
        "sale_date": ["transaction_date"],
        "revenue": ["sale_amount"],
    }
    lineage_client.add_dataset_transform_lineage(
        upstream=datasets["cleaned"].urn,
        downstream=datasets["transformed"].urn,
        column_lineage=column_mapping,
        query_text="Calculate revenue and profit metrics",
    )

    # Product Dimension Enrichment: Fuzzy column matching
    lineage_client.add_dataset_copy_lineage(
        upstream=datasets["product"].urn,
        downstream=datasets["transformed"].urn,
        column_lineage="auto_fuzzy",
    )

    # Transformed to Sales Summary Report: Explicit lineage
    lineage_client.add_dataset_transform_lineage(
        upstream=datasets["transformed"].urn,
        downstream=datasets["report"].urn,
        column_lineage={
            "report_date": ["sale_date"],
            "category": ["product_id"],
            "total_revenue": ["revenue"],
            "total_profit": ["revenue", "profit_margin"],
        },
        query_text="""
        CREATE TABLE sales_summary_report AS
        SELECT 
            sale_date AS report_date,
            p.category,
            SUM(revenue) AS total_revenue,
            SUM(revenue * profit_margin) AS total_profit
        FROM transformed_sales_data t
        JOIN product_dimension p ON t.product_id = p.product_id
        GROUP BY 1, 2
        """,
    )


if __name__ == "__main__":
    client = DataHubClient.from_env()
    lineage_client = LineageClient(client=client)

    # Create datasets
    datasets = create_lineage_datasets(client)

    # Establish lineage connections
    create_lineage_connections(lineage_client, datasets)
