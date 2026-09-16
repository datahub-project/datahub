# metadata-ingestion/examples/library/dbt_query_from_meta.py
"""
Example: Ingesting Query Entities from dbt meta.queries

This example shows how to configure the dbt source to emit Query entities
from the `meta.queries` field defined in your dbt model properties.

The dbt source automatically reads meta.queries from your manifest.json
and creates Query entities linked to the corresponding datasets.

Prerequisites:
1. Define meta.queries in your dbt model's schema.yml
2. Run `dbt docs generate` to create manifest.json
3. Configure the dbt ingestion recipe as shown below
"""

# Example dbt schema.yml configuration (for reference):
DBT_SCHEMA_YML_EXAMPLE = """
# models/schema.yml
version: 2

models:
  - name: customers
    description: "Customer dimension table"
    meta:
      queries:
        - name: "Active customers (30d)"
          description: "Standard query for customers active in the last 30 days"
          sql: |
            SELECT *
            FROM {{ ref('customers') }}
            WHERE active = true
              AND last_seen > CURRENT_DATE - INTERVAL '30 days'
          tags: ["production", "analytics"]
          terms: ["CustomerData"]

        - name: "Revenue by customer"
          description: "Aggregate revenue per customer for finance reports"
          sql: |
            SELECT customer_id, SUM(amount) as total_revenue
            FROM {{ ref('customers') }}
            GROUP BY customer_id
          tags: ["finance"]
          terms: ["RevenueMetrics"]
"""

# Example DataHub ingestion recipe:
INGESTION_RECIPE_EXAMPLE = """
source:
  type: dbt
  config:
    manifest_path: /path/to/dbt/target/manifest.json
    catalog_path: /path/to/dbt/target/catalog.json
    target_platform: snowflake
    
    # Query entity emission is enabled by default via entities_enabled.queries
    # To disable: entities_enabled: { queries: "NO" }
    
    # Limit queries per model (default: 100, set 0 for unlimited)
    max_queries_per_model: 100
    
    # Other commonly used options:
    entities_enabled:
      models: "YES"
      sources: "YES"
      seeds: "YES"
      queries: "YES"  # This controls meta.queries emission (default: YES)

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
"""

# Programmatic ingestion example:
if __name__ == "__main__":
    from datahub.ingestion.run.pipeline import Pipeline

    # Create a pipeline configuration
    pipeline_config = {
        "run_id": "dbt-query-ingestion",
        "source": {
            "type": "dbt",
            "config": {
                "manifest_path": "/path/to/dbt/target/manifest.json",
                "catalog_path": "/path/to/dbt/target/catalog.json",
                "target_platform": "snowflake",
                # Query emission is on by default
                # Uncomment to customize:
                # "entities_enabled": {"queries": "YES"},
                # "max_queries_per_model": 100,
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": "http://localhost:8080",
            },
        },
    }

    # Run the pipeline
    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()

    # After ingestion, Query entities will be:
    # - Visible in the "Queries" tab of each dataset
    # - URN format: urn:li:query:{model_name}_{query_name}
    # - Attributed to the "dbt_executor" actor
    # - Timestamped with manifest's generated_at
