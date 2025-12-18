from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery


class TestFivetranLogQuery:
    """Test cases for FivetranLogQuery with configurable limits."""

    def test_sync_logs_query_uses_configured_limit(self):
        """Test that get_sync_logs_query uses the configured max_jobs_per_connector limit."""
        custom_limit = 999
        query = FivetranLogQuery(max_jobs_per_connector=custom_limit)
        query.set_schema("test_schema")

        sql = query.get_sync_logs_query(
            syncs_interval=7,
            connector_ids=["connector1", "connector2"],
        )

        assert f"WHERE rn <= {custom_limit}" in sql

    def test_table_lineage_query_uses_configured_limit(self):
        """Test that get_table_lineage_query uses the configured max_table_lineage_per_connector limit."""
        custom_limit = 333
        query = FivetranLogQuery(max_table_lineage_per_connector=custom_limit)
        query.set_schema("test_schema")

        sql = query.get_table_lineage_query(
            connector_ids=["connector1", "connector2"],
        )

        assert (
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY created_at DESC) <= {custom_limit}"
            in sql
        )

    def test_column_lineage_query_uses_configured_limit(self):
        """Test that get_column_lineage_query uses the configured max_column_lineage_per_connector limit."""
        custom_limit = 5555
        query = FivetranLogQuery(max_column_lineage_per_connector=custom_limit)
        query.set_schema("test_schema")

        sql = query.get_column_lineage_query(
            connector_ids=["connector1", "connector2"],
        )

        assert (
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY created_at DESC) <= {custom_limit}"
            in sql
        )
