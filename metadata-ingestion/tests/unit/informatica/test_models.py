from datahub.ingestion.source.informatica.models import (
    IdmcConnection,
    InformaticaSourceReport,
)


class TestIdmcConnection:
    def test_from_api_response_reads_conn_params(self):
        conn = IdmcConnection.from_api_response(
            {
                "id": "01000A",
                "name": "prod-snowflake",
                "type": "SNOWFLAKE",
                "federatedId": "fed-123",
                "connParams": {
                    "Connection Type": "Snowflake_Cloud_Data_Warehouse",
                    "Host": "acme.snowflakecomputing.com",
                    "Database": "ANALYTICS",
                    "Schema": "PUBLIC",
                },
            }
        )
        assert conn.id == "01000A"
        assert conn.name == "prod-snowflake"
        assert conn.conn_type == "Snowflake_Cloud_Data_Warehouse"
        assert conn.base_type == "SNOWFLAKE"
        assert conn.federated_id == "fed-123"
        assert conn.host == "acme.snowflakecomputing.com"
        assert conn.database == "ANALYTICS"
        assert conn.schema == "PUBLIC"

    def test_from_api_response_falls_back_to_top_level_fields(self):
        conn = IdmcConnection.from_api_response(
            {
                "id": "01",
                "name": "legacy",
                "host": "legacy-host",
                "database": "DB",
                "schema": "S",
            }
        )
        assert conn.host == "legacy-host"
        assert conn.database == "DB"
        assert conn.schema == "S"
        assert conn.conn_type == ""

    def test_from_api_response_handles_missing_fields(self):
        conn = IdmcConnection.from_api_response({})
        assert conn.id == ""
        assert conn.name == ""
        assert conn.conn_type == ""
        assert conn.federated_id == ""


class TestInformaticaSourceReport:
    def test_report_api_call_increments_counter(self):
        report = InformaticaSourceReport()
        assert report.api_call_count == 0
        report.report_api_call()
        report.report_api_call()
        assert report.api_call_count == 2

    def test_report_object_failed_appends(self):
        report = InformaticaSourceReport()
        report.report_object_failed("mapping_A", "boom")
        assert any("mapping_A" in e for e in report.objects_failed)

    def test_report_connection_unresolved(self):
        report = InformaticaSourceReport()
        report.report_connection_unresolved("cid", "cname", "no platform")
        assert any("cname" in e and "cid" in e for e in report.connections_unresolved)

    def test_report_export_failed(self):
        report = InformaticaSourceReport()
        report.report_export_failed("job-1", "timed out")
        assert any("job-1" in e for e in report.export_jobs_failed)
