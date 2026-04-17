from datahub.ingestion.source.informatica.models import (
    ExportJobState,
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
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


class TestIdmcObjectParsing:
    def test_from_flat_prefers_document_type(self):
        obj = IdmcObject.from_flat(
            {
                "id": "x",
                "name": "n",
                "path": "/Explore/P",
                "documentType": "Folder",
                "createdBy": "alice",
                "lastUpdatedBy": "bob",
            },
            fallback_type="Project",
        )
        assert obj.object_type == "Folder"
        assert obj.created_by == "alice"
        assert obj.updated_by == "bob"

    def test_from_flat_uses_fallback_when_document_type_missing(self):
        obj = IdmcObject.from_flat({"id": "x", "name": "y"}, fallback_type="Project")
        assert obj.object_type == "Project"

    def test_from_properties_style_response(self):
        obj = IdmcObject.from_properties(
            {
                "properties": [
                    {"name": "id", "value": "p1"},
                    {"name": "name", "value": "pname"},
                    {"name": "path", "value": "/Explore/Foo"},
                    {"name": "documentType", "value": "DTEMPLATE"},
                ]
            },
            fallback_type="DTEMPLATE",
        )
        assert obj.id == "p1"
        assert obj.name == "pname"
        assert obj.path == "/Explore/Foo"
        assert obj.object_type == "DTEMPLATE"


class TestIdmcMappingParsing:
    def test_from_api_response(self):
        m = IdmcMapping.from_api_response(
            {
                "id": "v2-1",
                "name": "my_map",
                "assetFrsGuid": "guid-1",
                "valid": False,
                "description": "desc",
            }
        )
        assert m.v2_id == "v2-1"
        assert m.asset_frs_guid == "guid-1"
        assert m.valid is False


class TestIdmcMappingTaskParsing:
    def test_from_api_response(self):
        mt = IdmcMappingTask.from_api_response(
            {
                "id": "mt-1",
                "name": "nightly",
                "mappingId": "m-1",
                "mappingName": "my_map",
                "connectionId": "c-1",
                "createdBy": "alice",
            }
        )
        assert mt.v2_id == "mt-1"
        assert mt.mapping_id == "m-1"
        assert mt.connection_id == "c-1"
        assert mt.created_by == "alice"


class TestExportJobState:
    def test_from_api_value_valid(self):
        assert ExportJobState.from_api_value("SUCCESSFUL") == ExportJobState.SUCCESSFUL
        assert ExportJobState.from_api_value("FAILED") == ExportJobState.FAILED

    def test_from_api_value_unknown(self):
        assert ExportJobState.from_api_value(None) == ExportJobState.UNKNOWN
        assert ExportJobState.from_api_value("") == ExportJobState.UNKNOWN
        assert ExportJobState.from_api_value("BOGUS") == ExportJobState.UNKNOWN


class TestInformaticaSourceReport:
    def test_report_api_call_increments_counter(self):
        report = InformaticaSourceReport()
        report.report_api_call()
        report.report_api_call()
        assert report.api_call_count == 2

    def test_report_object_failed(self):
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
