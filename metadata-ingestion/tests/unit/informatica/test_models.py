from datahub.ingestion.source.informatica.models import (
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
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
        assert conn.db_schema == "PUBLIC"

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
        assert conn.db_schema == "S"
        assert conn.conn_type == ""


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

    def test_from_flat_builds_path_from_location_and_name(self):
        # Some IDMC v3 responses (notably TASKFLOW) return a folder path in
        # `location` instead of the canonical `path` field. The parser
        # combines `location` + `name` so the taskflow still nests under its
        # IDMC folder in the DataHub navigate tree.
        obj = IdmcObject.from_flat(
            {
                "id": "tf-1",
                "name": "Taskflow1",
                "location": "/Explore/Default/develop_test",
                "documentType": "TASKFLOW",
            },
            fallback_type="TASKFLOW",
        )
        assert obj.path == "/Explore/Default/develop_test/Taskflow1"

    def test_from_flat_uses_full_path_field_when_present(self):
        obj = IdmcObject.from_flat(
            {"id": "x", "name": "Y", "fullPath": "/Explore/P/F/Y"},
            fallback_type="TASKFLOW",
        )
        assert obj.path == "/Explore/P/F/Y"

    def test_from_flat_direct_path_wins_over_alternatives(self):
        obj = IdmcObject.from_flat(
            {
                "id": "x",
                "name": "Y",
                "path": "/Explore/P/F/Y",
                "location": "ignored-because-path-is-authoritative",
            },
            fallback_type="TASKFLOW",
        )
        assert obj.path == "/Explore/P/F/Y"

    def test_from_flat_empty_path_when_no_source_available(self):
        obj = IdmcObject.from_flat({"id": "x", "name": "Y"}, fallback_type="TASKFLOW")
        assert obj.path == ""


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
    def test_from_idmc_object_propagates_tags(self):
        obj = IdmcObject(
            id="mt-1",
            name="nightly",
            path="/Explore/Sales/ETL/nightly",
            object_type="MTT",
            tags=["pii", "critical"],
        )
        mt = IdmcMappingTask.from_idmc_object(obj)
        assert mt.v2_id == "mt-1"
        assert mt.tags == ["pii", "critical"]


class TestSchemaDriftTolerance:
    """IDMC occasionally adds new fields to its API responses. The
    connector must parse payloads with unknown fields cleanly (via
    ``extra='allow'``) rather than aborting ingestion — that's the whole
    point of using tolerant Pydantic models for API input.
    """

    def test_idmc_object_accepts_unknown_fields(self):
        obj = IdmcObject(
            id="x",
            name="y",
            path="/Explore/y",
            object_type="DTEMPLATE",
            # Simulated field IDMC might add in a future release:
            futureField="some-new-thing",  # type: ignore[call-arg]
        )
        # Declared fields are still populated correctly.
        assert obj.id == "x"
        assert obj.name == "y"
        assert obj.object_type == "DTEMPLATE"

    def test_idmc_mapping_accepts_unknown_fields(self):
        m = IdmcMapping.from_api_response(
            {
                "id": "v2-1",
                "name": "m",
                "assetFrsGuid": "guid-1",
                "mysteriousNewKey": 42,
                "nested": {"alsoNew": True},
            }
        )
        assert m.v2_id == "v2-1"
        assert m.asset_frs_guid == "guid-1"

    def test_idmc_connection_normalizes_empty_federated_id_to_none(self):
        # Regression guard: an empty-string federated_id is NOT a valid key
        # and would collide with real federated IDs in lineage lookup.
        conn = IdmcConnection(
            id="c",
            name="n",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="",
        )
        assert conn.federated_id is None
