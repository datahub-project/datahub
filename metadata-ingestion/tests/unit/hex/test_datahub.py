import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.hex.constants import HEX_PLATFORM_URN
from datahub.ingestion.source.hex.datahub import (
    AspectModel,
    EntitiesResponse,
    HexQueryFetcher,
    HexQueryFetcherReport,
    QueryProperties,
    QueryPropertiesValue,
    QueryResponse,
    QuerySubjectEntity,
    QuerySubjects,
    QuerySubjectsValue,
    Statement,
)
from datahub.metadata.urns import DatasetUrn, QueryUrn


class TestHexQueryFetcherExtractHexMetadata(unittest.TestCase):
    """Test cases for HexQueryFetcher._extract_hex_metadata method"""

    def setUp(self):
        self.mock_client = MagicMock()
        self.workspace_name = "some-hex-workspace"
        self.start_datetime = datetime(2023, 1, 1)
        self.report = HexQueryFetcherReport()
        self.fetcher = HexQueryFetcher(
            datahub_client=self.mock_client,
            workspace_name=self.workspace_name,
            start_datetime=self.start_datetime,
            report=self.report,
        )

    def test_extract_hex_metadata_with_matching_workspace(self):
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "context": "SCHEDULED_RUN", "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_name": "PlayNotebook", "project_url": "https://app.hex.tech/some-hex-workspace/hex/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf/draft/logic?selectedCellId=67c38da0-e631-4005-9750-5bdae2a2ef3f", "status": "In development", "trace_id": "f316f99947454a7e8aff2947f848f73d", "user_email": "alice@mail.com"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is not None
        project_id, workspace_name = result
        assert project_id == "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf"
        assert workspace_name == "some-hex-workspace"

    def test_extract_hex_metadata_with_non_matching_workspace(self):
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "context": "SCHEDULED_RUN", "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_name": "PlayNotebook", "project_url": "https://app.hex.tech/different-workspace/hex/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf/draft/logic?selectedCellId=67c38da0-e631-4005-9750-5bdae2a2ef3f", "status": "In development", "trace_id": "f316f99947454a7e8aff2947f848f73d", "user_email": "alice@mail.com"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is not None
        project_id, workspace_name = result
        assert project_id == "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf"
        assert workspace_name == "different-workspace"

    def test_extract_hex_metadata_without_url_returns_none(self):
        # missing project_url
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "context": "SCHEDULED_RUN", "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_name": "PlayNotebook", "status": "In development", "trace_id": "f316f99947454a7e8aff2947f848f73d", "user_email": "alice@mail.com"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is None

    def test_extract_hex_metadata_with_no_metadata(self):
        # no Hex metadata
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- This is a regular comment
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is None

    def test_extract_hex_metadata_with_invalid_json(self):
        # invalid JSON in Hex metadata
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", INVALID_JSON}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is None

    def test_extract_hex_metadata_with_missing_project_id(self):
        # missing project_id
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "project_url": "https://app.hex.tech/some-hex-workspace/hex/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf/draft/logic"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is None

    def test_extract_hex_metadata_with_invalid_url_format_returns_none(self):
        # invalid URL format in project_url
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_url": "https://invalid-url-format/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is None

    def test_extract_hex_metadata_with_custom_domain(self):
        # custom domain in project_url (single-tenant deployment)
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "context": "SCHEDULED_RUN", "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_name": "PlayNotebook", "project_url": "https://my-hex-instance.hex.tech/some-hex-workspace/hex/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf/draft/logic", "status": "In development", "trace_id": "f316f99947454a7e8aff2947f848f73d", "user_email": "alice@mail.com"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is not None
        project_id, workspace_name = result
        assert project_id == "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf"
        assert workspace_name == "some-hex-workspace"

    def test_extract_hex_metadata_with_http_protocol(self):
        # HTTP protocol (not HTTPS)
        sql = """
        select * 
        from "LONG_TAIL_COMPANIONS"."ANALYTICS"."PET_DETAILS" 
        limit 100
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "context": "SCHEDULED_RUN", "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_name": "PlayNotebook", "project_url": "http://app.hex.tech/some-hex-workspace/hex/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf/draft/logic", "status": "In development", "trace_id": "f316f99947454a7e8aff2947f848f73d", "user_email": "alice@mail.com"}
        """

        result = self.fetcher._extract_hex_metadata(sql)
        assert result is not None
        project_id, workspace_name = result
        assert project_id == "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf"
        assert workspace_name == "some-hex-workspace"

    def test_extract_hex_metadata_with_complex_urls(self):
        # complex workspace names and paths
        urls_to_test = [
            # URL with hyphens in workspace name
            """{"project_id": "123", "project_url": "https://app.hex.tech/my-complex-workspace-name/hex/project-id"}""",
            # URL with underscores
            """{"project_id": "123", "project_url": "https://app.hex.tech/workspace_with_underscores/hex/project-id"}""",
            # URL with special chars in domain
            """{"project_id": "123", "project_url": "https://my-custom-subdomain.hex.tech/some-hex-workspace/hex/project-id"}""",
            # URL with long path after /hex/
            """{"project_id": "123", "project_url": "https://app.hex.tech/some-hex-workspace/hex/project-id/draft/logic?selectedCellId=67c38da0-e631"}""",
        ]

        expected_workspaces = [
            "my-complex-workspace-name",
            "workspace_with_underscores",
            "some-hex-workspace",
            "some-hex-workspace",
        ]

        for i, url_json in enumerate(urls_to_test):
            sql = f"""
            select * from table
            -- Hex query metadata: {url_json}
            """

            result = self.fetcher._extract_hex_metadata(sql)
            assert result is not None, (
                f"Failed to extract metadata from URL: {url_json}"
            )
            project_id, workspace_name = result
            assert project_id == "123"
            assert workspace_name == expected_workspaces[i], (
                f"Expected workspace {expected_workspaces[i]} but got {workspace_name}"
            )


class TestHexQueryFetcherFetch(unittest.TestCase):
    """Test cases for the HexQueryFetcher.fetch method"""

    def setUp(self):
        self.mock_client = MagicMock()
        self.workspace_name = "workspace1"
        self.start_datetime = datetime(2023, 1, 1)
        self.report = HexQueryFetcherReport()

        self.fetcher = HexQueryFetcher(
            datahub_client=self.mock_client,
            workspace_name=self.workspace_name,
            start_datetime=self.start_datetime,
            report=self.report,
        )

        # valid test data
        self.query_urn_1 = QueryUrn.from_string("urn:li:query:query1")
        self.query_urn_2 = QueryUrn.from_string("urn:li:query:query2")
        self.dataset_urn_1 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        self.dataset_urn_2 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        self.entities_data = EntitiesResponse.parse_obj(
            {
                self.query_urn_1.urn(): AspectModel(
                    queryProperties=QueryProperties(
                        value=QueryPropertiesValue(
                            statement=Statement(
                                value="""SELECT * FROM table -- Hex query metadata: {"project_id": "project1", "project_url": "https://app.hex.tech/workspace1/hex/project1"}"""
                            ),
                            source=HEX_PLATFORM_URN.urn(),
                        )
                    ),
                    querySubjects=QuerySubjects(
                        value=QuerySubjectsValue(
                            subjects=[
                                QuerySubjectEntity(entity=self.dataset_urn_1.urn()),
                                QuerySubjectEntity(entity=self.dataset_urn_2.urn()),
                            ]
                        )
                    ),
                ),
                self.query_urn_2.urn(): AspectModel(
                    queryProperties=QueryProperties(
                        value=QueryPropertiesValue(
                            statement=Statement(
                                value="""SELECT * FROM table -- Hex query metadata: {"project_id": "project2", "project_url": "https://app.hex.tech/workspace1/hex/project2"}"""
                            ),
                            source=HEX_PLATFORM_URN.urn(),
                        )
                    ),
                    querySubjects=QuerySubjects(
                        value=QuerySubjectsValue(
                            subjects=[
                                QuerySubjectEntity(entity=self.dataset_urn_1.urn())
                            ]
                        )
                    ),
                ),
            }
        )

    @patch(
        "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
    )
    @patch("datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_entities")
    def test_fetch_with_valid_data(
        self, mock_fetch_query_entities, mock_fetch_query_urns
    ):
        mock_fetch_query_urns.return_value = [self.query_urn_1]
        mock_fetch_query_entities.return_value = self.entities_data

        results = list(self.fetcher.fetch())

        assert len(results) == 2
        assert all(isinstance(qr, QueryResponse) for qr in results)
        assert results[0].urn == self.query_urn_1
        assert results[0].hex_project_id == "project1"
        assert results[0].dataset_subjects == [self.dataset_urn_1, self.dataset_urn_2]
        assert results[1].urn == self.query_urn_2
        assert results[1].hex_project_id == "project2"
        assert results[1].dataset_subjects == [self.dataset_urn_1]

    @patch(
        "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
    )
    @patch("datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_entities")
    def test_fetch_with_missing_hex_query_metadata(
        self, mock_fetch_query_entities, mock_fetch_query_urns
    ):
        # force fail in query_urn_2
        self.entities_data[self.query_urn_2].queryProperties.value.statement.value = (  # type: ignore
            "SELECT * FROM table -- IT'S MISSING HERE"
        )
        mock_fetch_query_urns.return_value = [self.query_urn_1]
        mock_fetch_query_entities.return_value = self.entities_data

        results = list(self.fetcher.fetch())

        assert len(results) == 1
        assert all(isinstance(qr, QueryResponse) for qr in results)
        assert results[0].urn == self.query_urn_1
        assert results[0].hex_project_id == "project1"
        assert results[0].dataset_subjects == [self.dataset_urn_1, self.dataset_urn_2]

    @patch(
        "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
    )
    @patch("datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_entities")
    def test_fetch_with_missing_not_matching_workspace(
        self, mock_fetch_query_entities, mock_fetch_query_urns
    ):
        # force not match in query_urn_2
        self.entities_data[  # type: ignore
            self.query_urn_2
        ].queryProperties.value.statement.value = """SELECT * FROM table -- Hex query metadata: {"project_id": "project1", "project_url": "https://app.hex.tech/YET_ANOTHER_WORKSPACE/hex/project1"}"""
        mock_fetch_query_urns.return_value = [self.query_urn_1]
        mock_fetch_query_entities.return_value = self.entities_data

        results = list(self.fetcher.fetch())

        assert len(results) == 1
        assert all(isinstance(qr, QueryResponse) for qr in results)
        assert results[0].urn == self.query_urn_1
        assert results[0].hex_project_id == "project1"
        assert results[0].dataset_subjects == [self.dataset_urn_1, self.dataset_urn_2]

    @patch(
        "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
    )
    @patch("datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_entities")
    def test_fetch_with_no_subjects(
        self, mock_fetch_query_entities, mock_fetch_query_urns
    ):
        # force no subjects query_urn_2
        self.entities_data[  # type: ignore
            self.query_urn_2
        ].querySubjects.value.subjects = []
        mock_fetch_query_urns.return_value = [self.query_urn_1]
        mock_fetch_query_entities.return_value = self.entities_data

        results = list(self.fetcher.fetch())

        assert len(results) == 1
        assert all(isinstance(qr, QueryResponse) for qr in results)
        assert results[0].urn == self.query_urn_1
        assert results[0].hex_project_id == "project1"
        assert results[0].dataset_subjects == [self.dataset_urn_1, self.dataset_urn_2]

        @patch(
            "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
        )
        def test_fetch_with_no_query_urns_found(self, mock_fetch_query_urns):
            mock_fetch_query_urns.return_value = []

            results = list(self.fetcher.fetch())

            assert len(results) == 0

        @patch(
            "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
        )
        @patch(
            "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_entities"
        )
        def test_fetch_query_entities_fail(
            self, mock_fetch_query_entities, mock_fetch_query_urns
        ):
            mock_fetch_query_urns.return_value = [self.query_urn_1]
            mock_fetch_query_entities.side_effect = Exception(
                "Failed to fetch query entities"
            )

            results = list(self.fetcher.fetch())

            assert len(results) == 0
            assert self.report.errors == 1

        @patch(
            "datahub.ingestion.source.hex.datahub.HexQueryFetcher._fetch_query_urns_filter_hex_and_last_modified"
        )
        def test_fetch_query_urns_fail(self, mock_fetch_query_urns):
            mock_fetch_query_urns.side_effect = Exception("Failed to fetch query urns")

            results = list(self.fetcher.fetch())

            assert len(results) == 0
            assert self.report.errors == 1
