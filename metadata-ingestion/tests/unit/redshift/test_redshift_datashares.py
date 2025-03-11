import json
from typing import Dict, List, Union
from unittest.mock import patch

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.datashares import (
    InboundDatashare,
    OutboundDatashare,
    OutboundSharePlatformResource,
    RedshiftDatasharesHelper,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.redshift_schema import (
    PartialInboundDatashare,
    RedshiftDatabase,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.metadata.schema_classes import (
    PlatformResourceInfoClass,
    SerializedValueClass,
    SerializedValueContentTypeClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping


def get_redshift_config():
    return RedshiftConfig(
        host_port="localhost:5439",
        database="XXXXXXX",
        username="XXXXXXXXX",
        password="XXXX_password",
        platform_instance="consumer_instance",
    )


def get_datahub_graph():
    """
    Mock DataHubGraph instance for testing purposes.
    """
    graph = DataHubGraph(DatahubClientConfig(server="xxxx"))
    return graph


class TestDatasharesHelper:
    def test_generate_lineage_success(self):
        """
        Test generate_lineage method when share and graph exist, resources are found,
        and upstream namespace and database are successfully identified.
        """
        # Setup
        config = get_redshift_config()
        report = RedshiftReport()
        graph = get_datahub_graph()
        helper = RedshiftDatasharesHelper(config, report, graph)

        # Mock input data
        share = InboundDatashare(
            producer_namespace="producer_namespace",
            share_name="test_share",
            consumer_database="consumer_db",
        )
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]] = {
            "schema1": [
                RedshiftTable(name="table1", comment=None, created=None),
                RedshiftTable(name="table2", comment=None, created=None),
            ],
            "schema2": [RedshiftTable(name="table3", comment=None, created=None)],
        }

        # Mock PlatformResource.search_by_key
        def mock_search_by_key(*args, **kwargs):
            resource = PlatformResource.create(
                key=PlatformResourceKey(
                    platform="redshift",
                    platform_instance="producer_instance",
                    resource_type="OUTBOUND_DATASHARE",
                    primary_key="producer_namespace.some_share",
                ),
                value=OutboundSharePlatformResource(
                    namespace="producer_namespace",
                    platform_instance="producer_instance",
                    env="PROD",
                    source_database="producer_db",
                    share_name="test_share",
                ),
            )

            return [resource]

        with patch.object(PlatformResource, "search_by_key") as mocked_method:
            mocked_method.side_effect = mock_search_by_key
            result = list(helper.generate_lineage(share, tables))
        # Assert
        assert len(result) == 3
        expected_mappings = [
            KnownLineageMapping(
                upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer_instance.producer_db.schema1.table1,PROD)",
                downstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,consumer_instance.consumer_db.schema1.table1,PROD)",
            ),
            KnownLineageMapping(
                upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer_instance.producer_db.schema1.table2,PROD)",
                downstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,consumer_instance.consumer_db.schema1.table2,PROD)",
            ),
            KnownLineageMapping(
                upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer_instance.producer_db.schema2.table3,PROD)",
                downstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,consumer_instance.consumer_db.schema2.table3,PROD)",
            ),
        ]
        assert result == expected_mappings

    def test_generate_lineage_success_partial_inbound_share(self):
        """
        Test generate_lineage method when share and graph exist, resources are found,
        and upstream namespace and database are successfully identified.
        """
        # Setup
        config = get_redshift_config()
        report = RedshiftReport()
        graph = get_datahub_graph()
        helper = RedshiftDatasharesHelper(config, report, graph)

        # Mock input data
        share = PartialInboundDatashare(
            producer_namespace_prefix="producer_na",
            share_name="test_share",
            consumer_database="consumer_db",
        )
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]] = {
            "schema1": [
                RedshiftTable(name="table1", comment=None, created=None),
                RedshiftTable(name="table2", comment=None, created=None),
            ],
            "schema2": [RedshiftTable(name="table3", comment=None, created=None)],
        }

        # Mock PlatformResource.search_by_key
        def mock_search_by_filters(*args, **kwargs):
            resource = PlatformResource.create(
                key=PlatformResourceKey(
                    platform="redshift",
                    platform_instance="producer_instance",
                    resource_type="OUTBOUND_DATASHARE",
                    primary_key="producer_namespace.some_share",
                ),
                value=OutboundSharePlatformResource(
                    namespace="producer_namespace",
                    platform_instance="producer_instance",
                    env="PROD",
                    source_database="producer_db",
                    share_name="test_share",
                ),
            )

            return [resource]

        with patch.object(PlatformResource, "search_by_filters") as mocked_method:
            mocked_method.side_effect = mock_search_by_filters
            result = list(helper.generate_lineage(share, tables))
        # Assert
        assert len(result) == 3
        expected_mappings = [
            KnownLineageMapping(
                upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer_instance.producer_db.schema1.table1,PROD)",
                downstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,consumer_instance.consumer_db.schema1.table1,PROD)",
            ),
            KnownLineageMapping(
                upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer_instance.producer_db.schema1.table2,PROD)",
                downstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,consumer_instance.consumer_db.schema1.table2,PROD)",
            ),
            KnownLineageMapping(
                upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer_instance.producer_db.schema2.table3,PROD)",
                downstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,consumer_instance.consumer_db.schema2.table3,PROD)",
            ),
        ]
        assert result == expected_mappings

    def test_generate_lineage_missing_graph_reports_warning(self):
        """
        Test generate_lineage when share is provided but graph is not available.

        This test verifies that the method correctly handles the case where an InboundDatashare
        is provided, but the DataHubGraph is not available. It should set the
        self.is_shared_database flag to True and log a warning about missing upstream lineage.
        """
        # Setup
        config = get_redshift_config()
        report = RedshiftReport()
        graph = None
        helper = RedshiftDatasharesHelper(config, report, graph)

        share = InboundDatashare(
            producer_namespace="test_namespace",
            share_name="test_share",
            consumer_database="test_db",
        )
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]] = {}

        # Execute
        list(helper.generate_lineage(share, tables))

        # Assert
        assert len(report.warnings) == 1

        assert (
            list(report.warnings)[0].title
            == "Upstream lineage of inbound datashare will be missing"
        )
        assert "Missing datahub graph" in list(report.warnings)[0].message

    def test_generate_lineage_missing_producer_platform_resource(self):
        """
        Test generate_lineage when share is provided, graph exists, but no resources are found.

        This test verifies that the method handles the case where an inbound datashare is provided,
        the DataHubGraph is available, but no platform resources are found for the producer namespace.
        It should result in a warning being reported and no lineage mappings being generated.
        """
        # Setup
        config = get_redshift_config()
        report = RedshiftReport()
        graph = get_datahub_graph()
        helper = RedshiftDatasharesHelper(config, report, graph)

        # Create a mock InboundDatashare
        share = InboundDatashare(
            share_name="test_share",
            producer_namespace="test_namespace",
            consumer_database="test_db",
        )

        # Create mock tables
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]] = {
            "schema1": [RedshiftTable(name="table1", created=None, comment=None)]
        }

        # Mock the PlatformResource.search_by_key to return an empty list
        with patch.object(PlatformResource, "search_by_key") as mocked_method:
            mocked_method.return_value = []
            result = list(helper.generate_lineage(share, tables))

        # Assertions
        assert len(result) == 0, "No lineage mappings should be generated"
        assert len(report.infos) == 1
        assert (
            list(report.infos)[0].title
            == "Upstream lineage of inbound datashare will be missing"
        )
        assert "Missing platform resource" in list(report.infos)[0].message

    def test_generate_lineage_malformed_share_platform_resource(self):
        """
        Test generate_lineage method when share and graph exist, resources are found,
        but upstream_share is None due to error in parsing resource info.

        This test verifies that the method handles the case where the upstream namespace is found,
        but we failed to parse the value.
        It should result in a warning being reported and no lineage mappings being generated.
        """
        # Setup
        config = get_redshift_config()
        report = RedshiftReport()
        graph = get_datahub_graph()
        helper = RedshiftDatasharesHelper(config, report, graph)

        share = InboundDatashare(
            producer_namespace="produer_namespace",
            share_name="test_share",
            consumer_database="consumer_db",
        )
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]] = {
            "schema1": [RedshiftTable(name="table1", comment=None, created=None)]
        }

        # Mock PlatformResource.search_by_key to return a resource
        def mock_search_by_key(*args, **kwargs):
            resource = PlatformResource.create(
                key=PlatformResourceKey(
                    platform="redshift",
                    platform_instance="producer_instance",
                    resource_type="OUTBOUND_DATASHARE",
                    primary_key="producer_namespace.some_share",
                ),
                value={
                    "namespace": "producer_namespace",
                    "platform_instance": "producer_instance",
                    "env": "PROD",
                    "outbound_share_name_to_source_database": {},  # Empty dict to simulate missing share
                },
            )

            return [resource]

        with patch.object(PlatformResource, "search_by_key") as mocked_method:
            mocked_method.side_effect = mock_search_by_key
            result = list(helper.generate_lineage(share, tables))

        # Assert
        assert len(result) == 0
        assert len(report.warnings) == 1
        assert (
            list(report.warnings)[0].title
            == "Upstream lineage of inbound datashare will be missing"
        )
        assert (
            "Failed to parse platform resource for outbound datashare"
            in list(report.warnings)[0].message
        )

    def test_generate_lineage_shared_database_with_no_tables(self):
        """
        Test generate_lineage with valid share but empty tables dictionary.
        """
        config = get_redshift_config()
        report = RedshiftReport()
        graph = get_datahub_graph()  # Mock or actual instance
        helper = RedshiftDatasharesHelper(config, report, graph)

        share = InboundDatashare(
            producer_namespace="producer_namespace",
            consumer_database="db",
            share_name="share",
        )
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]] = {}

        with patch.object(PlatformResource, "search_by_key") as mocked_method:
            mocked_method.return_value = []
            result = list(helper.generate_lineage(share, tables))

        assert len(result) == 0

    def test_to_platform_resource_success(self):
        """
        Test the to_platform_resource method when shares list is not empty.

        This test verifies that the method correctly processes a non-empty list of OutboundDatashare objects,
        generates the appropriate PlatformResource, and yields the expected MetadataChangeProposalWrapper objects.
        It also checks that the outbound_shares_count in the report is set correctly.
        """
        # Setup
        config = RedshiftConfig(
            host_port="test_host",
            database="test_db",
            username="test_user",
            password="test_pass",
            platform_instance="test_instance",
        )
        report = RedshiftReport()
        helper = RedshiftDatasharesHelper(config, report, None)

        # Create test data
        shares = [
            OutboundDatashare(
                producer_namespace="test_namespace",
                share_name="share1",
                source_database="db1",
            ),
            OutboundDatashare(
                producer_namespace="test_namespace",
                share_name="share2",
                source_database="db2",
            ),
        ]

        # Execute the method
        result = list(helper.to_platform_resource(shares))

        # Assertions
        assert len(result) > 0, (
            "Expected at least one MetadataChangeProposalWrapper to be yielded"
        )
        assert report.outbound_shares_count == 2, (
            "Expected outbound_shares_count to be 2"
        )

        # Check the content of the first MetadataChangeProposalWrapper
        first_mcp = result[0]
        assert first_mcp.entityType == "platformResource", (
            "Expected entityType to be platformResource"
        )
        assert first_mcp.aspectName == "platformResourceInfo", (
            "Expected aspectName to be platformResourceInfo"
        )

        info = first_mcp.aspect
        assert isinstance(info, PlatformResourceInfoClass)

        assert info.resourceType == "OUTBOUND_DATASHARE"
        assert info.primaryKey == "test_namespace.share1"

        assert isinstance(info.value, SerializedValueClass)
        assert info.value.contentType == SerializedValueContentTypeClass.JSON
        assert info.value.blob == json.dumps(
            {
                "namespace": "test_namespace",
                "platform_instance": "test_instance",
                "env": "PROD",
                "source_database": "db1",
                "share_name": "share1",
            },
        ).encode("utf-8")

        # Check the content of the first MetadataChangeProposalWrapper
        fourth_mcp = result[3]
        assert fourth_mcp.entityType == "platformResource", (
            "Expected entityType to be platformResource"
        )
        assert fourth_mcp.aspectName == "platformResourceInfo", (
            "Expected aspectName to be platformResourceInfo"
        )

        info = fourth_mcp.aspect
        assert isinstance(info, PlatformResourceInfoClass)

        assert info.resourceType == "OUTBOUND_DATASHARE"
        assert info.primaryKey == "test_namespace.share2"

        assert isinstance(info.value, SerializedValueClass)
        assert info.value.contentType == SerializedValueContentTypeClass.JSON
        assert info.value.blob == json.dumps(
            {
                "namespace": "test_namespace",
                "platform_instance": "test_instance",
                "env": "PROD",
                "source_database": "db2",
                "share_name": "share2",
            },
        ).encode("utf-8")

    def test_to_platform_resource_edge_case_single_share(self):
        """
        Test the to_platform_resource method with a single share.
        This edge case should still produce a valid result.
        """
        config = get_redshift_config()
        report = RedshiftReport()
        helper = RedshiftDatasharesHelper(config, report, None)

        share = OutboundDatashare(
            producer_namespace="test", share_name="share1", source_database="db1"
        )

        result = list(helper.to_platform_resource([share]))

        assert len(result) > 0
        assert report.outbound_shares_count == 1

    def test_to_platform_resource_empty_input(self):
        """
        Test the to_platform_resource method with an empty list of shares.
        This should set the outbound_shares_count to 0 and return an empty iterable.
        """
        config = get_redshift_config()
        report = RedshiftReport()
        helper = RedshiftDatasharesHelper(config, report, None)

        result = list(helper.to_platform_resource([]))

        assert len(result) == 0
        assert report.outbound_shares_count == 0

    def test_to_platform_resource_exception_handling(self):
        """
        Test the exception handling in the to_platform_resource method.
        This should catch the exception and add a warning to the report.
        """
        config = get_redshift_config()
        report = RedshiftReport()
        helper = RedshiftDatasharesHelper(config, report, None)

        # Create a share with invalid data to trigger an exception
        invalid_share = OutboundDatashare(
            producer_namespace=None,  # type:ignore
            share_name="x",
            source_database="y",
        )

        list(helper.to_platform_resource([invalid_share]))

        assert len(report.warnings) == 1
        assert (
            list(report.warnings)[0].title
            == "Downstream lineage to outbound datashare may not work"
        )

    def test_database_get_inbound_datashare_success(self):
        db = RedshiftDatabase(
            name="db",
            type="shared",
            options='{"datashare_name":"xxx","datashare_producer_account":"1234","datashare_producer_namespace":"yyy"}',
        )

        assert db.get_inbound_share() == InboundDatashare(
            share_name="xxx",
            producer_namespace="yyy",
            consumer_database="db",
        )

    def test_database_get_partial_inbound_datashare_success(self):
        db = RedshiftDatabase(
            name="db",
            type="shared",
            options='{"datashare_name":"xxx","datashare_producer_account":"1234","datashare_producer_namespace":"yy',
        )

        assert db.get_inbound_share() == PartialInboundDatashare(
            share_name="xxx",
            producer_namespace_prefix="yy",
            consumer_database="db",
        )

    def test_database_no_inbound_datashare(self):
        db = RedshiftDatabase(
            name="db",
            type="local",
            options=None,
        )

        assert db.get_inbound_share() is None

    def test_shared_database_no_inbound_datashare(self):
        db = RedshiftDatabase(
            name="db",
            type="shared",
            options=None,
        )

        assert db.get_inbound_share() is None
