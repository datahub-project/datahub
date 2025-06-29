from unittest.mock import Mock

from datahub.ingestion.source.mock_data.datahub_mock_data_report import (
    DataHubMockDataReport,
)


class TestDataHubMockDataReport:
    def test_first_urn_capture(self):
        """Test that the first URN is captured correctly."""
        report = DataHubMockDataReport()

        assert report.first_urn_seen is None

        mock_workunit = Mock()
        mock_workunit.get_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:fake,test_table,PROD)"
        )

        report.report_workunit(mock_workunit)

        assert (
            report.first_urn_seen
            == "urn:li:dataset:(urn:li:dataPlatform:fake,test_table,PROD)"
        )

        mock_workunit2 = Mock()
        mock_workunit2.get_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:fake,test_table2,PROD)"
        )

        report.report_workunit(mock_workunit2)

        assert (
            report.first_urn_seen
            == "urn:li:dataset:(urn:li:dataPlatform:fake,test_table,PROD)"
        )

    def test_workunit_without_urn(self):
        """Test that workunits without get_urn method don't cause errors."""
        report = DataHubMockDataReport()

        mock_workunit = Mock()
        del mock_workunit.get_urn

        report.report_workunit(mock_workunit)

        assert report.first_urn_seen is None
