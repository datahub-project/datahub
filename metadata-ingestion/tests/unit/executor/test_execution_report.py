from datetime import datetime
from unittest.mock import patch

from datahub.executor.report.execution_report import ExecutionReport, format_report_line


class TestFormatReportLine:
    @patch("datahub.executor.report.execution_report.datetime")
    def test_format_report_line(self, mock_datetime):
        mock_datetime.datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = format_report_line("INFO", "Test message")

        assert result == "2023-01-01 12:00:00 INFO: Test message"


class TestExecutionReport:
    def test_execution_report_init(self):
        report = ExecutionReport("test-exec-id")

        assert report.exec_id == "test-exec-id"
        assert report.infos == []
        assert report.errors == []
        assert report._structured_report is None
        assert report._logs == ""

    @patch("builtins.print")
    def test_report_info_with_logging(self, mock_print):
        report = ExecutionReport("test-exec-id")

        with patch(
            "datahub.executor.report.execution_report.datetime"
        ) as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            report.report_info("Test info message")

        assert len(report.infos) == 1
        assert "2023-01-01 12:00:00 INFO: Test info message" in report.infos[0]
        mock_print.assert_called_once()
        assert "[exec_id=test-exec-id]" in mock_print.call_args[0][0]

    def test_report_info_without_logging(self):
        report = ExecutionReport("test-exec-id")

        with patch(
            "datahub.executor.report.execution_report.datetime"
        ) as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            report.report_info("Test info message", log=False)

        assert len(report.infos) == 1
        assert "2023-01-01 12:00:00 INFO: Test info message" in report.infos[0]

    @patch("builtins.print")
    def test_report_error_with_logging(self, mock_print):
        report = ExecutionReport("test-exec-id")

        with patch(
            "datahub.executor.report.execution_report.datetime"
        ) as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            report.report_error("Test error message")

        assert len(report.errors) == 1
        assert "2023-01-01 12:00:00 ERROR: Test error message" in report.errors[0]
        mock_print.assert_called_once()
        assert "[exec_id=test-exec-id]" in mock_print.call_args[0][0]

    def test_report_error_without_logging(self):
        report = ExecutionReport("test-exec-id")

        with patch(
            "datahub.executor.report.execution_report.datetime"
        ) as mock_datetime:
            mock_datetime.datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            report.report_error("Test error message", log=False)

        assert len(report.errors) == 1
        assert "2023-01-01 12:00:00 ERROR: Test error message" in report.errors[0]

    def test_set_and_get_logs(self):
        report = ExecutionReport("test-exec-id")
        test_logs = "Test log content"

        report.set_logs(test_logs)

        assert report.get_logs() == test_logs

    def test_set_and_get_structured_report(self):
        report = ExecutionReport("test-exec-id")
        test_report = "Test structured report content"

        report.set_structured_report(test_report)

        assert report.get_structured_report() == test_report

    def test_get_structured_report_default_none(self):
        report = ExecutionReport("test-exec-id")

        assert report.get_structured_report() is None
