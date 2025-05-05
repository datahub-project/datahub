import unittest
from io import BytesIO, TextIOBase
from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd

from datahub.ingestion.source.excel.excel_file import ExcelFile, ExcelTable
from datahub.ingestion.source.excel.report import ExcelSourceReport


class TestExcelFile(unittest.TestCase):
    def setUp(self):
        self.report = ExcelSourceReport()
        self.filename = "test_file.xlsx"

        self.bytes_data = b"mock bytes data"
        self.string_data = "mock string data"

        self.mock_wb = MagicMock()
        self.mock_wb.sheetnames = ["Sheet1", "Sheet2"]
        self.mock_wb.active.title = "Sheet1"

        self.mock_properties = MagicMock()
        self.mock_properties.title = "Test Title"
        self.mock_properties.creator = "Test Creator"
        self.mock_properties.subject = "Test Subject"
        self.mock_properties.description = "Test Description"
        self.mock_properties.keywords = "Test Keywords"
        self.mock_properties.category = "Test Category"
        self.mock_properties.lastModifiedBy = "Test Last Modified By"
        self.mock_properties.created = "2023-01-01"
        self.mock_properties.modified = "2023-01-02"
        self.mock_properties.contentStatus = "Test Status"
        self.mock_properties.revision = "Test Revision"
        self.mock_properties.version = "Test Version"
        self.mock_properties.language = "Test Language"
        self.mock_properties.identifier = "Test Identifier"

        self.mock_wb.properties = self.mock_properties

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_bytes(self, mock_load_workbook):
        mock_load_workbook.return_value = self.mock_wb
        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)

        result = excel_file.load_workbook()

        self.assertTrue(result)
        mock_load_workbook.assert_called_once()
        self.assertEqual(excel_file.sheet_list, ["Sheet1", "Sheet2"])
        self.assertEqual(excel_file.active_sheet, "Sheet1")

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_string(self, mock_load_workbook):
        mock_load_workbook.return_value = self.mock_wb
        excel_file = ExcelFile(self.filename, self.string_data, self.report)

        result = excel_file.load_workbook()

        self.assertTrue(result)
        mock_load_workbook.assert_called_once()
        self.assertEqual(excel_file.sheet_list, ["Sheet1", "Sheet2"])
        self.assertEqual(excel_file.active_sheet, "Sheet1")

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_readable_seekable_binary_io(self, mock_load_workbook):
        mock_load_workbook.return_value = self.mock_wb
        mock_file = BytesIO(self.bytes_data)
        excel_file = ExcelFile(self.filename, mock_file, self.report)

        result = excel_file.load_workbook()

        self.assertTrue(result)
        mock_load_workbook.assert_called_once()

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_readable_seekable_text_io(self, mock_load_workbook):
        mock_load_workbook.return_value = self.mock_wb

        class MockTextIO(TextIOBase):
            def __init__(self):
                super().__init__()

            def seekable(self) -> bool:
                return True

            def seek(self, offset: int, whence: int = 0) -> int:
                return 0

            def tell(self) -> int:
                return 0

            def read(self, size: Optional[int] = None) -> str:
                return "text data"

            def readable(self) -> bool:
                return True

        mock_file = MockTextIO()
        excel_file = ExcelFile(self.filename, mock_file, self.report)

        result = excel_file.load_workbook()

        self.assertTrue(result)
        mock_load_workbook.assert_called_once()

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_readable_non_seekable_string_content(
        self, mock_load_workbook
    ):
        mock_load_workbook.return_value = self.mock_wb

        mock_file = MagicMock()
        mock_file.read.return_value = "non-seekable string content"
        mock_file.seekable.return_value = False

        excel_file = ExcelFile(self.filename, mock_file, self.report)

        result = excel_file.load_workbook()

        self.assertTrue(result)
        mock_load_workbook.assert_called_once()

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_readable_non_seekable_bytes_content(
        self, mock_load_workbook
    ):
        mock_load_workbook.return_value = self.mock_wb

        mock_file = MagicMock()
        mock_file.read.return_value = b"non-seekable bytes content"
        mock_file.seekable.return_value = False

        excel_file = ExcelFile(self.filename, mock_file, self.report)

        result = excel_file.load_workbook()

        self.assertTrue(result)
        mock_load_workbook.assert_called_once()

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_invalid_data_type(self, mock_load_workbook):
        mock_load_workbook.side_effect = TypeError("Invalid data type")
        excel_file = ExcelFile(self.filename, 123, self.report)  # type: ignore

        result = excel_file.load_workbook()

        self.assertFalse(result)
        self.assertEqual(len(self.report.filtered), 1)
        self.assertIn(self.filename, self.report.filtered)

    @patch("openpyxl.load_workbook")
    def test_load_workbook_with_exception(self, mock_load_workbook):
        mock_load_workbook.side_effect = Exception("Test exception")
        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)

        result = excel_file.load_workbook()

        self.assertFalse(result)
        self.assertEqual(len(self.report.filtered), 1)
        self.assertIn(self.filename, self.report.filtered)

    def test_sheet_names_property(self):
        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)
        excel_file.sheet_list = ["Sheet1", "Sheet2"]

        sheet_names = excel_file.sheet_names

        self.assertEqual(sheet_names, ["Sheet1", "Sheet2"])

    def test_active_sheet_name_property(self):
        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)
        excel_file.active_sheet = "Sheet1"

        active_sheet = excel_file.active_sheet_name

        self.assertEqual(active_sheet, "Sheet1")

    def test_workbook_properties_property(self):
        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)
        excel_file.properties = {"title": "Test Title"}

        properties = excel_file.workbook_properties

        self.assertEqual(properties, {"title": "Test Title"})

    @patch("openpyxl.load_workbook")
    def test_get_tables(self, mock_load_workbook):
        mock_load_workbook.return_value = self.mock_wb
        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)
        excel_file.load_workbook()

        excel_file.get_table = MagicMock()  # type: ignore
        mock_table1 = ExcelTable(
            df=pd.DataFrame(),
            header_row=1,
            footer_row=5,
            row_count=4,
            column_count=3,
            metadata={},
            sheet_name="Sheet1",
        )
        mock_table2 = ExcelTable(
            df=pd.DataFrame(),
            header_row=1,
            footer_row=5,
            row_count=4,
            column_count=3,
            metadata={},
            sheet_name="Sheet2",
        )
        excel_file.get_table.side_effect = [mock_table1, mock_table2]

        tables = excel_file.get_tables()

        self.assertEqual(len(tables), 2)
        self.assertEqual(tables[0].sheet_name, "Sheet1")
        self.assertEqual(tables[1].sheet_name, "Sheet2")
        excel_file.get_table.assert_any_call("Sheet1")
        excel_file.get_table.assert_any_call("Sheet2")

    @patch("openpyxl.load_workbook")
    def test_get_table(self, mock_load_workbook):
        mock_sheet = MagicMock()

        mock_cell1 = MagicMock()
        mock_cell1.value = "Header1"
        mock_cell2 = MagicMock()
        mock_cell2.value = "Header2"
        mock_cell3 = MagicMock()
        mock_cell3.value = None

        row1 = [mock_cell1, mock_cell2, mock_cell3]

        mock_cell4 = MagicMock()
        mock_cell4.value = "Data1"
        mock_cell5 = MagicMock()
        mock_cell5.value = "Data2"
        mock_cell6 = MagicMock()
        mock_cell6.value = None
        row2 = [mock_cell4, mock_cell5, mock_cell6]

        mock_sheet.rows = [row1, row2]
        mock_sheet.title = "TestSheet"

        mock_wb = MagicMock()
        mock_wb.__getitem__.return_value = mock_sheet
        mock_wb.properties = self.mock_properties

        with patch(
            "datahub.ingestion.source.excel.excel_file.ExcelFile.find_header_row",
            return_value=0,
        ), patch(
            "datahub.ingestion.source.excel.excel_file.ExcelFile.find_footer_start",
            return_value=2,
        ), patch(
            "datahub.ingestion.source.excel.excel_file.ExcelFile.extract_metadata",
            return_value={},
        ):
            excel_file = ExcelFile(self.filename, self.bytes_data, self.report)
            excel_file.wb = mock_wb
            excel_file.properties = excel_file.read_excel_properties(mock_wb)

            table = excel_file.get_table("TestSheet")

            self.assertIsInstance(table, ExcelTable)
            self.assertEqual(table.sheet_name, "TestSheet")
            self.assertEqual(table.header_row, 1)
            self.assertEqual(table.footer_row, 2)
            self.assertEqual(list(table.df.columns), ["Header1", "Header2"])

    def test_find_header_row(self):
        test_cases = [
            {
                "rows": [
                    ["Title", None],
                    ["Name", "Age", "City"],
                    ["John", 25, "New York"],
                    ["Jane", 30, "Boston"],
                ],
                "expected": 1,
            },
            {
                "rows": [
                    ["Name", "Age", "City"],
                    ["John", 25, "New York"],
                    ["Jane", 30, "Boston"],
                    ["Tom", 45, "Chicago"],
                ],
                "expected": 0,
            },
            {
                "rows": [
                    ["Report", "Sales Summary"],
                    ["Date", "2023-01-01"],
                    [None, None, None],
                    ["Product", "Quantity", "Revenue"],
                    ["Item1", 100, 1000],
                    ["Item2", 200, 2000],
                    ["Item3", 300, 3000],
                ],
                "expected": 3,
            },
            {
                "rows": [
                    [None, None, None],
                    [None, None, None],
                ],
                "expected": 0,
            },
        ]

        excel_file = ExcelFile(self.filename, self.bytes_data, self.report)
        for i, case in enumerate(test_cases):
            result = excel_file.find_header_row(case["rows"])  # type: ignore

            self.assertEqual(result, case["expected"], f"Test case {i + 1} failed")

    def test_find_footer_start(self):
        test_cases = [
            {
                "rows": [
                    ["Name", "Age", "City"],
                    ["John", 25, "New York"],
                    ["Jane", 30, "Boston"],
                    ["Total", 55, None],
                ],
                "header_row_idx": 0,
                "expected": 3,
            },
            {
                "rows": [
                    ["Product", "Quantity", "Revenue"],
                    ["Item1", 100, 1000],
                    ["Item2", 200, 2000],
                    [None, None, None],
                    ["Note: Data is preliminary"],
                ],
                "header_row_idx": 0,
                "expected": 3,
            },
            {
                "rows": [
                    ["ID", "Value"],
                    [1, 100],
                    [2, 200],
                ],
                "header_row_idx": 0,
                "expected": 3,
            },
        ]

        for i, case in enumerate(test_cases):
            result = ExcelFile.find_footer_start(case["rows"], case["header_row_idx"])  # type: ignore

            self.assertEqual(result, case["expected"], f"Test case {i + 1} failed")

    def test_extract_metadata(self):
        test_cases = [
            {
                "rows": [
                    ["Title", "Sales Report"],
                    ["Date", "2023-01-01"],
                    ["Author", "John Doe"],
                ],
                "expected": {
                    "Title": "Sales Report",
                    "Date": "2023-01-01",
                    "Author": "John Doe",
                },
            },
            {
                "rows": [
                    ["Report Title:", "Q1 Performance"],
                    ["Generated on:", "2023-04-01"],
                ],
                "expected": {
                    "Report Title": "Q1 Performance",
                    "Generated on": "2023-04-01",
                },
            },
            {
                "rows": [
                    ["Title", "Monthly Summary"],
                    [None, None],
                    ["Data", "Value1", "Value2"],
                ],
                "expected": {"Title": "Monthly Summary"},
            },
        ]

        for i, case in enumerate(test_cases):
            result = ExcelFile.extract_metadata(case["rows"])  # type: ignore

            self.assertEqual(result, case["expected"], f"Test case {i + 1} failed")  # type: ignore

    def test_read_excel_properties(self):
        mock_wb = MagicMock()
        mock_wb.properties = self.mock_properties

        properties = ExcelFile.read_excel_properties(mock_wb)

        self.assertEqual(properties["title"], "Test Title")
        self.assertEqual(properties["author"], "Test Creator")
        self.assertEqual(properties["subject"], "Test Subject")
        self.assertEqual(properties["description"], "Test Description")

        mock_wb.custom_doc_props = MagicMock()
        mock_custom_prop1 = MagicMock()
        mock_custom_prop1.name = "CustomProp1"
        mock_custom_prop1.value = "Value1"

        mock_custom_prop2 = MagicMock()
        mock_custom_prop2.name = "title"
        mock_custom_prop2.value = "Custom Title"

        mock_wb.custom_doc_props.props = [mock_custom_prop1, mock_custom_prop2]

        properties = ExcelFile.read_excel_properties(mock_wb)

        self.assertEqual(properties["CustomProp1"], "Value1")
        self.assertEqual(properties["title"], "Test Title")
        self.assertEqual(properties["custom.title"], "Custom Title")
