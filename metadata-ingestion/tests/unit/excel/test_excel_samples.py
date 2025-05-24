import io

from datahub.ingestion.source.excel.excel_file import ExcelFile
from datahub.ingestion.source.excel.report import ExcelSourceReport


def test_sample_files(pytestconfig):
    file_names = [
        ("file_1.xlsx", "Monthly Reporting", 1, 5, 4, 17),
        ("file_1.xlsx", "Dec", 1, 4, 3, 14),
        ("file_1.xlsx", "Jan", 1, 5, 4, 14),
        ("file_1.xlsx", "Feb", 1, 5, 4, 14),
        ("file_2.xlsx", "Test Group Reporting ", 1, 19, 18, 46),
        ("file_3.xlsx", "Sheet1", 1, 5, 4, 209),
        ("file_4.xlsx", "in", 1, 3, 2, 252),
        ("file_5.xlsx", "Test1_Test", 4, 8, 4, 24),
        ("file_6.xlsx", "Test2_Test", 2, 6, 4, 24),
        ("file_7.xlsx", "12345678 (Current Month)", 1, 4, 3, 68),
        ("file_8.xlsx", "Test3_Test", 4, 8, 4, 24),
        ("file_9.xlsx", "Business Report", 6, 11, 5, 5),
        ("file_10.xlsx", "Sheet1", 0, 0, 0, 0),
        ("file_10.xlsx", "Sheet2", 0, 0, 0, 0),
    ]
    test_resources_dir = pytestconfig.rootpath / "tests/unit/excel"

    for file_name, sheet, header, footer, rows, columns in file_names:
        sample_file = test_resources_dir / f"data/{file_name}"
        report = ExcelSourceReport()

        assert sample_file.exists()

        with open(sample_file, "rb") as f:
            file_content = f.read()
        bytes_io = io.BytesIO(file_content)

        xls = ExcelFile(file_name, bytes_io, report)
        result = xls.load_workbook()
        assert result is True

        table = xls.get_table(sheet)

        if table is None:
            assert header == 0
        else:
            assert table.header_row == header
            assert table.footer_row == footer
            assert table.row_count == rows
            assert table.column_count == columns
