from datahub.ingestion.source.ge_data_profiler import (
    _safe_convert_value,
)


class TestGEProfilerHelpers:
    def test_safe_convert_value(self):
        """Test safe value conversion for different data types."""
        from datetime import date, datetime, time, timedelta

        # Test None values
        assert _safe_convert_value(None) == "None"

        # Test regular values
        assert _safe_convert_value("hello") == "hello"
        assert _safe_convert_value(123) == "123"
        assert _safe_convert_value(3.14) == "3.14"

        # Test datetime objects
        dt = datetime(2023, 1, 1, 12, 30, 45)
        assert _safe_convert_value(dt) == "2023-01-01T12:30:45"

        # Test date objects
        d = date(2023, 1, 1)
        assert _safe_convert_value(d) == "2023-01-01"

        # Test time objects
        t = time(12, 30, 45)
        assert _safe_convert_value(t) == "12:30:45"

        # Test timedelta objects
        td = timedelta(hours=2, minutes=30)
        assert _safe_convert_value(td) == "9000.0"  # total_seconds()
