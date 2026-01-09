"""Tests for the pages/_shared.py module."""

from scripts.streamlit_explorer.common import _shorten_urn


class TestShortenUrn:
    """Tests for the _shorten_urn helper function.

    The function simply truncates from the end with ellipsis if URN exceeds max_length.
    """

    def test_shorten_monitor_urn(self):
        """Test shortening a monitor URN truncates from end."""
        urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset2-v2,PROD),__system__volume)"
        result = _shorten_urn(urn)
        # Truncates from end, so starts with beginning of URN
        assert result.startswith("urn:li:monitor:")
        assert len(result) <= 60
        assert result.endswith("...")

    def test_shorten_monitor_urn_snowflake(self):
        """Test shortening a Snowflake monitor URN truncates from end."""
        urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD),__system__volume)"
        result = _shorten_urn(urn)
        # Truncates from end, so starts with beginning of URN
        assert result.startswith("urn:li:monitor:")
        assert len(result) <= 60
        assert result.endswith("...")

    def test_shorten_assertion_urn(self):
        """Test shortening an assertion URN truncates from end."""
        urn = "urn:li:assertion:dXJuOmxpOmRhdGFzZXQ6KHVybjpsaTpkYXRhUGxhdGZvcm06a2Fma2EsU2FtcGxlQ3lwcmVzc0thZmthRGF0YXNldCxQUk9EKQ==-__system__volume"
        result = _shorten_urn(urn)
        assert result.startswith("urn:li:assertion:")
        assert len(result) <= 60
        assert result.endswith("...")

    def test_shorten_empty_urn(self):
        """Test shortening an empty URN."""
        assert _shorten_urn("") == ""
        assert _shorten_urn(None) == ""  # type: ignore[arg-type]

    def test_shorten_short_urn(self):
        """Test that short URNs are not truncated."""
        urn = "urn:li:assertion:abc123"
        result = _shorten_urn(urn)
        assert result == urn  # No truncation needed
        assert "..." not in result

    def test_shorten_unknown_urn_format(self):
        """Test fallback for unknown URN formats."""
        urn = "urn:li:unknown:some-long-identifier-that-needs-truncation-to-fit-in-display"
        result = _shorten_urn(urn, max_length=40)
        assert len(result) <= 40
        assert result.endswith("...")
