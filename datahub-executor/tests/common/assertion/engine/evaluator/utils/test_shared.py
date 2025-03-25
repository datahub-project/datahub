import base64
from unittest.mock import MagicMock

import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    ASSERTION_TYPES_REQUIRING_TRAINING,
    encode_monitor_urn,
    is_field_metric_assertion,
    is_training_required,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionType,
)


class TestSharedUtils:
    """Test suite for shared utility functions."""

    @pytest.fixture
    def volume_assertion(self) -> MagicMock:
        """Create a volume assertion fixture."""
        assertion = MagicMock(spec=Assertion)
        assertion.type = AssertionType.VOLUME
        assertion.is_inferred = True
        assertion.field_assertion = None
        return assertion

    @pytest.fixture
    def freshness_assertion(self) -> MagicMock:
        """Create a freshness assertion fixture."""
        assertion = MagicMock(spec=Assertion)
        assertion.type = AssertionType.FRESHNESS
        assertion.is_inferred = True
        assertion.field_assertion = None
        return assertion

    @pytest.fixture
    def field_assertion(self) -> MagicMock:
        """Create a field assertion fixture."""
        assertion = MagicMock(spec=Assertion)
        assertion.type = AssertionType.FIELD
        assertion.is_inferred = True
        assertion.field_assertion = None
        return assertion

    @pytest.fixture
    def schema_assertion(self) -> MagicMock:
        """Create a schema assertion fixture."""
        assertion = MagicMock(spec=Assertion)
        assertion.type = AssertionType.DATA_SCHEMA
        assertion.is_inferred = True
        assertion.field_assertion = None
        return assertion

    @pytest.fixture
    def field_metric_assertion(self) -> MagicMock:
        """Create a field metric assertion fixture."""
        field_metric_assertion = MagicMock()
        field_assertion = MagicMock()
        field_assertion.field_metric_assertion = field_metric_assertion

        assertion = MagicMock(spec=Assertion)
        assertion.field_assertion = field_assertion
        return assertion

    @pytest.fixture
    def non_field_metric_assertion(self) -> MagicMock:
        """Create a non-field metric assertion fixture."""
        field_assertion = MagicMock()
        field_assertion.field_metric_assertion = None

        assertion = MagicMock(spec=Assertion)
        assertion.field_assertion = field_assertion
        return assertion

    @pytest.fixture
    def assertion_without_field_assertion(self) -> MagicMock:
        """Create an assertion without field_assertion fixture."""
        assertion = MagicMock(spec=Assertion)
        assertion.field_assertion = None
        return assertion

    def test_is_training_required_volume_inferred(
        self, volume_assertion: MagicMock
    ) -> None:
        """Test is_training_required with an inferred volume assertion."""
        assert is_training_required(volume_assertion) is True

    def test_is_training_required_freshness_inferred(
        self, freshness_assertion: MagicMock
    ) -> None:
        """Test is_training_required with an inferred freshness assertion."""
        assert is_training_required(freshness_assertion) is True

    def test_is_training_required_field_inferred(
        self, field_assertion: MagicMock
    ) -> None:
        """Test is_training_required with an inferred field assertion."""
        assert is_training_required(field_assertion) is True

    def test_is_training_required_schema_inferred(
        self, schema_assertion: MagicMock
    ) -> None:
        """Test is_training_required with an inferred schema assertion."""
        assert is_training_required(schema_assertion) is False

    def test_is_training_required_volume_not_inferred(
        self, volume_assertion: MagicMock
    ) -> None:
        """Test is_training_required with a non-inferred volume assertion."""
        volume_assertion.is_inferred = False
        assert is_training_required(volume_assertion) is False

    def test_is_training_required_freshness_not_inferred(
        self, freshness_assertion: MagicMock
    ) -> None:
        """Test is_training_required with a non-inferred freshness assertion."""
        freshness_assertion.is_inferred = False
        assert is_training_required(freshness_assertion) is False

    def test_is_training_required_field_not_inferred(
        self, field_assertion: MagicMock
    ) -> None:
        """Test is_training_required with a non-inferred field assertion."""
        field_assertion.is_inferred = False
        assert is_training_required(field_assertion) is False

    def test_is_field_metric_assertion_with_field_metric(
        self, field_metric_assertion: MagicMock
    ) -> None:
        """Test is_field_metric_assertion with a field metric assertion."""
        assert is_field_metric_assertion(field_metric_assertion) is True

    def test_is_field_metric_assertion_with_field_non_metric(
        self, non_field_metric_assertion: MagicMock
    ) -> None:
        """Test is_field_metric_assertion with a field non-metric assertion."""
        assert is_field_metric_assertion(non_field_metric_assertion) is False

    def test_is_field_metric_assertion_without_field_assertion(
        self, assertion_without_field_assertion: MagicMock
    ) -> None:
        """Test is_field_metric_assertion with an assertion without field_assertion."""
        assert is_field_metric_assertion(assertion_without_field_assertion) is False

    def test_encode_monitor_urn_simple(self) -> None:
        """Test encode_monitor_urn with a simple URN."""
        urn = "urn:li:monitor:123"
        encoded = encode_monitor_urn(urn)

        # Verify encoding format (base64 URL safe)
        assert isinstance(encoded, str)

        # Verify we can decode it back
        decoded_bytes = base64.urlsafe_b64decode(encoded.encode("utf-8"))
        decoded = decoded_bytes.decode("utf-8")
        assert decoded == urn

    def test_encode_monitor_urn_complex(self) -> None:
        """Test encode_monitor_urn with a more complex URN."""
        urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.my-table,PROD),field_stats)"
        encoded = encode_monitor_urn(urn)

        # Verify encoding format (base64 URL safe)
        assert isinstance(encoded, str)

        # Verify we can decode it back
        decoded_bytes = base64.urlsafe_b64decode(encoded.encode("utf-8"))
        decoded = decoded_bytes.decode("utf-8")
        assert decoded == urn

    def test_encode_monitor_urn_with_special_chars(self) -> None:
        """Test encode_monitor_urn with URN containing special characters."""
        urn = "urn:li:monitor:test/with+special&chars?"
        encoded = encode_monitor_urn(urn)

        # Verify encoding format (base64 URL safe)
        assert isinstance(encoded, str)

        # Verify we can decode it back
        decoded_bytes = base64.urlsafe_b64decode(encoded.encode("utf-8"))
        decoded = decoded_bytes.decode("utf-8")
        assert decoded == urn

    def test_assertion_types_requiring_training(self) -> None:
        """Test that ASSERTION_TYPES_REQUIRING_TRAINING contains the expected types."""
        assert AssertionType.VOLUME in ASSERTION_TYPES_REQUIRING_TRAINING
        assert AssertionType.FRESHNESS in ASSERTION_TYPES_REQUIRING_TRAINING
        assert AssertionType.FIELD in ASSERTION_TYPES_REQUIRING_TRAINING
        assert AssertionType.DATA_SCHEMA not in ASSERTION_TYPES_REQUIRING_TRAINING
