import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
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


class TestRuntimeParameters:
    """Test suite for runtime parameter template rendering."""

    def test_apply_runtime_parameters_success(self) -> None:
        """Test successful parameter substitution."""
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            apply_runtime_parameters,
        )

        result = apply_runtime_parameters(
            "SELECT * FROM table WHERE date = ${date}",
            {"date": "'2024-01-01'"},
        )
        assert result == "SELECT * FROM table WHERE date = '2024-01-01'"

    def test_apply_runtime_parameters_missing_param_includes_context(self) -> None:
        """Test that missing parameter error includes template and parameter context."""
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            apply_runtime_parameters,
        )
        from datahub_executor.common.exceptions import InvalidParametersException

        with pytest.raises(InvalidParametersException) as exc_info:
            apply_runtime_parameters(
                "SELECT * FROM table WHERE date = ${date} AND city = ${city}",
                {"date": "'2024-01-01'"},
            )

        # Verify error includes context (parameters is string representation)
        error = exc_info.value
        assert "'template'" in error.parameters
        assert "'provided_parameters'" in error.parameters
        assert "'missing_variable'" in error.parameters
        assert (
            "SELECT * FROM table WHERE date = ${date} AND city = ${city}"
            in error.parameters
        )
        assert "['date']" in error.parameters
        assert "'city'" in error.parameters

    def test_extract_variable_name(self) -> None:
        """Test variable name extraction from Jinja2 error messages."""
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            _extract_variable_name,
        )

        assert _extract_variable_name("'start' is undefined") == "start"
        assert _extract_variable_name("'city' is undefined") == "city"
        assert (
            _extract_variable_name("'date_partition' is undefined") == "date_partition"
        )
        assert _extract_variable_name("some other error") is None

    def test_render_sql_template_empty_result_includes_context(self) -> None:
        """Test that empty result error includes template and render context."""
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            render_sql_template,
        )
        from datahub_executor.common.exceptions import InvalidParametersException

        with pytest.raises(InvalidParametersException) as exc_info:
            render_sql_template("  ;  ", {}, require_nonempty=True)

        # Verify error includes context
        error = exc_info.value
        assert "original_template" in error.parameters
        assert "rendered_result" in error.parameters
        assert "cleaned_result" in error.parameters
        assert "provided_parameters" in error.parameters


class TestConcurrency:
    """Test suite for thread safety of template rendering.

    These tests verify that creating a fresh Jinja2 Environment per call
    avoids race conditions during concurrent template compilation and rendering.
    """

    def test_concurrent_template_compilation(self) -> None:
        """Test concurrent calls with different templates and parameters.

        Spawns 50+ threads calling apply_runtime_parameters simultaneously,
        each with different templates and parameters, to verify no race
        conditions occur during template compilation.
        """
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            apply_runtime_parameters,
        )

        def render_template(thread_id: int) -> tuple[int, str]:
            """Render a unique template for this thread."""
            template = f"SELECT * FROM table_{thread_id} WHERE id = ${{id}} AND status = ${{status}}"
            params = {"id": str(thread_id), "status": f"'active_{thread_id}'"}
            result = apply_runtime_parameters(template, params)
            return thread_id, result

        # Run 50 threads concurrently
        num_threads = 50
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(render_template, i) for i in range(num_threads)]
            results = {
                future.result()[0]: future.result()[1]
                for future in as_completed(futures)
            }

        # Verify all threads completed successfully with correct results
        assert len(results) == num_threads
        for thread_id in range(num_threads):
            expected = f"SELECT * FROM table_{thread_id} WHERE id = {thread_id} AND status = 'active_{thread_id}'"
            assert results[thread_id] == expected

    def test_concurrent_same_template_rendering(self) -> None:
        """Test multiple threads rendering the same template with different parameters.

        Verifies no parameter leakage between threads when using the same
        template structure concurrently.
        """
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            apply_runtime_parameters,
        )

        template = "SELECT * FROM users WHERE city = ${city} AND age > ${age}"

        def render_with_params(city: str, age: int) -> str:
            """Render the template with specific parameters."""
            params = {"city": f"'{city}'", "age": str(age)}
            return apply_runtime_parameters(template, params)

        # Test data: different cities and ages
        test_cases = [
            ("New York", 25),
            ("Chicago", 30),
            ("Los Angeles", 35),
            ("Boston", 40),
            ("Seattle", 45),
        ] * 10  # Repeat to create 50 threads

        # Run all threads concurrently
        with ThreadPoolExecutor(max_workers=len(test_cases)) as executor:
            futures = [
                executor.submit(render_with_params, city, age)
                for city, age in test_cases
            ]
            results = [future.result() for future in as_completed(futures)]

        # Verify all threads completed successfully
        assert len(results) == len(test_cases)

        # Verify correctness of results (check a sample)
        expected_results = {
            "SELECT * FROM users WHERE city = 'New York' AND age > 25",
            "SELECT * FROM users WHERE city = 'Chicago' AND age > 30",
            "SELECT * FROM users WHERE city = 'Los Angeles' AND age > 35",
            "SELECT * FROM users WHERE city = 'Boston' AND age > 40",
            "SELECT * FROM users WHERE city = 'Seattle' AND age > 45",
        }
        assert all(result in expected_results for result in results)

    def test_concurrent_stress_test(self) -> None:
        """Stress test with 1000+ high-frequency calls from multiple threads.

        Tests mix of simple and complex templates to verify consistent results
        under high load.
        """
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            apply_runtime_parameters,
        )

        def render_complex_template(iteration: int) -> str:
            """Render a complex template with multiple parameters."""
            template = (
                "SELECT t1.id, t1.name, t2.value "
                "FROM table1 t1 "
                "JOIN table2 t2 ON t1.id = t2.id "
                "WHERE t1.created_at >= ${start_date} "
                "AND t1.status IN (${statuses}) "
                "AND t2.value > ${threshold} "
                "ORDER BY ${order_by} "
                "LIMIT ${limit}"
            )
            params = {
                "start_date": f"'2024-01-{(iteration % 28) + 1:02d}'",
                "statuses": "'active', 'pending'",
                "threshold": str(iteration % 100),
                "order_by": "t1.created_at DESC",
                "limit": str((iteration % 50) + 1),
            }
            return apply_runtime_parameters(template, params)

        # Run 1000 iterations across 20 threads
        num_iterations = 1000
        max_workers = 20
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(render_complex_template, i)
                for i in range(num_iterations)
            ]
            results = [future.result() for future in as_completed(futures)]

        # Verify all completed successfully
        assert len(results) == num_iterations

        # Verify all results are non-empty SQL strings
        assert all(isinstance(result, str) and len(result) > 0 for result in results)
        assert all("SELECT" in result and "FROM" in result for result in results)

    def test_concurrent_template_errors(self) -> None:
        """Test multiple threads triggering UndefinedError simultaneously.

        Verifies error isolation - one thread's error doesn't affect others.
        """
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            apply_runtime_parameters,
        )
        from datahub_executor.common.exceptions import InvalidParametersException

        def render_with_missing_param(thread_id: int) -> tuple[int, bool, str | None]:
            """Attempt to render template with missing parameter.

            Returns: (thread_id, success, error_message)
            """
            template = f"SELECT * FROM table WHERE id = ${{id_{thread_id}}}"
            try:
                # Intentionally provide wrong parameter name
                result = apply_runtime_parameters(template, {"wrong_param": "value"})
                return thread_id, True, result  # Unexpected success
            except InvalidParametersException as e:
                # Expected error
                return thread_id, False, str(e)

        # Run 30 threads that all fail with missing parameters
        num_threads = 30
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(render_with_missing_param, i)
                for i in range(num_threads)
            ]
            results = [future.result() for future in as_completed(futures)]

        # Verify all threads raised exceptions (not success)
        assert len(results) == num_threads
        assert all(not success for _, success, _ in results)

        # Verify each error message contains the correct missing variable for that thread
        for _thread_id, success, error_msg in results:
            assert not success
            assert error_msg is not None
            assert "Missing required SQL runtime parameter" in error_msg
