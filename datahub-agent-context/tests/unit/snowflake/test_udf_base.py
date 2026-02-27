"""Tests for datahub.ai.snowflake.udfs.base module."""

from datahub_agent_context.snowflake.udfs.base import generate_python_udf_code


class TestGeneratePythonUdfCode:
    """Tests for generate_python_udf_code function."""

    def test_basic_udf_generation(self) -> None:
        """Test basic UDF SQL generation."""
        result = generate_python_udf_code(
            function_name="TEST_FUNCTION",
            parameters=[("param1", "STRING")],
            return_type="VARIANT",
            function_body="return {'success': True}",
        )

        assert "CREATE OR REPLACE FUNCTION TEST_FUNCTION(param1 STRING)" in result
        assert "RETURNS VARIANT" in result
        assert "LANGUAGE PYTHON" in result
        assert "RUNTIME_VERSION = '3.10'" in result
        assert "HANDLER = 'test_function'" in result

    def test_multiple_parameters(self) -> None:
        """Test UDF with multiple parameters."""
        result = generate_python_udf_code(
            function_name="MULTI_PARAM_FUNCTION",
            parameters=[
                ("param1", "STRING"),
                ("param2", "NUMBER"),
                ("param3", "VARIANT"),
            ],
            return_type="STRING",
            function_body="return str(param1)",
        )

        assert (
            "MULTI_PARAM_FUNCTION(param1 STRING, param2 NUMBER, param3 VARIANT)"
            in result
        )
        assert "def multi_param_function(param1, param2, param3):" in result

    def test_no_parameters(self) -> None:
        """Test UDF with no parameters."""
        result = generate_python_udf_code(
            function_name="NO_PARAM_FUNCTION",
            parameters=[],
            return_type="VARIANT",
            function_body="return {}",
        )

        assert "CREATE OR REPLACE FUNCTION NO_PARAM_FUNCTION()" in result
        assert "def no_param_function():" in result

    def test_package_dependencies(self) -> None:
        """Test that datahub-agent-context package is included."""
        result = generate_python_udf_code(
            function_name="TEST_FUNCTION",
            parameters=[("p", "STRING")],
            return_type="VARIANT",
            function_body="return {}",
        )

        assert "PACKAGES = ('datahub-agent-context==1.4.0.3')" in result

    def test_secrets_configuration(self) -> None:
        """Test that secrets are properly configured."""
        result = generate_python_udf_code(
            function_name="TEST_FUNCTION",
            parameters=[("p", "STRING")],
            return_type="VARIANT",
            function_body="return {}",
        )

        assert (
            "SECRETS = ('datahub_url_secret' = datahub_url, 'datahub_token_secret' = datahub_token)"
            in result
        )

    def test_external_access_integration(self) -> None:
        """Test that external access integration is configured."""
        result = generate_python_udf_code(
            function_name="TEST_FUNCTION",
            parameters=[("p", "STRING")],
            return_type="VARIANT",
            function_body="return {}",
        )

        assert "EXTERNAL_ACCESS_INTEGRATIONS = (datahub_access)" in result

    def test_imports_included(self) -> None:
        """Test that required imports are included."""
        result = generate_python_udf_code(
            function_name="TEST_FUNCTION",
            parameters=[("p", "STRING")],
            return_type="VARIANT",
            function_body="return {}",
        )

        assert "import _snowflake" in result
        assert "from datahub.sdk.main_client import DataHubClient" in result
        assert "from datahub_agent_context.context import DataHubContext" in result

    def test_function_body_indentation(self) -> None:
        """Test that function body is properly indented."""
        function_body = """x = 1
y = 2
return x + y"""

        result = generate_python_udf_code(
            function_name="TEST_FUNCTION",
            parameters=[],
            return_type="NUMBER",
            function_body=function_body,
        )

        # Check that body lines are indented within the function
        assert "def test_function():" in result
        assert "    x = 1" in result
        assert "    y = 2" in result
        assert "    return x + y" in result

    def test_handler_name_lowercase(self) -> None:
        """Test that handler name is lowercase version of function name."""
        result = generate_python_udf_code(
            function_name="MY_COOL_FUNCTION",
            parameters=[],
            return_type="VARIANT",
            function_body="return {}",
        )

        assert "HANDLER = 'my_cool_function'" in result
        assert "def my_cool_function():" in result

    def test_different_return_types(self) -> None:
        """Test UDF generation with different return types."""
        for return_type in ["VARIANT", "STRING", "NUMBER", "BOOLEAN"]:
            result = generate_python_udf_code(
                function_name="TEST_FUNCTION",
                parameters=[],
                return_type=return_type,
                function_body="return None",
            )

            assert f"RETURNS {return_type}" in result

    def test_complete_structure(self) -> None:
        """Test that generated UDF has all required components."""
        result = generate_python_udf_code(
            function_name="COMPLETE_FUNCTION",
            parameters=[("input", "STRING")],
            return_type="VARIANT",
            function_body="return {'input': input}",
        )

        # Verify complete SQL structure
        assert result.startswith("CREATE OR REPLACE FUNCTION")
        assert "RETURNS VARIANT" in result
        assert "LANGUAGE PYTHON" in result
        assert "RUNTIME_VERSION = '3.10'" in result
        assert (
            "ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository" in result
        )
        assert "PACKAGES = ('datahub-agent-context==1.4.0.3')" in result
        assert "SECRETS = " in result
        assert "EXTERNAL_ACCESS_INTEGRATIONS = " in result
        assert "HANDLER = " in result
        assert "AS $$" in result
        assert "import _snowflake" in result
        assert "def complete_function" in result
        assert "$$;" in result
