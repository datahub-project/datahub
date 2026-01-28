import datetime
from unittest.mock import patch

import botocore.exceptions
from botocore.stub import Stubber

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig


def test_empty_s3_key_handling():
    """Test that empty S3 keys in script paths are handled gracefully without crashing."""

    # Mock job with empty S3 key (malformed S3 URI)
    get_jobs_response_with_empty_key = {
        "Jobs": [
            {
                "Name": "test-job-empty-key",
                "Description": "Job with empty S3 key",
                "Role": "arn:aws:iam::123412341234:role/service-role/AWSGlueServiceRole-glue-crawler",
                "CreatedOn": datetime.datetime(2021, 6, 10, 16, 51, 25, 690000),
                "LastModifiedOn": datetime.datetime(2021, 6, 10, 16, 55, 35, 307000),
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {
                    "Name": "glueetl",
                    # This S3 URI has no key/path component - just bucket
                    "ScriptLocation": "s3://bucket-only/",
                    "PythonVersion": "3",
                },
            },
            {
                "Name": "test-job-bucket-only",
                "Description": "Job with bucket-only S3 URI",
                "Role": "arn:aws:iam::123412341234:role/service-role/AWSGlueServiceRole-glue-crawler",
                "CreatedOn": datetime.datetime(2021, 6, 10, 16, 51, 25, 690000),
                "LastModifiedOn": datetime.datetime(2021, 6, 10, 16, 55, 35, 307000),
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {
                    "Name": "glueetl",
                    # This S3 URI has no key/path component - just bucket without trailing slash
                    "ScriptLocation": "s3://bucket-only",
                    "PythonVersion": "3",
                },
            },
        ]
    }

    # Configuration with minimal settings
    config = GlueSourceConfig(
        aws_region="us-west-2",
        extract_transforms=True,
    )

    ctx = PipelineContext(run_id="test_run")

    # Create Glue source instance
    glue_source_instance = GlueSource(config=config, ctx=ctx)

    # Mock the Glue client responses
    with Stubber(glue_source_instance.glue_client) as glue_stubber:
        # Add response for get_databases (required by get_workunits_internal)
        glue_stubber.add_response("get_databases", {"DatabaseList": []}, {})

        # Add response for get_jobs
        glue_stubber.add_response("get_jobs", get_jobs_response_with_empty_key, {})

        # Initialize report counters to 0
        initial_invalid_count = (
            glue_source_instance.report.num_job_script_location_invalid
        )

        # Call the method that processes jobs - this should not crash
        workunits = list(glue_source_instance.get_workunits_internal())

        # Verify that the invalid script location counter was incremented
        # Should increment by 2 since we have 2 jobs with empty keys
        expected_invalid_count = initial_invalid_count + 2
        assert (
            glue_source_instance.report.num_job_script_location_invalid
            == expected_invalid_count
        )

        # Verify that we still get workunits for the jobs themselves (DataFlow entities)
        # even though their script processing failed
        job_names = {"test-job-empty-key", "test-job-bucket-only"}
        dataflow_workunits = [wu for wu in workunits if wu.id in job_names]
        assert (
            len(dataflow_workunits) == 2
        )  # Should have DataFlow entities for both jobs

        # Verify that no S3 get_object calls were made (since we skip processing empty keys)
        # This implicitly tests that we don't crash with the ParamValidationError


def test_get_dataflow_graph_with_empty_key_directly():
    """Test the get_dataflow_graph method directly with empty S3 keys."""

    config = GlueSourceConfig(
        aws_region="us-west-2",
        extract_transforms=True,
    )

    ctx = PipelineContext(run_id="test_run")
    glue_source_instance = GlueSource(config=config, ctx=ctx)

    # Test case 1: S3 URI with trailing slash (empty key)
    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"
    result = glue_source_instance.get_dataflow_graph("s3://bucket-only/", flow_urn)

    assert result is None
    assert glue_source_instance.report.num_job_script_location_invalid == 1

    # Test case 2: S3 URI without trailing slash (still empty key)
    result = glue_source_instance.get_dataflow_graph("s3://bucket-only", flow_urn)

    assert result is None
    assert glue_source_instance.report.num_job_script_location_invalid == 2

    # Test case 3: Valid S3 URI should still work (we won't call S3 but should pass validation)
    # This tests that our validation doesn't break valid paths
    with patch.object(glue_source_instance.s3_client, "get_object") as mock_s3_get:
        mock_s3_get.side_effect = Exception(
            "S3 call should happen but we're mocking it"
        )

        try:
            glue_source_instance.get_dataflow_graph(
                "s3://valid-bucket/path/to/script.py", flow_urn
            )
        except Exception as e:
            # Should reach S3 call and get our mocked exception, proving validation passed
            assert "S3 call should happen" in str(e)

        # Invalid count should remain the same (not incremented for valid path)
        assert glue_source_instance.report.num_job_script_location_invalid == 2


def test_original_error_scenario():
    """Test the exact scenario from the original error to ensure it's fixed."""

    config = GlueSourceConfig(
        aws_region="us-west-2",
        extract_transforms=True,
    )

    ctx = PipelineContext(run_id="test_run")
    glue_source_instance = GlueSource(config=config, ctx=ctx)

    # This should have triggered the original ParamValidationError:
    # "Invalid length for parameter Key, value: 0, valid min length: 1"
    flow_urn = "urn:li:dataFlow:(glue,problematic-job,PROD)"

    # This call should not raise ParamValidationError anymore
    result = glue_source_instance.get_dataflow_graph("s3://bucket/", flow_urn)

    # Should return None instead of crashing
    assert result is None

    # Should increment the invalid counter
    assert glue_source_instance.report.num_job_script_location_invalid == 1


def test_param_validation_error_handling():
    """Test that ParamValidationError from S3 get_object is properly caught and handled."""

    config = GlueSourceConfig(
        aws_region="us-west-2",
        extract_transforms=True,
    )

    ctx = PipelineContext(run_id="test_run")
    glue_source_instance = GlueSource(config=config, ctx=ctx)

    flow_urn = "urn:li:dataFlow:(glue,test-job,PROD)"

    # Test case 1: Mock ParamValidationError during S3 get_object call
    with patch.object(glue_source_instance.s3_client, "get_object") as mock_s3_get:
        # Simulate the ParamValidationError that would occur with invalid S3 parameters
        mock_s3_get.side_effect = botocore.exceptions.ParamValidationError(
            report="Invalid length for parameter Key, value: 0, valid min length: 1"
        )

        initial_invalid_count = (
            glue_source_instance.report.num_job_script_location_invalid
        )

        result = glue_source_instance.get_dataflow_graph(
            "s3://test-bucket/invalid-key", flow_urn
        )

        # Should return None instead of crashing
        assert result is None

        # Should increment the invalid script location counter
        assert (
            glue_source_instance.report.num_job_script_location_invalid
            == initial_invalid_count + 1
        )

        # Verify the S3 get_object was actually called (so we know the error handling is in the right place)
        mock_s3_get.assert_called_once_with(Bucket="test-bucket", Key="invalid-key")

    # Test case 2: Different ParamValidationError scenarios
    with patch.object(glue_source_instance.s3_client, "get_object") as mock_s3_get:
        # Simulate another type of ParamValidationError
        mock_s3_get.side_effect = botocore.exceptions.ParamValidationError(
            report="Parameter validation failed: Invalid bucket name"
        )

        initial_invalid_count = (
            glue_source_instance.report.num_job_script_location_invalid
        )

        result = glue_source_instance.get_dataflow_graph(
            "s3://invalid-bucket-name/script.py", flow_urn
        )

        # Should return None instead of crashing
        assert result is None

        # Should increment the invalid script location counter
        assert (
            glue_source_instance.report.num_job_script_location_invalid
            == initial_invalid_count + 1
        )

        # Verify the S3 get_object was called with the expected parameters
        mock_s3_get.assert_called_once_with(
            Bucket="invalid-bucket-name", Key="script.py"
        )


if __name__ == "__main__":
    test_empty_s3_key_handling()
    test_get_dataflow_graph_with_empty_key_directly()
    test_original_error_scenario()
    test_param_validation_error_handling()
    print("All tests passed!")
