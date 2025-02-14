from unittest.mock import patch

from botocore.stub import Stubber
from freezegun import freeze_time

import datahub.ingestion.source.aws.sagemaker_processors.models
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.aws.sagemaker import (
    SagemakerSource,
    SagemakerSourceConfig,
)
from datahub.ingestion.source.aws.sagemaker_processors.jobs import (
    job_type_to_info,
    job_types,
)
from datahub.testing.doctest import assert_doctest
from tests.test_helpers import mce_helpers
from tests.unit.sagemaker.test_sagemaker_source_stubs import (
    describe_endpoint_response_1,
    describe_endpoint_response_2,
    describe_feature_group_response_1,
    describe_feature_group_response_2,
    describe_feature_group_response_3,
    describe_group_response,
    describe_model_response_1,
    describe_model_response_2,
    get_first_model_package_incoming_response,
    get_model_group_incoming_response,
    get_second_model_package_incoming_response,
    job_stubs,
    list_actions_response,
    list_artifacts_response,
    list_contexts_response,
    list_endpoints_response,
    list_feature_groups_response,
    list_first_endpoint_incoming_response,
    list_first_endpoint_outgoing_response,
    list_groups_response,
    list_models_response,
    list_second_endpoint_incoming_response,
    list_second_endpoint_outgoing_response,
)

FROZEN_TIME = "2020-04-14 07:00:00"


def sagemaker_source() -> SagemakerSource:
    return SagemakerSource(
        ctx=PipelineContext(run_id="sagemaker-source-test"),
        config=SagemakerSourceConfig(aws_region="us-west-2"),
    )


@freeze_time(FROZEN_TIME)
def test_sagemaker_ingest(tmp_path, pytestconfig):
    sagemaker_source_instance = sagemaker_source()

    with Stubber(sagemaker_source_instance.sagemaker_client) as sagemaker_stubber:
        sagemaker_stubber.add_response(
            "list_actions",
            list_actions_response,
            {},
        )

        sagemaker_stubber.add_response(
            "list_artifacts",
            list_artifacts_response,
            {},
        )

        sagemaker_stubber.add_response(
            "list_contexts",
            list_contexts_response,
            {},
        )

        sagemaker_stubber.add_response(
            "list_associations",
            list_first_endpoint_incoming_response,
            {
                "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-first-endpoint"
            },
        )
        sagemaker_stubber.add_response(
            "list_associations",
            list_first_endpoint_outgoing_response,
            {
                "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-first-endpoint"
            },
        )

        sagemaker_stubber.add_response(
            "list_associations",
            list_second_endpoint_incoming_response,
            {
                "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-second-endpoint"
            },
        )
        sagemaker_stubber.add_response(
            "list_associations",
            list_second_endpoint_outgoing_response,
            {
                "SourceArn": "arn:aws:sagemaker:us-west-2:123412341234:action/deploy-the-second-endpoint"
            },
        )

        sagemaker_stubber.add_response(
            "list_associations",
            get_model_group_incoming_response,
            {
                "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:context/a-model-package-group-context"
            },
        )
        sagemaker_stubber.add_response(
            "list_associations",
            get_first_model_package_incoming_response,
            {
                "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-first-model-package-artifact"
            },
        )
        sagemaker_stubber.add_response(
            "list_associations",
            get_second_model_package_incoming_response,
            {
                "DestinationArn": "arn:aws:sagemaker:us-west-2:123412341234:artifact/the-second-model-package-artifact"
            },
        )

        sagemaker_stubber.add_response(
            "list_feature_groups",
            list_feature_groups_response,
            {},
        )
        sagemaker_stubber.add_response(
            "describe_feature_group",
            describe_feature_group_response_1,
            {
                "FeatureGroupName": "test-2",
            },
        )
        sagemaker_stubber.add_response(
            "describe_feature_group",
            describe_feature_group_response_2,
            {
                "FeatureGroupName": "test-1",
            },
        )
        sagemaker_stubber.add_response(
            "describe_feature_group",
            describe_feature_group_response_3,
            {
                "FeatureGroupName": "test",
            },
        )

        for job_type in job_types:
            job = job_stubs[job_type.value]

            job_info = job_type_to_info[job_type]

            sagemaker_stubber.add_response(
                job_info.list_command,
                job["list"],
                {},
            )

        for job_type in job_types:
            job = job_stubs[job_type.value]

            job_info = job_type_to_info[job_type]

            sagemaker_stubber.add_response(
                job_info.describe_command,
                job["describe"],
                {job_info.describe_name_key: job["describe_name"]},
            )

        sagemaker_stubber.add_response(
            "list_endpoints",
            list_endpoints_response,
            {},
        )

        sagemaker_stubber.add_response(
            "describe_endpoint",
            describe_endpoint_response_1,
            {"EndpointName": "the-first-endpoint"},
        )

        sagemaker_stubber.add_response(
            "describe_endpoint",
            describe_endpoint_response_2,
            {"EndpointName": "the-second-endpoint"},
        )

        sagemaker_stubber.add_response(
            "list_model_package_groups",
            list_groups_response,
            {},
        )

        sagemaker_stubber.add_response(
            "describe_model_package_group",
            describe_group_response,
            {"ModelPackageGroupName": "a-model-package-group"},
        )

        sagemaker_stubber.add_response(
            "list_models",
            list_models_response,
            {},
        )

        sagemaker_stubber.add_response(
            "describe_model",
            describe_model_response_1,
            {"ModelName": "the-first-model"},
        )

        sagemaker_stubber.add_response(
            "describe_model",
            describe_model_response_2,
            {"ModelName": "the-second-model"},
        )

        # Patch the client factory's get_client method to return the stubbed client for jobs
        with patch.object(
            sagemaker_source_instance.client_factory,
            "get_client",
            return_value=sagemaker_source_instance.sagemaker_client,
        ):
            # Run the test and generate the MCEs
            mce_objects = [
                wu.metadata for wu in sagemaker_source_instance.get_workunits()
            ]
            write_metadata_file(tmp_path / "sagemaker_mces.json", mce_objects)

    # Verify the output.
    test_resources_dir = pytestconfig.rootpath / "tests/unit/sagemaker"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "sagemaker_mces.json",
        golden_path=test_resources_dir / "sagemaker_mces_golden.json",
    )


def test_doc_test_run() -> None:
    assert_doctest(datahub.ingestion.source.aws.sagemaker_processors.models)
