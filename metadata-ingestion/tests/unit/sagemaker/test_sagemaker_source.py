import datetime
from unittest.mock import ANY, MagicMock, call, patch

import boto3
import pytest
from botocore.credentials import Credentials
from botocore.stub import Stubber
from freezegun import freeze_time

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
from tests.test_helpers import mce_helpers
from tests.unit.test_sagemaker_source_stubs import (
    describe_endpoint_response_1,
    describe_endpoint_response_2,
    describe_feature_group_response_1,
    describe_feature_group_response_2,
    describe_feature_group_response_3,
    describe_group_response,
    describe_model_response_1,
    describe_model_response_2,
    get_caller_identity_response,
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


def get_aws_client(mock_client_sts, mock_client_sagemaker):
    def _get_client(client_type, *args, **kwargs):
        if client_type == "sts":
            return mock_client_sts
        elif client_type == "sagemaker":
            return mock_client_sagemaker
        raise Exception("client type not handled by the mock")

    return _get_client


@pytest.fixture
def frozen_time():
    with freeze_time(FROZEN_TIME) as f:
        yield f


@patch("datahub.ingestion.source.aws.aws_common.Session")
def test_token_refreshing(mock_session, frozen_time):
    """
    We should move it to separate class once the same logic is shared by other AWS sources
    """
    sagemaker_source_instance = SagemakerSource(
        ctx=PipelineContext(run_id="sagemaker-source-test"),
        config=SagemakerSourceConfig(
            aws_region="us-west-2",
            aws_role="arn:aws:iam::123456789000:role/test-role-for-ingestor",
        ),
    )

    mock_client_sagemaker = boto3.client("sagemaker", region_name="us-west-2")
    mock_client_sts = boto3.client("sts")
    mock_session.return_value.client.side_effect = get_aws_client(
        mock_client_sts, mock_client_sagemaker
    )
    mock_session.return_value.profile_name = "mock_session"
    sagemaker_stubber = Stubber(mock_client_sagemaker)
    sts_stubber = Stubber(mock_client_sts)

    mock_session.return_value.get_credentials.return_value = Credentials(
        access_key="A", secret_key="B", token="C"
    )

    session_initialization_call_list = [
        call(region_name="us-west-2"),  # auth via auto-detect
        call().client("sts"),  # get sts client to print current role
        call().get_credentials(),  # get credentials
        call(
            aws_access_key_id="A",
            aws_secret_access_key="B",
            aws_session_token="C",
            region_name="us-west-2",
        ),  # use credentials to auth
        call().client("sts"),  # assume role
        call(
            aws_access_key_id="DDDDDDDDDDDDDDDD",
            aws_secret_access_key="EEEEEEEEEEEEEEEE",
            aws_session_token="FFFFFFFFFFFFFFFF",
            region_name="us-west-2",
        ),  # use new credentials to auth
        call().client("sts"),  # get sts client to print assumed role
        call().client("sagemaker", config=ANY),  # get sagemaker client
    ]

    # now session initialisation will be:
    # 1. auto-detect current role
    # 2. print the current identity (hence first get_caller_identity)
    # 3. iterate over normalized_roles and assume the fake role (hence assume_role call)
    # 4. print final identity before returning (hence second get_caller_identity)
    sts_stubber.add_response(
        "get_caller_identity",
        get_caller_identity_response,
        {},
    )
    # the role will expire 1 hour from now
    sts_stubber.add_response(
        "assume_role",
        {
            "Credentials": {
                "AccessKeyId": "DDDDDDDDDDDDDDDD",
                "SecretAccessKey": "EEEEEEEEEEEEEEEE",
                "SessionToken": "FFFFFFFFFFFFFFFF",
                "Expiration": datetime.datetime.now() + datetime.timedelta(hours=1),
            }
        },
        {
            "RoleArn": "arn:aws:iam::123456789000:role/test-role-for-ingestor",
            "RoleSessionName": "DatahubIngestionSource",
        },
    )
    sts_stubber.add_response(
        "get_caller_identity",
        {
            "UserId": "ABCDEFGHIJKLMNOPRSTUW:dummy-user",
            "Account": "123456789000",
            "Arn": "arn:aws:sts::123456789000:assumed-role/test-role-for-ingestor/DatahubIngestionSource",
        },
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
        describe_feature_group_response_1,
        {
            "FeatureGroupName": "test-2",
        },
    )
    sagemaker_stubber.add_response(
        "describe_feature_group",
        describe_feature_group_response_1,
        {
            "FeatureGroupName": "test-2",
        },
    )

    sts_stubber.activate()
    sagemaker_stubber.activate()

    # this will trigger initialization of the session
    sagemaker_source_instance.client_factory.get_client().describe_feature_group(
        FeatureGroupName="test-2"
    )

    assert (
        mock_session.mock_calls == session_initialization_call_list
    ), "Initialization was not done as expected after first get_client() call"

    frozen_time.tick(delta=datetime.timedelta(minutes=6))
    # this will reuse the session
    sagemaker_source_instance.client_factory.get_client().describe_feature_group(
        FeatureGroupName="test-2"
    )
    assert (
        mock_session.mock_calls == session_initialization_call_list
    ), "Expected cached session to be used after 6 minutes get_client() call"

    # this will trigger session refresh therefor we setup stub to answer properly
    frozen_time.tick(delta=datetime.timedelta(minutes=50))
    sts_stubber.add_response(
        "get_caller_identity",
        get_caller_identity_response,
        {},
    )
    # the role will expire 1 hour from now
    sts_stubber.add_response(
        "assume_role",
        {
            "Credentials": {
                "AccessKeyId": "DDDDDDDDDDDDDDDD",
                "SecretAccessKey": "EEEEEEEEEEEEEEEE",
                "SessionToken": "FFFFFFFFFFFFFFFF",
                "Expiration": datetime.datetime.now() + datetime.timedelta(hours=1),
            }
        },
        {
            "RoleArn": "arn:aws:iam::123456789000:role/test-role-for-ingestor",
            "RoleSessionName": "DatahubIngestionSource",
        },
    )
    sts_stubber.add_response(
        "get_caller_identity",
        {
            "UserId": "ABCDEFGHIJKLMNOPRSTUW:dummy-user",
            "Account": "123456789000",
            "Arn": "arn:aws:sts::123456789000:assumed-role/test-role-for-ingestor/DatahubIngestionSource",
        },
        {},
    )
    sagemaker_source_instance.client_factory.get_client().describe_feature_group(
        FeatureGroupName="test-2"
    )
    assert (
        mock_session.mock_calls
        == session_initialization_call_list + session_initialization_call_list
    ), "Expected session will be re-initialized after 56 minutes"

    sts_stubber.assert_no_pending_responses()
    sagemaker_stubber.assert_no_pending_responses()


@freeze_time(FROZEN_TIME)
def test_sagemaker_ingest(tmp_path, pytestconfig):
    sagemaker_source_instance = sagemaker_source()

    mock_session = MagicMock()
    mock_client_sagemaker = boto3.client("sagemaker", region_name="us-west-2")
    mock_client_sts = boto3.client("sts")
    mock_session.client.side_effect = get_aws_client(
        mock_client_sts, mock_client_sagemaker
    )
    sagemaker_stubber = Stubber(mock_client_sagemaker)
    sts_stubber = Stubber(mock_client_sts)

    # this is needed because running tests run on DEBUG-level log. This in turn causes `get_session` logic to
    # check session details against STS
    sts_stubber.add_response(
        "get_caller_identity",
        get_caller_identity_response,
        {},
    )

    sts_stubber.add_response(
        "get_caller_identity",
        get_caller_identity_response,
        {},
    )

    sts_stubber.activate()

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

    sagemaker_stubber.activate()

    with patch(
        "datahub.ingestion.source.aws.aws_common.Session", return_value=mock_session
    ):
        # Run the test and generate the MCEs
        mce_objects = [wu.metadata for wu in sagemaker_source_instance.get_workunits()]
        write_metadata_file(tmp_path / "sagemaker_mces.json", mce_objects)

    sagemaker_stubber.assert_no_pending_responses()
    sts_stubber.assert_no_pending_responses()

    # Verify the output.
    test_resources_dir = pytestconfig.rootpath / "tests/unit/sagemaker"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "sagemaker_mces.json",
        golden_path=test_resources_dir / "sagemaker_mces_golden.json",
    )
