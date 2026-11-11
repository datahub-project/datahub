import copy
import itertools
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import time_machine
from botocore.stub import Stubber

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.sagemaker import (
    SagemakerSource,
    SagemakerSourceConfig,
)
from datahub.ingestion.source.aws.sagemaker_processors.jobs import (
    job_type_to_info,
    job_types,
)
from datahub.utilities.urns.urn import guess_entity_type
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
ARN = "arn:aws:sagemaker:us-west-2:123412341234"
# The source tags job list responses in place, and test_sagemaker_source.py runs it on the
# shared fixtures; copy them at import, before any test runs.
PRISTINE_JOB_STUBS = copy.deepcopy(job_stubs)

# Source aspects in emission order, then the browsePathsV2 that AutoBrowsePathV2Processor
# derives from the entity's consecutive work units.
EXPECTED_ASPECTS: Dict[str, List[str]] = {
    "mlFeature": ["mlFeatureProperties"],
    "mlPrimaryKey": ["mlPrimaryKeyProperties"],
    "mlFeatureTable": ["browsePaths", "mlFeatureTableProperties", "browsePathsV2"],
    "dataset": ["datasetProperties"],
    "dataFlow": ["dataFlowInfo"],
    "dataJob": ["dataJobInfo", "browsePaths", "dataJobInputOutput", "browsePathsV2"],
    "mlModelDeployment": ["mlModelDeploymentProperties"],
    "mlModelGroup": [
        "mlModelGroupProperties",
        "ownership",
        "browsePaths",
        "browsePathsV2",
    ],
    "mlModel": ["mlModelProperties", "browsePaths", "browsePathsV2"],
}


def _stub_responses() -> List[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    responses: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = [
        ("list_actions", list_actions_response, {}),
        ("list_artifacts", list_artifacts_response, {}),
        ("list_contexts", list_contexts_response, {}),
        (
            "list_associations",
            list_first_endpoint_incoming_response,
            {"DestinationArn": f"{ARN}:action/deploy-the-first-endpoint"},
        ),
        (
            "list_associations",
            list_first_endpoint_outgoing_response,
            {"SourceArn": f"{ARN}:action/deploy-the-first-endpoint"},
        ),
        (
            "list_associations",
            list_second_endpoint_incoming_response,
            {"DestinationArn": f"{ARN}:action/deploy-the-second-endpoint"},
        ),
        (
            "list_associations",
            list_second_endpoint_outgoing_response,
            {"SourceArn": f"{ARN}:action/deploy-the-second-endpoint"},
        ),
        (
            "list_associations",
            get_model_group_incoming_response,
            {"DestinationArn": f"{ARN}:context/a-model-package-group-context"},
        ),
        (
            "list_associations",
            get_first_model_package_incoming_response,
            {"DestinationArn": f"{ARN}:artifact/the-first-model-package-artifact"},
        ),
        (
            "list_associations",
            get_second_model_package_incoming_response,
            {"DestinationArn": f"{ARN}:artifact/the-second-model-package-artifact"},
        ),
        ("list_feature_groups", list_feature_groups_response, {}),
        (
            "describe_feature_group",
            describe_feature_group_response_1,
            {"FeatureGroupName": "test-2"},
        ),
        (
            "describe_feature_group",
            describe_feature_group_response_2,
            {"FeatureGroupName": "test-1"},
        ),
        (
            "describe_feature_group",
            describe_feature_group_response_3,
            {"FeatureGroupName": "test"},
        ),
    ]
    for job_type in job_types:
        job_info = job_type_to_info[job_type]
        responses.append(
            (job_info.list_command, PRISTINE_JOB_STUBS[job_type.value]["list"], {})
        )
    for job_type in job_types:
        job = PRISTINE_JOB_STUBS[job_type.value]
        job_info = job_type_to_info[job_type]
        responses.append(
            (
                job_info.describe_command,
                job["describe"],
                {job_info.describe_name_key: job["describe_name"]},
            )
        )
    responses += [
        ("list_endpoints", list_endpoints_response, {}),
        (
            "describe_endpoint",
            describe_endpoint_response_1,
            {"EndpointName": "the-first-endpoint"},
        ),
        (
            "describe_endpoint",
            describe_endpoint_response_2,
            {"EndpointName": "the-second-endpoint"},
        ),
        ("list_model_package_groups", list_groups_response, {}),
        (
            "describe_model_package_group",
            # No creator user profile, so the group's Ownership is written with an empty owners list.
            {**describe_group_response, "CreatedBy": {}},
            {"ModelPackageGroupName": "a-model-package-group"},
        ),
        ("list_models", list_models_response, {}),
        ("describe_model", describe_model_response_1, {"ModelName": "the-first-model"}),
        (
            "describe_model",
            describe_model_response_2,
            {"ModelName": "the-second-model"},
        ),
    ]
    return responses


def _aspect_stream(workunits: List[MetadataWorkUnit]) -> List[Tuple[str, str]]:
    """(urn, aspect name) in stream order; fails on any work unit that is not an MCP."""
    stream: List[Tuple[str, str]] = []
    for wu in workunits:
        metadata = wu.metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper), wu.id
        assert metadata.entityUrn and metadata.aspectName
        stream.append((metadata.entityUrn, metadata.aspectName))
    return stream


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sagemaker_entity_aspects_stay_consecutive() -> None:
    source = SagemakerSource(
        ctx=PipelineContext(run_id="sagemaker-stream-order-test"),
        config=SagemakerSourceConfig(aws_region="us-west-2"),
    )
    with Stubber(source.sagemaker_client) as stubber:
        for method, response, params in _stub_responses():
            stubber.add_response(method, copy.deepcopy(response), params)
        with patch.object(
            source.client_factory,
            "get_client",
            return_value=source.sagemaker_client,
        ):
            workunits = list(source.get_workunits())

    assert not source.report.failures

    # Only auto-status writes status, for every URN at the end of the stream; the source adds none.
    aspect_stream = _aspect_stream(workunits)
    first_status = [aspect for _, aspect in aspect_stream].index("status")
    assert all(aspect == "status" for _, aspect in aspect_stream[first_status:])
    stream = aspect_stream[:first_status]
    runs = [
        (urn, [aspect for _, aspect in group])
        for urn, group in itertools.groupby(stream, key=lambda pair: pair[0])
    ]

    run_urns = [urn for urn, _ in runs]
    assert len(run_urns) == len(set(run_urns)), "an entity's aspects are split"
    for urn, aspects in runs:
        assert aspects == EXPECTED_ASPECTS[guess_entity_type(urn)], urn

    run_types = [
        "feature"
        if guess_entity_type(urn) in {"mlFeature", "mlPrimaryKey"}
        else guess_entity_type(urn)
        for urn in run_urns
    ]
    collapsed = [entity_type for entity_type, _ in itertools.groupby(run_types)]
    assert collapsed == (
        ["feature", "mlFeatureTable"] * 3
        + ["dataset"]
        + ["dataFlow", "dataJob"] * 7
        + ["mlModelDeployment", "mlModelGroup", "mlModel"]
    )

    # Each job follows its own flow.
    for i, urn in enumerate(run_urns):
        if guess_entity_type(urn) == "dataJob":
            assert urn.startswith(f"urn:li:dataJob:({run_urns[i - 1]},"), urn
