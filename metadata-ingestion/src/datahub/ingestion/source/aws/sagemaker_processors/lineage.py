from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, DefaultDict, Dict, List, Set

from datahub.ingestion.source.aws.sagemaker_processors.common import (
    SagemakerSourceReport,
)

if TYPE_CHECKING:
    from mypy_boto3_sagemaker import SageMakerClient
    from mypy_boto3_sagemaker.type_defs import (
        ActionSummaryTypeDef,
        ArtifactSummaryTypeDef,
        AssociationSummaryTypeDef,
        ContextSummaryTypeDef,
    )


@dataclass
class LineageInfo:
    """
    Helper class for containing extracted SageMaker lineage info.
    """

    # map from model URIs to deployed endpoints
    model_uri_endpoints: DefaultDict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    # map from model images to deployed endpoints
    model_image_endpoints: DefaultDict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    # map from group ARNs to model URIs
    model_uri_to_groups: DefaultDict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    # map from group ARNs to model images
    model_image_to_groups: DefaultDict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )


@dataclass
class LineageProcessor:
    sagemaker_client: "SageMakerClient"
    env: str
    report: SagemakerSourceReport
    nodes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    lineage_info: LineageInfo = field(default_factory=LineageInfo)

    def get_all_actions(self) -> List["ActionSummaryTypeDef"]:
        """
        List all actions in SageMaker.
        """

        actions = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_actions
        paginator = self.sagemaker_client.get_paginator("list_actions")
        for page in paginator.paginate():
            actions += page["ActionSummaries"]

        return actions

    def get_all_artifacts(self) -> List["ArtifactSummaryTypeDef"]:
        """
        List all artifacts in SageMaker.
        """

        artifacts = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_artifacts
        paginator = self.sagemaker_client.get_paginator("list_artifacts")
        for page in paginator.paginate():
            artifacts += page["ArtifactSummaries"]

        return artifacts

    def get_all_contexts(self) -> List["ContextSummaryTypeDef"]:
        """
        List all contexts in SageMaker.
        """

        contexts = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_contexts
        paginator = self.sagemaker_client.get_paginator("list_contexts")
        for page in paginator.paginate():
            contexts += page["ContextSummaries"]

        return contexts

    def get_incoming_edges(self, node_arn: str) -> List["AssociationSummaryTypeDef"]:
        """
        Get all incoming edges for a node in the lineage graph.
        """

        edges = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_associations
        paginator = self.sagemaker_client.get_paginator("list_associations")
        for page in paginator.paginate(DestinationArn=node_arn):
            edges += page["AssociationSummaries"]

        return edges

    def get_outgoing_edges(self, node_arn: str) -> List["AssociationSummaryTypeDef"]:
        """
        Get all outgoing edges for a node in the lineage graph.
        """
        edges = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_associations
        paginator = self.sagemaker_client.get_paginator("list_associations")
        for page in paginator.paginate(SourceArn=node_arn):
            edges += page["AssociationSummaries"]

        return edges

    def get_model_deployment_lineage(self, deployment_node_arn: str) -> None:
        """
        Get the lineage of a model deployment (input models and output endpoints).
        """

        # if a node's action type is a ModelDeployment, then the incoming edges will be
        # the model(s) being deployed, and the outgoing edges will be the endpoint(s) created
        incoming_edges = self.get_incoming_edges(deployment_node_arn)
        outgoing_edges = self.get_outgoing_edges(deployment_node_arn)

        # models are linked to endpoints not by their ARNs, but by their output files and/or images
        model_uris = set()
        model_images = set()

        # check the incoming edges for model URIs and images
        for edge in incoming_edges:
            source_node = self.nodes.get(edge["SourceArn"])

            if source_node is None:
                continue

            source_uri = source_node.get("Source", {}).get("SourceUri")

            if edge["SourceType"] == "Model" and source_uri is not None:
                model_uris.add(source_uri)
            elif edge["SourceType"] == "Image" and source_uri is not None:
                model_images.add(source_uri)

        model_endpoints = set()

        # check the outgoing edges for endpoints resulting from the deployment
        for edge in outgoing_edges:
            destination_node = self.nodes[edge["DestinationArn"]]

            if destination_node is None:
                continue

            source_uri = destination_node.get("Source", {}).get("SourceUri")
            source_type = destination_node.get("Source", {}).get("SourceType")

            if (
                edge["DestinationType"] == "Endpoint"
                and source_uri is not None
                and source_type == "ARN"
            ):
                model_endpoints.add(source_uri)

        for model_uri in model_uris:
            self.lineage_info.model_uri_endpoints[model_uri] |= model_endpoints
        for model_image in model_images:
            self.lineage_info.model_image_endpoints[model_image] |= model_endpoints

    def get_model_group_lineage(
        self, model_group_node_arn: str, node: Dict[str, Any]
    ) -> None:
        """
        Get the lineage of a model group (models part of the group).
        """

        model_group_arn = node.get("Source", {}).get("SourceUri")
        model_source_type = node.get("Source", {}).get("SourceType")

        # if group ARN is invalid
        if model_group_arn is None or model_source_type != "ARN":
            return

        # check incoming edges for model packages under the group
        group_incoming_edges = self.get_incoming_edges(model_group_node_arn)

        for edge in group_incoming_edges:
            # if edge is a model package, then look for models in its source edges
            if edge["SourceType"] == "Model":
                model_package_incoming_edges = self.get_incoming_edges(
                    edge["SourceArn"]
                )

                # check incoming edges for models under the model package
                for model_package_edge in model_package_incoming_edges:
                    source_node = self.nodes.get(model_package_edge["SourceArn"])

                    if source_node is None:
                        continue

                    source_uri = source_node.get("Source", {}).get("SourceUri")

                    # add model_group_arn -> model_uri mapping
                    if (
                        model_package_edge["SourceType"] == "Model"
                        and source_uri is not None
                    ):
                        self.lineage_info.model_uri_to_groups[source_uri].add(
                            model_group_arn
                        )

                    # add model_group_arn -> model_image mapping
                    elif (
                        model_package_edge["SourceType"] == "Image"
                        and source_uri is not None
                    ):
                        self.lineage_info.model_image_to_groups[source_uri].add(
                            model_group_arn
                        )

    def get_lineage(self) -> LineageInfo:
        """
        Get the lineage of all artifacts in SageMaker.
        """

        for action in self.get_all_actions():
            self.nodes[action["ActionArn"]] = {**action, "node_type": "action"}
        for artifact in self.get_all_artifacts():
            self.nodes[artifact["ArtifactArn"]] = {**artifact, "node_type": "artifact"}
        for context in self.get_all_contexts():
            self.nodes[context["ContextArn"]] = {**context, "node_type": "context"}

        for node_arn, node in self.nodes.items():
            # get model-endpoint lineage
            if (
                node["node_type"] == "action"
                and node.get("ActionType") == "ModelDeployment"
            ):
                self.get_model_deployment_lineage(node_arn)

            # get model-group lineage
            if (
                node["node_type"] == "context"
                and node.get("ContextType") == "ModelGroup"
            ):
                self.get_model_group_lineage(node_arn, node)

        return self.lineage_info
