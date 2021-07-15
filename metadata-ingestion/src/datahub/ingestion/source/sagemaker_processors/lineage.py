from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, DefaultDict, Set
from collections import defaultdict

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sagemaker_processors.common import SagemakerSourceReport


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


@dataclass
class LineageProcessor:
    sagemaker_client: Any
    env: str
    report: SagemakerSourceReport

    def get_all_actions(self) -> List[Dict[str, Any]]:
        """
        List all actions in SageMaker.
        """

        actions = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_actions
        paginator = self.sagemaker_client.get_paginator("list_actions")
        for page in paginator.paginate():
            actions += page["ActionSummaries"]

        return actions

    def get_all_artifacts(self) -> List[Dict[str, Any]]:
        """
        List all artifacts in SageMaker.
        """

        artifacts = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_artifacts
        paginator = self.sagemaker_client.get_paginator("list_artifacts")
        for page in paginator.paginate():
            artifacts += page["ArtifactSummaries"]

        return artifacts

    def get_all_contexts(self) -> List[Dict[str, Any]]:
        """
        List all contexts in SageMaker.
        """

        contexts = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_contexts
        paginator = self.sagemaker_client.get_paginator("list_contexts")
        for page in paginator.paginate():
            contexts += page["ContextSummaries"]

        return contexts

    def get_incoming_edges(self, node_arn: str) -> List[Dict[str, Any]]:
        """
        Get all incoming edges for a node in the lineage graph.
        """

        edges = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_associations
        paginator = self.sagemaker_client.get_paginator("list_associations")
        for page in paginator.paginate(DestinationArn=node_arn):
            edges += page["AssociationSummaries"]

        return edges

    def get_outgoing_edges(self, node_arn: str) -> List[Dict[str, Any]]:
        """
        Get all outgoing edges for a node in the lineage graph.
        """
        edges = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_associations
        paginator = self.sagemaker_client.get_paginator("list_associations")
        for page in paginator.paginate(SourceArn=node_arn):
            edges += page["AssociationSummaries"]

        return edges

    def get_lineage(self):
        """
        Get the lineage of all artifacts in SageMaker.
        """

        nodes = {}

        edges = {}

        for action in self.get_all_actions():
            nodes[action["ActionArn"]] = {**action, "node_type": "action"}
        for artifact in self.get_all_artifacts():
            nodes[artifact["ArtifactArn"]] = {**artifact, "node_type": "artifact"}
        for context in self.get_all_contexts():
            nodes[context["ContextArn"]] = {**context, "node_type": "context"}

        # a map from model URIs to a set of ARNs for endpoints
        model_uri_endpoints = defaultdict(set)
        # a map from model images to a set of ARNs for endpoints
        model_image_endpoints = defaultdict(set)

        for node_arn, node in nodes.items():

            if node["node_type"] == "action":
                # if a node's action type is a ModelDeployment, then the incoming edges will be
                # the model(s) being deployed, and the outgoing edges will be the endpoint(s) created
                if node["ActionType"] == "ModelDeployment":
                    incoming_edges = self.get_incoming_edges(node_arn)
                    outgoing_edges = self.get_outgoing_edges(node_arn)

                    model_uris = set()
                    model_images = set()

                    for edge in incoming_edges:

                        source_node = nodes.get(edge["SourceArn"])

                        if source_node is None:
                            continue

                        source_uri = source_node.get("Source", {}).get("SourceUri")

                        if edge["SourceType"] == "Model" and source_uri is not None:
                            model_uris.add(
                                model_uri_endpoints[edge["Source"]["SourceUri"]]
                            )
                        elif edge["SourceType"] == "Image" and source_uri is not None:
                            model_images.add([edge["Source"]["ImageUri"]])

                    model_endpoints = set()

                    for edge in outgoing_edges:

                        destination_node = nodes[edge["DestinationArn"]]

                        if destination_node is None:
                            continue

                        source_uri = destination_node.get("Source", {}).get("SourceUri")
                        source_type = destination_node.get("Source", {}).get(
                            "SourceType"
                        )

                        if (
                            edge["DestinationType"] == "Endpoint"
                            and source_uri is not None
                            and source_type == "ARN"
                        ):

                            model_endpoints.add(source_uri)

                    for endpoint in model_endpoints:
                        model_uri_endpoints[endpoint] |= model_uris
                        model_image_endpoints[endpoint] |= model_images

        return LineageInfo(
            model_uri_endpoints=model_uri_endpoints,
            model_image_endpoints=model_image_endpoints,
        )
