from datahub.ingestion.graph.client import get_default_graph, DataHubGraph
import sys

import click

@click.command()
@click.option('--urn', type=str, required=True, help='The urn of the dataset that you want to get lineage for')
@click.option('--upstream-only', is_flag=True, default=False)
@click.option('--downstream-only', is_flag=True, default=False)
def get_lineage(urn: str, upstream_only: bool, downstream_only: bool):

    dataset_urn = urn
    graph: DataHubGraph
    with get_default_graph() as graph:
        if not downstream_only:
            # get upstreams
            print("Upstream entities")
            print("-----------------")
            # Note this only uses the "DownstreamOf" relationship to find downstream entities.
            # There are other relationship types like "DerivedFrom" that are also used to show lineage on the UI.
            for entity in graph.get_related_entities(dataset_urn, relationship_types=["DownstreamOf"], direction=DataHubGraph.RelationshipDirection.OUTGOING):
                print("  -" + entity.urn)
            print("-----------------")

        if not upstream_only:
            # get downstreams
            print("Downstream entities")
            print("-----------------")
            # Note this only uses the "DownstreamOf" relationship to find downstream entities.
            # There are other relationship types like "DerivedFrom" that are also used to show lineage on the UI.
            for entity in graph.get_related_entities(dataset_urn, relationship_types=["DownstreamOf"], direction=DataHubGraph.RelationshipDirection.INCOMING):
                print("  -" + entity.urn)
            print("-----------------")

if __name__ == '__main__':
    get_lineage()