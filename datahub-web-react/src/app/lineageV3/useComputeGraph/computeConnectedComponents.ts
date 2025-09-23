import { GraphStoreFields, LineageEntity, LineageNode, NodeContext } from '@app/lineageV3/common';
import getConnectedComponents from '@app/lineageV3/traversals/getConnectedComponents';
import topologicalSort from '@app/lineageV3/traversals/topologicalSort';

interface Output {
    displayedNodesByRoots: Array<[LineageEntity[], LineageNode[]]>;
    parents: Map<string, Set<string>>;
}

/**
 * Topologically sort the DAG and compute the parents of each data job.
 * Assumes passed in nodes form a directed acyclic graph.
 * @returns An object containing:
 *  displayedNodesByRoots: A map of roots to their connected component
 *  parents: A map of each data job urn to its upstream parents
 */
export default function computeConnectedComponents(context: Pick<NodeContext, GraphStoreFields>): Output {
    const { nodes, adjacencyList } = context;
    const urns = Array.from(nodes.keys());
    const roots = new Set(Array.from(nodes.keys()).filter((id) => !adjacencyList.UPSTREAM.get(id)?.size));

    const orderedUrns: string[] = topologicalSort(new Set(urns), adjacencyList.DOWNSTREAM);
    const orderedNodes = orderedUrns.map((id) => nodes.get(id)).filter((node): node is LineageEntity => !!node);

    const components: Set<string>[] = getConnectedComponents(
        new Set(urns),
        new Map(
            urns.map((urn) => [
                urn,
                new Set([...(adjacencyList.DOWNSTREAM.get(urn) || []), ...(adjacencyList.UPSTREAM.get(urn) || [])]),
            ]),
        ),
    );
    const displayedNodesByRoots = components.map((component) => {
        return [
            orderedNodes.filter((n) => component.has(n.urn) && roots.has(n.urn)),
            orderedNodes.filter((n) => component.has(n.urn)),
        ] as [LineageEntity[], LineageNode[]];
    });

    return { displayedNodesByRoots, parents: adjacencyList.UPSTREAM };
}
