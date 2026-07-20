import { NodeContext, addToAdjacencyList, isTransformational, parseEdgeId, setDefault } from '@app/lineageV3/common';
import { GraphStore } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';

import { LineageDirection } from '@types';

type Urn = string;

/**
 * An edge is revealed if either endpoint has been expanded toward the other; unrevealed edges are
 * hidden, even between displayed nodes (e.g. two members of the same data product).
 * Transformational nodes are displayed by expanding the nodes around them rather than being
 * expanded themselves, so they reveal their edges just by being displayed.
 */
function isEdgeRevealed(upstream: Urn, downstream: Urn, graphStore: GraphStore): boolean {
    const upstreamNode = graphStore.nodes.get(upstream);
    if (
        upstreamNode &&
        (isTransformational(upstreamNode, graphStore.rootType) || upstreamNode.isExpanded[LineageDirection.Downstream])
    ) {
        return true;
    }
    const downstreamNode = graphStore.nodes.get(downstream);
    return (
        !!downstreamNode &&
        (isTransformational(downstreamNode, graphStore.rootType) ||
            downstreamNode.isExpanded[LineageDirection.Upstream])
    );
}

/**
 * Returns a copy of the graph store restricted to revealed edges, so that node positioning (both
 * within bounding boxes and at the top level) and edge creation only consider shown edges.
 */
export default function filterToRevealedEdges(graphStore: GraphStore): GraphStore {
    const edges: NodeContext['edges'] = new Map();
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    graphStore.edges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        if (!isEdgeRevealed(upstream, downstream, graphStore)) return;
        edges.set(edgeId, edge);
        addToAdjacencyList(adjacencyList, LineageDirection.Downstream, upstream, downstream);
        if (edge.via) {
            setDefault(adjacencyList[LineageDirection.Upstream], edge.via, new Set()).add(upstream);
            setDefault(adjacencyList[LineageDirection.Downstream], edge.via, new Set()).add(downstream);
        }
    });
    return { ...graphStore, edges, adjacencyList };
}
