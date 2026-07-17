import { Edge } from 'reactflow';

import { LINEAGE_TABLE_EDGE_NAME } from '@app/lineageV3/LineageEdge/LineageTableEdge';
import {
    EdgeId,
    LineageEdge,
    LineageEdgeData,
    LineageFilter,
    createEdgeId,
    isTransformational,
    parseEdgeId,
} from '@app/lineageV3/common';
import { LINEAGE_ARROW_MARKER } from '@app/lineageV3/lineageSVGs';
import { GraphStore } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.utils';

import { LineageDirection } from '@types';

type Urn = string;

interface EdgeEndpoint {
    id: string;
    urn: Urn;
    dataProduct?: Urn;
}

/**
 * Creates all flow edges: the graph store's entity-level edges between displayed nodes, plus an
 * edge attaching each lineage filter node to its parent. Member endpoints map to all of their
 * displayed nodes (an entity in multiple data products has one node per data product), and edges
 * through displayed query nodes are split into two segments, as in NodeBuilder.createEdges.
 */
export default function buildFlowEdges(
    graphStore: GraphStore,
    displayedMembership: Map<Urn, Urn[]>,
    displayedFreeIds: Set<string>,
    filterNodes: LineageFilter[],
): Edge<LineageEdgeData>[] {
    const flowEdges = new Map<string, Edge<LineageEdgeData>>();

    const endpointsFor = (urn: Urn): EdgeEndpoint[] => {
        const dataProducts = displayedMembership.get(urn);
        if (dataProducts) {
            return dataProducts.map((dataProduct) => ({
                id: createMemberNodeId(dataProduct, urn),
                urn,
                dataProduct,
            }));
        }
        if (displayedFreeIds.has(urn)) return [{ id: urn, urn }];
        return [];
    };

    graphStore.edges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        const segments: [Urn, Urn][] =
            edge.via && displayedFreeIds.has(edge.via)
                ? [
                      [upstream, edge.via],
                      [edge.via, downstream],
                  ]
                : [[upstream, downstream]];

        segments.forEach(([source, target]) => {
            endpointsFor(source).forEach((sourceEndpoint) => {
                endpointsFor(target).forEach((targetEndpoint) => {
                    const id = createEdgeId(sourceEndpoint.id, targetEndpoint.id);
                    if (!flowEdges.has(id)) {
                        flowEdges.set(id, createFlowEdge(id, sourceEndpoint, targetEndpoint, edge, edgeId, graphStore));
                    }
                });
            });
        });
    });

    // Attach each lineage filter node to its parent's displayed nodes
    filterNodes.forEach((node) => {
        const parentNode = graphStore.nodes.get(node.parent);
        const parentMarker =
            parentNode && !isTransformational(parentNode, graphStore.rootType) ? LINEAGE_ARROW_MARKER : undefined;
        endpointsFor(node.parent).forEach((endpoint) => {
            const [source, target] =
                node.direction === LineageDirection.Upstream ? [node.id, endpoint.id] : [endpoint.id, node.id];
            const id = createEdgeId(source, target);
            const markerEnd = node.direction === LineageDirection.Upstream ? parentMarker : LINEAGE_ARROW_MARKER;
            flowEdges.set(id, { id, source, target, type: LINEAGE_TABLE_EDGE_NAME, markerEnd });
        });
    });

    return Array.from(flowEdges.values());
}

function createFlowEdge(
    id: string,
    sourceEndpoint: EdgeEndpoint,
    targetEndpoint: EdgeEndpoint,
    edge: LineageEdge,
    originalId: EdgeId,
    graphStore: GraphStore,
): Edge<LineageEdgeData> {
    const targetNode = graphStore.nodes.get(targetEndpoint.urn);
    const markerEnd =
        targetNode && !isTransformational(targetNode, graphStore.rootType) ? LINEAGE_ARROW_MARKER : undefined;
    return {
        id,
        source: sourceEndpoint.id,
        target: targetEndpoint.id,
        type: LINEAGE_TABLE_EDGE_NAME,
        markerEnd,
        data: { ...edge, originalId },
    };
}
