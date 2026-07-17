import {
    GraphStoreFields,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageToggles,
    NodeContext,
    filterAdjacencyList,
    setDefault,
} from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeLineageGraph from '@app/lineageV3/useComputeGraph/computeLineageGraph';
import buildFlowEdges from '@app/lineageV3/useComputeGraph/dataProduct/buildFlowEdges';
import { BoxLayout, GraphStore } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';
import {
    MAX_DISPLAYED_DATA_PRODUCT_MEMBERS,
    collectDataProductGroups,
    computeMembership,
} from '@app/lineageV3/useComputeGraph/dataProduct/dataProductGroups';
import layoutDataProductInterior, {
    createBoundingBoxNode,
} from '@app/lineageV3/useComputeGraph/dataProduct/layoutDataProductInterior';
import positionTopLevelNodes from '@app/lineageV3/useComputeGraph/dataProduct/positionTopLevelNodes';
import filterToRevealedEdges from '@app/lineageV3/useComputeGraph/dataProduct/revealedEdges';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';
import { generateCompareNodesFunction } from '@app/lineageV3/useComputeGraph/orderNodes';

import { EntityType, LineageDirection } from '@types';

type Urn = string;

/**
 * Builds the node filter for the data product graph: nodes are hidden until their data product
 * membership is known (since membership determines how they are rendered), and data process
 * instances are hidden unless `showDataProcessInstances` is set.
 */
function createDataProductNodeFilter(
    rootUrn: Urn,
    showDataProcessInstances: boolean,
): (node: LineageEntity) => boolean {
    return (node: LineageEntity) =>
        (showDataProcessInstances || node.type !== EntityType.DataProcessInstance) &&
        (node.urn === rootUrn || node.type === EntityType.Query || node.dataProducts !== undefined);
}

/**
 * Computes the lineage graph for a DataProduct, arranged as a graph of bounding-box containers:
 * 1. Displayed nodes are computed globally via `computeLineageGraph`, seeded by the home data
 *    product's members (up to MAX_DISPLAYED_DATA_PRODUCT_MEMBERS, since the home product has no
 *    lineage of its own), using the standard impact analysis display rules (per-node filters,
 *    expansion state, lineage filter nodes). Members of data products sort before other children,
 *    so they're prioritized by pagination.
 * 2. Displayed entities are grouped by data product membership. Displayed members of each data
 *    product are laid out horizontally via NodeBuilder, determining the size of the data product's
 *    bounding box. An entity in multiple data products gets one node per data product.
 * 3. Bounding boxes and free entities (displayed entities not in any data product) are positioned
 *    together via BoundingBoxNodeBuilder, in a single graph rooted at the home data product's
 *    bounding box: lineage between two data products' members becomes an edge between their boxes,
 *    and lineage between a member and a free entity becomes an edge between the box and the free
 *    entity. Boxes are placed first, from the data-product-to-data-product lineage; free entities
 *    are then arranged around them in layers.
 *
 * Everything is based on the displayed nodes alone: a data product whose members are only
 * connected through non-displayed entities does not get a bounding box, and edges are the direct
 * lineage edges between displayed nodes — shown only if revealed by expansion (`isEdgeRevealed`).
 */
export default function computeDataProductGraph(
    urn: string,
    context: Pick<
        NodeContext,
        GraphStoreFields | LineageToggles | 'rootType' | 'outputPortsOnly' | 'dataProductEntities'
    >,
    ignoreSchemaFieldStatus: boolean,
    showFilterNodes = true,
) {
    const {
        nodes,
        edges,
        adjacencyList,
        rootType,
        showDataProcessInstances,
        showGhostEntities,
        outputPortsOnly,
        dataProductEntities,
    } = context;

    // An entity is an output port if it's an output port of any of its data products.
    const isOutputPort = (id: Urn) => !!nodes.get(id)?.dataProducts?.some((dataProduct) => dataProduct.isOutputPort);

    // Note: `hideTransformations` and `hideDataProcessInstances` connect edges by traversing from the
    // root urn, which has no lineage of its own here, so data process instances are excluded via the
    // filter instead and transformations are always shown.
    const config: HideNodesConfig = {
        hideTransformations: false,
        hideDataProcessInstances: false,
        hideGhostEntities: !showGhostEntities,
        ignoreSchemaFieldStatus,
    };
    const nodeFilter = createDataProductNodeFilter(urn, showDataProcessInstances);
    const graphStore: GraphStore = {
        ...hideNodes(urn, rootType, config, { nodes, edges, adjacencyList }, nodeFilter),
        rootType,
    };
    // Positioning and edge creation only consider edges that are actually shown
    const revealedGraphStore = filterToRevealedEdges(graphStore);

    const groups = collectDataProductGroups(urn, nodes, graphStore.nodes, dataProductEntities);
    const membership = computeMembership(groups);

    // The home data product has no lineage of its own, so seed the traversal with its members (up to
    // MAX_DISPLAYED_DATA_PRODUCT_MEMBERS); the rest of the graph is reached through their lineage.
    const seedNodes = Array.from(groups.get(urn)?.memberUrns ?? [])
        .slice(0, MAX_DISPLAYED_DATA_PRODUCT_MEMBERS)
        .map((memberUrn) => graphStore.nodes.get(memberUrn))
        .filter((node): node is LineageEntity => !!node);

    // Step 1: Compute displayed nodes globally. The hide flags are overridden to match `config`
    // above, so `computeLineageGraph` recomputes the same graph store internally.
    const compareLineageNodes = generateCompareNodesFunction(rootType);
    const { displayedNodes, lineageFilters } = computeLineageGraph(
        urn,
        { ...context, hideTransformations: false, showDataProcessInstances: true },
        ignoreSchemaFieldStatus,
        {
            nodeFilter,
            // Prioritize output ports first, then data product members, so both are kept by pagination.
            compareNodes: (a, b) =>
                Number(isOutputPort(b)) - Number(isOutputPort(a)) ||
                Number(membership.has(b)) - Number(membership.has(a)) ||
                compareLineageNodes(a, b),
            seedNodes,
            createFilterNodes: showFilterNodes,
        },
    );

    // When filtering to output ports, keep only the home product's output ports, their adjacent
    // nodes (in any data product or none), and the home data product itself.
    const shownNodes = outputPortsOnly
        ? (() => {
              const keep = new Set<Urn>([urn]);
              displayedNodes.forEach((node) => {
                  const isHomeOutputPort = nodes
                      .get(node.id)
                      ?.dataProducts?.some((dataProduct) => dataProduct.urn === urn && dataProduct.isOutputPort);
                  if (!isHomeOutputPort) return;
                  keep.add(node.id);
                  revealedGraphStore.adjacencyList[LineageDirection.Upstream].get(node.id)?.forEach((n) => keep.add(n));
                  revealedGraphStore.adjacencyList[LineageDirection.Downstream]
                      .get(node.id)
                      ?.forEach((n) => keep.add(n));
              });
              return displayedNodes.filter((node) => keep.has(node.id));
          })()
        : displayedNodes;
    const displayedIds = new Set(shownNodes.map((node) => node.id));

    // Step 2 (+ 4): Lay out the displayed members within each data product, sizing its bounding box
    const boxes = new Map<Urn, BoxLayout>();
    groups.forEach((group) => {
        const displayedMemberUrns = new Set(Array.from(group.memberUrns).filter((member) => displayedIds.has(member)));
        if (!displayedMemberUrns.size) return;
        const box = layoutDataProductInterior(
            { ...group, memberUrns: displayedMemberUrns },
            revealedGraphStore,
            ignoreSchemaFieldStatus,
        );
        if (box) boxes.set(group.urn, box);
    });

    if (!boxes.has(urn)) {
        return {
            flowNodes: [],
            flowEdges: [],
            resetPositions: false,
            lineageFilters,
            adjacencyList: filterAdjacencyList(revealedGraphStore.adjacencyList, displayedIds),
        };
    }

    // Emit member nodes right after step 2: their positions are relative to their bounding box (a
    // ReactFlow parent), so they're final once the box is laid out. Bounding box nodes are created
    // here too, so they precede their children in `flowNodes`; their own positions are filled in
    // after step 3 places them. `memberOffsets` records each member's row offset within its box, so
    // step 3 can align a data product's external neighbors with the member they connect to.
    const flowNodes: LineageVisualizationNode[] = [];
    const boxFlowNodes = new Map<Urn, LineageVisualizationNode>();
    const memberOffsets = new Map<Urn, Map<Urn, number>>();
    boxes.forEach((box) => {
        const boxNode = createBoundingBoxNode(box, undefined);
        boxFlowNodes.set(box.group.urn, boxNode);
        flowNodes.push(boxNode, ...box.memberNodes);

        const offsets = new Map<Urn, number>();
        box.memberNodes.forEach((member) => offsets.set((member.data as LineageEntity).urn, member.position.y));
        memberOffsets.set(box.group.urn, offsets);
    });

    // Nodes rendered inside a bounding box; all other displayed nodes are free nodes
    const displayedMembership = new Map<Urn, Urn[]>();
    boxes.forEach((box) =>
        box.group.memberUrns.forEach((member) => setDefault(displayedMembership, member, []).push(box.group.urn)),
    );
    const freeNodes = shownNodes.filter((node) => node.id !== urn && !displayedMembership.has(node.id));
    const displayedFreeIds = new Set(freeNodes.map((node) => node.id));

    // Step 3: Position bounding boxes and free nodes together, based on the lineage between them,
    // aligning each box's external neighbors with the member row they connect to.
    const { positions, freeFlowNodes } = positionTopLevelNodes(
        urn,
        boxes,
        freeNodes,
        displayedFreeIds,
        revealedGraphStore,
        displayedMembership,
        memberOffsets,
        ignoreSchemaFieldStatus,
    );

    // Fill in the positions computed for the bounding box nodes emitted above
    boxFlowNodes.forEach((boxNode, boxUrn) => {
        const position = positions.get(boxUrn);
        // eslint-disable-next-line no-param-reassign
        if (position) boxNode.position = position;
    });
    flowNodes.push(...freeFlowNodes);

    const filterNodes = freeNodes.filter((node): node is LineageFilter => node.type === LINEAGE_FILTER_TYPE);
    const flowEdges = buildFlowEdges(revealedGraphStore, displayedMembership, displayedFreeIds, filterNodes);

    return {
        flowNodes,
        flowEdges,
        resetPositions: false,
        lineageFilters,
        // For node highlighting: only shown nodes and revealed edges
        adjacencyList: filterAdjacencyList(revealedGraphStore.adjacencyList, displayedIds),
    };
}
