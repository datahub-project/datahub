import { Maybe } from 'graphql/jsutils/Maybe';
import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import { PLATFORM_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '../searchV2/utils/constants';
import { ColumnQueryData } from '../shared/EntitySidebarContext';
import {
    ChildMap,
    ColumnRef,
    createColumnQueryRef,
    createColumnRef,
    FineGrainedLineage,
    FineGrainedLineageMap,
    LINEAGE_FILTER_ID_PREFIX,
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageNodesContext,
    parseColumnRef,
    setDefault,
} from './common';
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from './lineageUtils';
import NodeBuilder, { NodeWithMetadata } from './NodeBuilder';

interface FineGrainedLineageData {
    direct: FineGrainedLineage;
    indirect: FineGrainedLineage;
    columnQueryData: Map<ColumnRef, ColumnQueryData>;
}

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: NodeWithMetadata[];
    flowEdges: Edge[];
    childMaps: Record<LineageDirection, ChildMap>;
}

export default function useProcessData(urn: string, type: EntityType): ProcessedData {
    const { nodes, nodeVersion, dataVersion, displayVersion } = useContext(LineageNodesContext);

    const fineGrainedLineage = useMemo(
        () => getFineGrainedLineage(nodes),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, dataVersion],
    );

    const [flowNodes, flowEdges, childMaps] = useMemo(
        () => {
            const childMap = getChildMaps(nodes);
            const filteredNodes = filterNodes(urn, nodes, childMap);
            const nodeBuilder = new NodeBuilder(urn, type, filteredNodes);
            return [nodeBuilder.createNodes(), nodeBuilder.createEdges(), childMap];
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, nodeVersion, displayVersion],
    );

    return { flowNodes, flowEdges, fineGrainedLineage, childMaps };
}

/**
 * Filters nodes based on per-node filters.
 * @param urn The urn of the root node.
 * @param nodes The list of urns fetched for the lineage visualization.
 * @param childMaps Per direction, an association list of urns to their neighbors in that direction.
 * @returns A list of nodes to display in topological order.
 */
function filterNodes(
    urn: string,
    nodes: Map<string, LineageEntity>,
    childMaps: Record<LineageDirection, ChildMap>,
): LineageNode[] {
    const rootNode = nodes.get(urn);
    if (!rootNode) {
        return [];
    }

    const orderedNodes = Array.from(nodes.values()).sort((a, b) => a.urn.localeCompare(b.urn));

    const displayedNodes: LineageNode[] = [rootNode];
    const seenNodes = new Set<string>([urn]);
    const queue = [urn]; // Note: uses array for queue, slow for large graphs
    while (queue.length > 0) {
        const current = queue.shift() as string; // Just checked length
        const node = nodes.get(current);
        const directionsToSearch = node?.direction ? [node.direction] : Object.values(LineageDirection);
        const filteredChildren = directionsToSearch
            .map((direction) =>
                applyFilters(current, direction, orderedNodes, childMaps[direction], nodes.get(current)),
            )
            .flat();
        filteredChildren.forEach((child) => {
            if (!seenNodes.has(child.id)) {
                displayedNodes.push(child);
                seenNodes.add(child.id);
                queue.push(child.id);
            }
        });
    }

    return displayedNodes;
}

function applyFilters(
    urn: string,
    direction: LineageDirection,
    orderedNodes: LineageEntity[],
    childMap: ChildMap,
    node?: LineageEntity,
): LineageNode[] {
    // TODO: Only create lineage filter nodes for entity nodes (not transformational)
    const filters = node?.filters?.[direction];
    const children = childMap.get(urn);
    if (!children || filters?.display === false) {
        return [];
    }
    let orderedChildren = orderedNodes.filter((n) => children?.has(n.urn));

    filters?.facetFilters?.forEach((values, facet) => {
        if (!values.size) {
            return;
        }
        if (facet === PLATFORM_FILTER_NAME) {
            orderedChildren = orderedChildren.filter((n) => {
                const platform = n.entity?.platform?.urn || n.backupEntity?.platform?.urn;
                return platform && values.has(platform);
            });
        } else if (facet === TYPE_NAMES_FILTER_NAME) {
            orderedChildren = orderedChildren.filter((n) => {
                const subtype = n.entity?.subtype || n.backupEntity?.subTypes?.typeNames?.[0];
                return subtype && values.has(subtype);
            });
        }
    });

    const shownNodes = orderedChildren.slice(
        Math.max(0, orderedChildren.length - (filters?.limit || orderedChildren.length)),
    );
    const result: LineageNode[] = [];
    if (children.size > LINEAGE_FILTER_PAGINATION && (!node?.direction || node.direction === direction)) {
        const dir = direction === LineageDirection.Upstream ? 'u:' : 'd:';
        const filterNode: LineageFilter = {
            // id starts with 's' so it is always sorted first, before urn:li:...
            id: `${LINEAGE_FILTER_ID_PREFIX}${dir}${urn}`,
            type: LINEAGE_FILTER_TYPE,
            parent: urn,
            parents: new Set([urn]),
            nonTransformationalParents: new Set([urn]),
            direction,
            contents: Array.from(children),
            shown: new Set(shownNodes.map((n) => n.urn)),
        };
        result.push(filterNode);
    }
    result.push(...shownNodes);
    return result;
}

function getChildMaps(nodes: Map<string, LineageEntity>): Record<LineageDirection, ChildMap> {
    const upstreamChildren = new Map<string, Set<string>>();
    const downstreamChildren = new Map<string, Set<string>>();
    nodes.forEach((node) => {
        node.parents.forEach((parent) => {
            if (node.direction === LineageDirection.Upstream) {
                setDefault(upstreamChildren, parent, new Set()).add(node.urn);
                setDefault(downstreamChildren, node.urn, new Set()).add(parent);
            } else if (node.direction === LineageDirection.Downstream) {
                setDefault(downstreamChildren, parent, new Set()).add(node.urn);
                setDefault(upstreamChildren, node.urn, new Set()).add(parent);
            }
        });
    });
    return {
        [LineageDirection.Upstream]: upstreamChildren,
        [LineageDirection.Downstream]: downstreamChildren,
    };
}

function getFineGrainedLineage(nodes: Map<string, LineageEntity>): FineGrainedLineageData {
    // Edges through query nodes
    const forward: FineGrainedLineageMap = new Map();
    const backward: FineGrainedLineageMap = new Map();
    // Edges skip query nodes
    const forwardDirect: FineGrainedLineageMap = new Map();
    const backwardDirect: FineGrainedLineageMap = new Map();

    const columnQueryData: Map<ColumnRef, ColumnQueryData> = new Map();

    nodes.forEach((node) => {
        node.entity?.fineGrainedLineages?.forEach((entry) => {
            const queryRef = entry.query && createColumnQueryRef(entry.query, entry.upstreams, entry.downstreams);
            if (queryRef) {
                const [queryNodeId] = parseColumnRef(queryRef);
                columnQueryData.set(queryNodeId, {
                    inputColumns: entry.upstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    outputColumns: entry.downstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    transformOperation: entry.transformOperation || undefined,
                });
            }
            entry.upstreams?.forEach((from) => {
                const fromRef = createColumnRef(from.urn, from.path);
                entry.downstreams?.forEach((to) => {
                    if (nodes.has(to.urn)) {
                        addFineGrainedEdges(
                            forward,
                            forwardDirect,
                            fromRef,
                            createColumnRef(to.urn, to.path),
                            queryRef,
                        );
                    }
                });
            });
            entry.downstreams?.forEach((from) => {
                const fromRef = createColumnRef(from.urn, from.path);
                entry.upstreams?.forEach((to) => {
                    if (nodes.has(to.urn)) {
                        addFineGrainedEdges(
                            backward,
                            backwardDirect,
                            fromRef,
                            createColumnRef(to.urn, to.path),
                            queryRef,
                        );
                    }
                });
            });
        });
        node.entity?.inputFields?.fields?.forEach((input) => {
            if (input?.schemaFieldUrn && input?.schemaField) {
                const upstreamUrn = getSourceUrnFromSchemaFieldUrn(input.schemaFieldUrn);
                const upstreamRef = createColumnRef(upstreamUrn, getFieldPathFromSchemaFieldUrn(input.schemaFieldUrn));
                const downstreamRef = createColumnRef(node.urn, input.schemaField.fieldPath);
                if (nodes.has(upstreamUrn)) {
                    addFineGrainedEdges(forward, forwardDirect, upstreamRef, downstreamRef, undefined);
                    addFineGrainedEdges(backward, backwardDirect, downstreamRef, upstreamRef, undefined);
                }
            }
        });
    });

    return {
        indirect: { forward, backward },
        direct: { forward: forwardDirect, backward: backwardDirect },
        columnQueryData,
    };
}

function addFineGrainedEdges(
    map: FineGrainedLineageMap,
    directMap: FineGrainedLineageMap,
    from: ColumnRef,
    to: ColumnRef,
    query: Maybe<ColumnRef>,
) {
    if (query) {
        setDefault(map, from, []).push(query);
        setDefault(map, query, []).push(to);
    } else {
        setDefault(map, from, []).push(to);
    }
    setDefault(directMap, from, []).push(to);
}
