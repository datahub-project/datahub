import { useEffect, useState } from 'react';
import { useSearchAcrossLineageStructureLazyQuery } from '../../graphql/search.generated';
import { Entity, EntityType, LineageDirection, Maybe, SearchAcrossLineageInput } from '../../types.generated';
import EntityRegistry from '../entityV2/EntityRegistry';
import { DBT_URN } from '../ingest/source/builder/constants';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import { DEGREE_FILTER_NAME } from '../search/utils/constants';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import {
    addToAdjacencyList,
    FetchStatus,
    Filters,
    getEdgeId,
    isQuery,
    isTransformational,
    isUrnDbt,
    isUrnTransformational,
    LINEAGE_FILTER_PAGINATION,
    LineageEntity,
    NodeContext,
    reverseDirection,
    setDefault,
} from './common';

/**
 * Fetches the lineage structure for a given urn and direction, and updates the nodes map with the results.
 * @param urn Urn for which to fetch lineage
 * @param context LineageExploreContext storing a map of urn to LineageNode
 * @param direction Direction for which to fetch lineage
 * @param lazy Whether to fetch the lineage immediately
 * @param maxDepth Whether to fetch all lineage, default depth 1
 * @param skipCache Whether to bypass Apollo and Elasticsearch caches
 */
export default function useSearchAcrossLineage(
    urn: string,
    context: NodeContext,
    direction: LineageDirection,
    lazy?: boolean,
    maxDepth?: boolean,
    skipCache?: boolean,
): {
    fetchLineage: () => void;
    processed: boolean;
} {
    const entityRegistry = useEntityRegistryV2();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { nodes, edges, adjacencyList, rootUrn, setNodeVersion, setDisplayVersion } = context;

    const input: SearchAcrossLineageInput = {
        urn,
        direction,
        start: 0,
        count: 10000,
        orFilters: [
            {
                and: [
                    {
                        field: DEGREE_FILTER_NAME,
                        values: maxDepth ? ['1', '2', '3+'] : ['1'],
                    },
                ],
            },
        ],
        lineageFlags: {
            startTimeMillis,
            endTimeMillis,
            entitiesExploredPerHopLimit: LINEAGE_FILTER_PAGINATION,
            ignoreAsHops: [
                {
                    entityType: EntityType.Dataset,
                    platforms: [DBT_URN],
                },
                { entityType: EntityType.DataJob },
            ],
        },
        searchFlags: {
            skipCache: !!skipCache,
        },
    };

    const [processed, setProcessed] = useState(false);
    const [fetchLineage, { data }] = useSearchAcrossLineageStructureLazyQuery({
        variables: { input },
        fetchPolicy: skipCache ? 'no-cache' : undefined,
    });
    useEffect(() => {
        if (!lazy) {
            fetchLineage();
        }
    }, [fetchLineage, lazy]);

    useEffect(() => {
        const smallContext = { nodes, edges, adjacencyList };
        let addedNode = false;

        data?.searchAcrossLineage?.searchResults.forEach((result) => {
            addedNode = addedNode || !nodes.has(result.entity.urn);
            const node = setDefault(
                nodes,
                result.entity.urn,
                entityNodeDefault(result.entity.urn, result.entity.type, direction),
            );
            if (result.explored || result.ignoredAsHop) {
                node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
                node.isExpanded = { ...node.isExpanded, [direction]: true };
            }

            result.paths?.forEach((path) => {
                if (!path) return;
                const parent = path.path[path.path.length - 2];
                if (!parent) return;
                if (isQuery(parent)) {
                    const grandparent = path.path[path.path.length - 3];
                    if (grandparent) {
                        addToAdjacencyList(adjacencyList, direction, grandparent.urn, result.entity.urn);
                    }
                } else {
                    addToAdjacencyList(adjacencyList, direction, parent.urn, result.entity.urn);
                }

                addQueryNodes(path.path, direction, smallContext);
            });
        });

        const node = nodes.get(urn);
        if (data && node) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
        }

        if (data) {
            pruneParentsThroughDbt(urn, direction, smallContext, entityRegistry);
            setProcessed(true);
            if (addedNode) setNodeVersion((version) => version + 1);
            else setDisplayVersion(([version, n]) => [version + 1, n]);
        }
    }, [
        urn,
        data,
        direction,
        nodes,
        edges,
        adjacencyList,
        rootUrn,
        setNodeVersion,
        setDisplayVersion,
        maxDepth,
        entityRegistry,
        setProcessed,
    ]);

    return { fetchLineage, processed };
}

/**
 * Remove direct edges between non-transformational nodes, if there is a path between them through only dbt nodes (and query nodes).
 * This prevents the graph from being cluttered with effectively duplicate edges.
 * @param urn Urn for which to remove parent edges.
 * @param direction Direction to look for parents.
 * @param context Lineage node context.
 * @param entityRegistry EntityRegistry, used to get EntityType from an urn.
 */
export function pruneParentsThroughDbt(
    urn: string,
    direction: LineageDirection,
    context: Pick<NodeContext, 'adjacencyList' | 'edges'>,
    entityRegistry: EntityRegistry,
) {
    const { adjacencyList, edges } = context;
    if (isUrnTransformational(urn, entityRegistry)) return;

    const urnsToPrune = new Set<string>();
    const stack = Array.from(adjacencyList[direction].get(urn) || []).filter((p) => isUrnDbt(p, entityRegistry));
    const seen = new Set<string>(stack);
    for (let u = stack.pop(); u; u = stack.pop()) {
        Array.from(adjacencyList[direction].get(u) || []).forEach((parent) => {
            if (isUrnDbt(parent, entityRegistry)) {
                if (!seen.has(parent)) {
                    stack.push(parent);
                    seen.add(parent);
                }
            } else {
                urnsToPrune.add(parent);
            }
        });
    }

    urnsToPrune.forEach((parent) => {
        const edge = edges.get(getEdgeId(urn, parent, direction));
        if (edge) edge.isDisplayed = false;
    });
}

export function entityNodeDefault(urn: string, type: EntityType, direction: LineageDirection): LineageEntity {
    const otherDirection =
        direction === LineageDirection.Upstream ? LineageDirection.Downstream : LineageDirection.Upstream;
    return {
        id: urn,
        urn,
        type,
        direction, // TODO: Handle a node that is both upstream and downstream?
        isExpanded: {
            [direction]: isTransformational({ urn, type }),
            [otherDirection]: false,
        } as Record<LineageDirection, boolean>,
        fetchStatus: {
            [direction]: FetchStatus.UNFETCHED,
            [otherDirection]: FetchStatus.UNNEEDED,
        } as Record<LineageDirection, FetchStatus>,
        filters: {
            [direction]: {
                limit: LINEAGE_FILTER_PAGINATION,
                facetFilters: new Map(),
            },
        } as Record<LineageDirection, Filters>,
    };
}

export function addQueryNodes(
    maybePath: Array<Maybe<Pick<Entity, 'urn' | 'type'>>> | undefined,
    direction: LineageDirection,
    context: Pick<NodeContext, 'nodes' | 'edges' | 'adjacencyList'>,
) {
    const { nodes, edges, adjacencyList } = context;

    const path = maybePath?.filter((p): p is Pick<Entity, 'urn' | 'type'> => !!p) || [];
    path.forEach((node, i) => {
        if (!node || node.type !== EntityType.Query || i === 0 || i === path.length - 1) return;
        setDefault(nodes, node.urn, {
            id: node.urn,
            urn: node.urn,
            type: node.type,
            direction,
            isExpanded: {
                [LineageDirection.Upstream]: true,
                [LineageDirection.Downstream]: true,
            },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
                [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
            },
        });
        edges.set(getEdgeId(path[i - 1].urn, path[i + 1].urn, direction), {
            isDisplayed: true,
            isManual: false,
            via: node.urn,
        });
        setDefault(adjacencyList[direction], node.urn, new Set()).add(path[i + 1].urn);
        setDefault(adjacencyList[reverseDirection(direction)], node.urn, new Set()).add(path[i - 1].urn);
    });
}
