import { notification } from '@components';
import i18next from 'i18next';
import { useEffect, useRef, useState } from 'react';

import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import {
    FetchStatus,
    Filters,
    LINEAGE_FILTER_PAGINATION,
    LineageEntity,
    NodeContext,
    addToAdjacencyList,
    generateIgnoreAsHops,
    getEdgeId,
    isQuery,
    isTransformational,
    reverseDirection,
    setDefault,
} from '@app/lineageV3/common';
import pruneAllDuplicateEdges from '@app/lineageV3/queries/pruneAllDuplicateEdges';
import { DEGREE_FILTER_NAME } from '@app/search/utils/constants';

import { useSearchAcrossLineageStructureLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, LineageDirection, Maybe, SearchAcrossLineageInput } from '@types';

const PER_HOP_LIMIT = 2;

// Fail a stalled lineage fetch visibly instead of spinning forever (e.g. a slow time-filtered query
// at scale). useResetLineageGraph zeroes nodeVersion on a time-range change, so a request that never
// returns would otherwise leave the graph stuck behind the "initialized && nodeVersion > 0" gate.
// Backstop only: the backend bounds the graph query at `elasticsearch.search.graph.timeoutSeconds`
// (default 50s) and returns a DEADLINE_EXCEEDED/504 that onError already handles, so a genuine
// server timeout surfaces well before this fires. Keep this above that budget so a slow-but-
// successful traversal isn't aborted client-side; it only catches stalls the server guardrail can't
// (network/gateway death, or a request that never returns at all).
const LINEAGE_FETCH_TIMEOUT_MS = 60_000;

export const DEFAULT_SEARCH_FLAGS = {
    groupingSpec: { groupingCriteria: [] },
    filterNonLatestVersions: false,
};

/**
 * Fetches the lineage structure for a given urn and direction, and updates the nodes map with the results.
 * @param urn Urn for which to fetch lineage
 * @param type EntityType of the urn
 * @param context LineageExploreContext storing a map of urn to LineageNode
 * @param direction Direction for which to fetch lineage
 * @param lazy Whether to fetch the lineage immediately
 * @param maxDepth Whether to fetch all lineage, default depth 1
 * @param skipCache Whether to bypass Apollo and Elasticsearch caches
 */
export default function useSearchAcrossLineage(
    urn: string,
    type: EntityType,
    context: NodeContext,
    direction: LineageDirection,
    lazy?: boolean,
    maxDepth?: boolean,
    skipCache?: boolean,
): {
    fetchLineage: () => void;
    processed: boolean;
} {
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { nodes, edges, adjacencyList, rootUrn, rootType, setNodeVersion, setDisplayVersion } = context;

    const input: SearchAcrossLineageInput = {
        urn,
        direction,
        types: type === EntityType.SchemaField ? [EntityType.SchemaField] : undefined,
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
            entitiesExploredPerHopLimit: maxDepth ? PER_HOP_LIMIT : undefined,
            ignoreAsHops: generateIgnoreAsHops(rootType),
        },
        searchFlags: {
            ...DEFAULT_SEARCH_FLAGS,
            skipCache: !!skipCache,
        },
    };

    const [processed] = useState(new Set<string>());

    // Marks the current fetch resolved (success, error, or timeout) so a stalled request that later
    // errors — or vice versa — is handled only once.
    const settledRef = useRef(false);

    const handleFetchFailure = () => {
        if (settledRef.current) return;
        settledRef.current = true;
        // Reuse existing states: mark the fetch terminal so the node stops showing a spinner, and bump
        // the versions to release the loading gate (useResetLineageGraph zeroed nodeVersion).
        const node = nodes.get(urn);
        if (node) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
        }
        processed.add(urn);
        setNodeVersion((version) => version + 1);
        setDisplayVersion(([version]) => [version + 1, []]);
        notification.error({ message: i18next.t('lineage:timeSelector.loadError') });
    };
    const handleFetchFailureRef = useRef(handleFetchFailure);
    handleFetchFailureRef.current = handleFetchFailure;

    const [fetchLineage, { loading }] = useSearchAcrossLineageStructureLazyQuery({
        variables: { input },
        fetchPolicy: skipCache ? 'no-cache' : undefined,
        onCompleted: (data) => {
            settledRef.current = true;
            const smallContext = { nodes, edges, adjacencyList, setDisplayVersion, rootType };
            let addedNode = false;

            data?.searchAcrossLineage?.searchResults?.forEach((result) => {
                addedNode = addedNode || !nodes.has(result.entity.urn);
                const node = setEntityNodeDefault(result.entity.urn, result.entity.type, direction, smallContext);
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
                            edges.set(getEdgeId(grandparent.urn, result.entity.urn, direction), { isDisplayed: true });
                            addToAdjacencyList(adjacencyList, direction, grandparent.urn, result.entity.urn);
                        }
                    } else {
                        edges.set(getEdgeId(parent.urn, result.entity.urn, direction), { isDisplayed: true });
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
                pruneAllDuplicateEdges(urn, direction, smallContext);
                processed.add(urn);
                if (addedNode) setNodeVersion((version) => version + 1);

                const nodesToZoom = urn === rootUrn ? [] : [urn, ...(adjacencyList[direction].get(urn) || [])];
                setDisplayVersion(([version]) => [version + 1, nodesToZoom]);
            }
        },
        onError: () => handleFetchFailureRef.current(),
    });

    useEffect(() => {
        if (!lazy) {
            fetchLineage();
        }
    }, [fetchLineage, lazy]);

    // Guard against a stalled fetch hanging the graph indefinitely: if the request is still in flight
    // after the timeout, fail it visibly instead of spinning forever.
    useEffect(() => {
        if (!loading) return undefined;
        settledRef.current = false;
        const timer = setTimeout(() => handleFetchFailureRef.current(), LINEAGE_FETCH_TIMEOUT_MS);
        return () => clearTimeout(timer);
    }, [loading]);

    return { fetchLineage, processed: processed.has(urn) };
}

export function setEntityNodeDefault(
    urn: string,
    type: EntityType,
    direction: LineageDirection,
    { nodes, rootType }: Pick<NodeContext, 'nodes' | 'rootType'>,
): LineageEntity {
    const node = setDefault(nodes, urn, entityNodeDefault(urn, type, direction, rootType));
    if (node.direction && node.direction !== direction && !node.inCycle) {
        // Node is both upstream and downstream
        node.inCycle = true;
        if (node.fetchStatus[direction] === FetchStatus.UNNEEDED) {
            node.fetchStatus[direction] = FetchStatus.UNFETCHED;
        }
    }
    return node;
}

function defaultLineageFilter(): Filters {
    return { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() };
}

export function entityNodeDefault(
    urn: string,
    type: EntityType,
    direction: LineageDirection,
    rootType: EntityType,
): LineageEntity {
    const otherDirection =
        direction === LineageDirection.Upstream ? LineageDirection.Downstream : LineageDirection.Upstream;
    return {
        id: urn,
        urn,
        type,
        direction, // TODO: Handle a node that is both upstream and downstream?
        isExpanded: {
            [direction]: isTransformational({ urn, type }, rootType),
            [otherDirection]: false,
        } as Record<LineageDirection, boolean>,
        fetchStatus: {
            [direction]: FetchStatus.UNFETCHED,
            [otherDirection]: FetchStatus.UNNEEDED,
        } as Record<LineageDirection, FetchStatus>,
        filters: {
            [LineageDirection.Upstream]: defaultLineageFilter(),
            [LineageDirection.Downstream]: defaultLineageFilter(),
        },
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
            filters: {
                [LineageDirection.Upstream]: defaultLineageFilter(),
                [LineageDirection.Downstream]: defaultLineageFilter(),
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
