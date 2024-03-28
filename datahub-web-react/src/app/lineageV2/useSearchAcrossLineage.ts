import {useEffect, useState} from 'react';
import {useSearchAcrossLineageStructureLazyQuery} from '../../graphql/search.generated';
import {Entity, EntityType, LineageDirection, SearchAcrossLineageInput} from '../../types.generated';
import {DBT_URN} from '../ingest/source/builder/constants';
import {useGetLineageTimeParams} from '../lineage/utils/useGetLineageTimeParams';
import {DEGREE_FILTER_NAME} from '../search/utils/constants';
import {
    FetchStatus,
    Filters,
    isDbt,
    isQuery,
    isTransformational,
    LINEAGE_FILTER_PAGINATION,
    LineageEntity,
    NodeContext,
    setDefault,
} from './common';

/**
 * Fetches the lineage structure for a given urn and direction, and updates the nodes map with the results.
 * @param urn Urn for which to fetch lineage
 * @param context LineageExploreContext storing a map of urn to LineageNode
 * @param direction Direction for which to fetch lineage
 * @param lazy Whether to fetch the lineage immediately
 * @param maxDepth Whether to fetch all lineage, default depth 1
 */
export default function useSearchAcrossLineage(
    urn: string,
    context: NodeContext,
    direction: LineageDirection,
    lazy?: boolean,
    maxDepth?: boolean,
): { fetchLineage: () => void; processed: boolean } {
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { nodes, rootUrn, setNodeVersion } = context;

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
    };

    const [processed, setProcessed] = useState(false);
    const [fetchLineage, { data }] = useSearchAcrossLineageStructureLazyQuery({ variables: { input } });
    useEffect(() => {
        if (!lazy) {
            fetchLineage();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [fetchLineage, lazy]);

    useEffect(() => {
        // Add query nodes before adding regular nodes for nonTransformationalParent calculation
        data?.searchAcrossLineage?.searchResults.forEach((result) => {
            result.paths?.forEach((path) => {
                const filteredPath = path?.path.filter((p): p is Pick<Entity, 'urn' | 'type'> => !!p) || [];
                addQueryNodes(filteredPath, nodes, direction);
            });
        });

        const urns = new Set<string>();
        data?.searchAcrossLineage?.searchResults.forEach((result) => {
            urns.add(result.entity.urn);
            const node = setDefault(
                nodes,
                result.entity.urn,
                entityNodeDefault(result.entity.urn, result.entity.type, direction),
            );

            const newParents =
                result.paths?.map((path) => path?.path?.[path?.path.length - 2]?.urn).filter((p): p is string => !!p) ||
                [];
            node.parents = new Set([...node.parents, ...newParents]);

            if (maxDepth) {
                node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
            }
        });

        urns.forEach((u) => pruneParentsThroughDbt(u, nodes));

        const node = nodes.get(urn);
        if (data && node) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
        }

        if (data) {
            setNodeVersion((version) => version + 1);
            setProcessed(true);
        }
    }, [urn, data, direction, nodes, rootUrn, setNodeVersion, maxDepth, setProcessed]);

    return { fetchLineage, processed };
}

/**
 * Remove direct edges between non-transformational nodes, if there is a path between them through only dbt nodes (and query nodes).
 * This prevents the graph from being cluttered with effectively duplicate edges.
 * @param urn Urn for which to remove parent edges.
 * @param nodes All nodes in the graph, used to look up other nodes' parents.
 */
export function pruneParentsThroughDbt(urn: string, nodes: NodeContext['nodes']) {
    const node = nodes.get(urn);
    if (!node || isTransformational(node)) return;

    const urnsToPrune = new Set<string>();
    const stack = Array.from(node.parents).filter((p) => {
        const n = nodes.get(p);
        return n && isDbt(n);
    });
    const seen = new Set<string>(stack);
    for (let u = stack.pop(); u; u = stack.pop()) {
        const n = nodes.get(u);
        if (!n) return;

        n.parents.forEach((parent) => {
            const p = nodes.get(parent);
            if (!p) return;

            if (isDbt(p) && !seen.has(parent)) {
                stack.push(parent);
                seen.add(parent);
            } else {
                urnsToPrune.add(parent);
            }
        });
    }

    node.parents = new Set(
        Array.from(node.parents).filter((parentUrn) => {
            const parent = nodes.get(parentUrn);
            if (parent && isQuery(parent)) {
                return Array.from(parent.parents).some((grandparentUrn) => !urnsToPrune.has(grandparentUrn));
            }
            return !urnsToPrune.has(parentUrn);
        }),
    );
    if (!node.prunedParents) {
        node.prunedParents = urnsToPrune;
    } else {
        node.prunedParents = new Set([...node.prunedParents, ...urnsToPrune]);
    }
}

function entityNodeDefault(urn: string, type: EntityType, direction: LineageDirection): LineageEntity {
    const otherDirection =
        direction === LineageDirection.Upstream ? LineageDirection.Downstream : LineageDirection.Upstream;
    return {
        id: urn,
        urn,
        type,
        direction, // TODO: Handle a node that is both upstream and downstream?
        parents: new Set(),
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

function addQueryNodes(
    path: Array<Pick<Entity, 'urn' | 'type'>>,
    nodes: NodeContext['nodes'],
    direction: LineageDirection,
) {
    path.forEach((node, i) => {
        if (!node || node.type !== EntityType.Query) return;
        const queryNode = setDefault(nodes, node.urn, {
            id: node.urn,
            urn: node.urn,
            type: node.type,
            parents: new Set<string>(),
            direction,
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
                [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
            },
        });

        const parent = path[i - 1]?.urn;
        if (parent) queryNode.parents.add(parent);
    });
}
