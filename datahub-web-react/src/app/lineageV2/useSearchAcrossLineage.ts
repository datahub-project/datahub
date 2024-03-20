import {useEffect, useState} from 'react';
import {useSearchAcrossLineageStructureLazyQuery} from '../../graphql/search.generated';
import {Entity, EntityType, LineageDirection, SearchAcrossLineageInput} from '../../types.generated';
import {useGetLineageTimeParams} from '../lineage/utils/useGetLineageTimeParams';
import {DEGREE_FILTER_NAME} from '../search/utils/constants';
import {FetchStatus, getNonTransformationalParents, NodeContext, setNodeDefault, TRANSFORMATION_TYPES,} from './common';

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
        startTimeMillis: startTimeMillis || undefined,
        endTimeMillis: endTimeMillis || undefined,
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

        data?.searchAcrossLineage?.searchResults.forEach((result) => {
            const node = setNodeDefault({
                nodes,
                urn: result.entity.urn,
                type: result.entity.type,
                direction,
                maxDepth,
            });

            const newParents =
                result.paths?.map((path) => path?.path?.[path?.path.length - 2]?.urn).filter((p): p is string => !!p) ||
                [];
            const newNTParents = newParents.map((p) => getNonTransformationalParents(p, nodes, rootUrn)).flat();
            node.parents = new Set([...node.parents, ...newParents]);
            node.nonTransformationalParents = new Set([...node.nonTransformationalParents, ...newNTParents]);

            // TODO: Clean up logic, a little redundant with setNodeDefault
            if (maxDepth) {
                node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
            }
        });

        // TODO: Check if searchAcrossLineage is producing duplicate results,
        // e.g. if there's multiple transformations between the same nodes

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

function addQueryNodes(
    path: Array<Pick<Entity, 'urn' | 'type'>>,
    nodes: NodeContext['nodes'],
    direction: LineageDirection,
) {
    path.forEach((node, i) => {
        const parent = path[i - 1]?.urn;
        // TODO: Replace with findLast when it's available
        const positionalParent = path
            .slice(0, i)
            .reduceRight<string | undefined>(
                (acc, p) => acc || (!TRANSFORMATION_TYPES.includes(p.type) ? p.urn : undefined),
                undefined,
            );
        if (node && !nodes.has(node.urn) && node.type === EntityType.Query) {
            nodes.set(node.urn, {
                id: node.urn,
                urn: node.urn,
                type: node.type,
                parents: new Set(parent ? [parent] : []),
                nonTransformationalParents: new Set(positionalParent ? [positionalParent] : []),
                direction,
                fetchStatus: {
                    [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
                    [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
                },
            });
        }
    });
}
