import { useEffect, useState } from 'react';
import { useSearchAcrossLineageStructureLazyQuery } from '../../graphql/search.generated';
import { LineageDirection, SearchAcrossLineageInput, VersionedDataset } from '../../types.generated';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import { DEGREE_FILTER_NAME } from '../search/utils/constants';
import { FetchStatus, NodeContext, Path, setNodeDefault, TRANSFORMATION_TYPES } from './common';

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
    const { nodes, setNodeVersion } = context;

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
        data?.searchAcrossLineage?.searchResults.forEach((result) => {
            const paths: Path[][] = [];
            const parent = nodes.get(urn);
            result.paths?.forEach((path) => {
                if (path) {
                    const mappedPath = path.path.filter((p): p is Path => !!p && p.urn !== result.entity.urn);
                    parent?.paths.forEach((parentPath) => {
                        paths.push([...parentPath, ...mappedPath]);
                    });
                }
            });

            const node = setNodeDefault({
                nodes,
                urn: result.entity.urn,
                type: result.entity.type,
                direction,
                maxDepth,
            });
            node.paths.push(...paths);
            // TODO: Clean up logic, a little redundant with setNodeDefault
            if (maxDepth) {
                node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
            }
        });

        // TODO: Check if searchAcrossLineage is producing duplicate results,
        // e.g. if there's multiple transformations between the same nodes

        // Add query nodes
        data?.searchAcrossLineage?.searchResults.forEach((result) => {
            result.paths?.forEach((path) => {
                const filteredPath = path?.path.filter((p): p is Pick<VersionedDataset, 'urn' | 'type'> => !!p) || [];
                filteredPath.forEach((node, i) => {
                    if (node && !nodes.has(node.urn) && TRANSFORMATION_TYPES.includes(node.type)) {
                        nodes.set(node.urn, {
                            id: node.urn,
                            urn: node.urn,
                            type: node.type,
                            paths: [filteredPath.slice(0, i)],
                            direction,
                            fetchStatus: {
                                [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
                                [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
                            },
                        });
                    }
                });
            });
        });

        const node = nodes.get(urn);
        if (data && node) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.COMPLETE };
        }

        if (data) {
            setNodeVersion((version) => version + 1);
            setProcessed(true);
        }
    }, [urn, data, direction, nodes, setNodeVersion, maxDepth, setProcessed]);

    return { fetchLineage, processed };
}
