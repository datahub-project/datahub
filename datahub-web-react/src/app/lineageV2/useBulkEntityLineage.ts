import { useCallback, useContext, useEffect, useState } from 'react';
import { useGetBulkEntityLineageV2Query } from '../../graphql/lineage.generated';
import { LineageDirection, Status } from '../../types.generated';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import usePrevious from '../shared/usePrevious';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import {
    addToAdjacencyList,
    FetchStatus,
    getEdgeId,
    isQuery,
    LineageEdge,
    LineageEntity,
    LineageNodesContext,
    NodeContext,
    setDefault,
} from './common';
import { FetchedEntityV2Relationship } from './types';
import { addQueryNodes, entityNodeDefault, pruneDuplicateEdges } from './useSearchAcrossLineage';

export default function useBulkEntityLineage(shownUrns: string[]): (urn: string) => void {
    const { nodes, edges, adjacencyList, setDataVersion, setDisplayVersion } = useContext(LineageNodesContext);
    shownUrns.sort();
    const prevShownUrns = usePrevious(shownUrns);
    const [urnsToFetch, setUrnsToFetch] = useState<string[]>([]);
    useEffect(() => {
        // TODO: Implement string[] equality?
        if (JSON.stringify(prevShownUrns) !== JSON.stringify(shownUrns)) {
            setUrnsToFetch(
                shownUrns.filter((urn) => {
                    const node = nodes.get(urn);
                    return !node?.entity;
                }),
            );
        }
    }, [nodes, prevShownUrns, shownUrns]);

    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const { data, refetch } = useGetBulkEntityLineageV2Query({
        skip: !urnsToFetch?.length,
        fetchPolicy: 'cache-first',
        variables: {
            urns: urnsToFetch,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: true,
            showColumns: true,
        },
    });

    const entityRegistry = useEntityRegistryV2();

    useEffect(() => {
        const smallContext = { nodes, edges, adjacencyList };
        let changed = false;
        data?.entities?.forEach((rawEntity) => {
            if (!rawEntity) return;
            const config = entityRegistry.getLineageVizConfigV2(rawEntity.type, rawEntity);
            if (!config) return;
            const entity = { ...config, lineageAssets: entityRegistry.getLineageAssets(rawEntity.type, rawEntity) };

            const node = nodes.get(entity.urn);
            if (node) {
                node.entity = entity;
                node.rawEntity = rawEntity;
                node.isSoftDeleted = entity.status?.removed;
                changed = true;

                // TODO: Remove once using bulk edges query
                if (!isQuery(node)) {
                    entity.downstreamRelationships?.forEach((relationship) =>
                        processEdge(node, relationship, LineageDirection.Downstream, smallContext),
                    );
                    entity.upstreamRelationships?.forEach((relationship) => {
                        processEdge(node, relationship, LineageDirection.Upstream, smallContext);
                    });
                    pruneDuplicateEdges(node.urn, LineageDirection.Upstream, smallContext, entityRegistry);
                    pruneDuplicateEdges(node.urn, LineageDirection.Downstream, smallContext, entityRegistry);
                }
            }
        });
        if (changed) {
            setDataVersion((version) => version + 1);
            setDisplayVersion(([version, n]) => [version + 1, n]); // TODO: Also remove with above todo
        }
    }, [data, nodes, edges, adjacencyList, entityRegistry, setDataVersion, setDisplayVersion]);

    return useCallback(
        (urn: string) =>
            refetch({
                urns: [urn],
                startTimeMillis,
                endTimeMillis,
                separateSiblings: true,
                showColumns: true,
            }),
        [refetch, startTimeMillis, endTimeMillis],
    );
}

function processEdge(
    node: LineageEntity,
    relationship: FetchedEntityV2Relationship,
    direction: LineageDirection,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges'>,
): void {
    const { adjacencyList, nodes, edges } = context;

    if (relationship.entity && !isQuery(relationship.entity)) {
        if ('status' in relationship.entity && (relationship.entity.status as Status | undefined)?.removed) {
            return;
        }

        if ([FetchStatus.COMPLETE, FetchStatus.LOADING].includes(node.fetchStatus[direction])) {
            // Add nodes that should be in the graph
            // TODO: Bust search across lineage cache?
            setDefault(
                nodes,
                relationship.urn,
                entityNodeDefault(relationship.urn, relationship.entity.type, direction),
            );
        }

        if (nodes.has(relationship.urn)) {
            const edgeId = getEdgeId(node.urn, relationship.urn, direction);
            edges.set(edgeId, { ...edges.get(edgeId), ...makeLineageEdge(relationship) });
            addToAdjacencyList(adjacencyList, direction, node.urn, relationship.urn);

            relationship.paths?.forEach((path) => {
                addQueryNodes(path?.path, direction, context);
            });
        }
    }
}

function makeLineageEdge({
    createdOn,
    createdActor,
    updatedOn,
    updatedActor,
    isManual,
}: FetchedEntityV2Relationship): LineageEdge {
    return {
        created: createdOn ? { timestamp: createdOn, actor: createdActor ?? undefined } : undefined,
        updated: updatedOn ? { timestamp: updatedOn, actor: updatedActor ?? undefined } : undefined,
        isManual: isManual ?? false,
        isDisplayed: true,
    };
}
