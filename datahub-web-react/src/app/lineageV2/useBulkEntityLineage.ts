import pruneAllDuplicateEdges from '@app/lineageV2/pruneAllDuplicateEdges';
import { useAppConfig } from '@app/useAppConfig';
import { useCallback, useContext, useEffect, useState } from 'react';
import { useGetBulkEntityLineageV2Query } from '../../graphql/lineage.generated';
import { EntityType, LineageDirection } from '../../types.generated';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import usePrevious from '../shared/usePrevious';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import {
    addToAdjacencyList,
    FetchStatus,
    getEdgeId,
    isQuery,
    isTransformational,
    LineageEdge,
    LineageEntity,
    LineageNodesContext,
    NodeContext,
    useIgnoreSchemaFieldStatus,
} from './common';
import { FetchedEntityV2Relationship } from './types';
import { addQueryNodes, setEntityNodeDefault } from './useSearchAcrossLineage';

const BATCH_SIZE = 10;

export default function useBulkEntityLineage(shownUrns: string[]): (urn: string) => void {
    const flags = useAppConfig().config.featureFlags;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const {
        rootType,
        nodes,
        edges,
        adjacencyList,
        hideTransformations,
        showGhostEntities,
        dataVersion,
        setDataVersion,
        setDisplayVersion,
    } = useContext(LineageNodesContext);
    const prevShownUrns = usePrevious(shownUrns);

    const [memoizedShownUrns, setMemoizedShownUrns] = useState<string[]>([]);
    useEffect(() => {
        // TODO: Implement string[] equality?
        const sortedShownUrns = shownUrns.slice().sort();
        const prevSortedShownUrns = prevShownUrns?.slice()?.sort();
        if (JSON.stringify(prevSortedShownUrns) !== JSON.stringify(sortedShownUrns)) {
            setMemoizedShownUrns(shownUrns);
        }
    }, [prevShownUrns, shownUrns]);

    const [urnsToFetch, setUrnsToFetch] = useState<string[]>([]);
    useEffect(() => {
        setUrnsToFetch((oldUrnsToFetch) => {
            let newUrnsToFetch = memoizedShownUrns
                .filter((urn) => {
                    const node = nodes.get(urn);
                    return !node?.entity;
                })
                .slice(0, BATCH_SIZE);
            if (
                !newUrnsToFetch.length &&
                rootType === EntityType.SchemaField &&
                ignoreSchemaFieldStatus &&
                hideTransformations
            ) {
                newUrnsToFetch = Array.from(nodes.values())
                    .filter((node) => isTransformational(node) && !node.entity)
                    .map((node) => node.urn);
            }
            if (JSON.stringify(oldUrnsToFetch) !== JSON.stringify(newUrnsToFetch)) {
                return newUrnsToFetch;
            }
            return oldUrnsToFetch;
        });
    }, [
        nodes,
        dataVersion,
        memoizedShownUrns,
        showGhostEntities,
        rootType,
        ignoreSchemaFieldStatus,
        hideTransformations,
    ]);

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
            includeGhostEntities: showGhostEntities || (rootType === EntityType.SchemaField && ignoreSchemaFieldStatus),
        },
    });

    const entityRegistry = useEntityRegistryV2();

    useEffect(() => {
        const smallContext = { nodes, edges, adjacencyList, setDisplayVersion };
        let changed = false;
        data?.entities?.forEach((rawEntity) => {
            if (!rawEntity) return;
            const config = entityRegistry.getLineageVizConfigV2(rawEntity.type, rawEntity, flags);
            if (!config) return;
            const entity = { ...config, lineageAssets: entityRegistry.getLineageAssets(rawEntity.type, rawEntity) };

            const node = nodes.get(entity.urn);
            if (node) {
                node.entity = entity;
                node.rawEntity = rawEntity;
                changed = true;

                // TODO: Remove once using bulk edges query
                if (!isQuery(node)) {
                    entity.downstreamRelationships?.forEach((relationship) =>
                        processEdge(node, relationship, LineageDirection.Downstream, smallContext),
                    );
                    entity.upstreamRelationships?.forEach((relationship) => {
                        processEdge(node, relationship, LineageDirection.Upstream, smallContext);
                    });
                    pruneAllDuplicateEdges(node.urn, null, smallContext, entityRegistry);
                }
            }
        });
        if (changed) {
            setDataVersion((version) => version + 1);
            setDisplayVersion(([version, n]) => [version + 1, n]); // TODO: Also remove with above todo
        }
    }, [data, nodes, edges, adjacencyList, entityRegistry, setDataVersion, setDisplayVersion, flags]);

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
        if (node.fetchStatus[direction] !== FetchStatus.UNNEEDED) {
            // Add nodes that should be in the graph
            // TODO: Bust search across lineage cache?
            setEntityNodeDefault(relationship.urn, relationship.entity.type, direction, nodes);
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
        isManual: isManual ?? undefined,
        isDisplayed: true,
    };
}
