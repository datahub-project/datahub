import { notification } from '@components';
import i18next from 'i18next';
import { useCallback, useContext, useEffect, useRef, useState } from 'react';

import {
    FetchStatus,
    LineageEdge,
    LineageEntity,
    LineageNodesContext,
    NodeContext,
    addToAdjacencyList,
    getEdgeId,
    isQuery,
    isTransformational,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import pruneAllDuplicateEdges from '@app/lineageV3/queries/pruneAllDuplicateEdges';
import { getNodeForBulkResult } from '@app/lineageV3/queries/useBulkEntityLineage.utils';
import {
    LOAD_ERROR_NOTIFICATION_KEY,
    addQueryNodes,
    setEntityNodeDefault,
} from '@app/lineageV3/queries/useSearchAcrossLineage';
import { FetchedEntityV2Relationship } from '@app/lineageV3/types';
import { useGetLineageTimeParams } from '@app/lineageV3/utils/useGetLineageTimeParams';
import usePrevious from '@app/sharedV2/usePrevious';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetBulkEntityLineageV2Query } from '@graphql/lineage.generated';
import { EntityType, LineageDirection } from '@types';

const BATCH_SIZE = 10;

export default function useBulkEntityLineage(shownUrns: string[]): (urn: string) => void {
    const flags = useAppConfig().config.featureFlags;
    const entityRegistry = useEntityRegistry();
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
    // Urns whose batch failed/stalled — excluded from refetch so a persistent failure doesn't loop.
    const failedUrnsRef = useRef(new Set<string>());
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const errorShownRef = useRef(false);

    // A new time range is a fresh attempt: forget past failures and re-arm the error notice.
    useEffect(() => {
        failedUrnsRef.current.clear();
        errorShownRef.current = false;
    }, [startTimeMillis, endTimeMillis]);

    const handleBulkFetchFailure = () => {
        // Don't re-request the batch that failed/stalled; drop it so the next batch can make progress.
        urnsToFetch.forEach((urn) => failedUrnsRef.current.add(urn));
        setUrnsToFetch([]);
        if (!errorShownRef.current) {
            errorShownRef.current = true;
            notification.error({ key: LOAD_ERROR_NOTIFICATION_KEY, message: i18next.t('lineage:loadError') });
        }
    };

    const { refetch, loading } = useGetBulkEntityLineageV2Query({
        skip: !urnsToFetch?.length,
        fetchPolicy: 'cache-first',
        variables: {
            urns: urnsToFetch,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: true,
            showColumns: true,
            includeGhostEntities:
                showGhostEntities || (rootType === EntityType?.SchemaField && ignoreSchemaFieldStatus),
        },
        onCompleted: (data) => {
            const smallContext = { nodes, edges, adjacencyList, setDisplayVersion, rootType };
            let changed = false;
            // Results are positional & 1:1 with the requested urns. A neighbor the user
            // can't view returns as a Restricted placeholder with a re-encrypted urn that
            // won't match its node key, so match those back by request position.
            const requestedUrns = data?.entities?.length === urnsToFetch.length ? urnsToFetch : [];
            data?.entities?.forEach((rawEntity, index) => {
                if (!rawEntity) return;
                const config = entityRegistry.getLineageVizConfigV2(rawEntity.type, rawEntity, flags);
                if (!config) return;
                const entity = { ...config, lineageAssets: entityRegistry.getLineageAssets(rawEntity.type, rawEntity) };

                const node = getNodeForBulkResult(nodes, entity.urn, rawEntity.type, requestedUrns, index);
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
        },
        onError: handleBulkFetchFailure,
    });

    useEffect(() => {
        // Changing the variables aborts the in-flight request, losing that batch's results
        if (loading) return;
        setUrnsToFetch((oldUrnsToFetch) => {
            let newUrnsToFetch = memoizedShownUrns
                .filter((urn) => {
                    const node = nodes.get(urn);
                    return !node?.entity && !failedUrnsRef.current.has(urn);
                })
                .slice(0, BATCH_SIZE);
            if (
                !newUrnsToFetch.length &&
                rootType === EntityType.SchemaField &&
                ignoreSchemaFieldStatus &&
                hideTransformations
            ) {
                newUrnsToFetch = Array.from(nodes.values())
                    .filter(
                        (node) =>
                            isTransformational(node, rootType) && !node.entity && !failedUrnsRef.current.has(node.urn),
                    )
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
        loading,
        // Re-select after a time-range change so urns cleared from failedUrnsRef are retried.
        startTimeMillis,
        endTimeMillis,
    ]);

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
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges' | 'rootType'>,
): void {
    const { adjacencyList, nodes, edges } = context;

    if (relationship.entity && !isQuery(relationship.entity)) {
        if (node.fetchStatus[direction] !== FetchStatus.UNNEEDED) {
            // Add nodes that should be in the graph
            // TODO: Bust search across lineage cache?
            setEntityNodeDefault(relationship.urn, relationship.entity.type, direction, context);
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
