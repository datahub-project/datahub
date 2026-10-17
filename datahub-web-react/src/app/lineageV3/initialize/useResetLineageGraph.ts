import { useEffect } from 'react';

import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageEntity, NodeContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

/**
 * Resets the lineage node context whenever the home node changes, and clears fetched edges
 * whenever `showGhostEntities` changes, since ghost entities affect which edges are considered
 * fetched.
 *
 * @param resetOnTimeRangeChange also reset when the selected lineage time range changes. Only
 * correct for callers whose fetch query actually varies by time range -- otherwise the reset wipes
 * freshly-fetched edges with no refetch to replace them. In particular `appConfig` resolving its
 * default lookback window on load flips the time params from undefined to a value, which fires
 * this reset once during startup. Callers that do not send the time params to their query should
 * pass `false`.
 */
export default function useResetLineageGraph(
    context: NodeContext,
    urn: string,
    type: EntityType,
    makeInitialNode: () => LineageEntity,
    resetOnTimeRangeChange = true,
) {
    const { nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion, showGhostEntities } = context;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    // Extracted rather than inlined in the dependency array: react-hooks/exhaustive-deps requires
    // statically checkable deps.
    const resetStartTimeMillis = resetOnTimeRangeChange ? startTimeMillis : undefined;
    const resetEndTimeMillis = resetOnTimeRangeChange ? endTimeMillis : undefined;

    useEffect(() => {
        nodes.clear();
        adjacencyList[LineageDirection.Upstream].clear();
        adjacencyList[LineageDirection.Downstream].clear();
        edges.clear();
        nodes.set(urn, makeInitialNode());
        setNodeVersion(0);
        setDisplayVersion([0, []]);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [
        urn,
        type,
        resetStartTimeMillis,
        resetEndTimeMillis,
        nodes,
        adjacencyList,
        edges,
        setNodeVersion,
        setDisplayVersion,
    ]);

    // Only a schema-field page cares about ignoreSchemaFieldStatus. Folding the type check into the
    // dependency keeps the flag out of this effect's deps for every other page: the flag flips when
    // appConfig finishes loading, and that must not clear a DataFlow's or Dataset's freshly-fetched
    // edges the way a real showGhostEntities change does.
    const skipForSchemaField = type === EntityType.SchemaField && ignoreSchemaFieldStatus;

    useEffect(() => {
        // Not necessary if on schema field page and ignoring status
        if (!skipForSchemaField) {
            adjacencyList[LineageDirection.Upstream].clear();
            adjacencyList[LineageDirection.Downstream].clear();
            edges.clear();
            nodes.forEach((node) => {
                // eslint-disable-next-line no-param-reassign
                node.entity = undefined;
            });
            setDisplayVersion([0, []]);
        }
    }, [showGhostEntities, skipForSchemaField, nodes, adjacencyList, edges, setDisplayVersion]);
}
