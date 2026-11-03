import { useEffect } from 'react';

import { LineageEntity, NodeContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import { useGetLineageTimeParams } from '@app/lineageV3/utils/useGetLineageTimeParams';

import { EntityType, LineageDirection } from '@types';

/**
 * Resets the lineage node context whenever the home node or the lineage time range changes, and
 * clears fetched edges whenever `showGhostEntities` changes, since ghost entities affect which
 * edges are considered fetched. Initializers whose member query does not take the time params are
 * remounted on a time-range change (see LineageExplorer) so their members are fetched again.
 */
export default function useResetLineageGraph(
    context: NodeContext,
    urn: string,
    type: EntityType,
    makeInitialNode: () => LineageEntity,
) {
    const { nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion, showGhostEntities } = context;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    useEffect(() => {
        nodes.clear();
        adjacencyList[LineageDirection.Upstream].clear();
        adjacencyList[LineageDirection.Downstream].clear();
        edges.clear();
        nodes.set(urn, makeInitialNode());
        setNodeVersion(0);
        setDisplayVersion([0, []]);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [urn, type, startTimeMillis, endTimeMillis, nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion]);

    // Only schema-field pages react to ignoreSchemaFieldStatus; it flips when appConfig loads, which
    // must not clear other pages' fetched edges.
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
