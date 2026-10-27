import { useEffect } from 'react';

import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageEntity, NodeContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

/**
 * Resets the lineage node context whenever the home node or selected time range changes, and
 * clears fetched edges whenever `showGhostEntities` changes, since ghost entities affect which
 * edges are considered fetched.
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

    useEffect(() => {
        // Not necessary if on schema field page and ignoring status
        if (!(type === EntityType.SchemaField && ignoreSchemaFieldStatus)) {
            adjacencyList[LineageDirection.Upstream].clear();
            adjacencyList[LineageDirection.Downstream].clear();
            edges.clear();
            nodes.forEach((node) => {
                // eslint-disable-next-line no-param-reassign
                node.entity = undefined;
            });
            setDisplayVersion([0, []]);
        }
    }, [showGhostEntities, ignoreSchemaFieldStatus, type, nodes, adjacencyList, edges, setDisplayVersion]);
}
