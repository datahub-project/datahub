import React, { useContext, useEffect } from 'react';
import { ReactFlowProvider } from 'reactflow';

import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { FetchStatus, LineageEntity, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import useFetchChildDataJobs from '@app/lineageV3/initialize/useFetchChildDataJobs';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

export default function ImpactAnalysisNodeInitializer(props: Props) {
    const { urn, type } = props;
    const initialized = useInitializeNodes(urn, type);

    return (
        <ReactFlowProvider>
            <LineageDisplay initialized={initialized} />
        </ReactFlowProvider>
    );
}

/**
 * Initialize lineage node context with all DataJobs part of the DataFlow.
 */
function useInitializeNodes(urn: string, type: EntityType): boolean {
    const context = useContext(LineageNodesContext);

    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion, showGhostEntities } = context;
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

    useEffect(() => {
        // Reset graph if home node or time range changes
        nodes.clear();
        adjacencyList[LineageDirection.Upstream].clear();
        adjacencyList[LineageDirection.Downstream].clear();
        edges.clear();
        nodes.set(urn, makeInitialNode(urn, type));
        setNodeVersion(0);
        setDisplayVersion([0, []]);
    }, [urn, type, startTimeMillis, endTimeMillis, nodes, adjacencyList, edges, setNodeVersion, setDisplayVersion]);

    useEffect(() => {
        // Reset edges if showGhostEntities changes. Not necessary if on schema field page and ignoring status
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

    return useFetchChildDataJobs();
}

function makeInitialNode(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}
