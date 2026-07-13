import React, { useContext } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { FetchStatus, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import useFetchChildDataJobs from '@app/lineageV3/initialize/useFetchChildDataJobs';
import useResetLineageGraph from '@app/lineageV3/initialize/useResetLineageGraph';

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
    useResetLineageGraph(context, urn, type, () => makeInitialNode(urn, type));

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
