import React, { useContext } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { FetchStatus, LINEAGE_FILTER_PAGINATION, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import useFetchChildDataJobs from '@app/lineageV3/initialize/useFetchChildDataJobs';
import useResetLineageGraph from '@app/lineageV3/initialize/useResetLineageGraph';
import useSearchAcrossLineage from '@app/lineageV3/queries/useSearchAcrossLineage';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

export default function DataFlowGraphInitializer(props: Props) {
    const { urn, type } = props;
    const initialized = useInitializeNodes(urn, type);

    return (
        <ReactFlowProvider>
            <LineageDisplay initialized={initialized} />
        </ReactFlowProvider>
    );
}

/**
 * Initialize lineage node context with all DataJobs part of the DataFlow, and the DataFlow's own lineage.
 */
function useInitializeNodes(urn: string, type: EntityType): boolean {
    const context = useContext(LineageNodesContext);
    useResetLineageGraph(context, urn, type, () => makeInitialNode(urn, type));

    const childJobsInitialized = useFetchChildDataJobs();
    const { processed: upstreamProcessed } = useSearchAcrossLineage(urn, type, context, LineageDirection.Upstream);
    const { processed: downstreamProcessed } = useSearchAcrossLineage(urn, type, context, LineageDirection.Downstream);

    return childJobsInitialized && upstreamProcessed && downstreamProcessed;
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
            [LineageDirection.Upstream]: FetchStatus.LOADING,
            [LineageDirection.Downstream]: FetchStatus.LOADING,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
        },
    };
}
