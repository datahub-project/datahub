import React, { useContext } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import LineageGraphContext from '@app/lineageV3/LineageGraphContext';
import { FetchStatus, LINEAGE_FILTER_PAGINATION, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import useResetLineageGraph from '@app/lineageV3/initialize/useResetLineageGraph';
import useSearchAcrossLineage from '@app/lineageV3/queries/useSearchAcrossLineage';
import { MODULE_VIEW_MAX_PER_LEVEL } from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';

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
 * Initialize lineage node context with upstreams and downstreams of the given urn.
 */
function useInitializeNodes(urn: string, type: EntityType): boolean {
    const context = useContext(LineageNodesContext);
    const { isModuleView } = useContext(LineageGraphContext);
    const initialLimit = isModuleView ? MODULE_VIEW_MAX_PER_LEVEL : LINEAGE_FILTER_PAGINATION;
    useResetLineageGraph(context, urn, type, () => makeInitialNode(urn, type, initialLimit));

    const { processed: upstreamProcessed } = useSearchAcrossLineage(urn, type, context, LineageDirection.Upstream);
    const { processed: downstreamProcessed } = useSearchAcrossLineage(urn, type, context, LineageDirection.Downstream);

    return upstreamProcessed && downstreamProcessed;
}

function makeInitialNode(urn: string, type: EntityType, limit: number): LineageEntity {
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
            [LineageDirection.Upstream]: { limit, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit, facetFilters: new Map() },
        },
    };
}
