import React, { useContext } from 'react';
import { ReactFlowProvider } from 'reactflow';

import LineageDisplay from '@app/lineageV3/LineageDisplay';
import { BOUNDING_BOX_MEMBER_PAGE_SIZE, FetchStatus, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import useFetchSemanticModelEntities from '@app/lineageV3/initialize/useFetchSemanticModelEntities';
import useResetLineageGraph from '@app/lineageV3/initialize/useResetLineageGraph';

import { EntityType, LineageDirection } from '@types';

interface Props {
    urn: string;
    type: EntityType;
}

/**
 * Initializes the lineage graph for a SemanticModel by fetching its member entities
 * (Semantic Model Datasets and Metrics) and registering them as nodes. Each member's own
 * upstream/downstream lineage is then fetched on demand, giving a container / bounding-box
 * view. The SemanticModel itself is not a lineage hop.
 */
export default function SemanticModelGraphInitializer({ urn, type }: Props) {
    const initialized = useInitializeNodes(urn, type);

    return (
        <ReactFlowProvider>
            <LineageDisplay initialized={initialized} />
        </ReactFlowProvider>
    );
}

function useInitializeNodes(urn: string, type: EntityType): boolean {
    const context = useContext(LineageNodesContext);
    useResetLineageGraph(context, urn, type, () => makeRootNode(urn, type));

    return useFetchSemanticModelEntities();
}

function makeRootNode(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            // The SemanticModel root node has no direct lineage of its own;
            // lineage is derived from its member entities.
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
        boundingBoxLimit: BOUNDING_BOX_MEMBER_PAGE_SIZE,
    };
}
