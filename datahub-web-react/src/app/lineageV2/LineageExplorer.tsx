import React, { useContext, useEffect, useState } from 'react';
import { ReactFlowProvider } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import TabFullsizedContext from '../shared/TabFullsizedContext';
import {
    FetchStatus,
    LINEAGE_FILTER_PAGINATION,
    LineageEdge,
    LineageEntity,
    LineageNodesContext,
    NodeContext,
} from './common';
import LineageDisplay from './LineageDisplay';
import useSearchAcrossLineage from './useSearchAcrossLineage';

type Props = {
    urn: string;
    type: EntityType;
    embedded?: boolean;
};

export default function LineageExplorer(props: Props) {
    const { urn, type } = props;
    const [nodes] = useState(new Map<string, LineageEntity>());
    const [edges] = useState(new Map<string, Map<string, LineageEdge>>());
    const [nodeVersion, setNodeVersion] = useState(0);
    const [dataVersion, setDataVersion] = useState(0);
    const [displayVersion, setDisplayVersion] = useState<[number, string[]]>([0, []]);
    const context = {
        rootUrn: urn,
        nodes,
        edges,
        nodeVersion,
        setNodeVersion,
        dataVersion,
        setDataVersion,
        displayVersion,
        setDisplayVersion,
    };

    const loaded = useInitializeNodes(context, urn, type, setNodeVersion);

    const { setTabFullsize } = useContext(TabFullsizedContext);
    useEffect(() => {
        return () => {
            setTabFullsize(false);
        };
    }, [setTabFullsize]);

    return (
        <LineageNodesContext.Provider value={context}>
            <ReactFlowProvider>
                <LineageDisplay {...props} loaded={loaded} />
            </ReactFlowProvider>
        </LineageNodesContext.Provider>
    );
}

/**
 * Initialize lineage node context with upstreams and downstreams of the given urn.
 */
function useInitializeNodes(
    context: NodeContext,
    urn: string,
    type: EntityType,
    setNodeVersion: NodeContext['setNodeVersion'],
): boolean {
    useEffect(() => {
        context.nodes.clear();
        context.nodes.set(urn, makeInitialNode(urn, type));
        setNodeVersion((prev) => prev + 1);
    }, [urn, type, context.nodes, setNodeVersion]);

    const { processed: upstreamProcessed } = useSearchAcrossLineage(urn, context, LineageDirection.Upstream);
    const { processed: downstreamProcessed } = useSearchAcrossLineage(urn, context, LineageDirection.Downstream);

    return upstreamProcessed && downstreamProcessed;
}

function makeInitialNode(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        parents: new Set(),
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
