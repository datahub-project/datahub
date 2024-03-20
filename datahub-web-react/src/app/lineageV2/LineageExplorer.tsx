import React, { useContext, useEffect, useState } from 'react';
import { ReactFlowProvider } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import TabFullsizedContext from '../shared/TabFullsizedContext';
import { FetchStatus, LINEAGE_FILTER_PAGINATION, LineageEntity, LineageNodesContext, NodeContext } from './common';
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
    const [nodeVersion, setNodeVersion] = useState(0);
    const [dataVersion, setDataVersion] = useState(0);
    const [displayVersion, setDisplayVersion] = useState<[number, string[]]>([0, []]);
    const context = {
        rootUrn: urn,
        nodes,
        nodeVersion,
        setNodeVersion,
        dataVersion,
        setDataVersion,
        displayVersion,
        setDisplayVersion,
    };

    const loaded = useInitializeNodes(context, urn, type);

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
function useInitializeNodes(context: NodeContext, urn: string, type: EntityType): boolean {
    useEffect(() => {
        context.nodes.clear();
        context.nodes.set(urn, makeInitialNode(urn, type));
    }, [urn, type, context.nodes]);

    const { processed: upstreamProcessed } = useSearchAcrossLineage(
        urn,
        context,
        LineageDirection.Upstream,
        undefined,
        undefined,
    );
    const { processed: downstreamProcessed } = useSearchAcrossLineage(urn, context, LineageDirection.Downstream);

    return upstreamProcessed && downstreamProcessed;
}

function makeInitialNode(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        parents: new Set(),
        nonTransformationalParents: new Set(),
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
        },
    };
}
