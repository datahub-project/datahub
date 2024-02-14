import React, { useEffect, useState } from 'react';
import { ReactFlowProvider } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
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

    return (
        <LineageNodesContext.Provider value={context}>
            <ReactFlowProvider>{loaded && <LineageDisplay {...props} />}</ReactFlowProvider>
        </LineageNodesContext.Provider>
    );
}

/**
 * Initialize lineage node context with upstreams and downstreams of the given urn.
 */
function useInitializeNodes(context: NodeContext, urn: string, type: EntityType): boolean {
    useEffect(() => {
        context.nodes.set(urn, {
            id: urn,
            urn,
            type,
            paths: [[]],
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
                [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
            },
            filters: {
                [LineageDirection.Upstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
                [LineageDirection.Downstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
            },
        });
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
