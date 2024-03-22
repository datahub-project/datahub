import { useContext, useMemo } from 'react';
import { EntityType } from '../../types.generated';
import { FetchStatus, isTransformational, LineageNodesContext } from './common';

const NO_FETCH_STATUSES = [FetchStatus.COMPLETE, FetchStatus.UNNEEDED, FetchStatus.LOADING];

export default function useGetUnfetchedTransformationalNodes() {
    const { nodes, nodeVersion } = useContext(LineageNodesContext);

    return useMemo(() => {
        return Array.from(nodes.values()).filter(
            (node) =>
                isTransformational(node) &&
                node.type !== EntityType.Query &&
                node.direction &&
                node.fetchStatus[node.direction] &&
                !NO_FETCH_STATUSES.includes(node.fetchStatus[node.direction]),
        );
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [nodes, nodeVersion]);
}
