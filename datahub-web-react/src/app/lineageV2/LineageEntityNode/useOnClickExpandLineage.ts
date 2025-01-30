import { useContext } from 'react';
import { EntityType, LineageDirection } from '../../../types.generated';
import { FetchStatus, LineageNodesContext } from '../common';
import useSearchAcrossLineage from '../useSearchAcrossLineage';

export function useOnClickExpandLineage(urn: string, type: EntityType, direction: LineageDirection, maxDepth: boolean) {
    const context = useContext(LineageNodesContext);
    const { nodes, adjacencyList, setDataVersion, setDisplayVersion } = context;
    const { fetchLineage } = useSearchAcrossLineage(urn, type, context, direction, true, maxDepth);

    return function onClick(e?: React.MouseEvent<HTMLDivElement | HTMLSpanElement, MouseEvent>) {
        e?.stopPropagation();
        const node = nodes.get(urn);
        const fetchStatus = node?.fetchStatus?.[direction];
        if (node && fetchStatus !== FetchStatus.COMPLETE) {
            node.fetchStatus = { ...node.fetchStatus, [direction]: FetchStatus.LOADING };
            node.isExpanded = { ...node.isExpanded, [direction]: true };
            setDataVersion((v) => v + 1);
            fetchLineage();
        } else if (node && !node.isExpanded[direction]) {
            node.isExpanded = { ...node.isExpanded, [direction]: true };
            setDataVersion((v) => v + 1);
            setDisplayVersion(([version]) => [version + 1, [urn, ...(adjacencyList[direction].get(urn) || [])]]);
        }
    };
}
