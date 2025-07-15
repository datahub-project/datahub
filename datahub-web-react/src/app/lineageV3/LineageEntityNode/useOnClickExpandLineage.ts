import { useContext } from 'react';

import { FetchStatus, LineageNodesContext } from '@app/lineageV3/common';
import useSearchAcrossLineage from '@app/lineageV3/queries/useSearchAcrossLineage';

import { EntityType, LineageDirection } from '@types';

export function useOnClickExpandLineage(
    urn: string,
    type: EntityType,
    direction: LineageDirection,
    maxDepth: boolean,
    parentDataJob?: string,
) {
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
        if (parentDataJob && node && node.isExpanded[direction]) {
            nodes.forEach((n) => {
                if (n.urn !== urn && n.parentDataJob === parentDataJob) {
                    // eslint-disable-next-line no-param-reassign
                    n.isExpanded = { ...n.isExpanded, [direction]: false };
                }
            });
        }
    };
}
