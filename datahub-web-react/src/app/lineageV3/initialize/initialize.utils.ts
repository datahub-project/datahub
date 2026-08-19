import { useEffect, useState } from 'react';

import { CONTAINER_MEMBER_PAGE_SIZE, FetchStatus, LineageEntity, NodeContext } from '@app/lineageV3/common';

import { Entity, LineageDirection } from '@types';

export function useContainerMemberPagination(rootUrn: string, nodes: NodeContext['nodes']) {
    const [start, setStart] = useState(0);
    const [total, setTotal] = useState<number | undefined>(undefined);
    const [initialized, setInitialized] = useState(false);
    const limit = nodes.get(rootUrn)?.boundingBoxLimit ?? CONTAINER_MEMBER_PAGE_SIZE;

    useEffect(() => {
        setStart(0);
        setTotal(undefined);
        setInitialized(false);
    }, [rootUrn]);

    const target = Math.min(limit, total ?? limit);
    useEffect(() => {
        if (start + CONTAINER_MEMBER_PAGE_SIZE < target) {
            setStart((prev) => prev + CONTAINER_MEMBER_PAGE_SIZE);
        }
    }, [start, target]);

    return {
        start,
        setTotal,
        initialized,
        setInitialized,
    };
}

export function createContainerMemberNode({ urn, type }: Entity, rootContainerUrn?: string): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        containers: rootContainerUrn ? [{ urn: rootContainerUrn, isOutputPort: false }] : undefined,
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: undefined, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: undefined, facetFilters: new Map() },
        },
    };
}
