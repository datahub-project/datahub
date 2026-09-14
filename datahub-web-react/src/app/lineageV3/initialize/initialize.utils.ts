import { useEffect, useState } from 'react';

import {
    BOUNDING_BOX_MEMBER_PAGE_SIZE,
    FetchStatus,
    LINEAGE_FILTER_PAGINATION,
    LineageEntity,
    NodeContext,
} from '@app/lineageV3/common';

import { Entity, LineageDirection } from '@types';

export function useBoundingBoxMemberPagination(rootUrn: string, nodes: NodeContext['nodes']) {
    const [start, setStart] = useState(0);
    const [total, setTotal] = useState<number | undefined>(undefined);
    const [initialized, setInitialized] = useState(false);
    const limit = nodes.get(rootUrn)?.boundingBoxLimit ?? BOUNDING_BOX_MEMBER_PAGE_SIZE;

    useEffect(() => {
        setStart(0);
        setTotal(undefined);
        setInitialized(false);
    }, [rootUrn]);

    const target = Math.min(limit, total ?? limit);
    useEffect(() => {
        if (start + BOUNDING_BOX_MEMBER_PAGE_SIZE < target) {
            setStart((prev) => prev + BOUNDING_BOX_MEMBER_PAGE_SIZE);
        }
    }, [start, target]);

    return {
        start,
        setTotal,
        initialized,
        setInitialized,
    };
}

export function createBoundingBoxMemberNode({ urn, type }: Entity, rootBoundingBoxUrn?: string): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        boundingBoxes: rootBoundingBoxUrn ? [{ urn: rootBoundingBoxUrn, isOutputPort: false }] : undefined,
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
        },
    };
}
