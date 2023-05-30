import React, { useCallback } from 'react';
import useBrowseV2Query from './useBrowseV2Query';
import useIntersect from '../../shared/useIntersect';
import { BROWSE_LOAD_MORE_MARGIN } from './constants';

type Props = {
    skip: boolean;
};

const useBrowsePagination = ({ skip }: Props) => {
    const { loaded, error, groups, pathResult, advancePage, refetch } = useBrowseV2Query({ skip });

    const { observableRef } = useIntersect({
        skip,
        options: { rootMargin: BROWSE_LOAD_MORE_MARGIN },
        onIntersect: advancePage,
    });

    const retry = useCallback(() => refetch(), [refetch]);

    return {
        loaded,
        error,
        groups,
        pathResult,
        observable: <div ref={observableRef} style={{ width: '1px', height: '1px' }} />,
        retry,
    } as const;
};

export default useBrowsePagination;
