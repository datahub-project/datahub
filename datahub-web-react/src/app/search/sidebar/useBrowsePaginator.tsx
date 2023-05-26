import React, { useCallback } from 'react';
import useBrowseV2Query from './useBrowseV2Query';
import { AggregationMetadata, EntityType } from '../../../types.generated';
import useIntersect from '../../shared/useIntersect';
import { BROWSE_LOAD_MORE_DELAY, BROWSE_LOAD_MORE_MARGIN } from './constants';

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata;
    path: Array<string>;
    skip: boolean;
};

const useBrowsePaginator = ({ entityAggregation, environmentAggregation, platformAggregation, path, skip }: Props) => {
    const { loaded, error, groups, pathResult, fetchNextPage, refetch } = useBrowseV2Query({
        entityType: entityAggregation.value as EntityType,
        environment: environmentAggregation?.value,
        platform: platformAggregation.value,
        path,
        skip,
    });

    const { observableRef } = useIntersect({
        skip,
        initialDelay: BROWSE_LOAD_MORE_DELAY,
        options: { rootMargin: BROWSE_LOAD_MORE_MARGIN },
        onIntersect: fetchNextPage,
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

export default useBrowsePaginator;
